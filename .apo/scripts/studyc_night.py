#!/usr/bin/env python3
"""Study C stage B, overnight: runs one stack's trial queue until the stop time, then reports.

Usage:
  studyc_night.py --stack microvm|gvisor --stop 2026-10-12T15:00Z [--combine-by 2026-10-12T12:00Z]
                  [--dry-run] [--report-only]
  studyc_night.py --summary          # both stacks' verdicts in one short file (brain dir)

The queue is .apo/results/studyc-queue-<stack>.json (written from DEFAULT_QUEUE on first use).
It is re-read before every trial, so items can be added or marked while the driver runs; set
"final": true once every lever that will be written is in it. Each item:
  {"id": "A", "flags": ["ATE_APO_RESEED_FAST=1"]}           code lever: branch apo/studyc-<stack>-<id>
  {"id": "layout8", "sets": {"WORKER_COUNT": "8", ...}}    no code: base tree with --set overrides
  {"id": "v000", "labels": ["studyc-v000-gvisor-3"]}        a v000 run (no switches), not judged
  optional: "runs" (default 2), "labels" (default studyc-<id>-<stack>-<i>), "second_if": "no_oom"
  (run 2 only when run 1's pre-warm had no OOM kill and no failure).

Loop: the first item in queue order with a run still to do and everything it needs (a code
lever's branch) runs next; after every trial the scan starts from the top again, so a lever that
appears mid-run takes its place in the order. A label with an OK run is skipped; a label that
failed MAX_ATTEMPTS times is given up. A code lever whose branch doesn't exist yet is waited for.
When nothing is runnable and the queue is final (or --combine-by has passed), the winners are run
together x2 (studyc.py combined(): branches merged, switches and overrides combined; of two
winners that set the same knob, the one with the higher mean fine ceiling keeps it).

No trial is launched after --stop. The morning report (studyc-report-<stack>.md in results and
the brain dir) is rewritten after every trial and at exit: per-run rows with the commit each ran,
per-level tables, stalls and reseed timing, lever A's attempt log, verdicts (studyc.py win rule
plus "switch on in every worker"), and the tick-research summary when tick_research.md exists.
Nothing is folded or pushed.
"""

import argparse
import collections
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import night  # noqa: E402  pylint: disable=wrong-import-position
import studyc  # noqa: E402  pylint: disable=wrong-import-position

ROOT, RES, BRAIN = night.ROOT, night.RES, night.BRAIN
MAX_ATTEMPTS = 2
WAIT_POLL_S = 60
TICK_RESEARCH = os.path.join(BRAIN, "tick_research.md")

DEFAULT_QUEUE = {
    "microvm": {"final": False, "items": [
        {"id": "A", "flags": ["ATE_APO_RESEED_FAST=1"]},
        # 100 x 1.76 CPU asked for; the metal node allows 110 pods and 11 run there already,
        # so 96 workers x 1833m (175.97 CPU, the same total as 4 x 44).
        {"id": "layout96", "sets": {"WORKER_COUNT": "96", "WORKER_CPU_REQUEST": "1833m"}},
        # Run 1 is the Stage A probe (A2), same settings.
        {"id": "layout8", "sets": {"WORKER_COUNT": "8", "WORKER_CPU_REQUEST": "22"}},
        {"id": "mem512", "sets": {"ACTOR_MEMORY": "512Mi"}, "second_if": "no_oom"},
    ]},
    "gvisor": {"final": False, "items": [
        {"id": "v000", "labels": ["studyc-v000-gvisor-3"]},
        {"id": "6", "flags": ["ATE_APO_RESTORE_ONESHOT=1"]},
        {"id": "7", "flags": ["ATE_APO_EARLY_RESTORE=1"]},
        {"id": "5", "flags": ["ATE_APO_TEARDOWN_PAUSE_ONLY=1"]},
    ]},
}


def queue_path(stack: str) -> str:
  return os.path.join(RES, f"studyc-queue-{stack}.json")


def load_queue(stack: str) -> Dict:
  q = night.load_json(queue_path(stack))
  if not q:
    q = json.loads(json.dumps(DEFAULT_QUEUE[stack]))
    with open(queue_path(stack), "w", encoding="utf-8") as f:
      json.dump(q, f, indent=2)
  return q


def is_v000(item: Dict) -> bool:
  return item["id"] == "v000"


class NightC(studyc.StudyC):
  def __init__(self, stack: str, dry_run: bool):
    super().__init__(stack, dry_run, "night")
    self.combine_by = datetime(2100, 1, 1, tzinfo=timezone.utc)
    self.notes: Dict[str, str] = {}  # item id -> why it's waiting, skipped or given up

  # --- queue ----------------------------------------------------------------------

  def item_labels(self, item: Dict) -> List[str]:
    return item.get("labels") or [f"studyc-{item['id']}-{self.stack}-{i}"
                                  for i in range(1, int(item.get("runs", studyc.RUNS_PER_LEVER)) + 1)]

  def last_dir(self, label: str) -> Optional[str]:
    ds = self.dirs(label)
    return ds[-1] if ds else None

  def prewarm_problem(self, d: Optional[str]) -> str:
    """Why run d's pre-warm counts as OOM-hit or failed ('' when clean)."""
    if not d:
      return "run 1 has no result"
    err = night.load_json(os.path.join(d, "error.json"))
    if err.get("phase") == "PREWARM":
      return f"run 1 pre-warm failed: {err.get('message', '')[:120]}"
    m = night.summary(d)
    if m.get("node_oom_events"):
      return f"run 1 had {m['node_oom_events']} Substrate OOM kills"
    if m.get("prewarm_failures"):
      return f"run 1 had {m['prewarm_failures']} pre-warm failures"
    return ""

  def item_state(self, item: Dict) -> Tuple[str, Optional[str], str]:
    """(state, label, why): state is run, wait, done, skipped or failed."""
    labels = self.item_labels(item)
    for i, label in enumerate(labels):
      if self.done_ok(label):
        continue
      tries = len(self.dirs(label))
      if tries >= MAX_ATTEMPTS:
        return "failed", None, f"{label}: {tries} attempts, none OK"
      if i > 0 and item.get("second_if") == "no_oom":
        why = self.prewarm_problem(self.done_ok(labels[0]) or self.last_dir(labels[0]))
        if why:
          return "skipped", None, f"run 2 skipped: {why}"
      if item.get("flags") and not studyc.branch_exists(f"apo/studyc-{self.stack}-{item['id']}"):
        return "wait", None, f"no branch apo/studyc-{self.stack}-{item['id']} yet"
      return "run", label, ""
    return "done", None, ""

  def workdir(self, item: Dict) -> Optional[str]:
    if not item.get("flags"):
      return ROOT
    wt, why = self.worktree(item["id"])
    if not wt:
      self.notes[item["id"]] = why
    return wt

  def args(self, item: Dict) -> List[str]:
    return self.fine_args() + self.lever_args(item)

  # --- combine ----------------------------------------------------------------------

  def mean_ceiling(self, item: Dict) -> float:
    cs = [night.summary(d).get("fine_ceiling_per_s") or 0 for d in
          (self.done_ok(l) for l in self.item_labels(item)) if d]
    return statistics.mean(cs) if cs else 0.0

  def combo(self, winners: List[Dict]) -> Dict:
    """Winners together; a knob two winners set comes from the one with the higher mean ceiling."""
    combo = self.combined(winners)  # flags, branch apo/studyc-<stack>-combined
    sets: Dict[str, str] = {}
    for w in sorted(winners, key=self.mean_ceiling):  # best last, so it wins clashes
      sets.update(w.get("sets") or {})
    combo["sets"] = sets
    combo["labels"] = [f"studyc-combined-{self.stack}-{i}" for i in range(1, studyc.RUNS_PER_LEVER + 1)]
    combo["of"] = [w["id"] for w in winners]
    return combo

  # --- run ------------------------------------------------------------------------------

  def run_night(self) -> None:
    self.log(f"Study C overnight start (dry_run={self.dry}, pid {os.getpid()}), stop "
             f"{night.STOP_LAUNCH:%Y-%m-%d %H:%M} UTC, combine by {self.combine_by:%H:%M} UTC")
    try:
      self.wait_idle()
      while True:
        if self.now() >= night.STOP_LAUNCH:
          self.stop_reason = self.stop_reason or f"stop time {night.STOP_LAUNCH:%H:%M} UTC: no new trials"
          self.log(self.stop_reason)
          return
        q = load_queue(self.stack)
        states = [(it, *self.item_state(it)) for it in q["items"]]
        for it, st, _, why in states:
          if why:
            self.notes[it["id"]] = why
        nxt = next(((it, label) for it, st, label, _ in states if st == "run"), None)
        if nxt:
          it, label = nxt
          wd = self.workdir(it)
          if wd is None:
            continue
          self.log(f"next: {label} ({it['id']})")
          self.run_label(label, wd, self.args(it))
          self.safe_report()
          if self.dry:
            return
          continue
        waiting = [it["id"] for it, st, _, _ in states if st == "wait"]
        if waiting and not q.get("final") and self.now() < self.combine_by:
          if self.dry:
            self.plan.append(f"wait for lever branches {waiting} (poll every {WAIT_POLL_S}s)")
            return
          self.set_state(step="wait-branches", trial="", waiting=waiting)
          time.sleep(WAIT_POLL_S)
          continue
        if self.run_combined(q):
          continue
        self.stop_reason = self.stop_reason or "queue done"
        self.log(self.stop_reason)
        return
    except Exception as e:  # pylint: disable=broad-except
      self.stop_reason = f"driver error: {type(e).__name__}: {e}"
      self.log(self.stop_reason)
    finally:
      if not self.dry:
        self.set_state(step="done", trial="", stop_reason=self.stop_reason)
        self.safe_report()
        self.log("Study C overnight driver done; node left up and idle")

  def run_combined(self, q: Dict) -> bool:
    """Runs the next combined-winners trial; False when there's none (fewer than 2 winners or done)."""
    winners = [it for it in q["items"] if not is_v000(it) and self.verdict(it)["win"]]
    if len(winners) < 2:
      self.notes["combined"] = f"{len(winners)} winner(s): {[w['id'] for w in winners]}; nothing to combine"
      return False
    try:
      combo = self.combo(winners)
    except Exception as e:  # pylint: disable=broad-except  (merge conflict)
      self.notes["combined"] = f"could not combine {[w['id'] for w in winners]}: {e}"
      return False
    st, label, why = self.item_state(combo)
    if st != "run":
      self.notes["combined"] = why or f"{st} ({combo['of']})"
      return False
    wd = self.workdir(combo)
    if wd is None:
      return False
    self.log(f"next: {label} (combined {combo['of']}, sets {combo['sets']}, flags {combo['flags']})")
    self.run_label(label, wd, self.args(combo))
    self.safe_report()
    return not self.dry

  # --- report ---------------------------------------------------------------------------

  def safe_report(self) -> None:
    try:
      self.write_night_report()
    except Exception as e:  # pylint: disable=broad-except
      self.log(f"report failed: {type(e).__name__}: {e}")

  @staticmethod
  def reseed_line(d: Optional[str]) -> str:
    """Stall and reseed timing of run d, and lever A's attempt log when it has one."""
    if not d:
      return "no result"
    m = night.summary(d)
    out = (f"stalls {m.get('restore_stalls')} / {m.get('restore_records')} restores, bursts "
           f"{m.get('restore_stall_bursts')}, workers with stalls {m.get('restore_stall_pods')}; "
           f"in reseed {m.get('restore_stalls_in_reseed')}, agent_dial {m.get('restore_stalls_in_agent_dial')}, "
           f"probe {m.get('restore_stalls_in_probe')}; reseed p50/p90 {m.get('stage_restore_reseed_p50_ms')}/"
           f"{m.get('stage_restore_reseed_p90_ms')} ms; reseed retries {m.get('reseed_retries')}")
    ev = os.path.join(d, "apo_events.jsonl")
    if os.path.exists(ev):
      recs = [r for r in (studyc.rx.json_record(l) for l in open(ev, encoding="utf-8")) if r]
      won = collections.Counter(r.get("attempt") for r in recs if r.get("msg") == "APO reseed answered")
      slow = [r for r in recs if r.get("msg") == "APO reseed answered" and (r.get("total_ms") or 0) >= 1000]
      if won:
        out += (f"; lever A: answered on attempt {dict(sorted(won.items(), key=lambda kv: str(kv[0])))}, "
                f"{len(slow)} took >= 1 s (max {max((r.get('total_ms') or 0) for r in recs):.0f} ms)")
    return out

  def write_night_report(self) -> str:
    lo, hi, step = self.sc["fine"]
    q = load_queue(self.stack)
    need = studyc.win_ceiling(self.sc["v000_mean"], step)
    L = [f"# Study C stage B (overnight): stack-{self.stack}", "",
         f"_Written {self.now():%Y-%m-%d %H:%M UTC} by .apo/scripts/studyc_night.py._", "",
         f"**Stopped / finished because:** {self.stop_reason or 'still running'}", "",
         f"Fine ramp {lo:g}->{hi:g} step {step:g} (same FINE_FROM as every v000 run). **Win** (both runs): "
         f"fine ceiling >= {need:g}/s (10% over the v000 mean {self.sc['v000_mean']}/s); at the clean levels "
         f"shared with v000, resume p90 within +{self.sc['resume_cv_pct']}% and suspend p90 within "
         f"+{self.sc['suspend_cv_pct']}% (stack CV), err+ref within +{studyc.ERR_TOL_PTS} pts; 0 Substrate "
         f"OOM kills, leaked <= {studyc.MAX_LEAKED}, 0 crashed; the lever's switch on in every worker.", "",
         "## Runs", "", self.RUN_HEADER, "|" + "---|" * 16]
    for lbl in self.sc["v000_dirs"]:
      L.append(self.run_row(f"v000 {lbl}", self.done_ok(lbl)))
    items = list(q["items"])
    for it in items:  # waiting / skipped / given-up reasons, also for --report-only
      st, _, why = self.item_state(it)
      if why and st != "run":
        self.notes[it["id"]] = why
    combined = [{"id": "combined", "labels": [f"studyc-combined-{self.stack}-{i}" for i in (1, 2)]}]
    for it in items + combined:
      for lbl in self.item_labels(it):
        if self.dirs(lbl):
          L.append(self.run_row(f"{it['id']} {lbl}", self.done_ok(lbl) or self.last_dir(lbl)))
      if it["id"] in self.notes and not any(self.done_ok(l) for l in self.item_labels(it)):
        L.append(f"| {it['id']} | {self.notes[it['id']]} |" + " |" * 14)
    rss0 = [x for x in (night.summary(d).get("snapshot_plugin_rss_mib") for d in self.v000_runs()) if x]
    L += ["", "## Verdicts", ""]
    for it in items + combined:
      if is_v000(it) or not any(self.dirs(l) for l in self.item_labels(it)):
        if it["id"] in self.notes and not is_v000(it):
          L.append(f"- **{it['id']}: not run** ({self.notes[it['id']]})")
        continue
      v = self.verdict(it)
      rss = [r["rss"] for r in v["rows"] if r["rss"]]
      x = round(statistics.mean(rss) / statistics.mean(rss0), 2) if rss and rss0 else None
      ceil = ", ".join(str(r["ceiling"]) for r in v["rows"])
      L.append(f"- **{it['id']}: {'WIN' if v['win'] else 'NO WIN'}** (fine ceilings {ceil or '-'}; "
               f"RSS {x}x v000)" + (f" [{it.get('sets') or ''} {it.get('flags') or ''}]"))
      L += [f"  - {w}" for w in v["why"]]
    L += ["", "## Stalls and reseed timing per run", ""]
    for lbl in [l for l in self.sc["v000_dirs"] if l.startswith("studyc-v000")] + \
               [l for it in items + combined if not is_v000(it) for l in self.item_labels(it)]:
      d = self.done_ok(lbl)
      if d:
        L.append(f"- `{lbl}`: {self.reseed_line(d)}")
    L += ["", "## Per-level results", ""]
    for it in items + combined:
      for lbl in self.item_labels(it):
        d = self.done_ok(lbl) or self.last_dir(lbl)
        if d:
          L += [f"### {lbl}", ""] + self.level_table(d) + [""]
    if os.path.exists(TICK_RESEARCH):
      text = open(TICK_RESEARCH, encoding="utf-8").read()
      summ = text.split("## Summary", 1)[1].split("\n## ", 1)[0].strip() if "## Summary" in text else ""
      L += ["## Reseed tick research (summary)", "", summ or "(see tick_research.md)", "",
            f"Full notes: {TICK_RESEARCH}", ""]
    L += ["Nothing was folded or pushed; folding needs your approval."]
    return self.write("studyc-report", L)


def write_summary() -> str:
  """Both stacks' verdicts and best ceilings in one short file."""
  L = ["# Study C overnight: summary", "", f"_Written {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}._", ""]
  for stack in ("microvm", "gvisor"):
    n = NightC(stack, True)
    n.log = lambda msg: None  # read-only
    q = load_queue(stack)
    lo, hi, step = n.sc["fine"]
    L.append(f"## {stack} (win >= {studyc.win_ceiling(n.sc['v000_mean'], step):g}/s, v000 mean {n.sc['v000_mean']}/s)")
    L.append("")
    for it in q["items"] + [{"id": "combined", "labels": [f"studyc-combined-{stack}-{i}" for i in (1, 2)]}]:
      if is_v000(it):
        cs = [night.summary(d).get("fine_ceiling_per_s") for d in (n.done_ok(l) for l in n.item_labels(it)) if d]
        L.append(f"- v000 extra run: fine ceiling {cs or 'not run'}")
        continue
      if not any(n.dirs(l) for l in n.item_labels(it)):
        L.append(f"- {it['id']}: not run")
        continue
      v = n.verdict(it)
      L.append(f"- {it['id']}: **{'WIN' if v['win'] else 'NO WIN'}**, fine ceilings "
               f"{[r['ceiling'] for r in v['rows']]}" + ("" if v["win"] else f" ({'; '.join(v['why'][:2])})"))
    L += ["", f"Details: studyc_report_{stack}.md", ""]
  text = "\n".join(L) + "\n"
  with open(os.path.join(BRAIN, "studyc_summary.md"), "w", encoding="utf-8") as f:
    f.write(text)
  return text


def main() -> int:
  ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  ap.add_argument("--stack", choices=sorted(studyc.STACK))
  ap.add_argument("--stop", type=studyc.parse_stop, help="no new trial after this UTC time")
  ap.add_argument("--combine-by", type=studyc.parse_stop,
                  help="combine the winners at this time even if lever branches are still missing")
  ap.add_argument("--dry-run", action="store_true")
  ap.add_argument("--report-only", action="store_true")
  ap.add_argument("--summary", action="store_true")
  a = ap.parse_args()
  if a.summary:
    print(write_summary())
    return 0
  if not a.stack:
    ap.error("--stack is required")
  night.STOP_LAUNCH = a.stop or datetime(2100, 1, 1, tzinfo=timezone.utc)
  n = NightC(a.stack, a.dry_run)
  if a.combine_by:
    n.combine_by = a.combine_by
  if a.report_only:
    print(n.write_night_report())
    return 0
  n.run_night()
  if a.dry_run:
    print(f"\nDRY RUN for stack-{a.stack} (nothing launched):")
    for i, step in enumerate(n.plan, 1):
      print(f"  {i}. {step}")
    for k, why in n.notes.items():
      print(f"  - {k}: {why}")
  return 0


if __name__ == "__main__":
  sys.exit(main())
