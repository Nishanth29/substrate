#!/usr/bin/env python3
"""Phase 4 (Study C) driver: lever trials on one stack, one trial at a time, with a report.

Usage:
  studyc.py --stack microvm|gvisor [--plan stagea|stageb] [--levers A,2] [--stop 2026-10-12T15:00Z]
            [--dry-run] [--report-only]

--plan stagea: the v000 recheck fine ramp with the Stage A1 instrumentation (and the wider fine
  range), then on microVM the A2 layout probe (WORKER_COUNT=8 x WORKER_CPU_REQUEST=22, same
  total CPU). Report: recheck vs the v000 fine ceilings, layout vs recheck, stall split.
--plan stageb (default): per lever, in LEVERS order (or --levers), a fine ramp x2 with the
  lever on, built from its own worktree (branch apo/studyc-<stack>-<lever>, worktree
  ../substrate-apo-studyc-<stack>-<lever>; created from the branch if missing). A lever whose
  branch doesn't exist yet is skipped (its code isn't written). The switch is the lever's
  ATE_APO_* flag (run_experiment.py APO_FLAGS, default off), so the lever build serves v000 too.
  A "layout" lever has no code: it runs the base tree with --set overrides. After the single
  levers, the winners are run together x2 (branch apo/studyc-<stack>-combined = the base
  plus the winners' branches merged; flags and overrides combined).

Win rule per lever (both runs): fine ceiling >= WIN_CEILING (10% over the v000 mean, rounded up to
the fine step: microVM 5.0/s, gVisor 13.5/s); at the clean levels it shares with v000, resume and
suspend p90 not worse than the v000 mean by more than the stack CV, err+ref not worse by more
than 0.5 pts; 0 Substrate OOM kills, leaked_objects <= 5, crashed_actors 0. Snapshot-plugin RSS
is reported. Fine ceilings are compared only at the same FINE_FROM (the wake/park p90 gates use the
first level of the run as the baseline), so every run here starts at the stack's FINE_FROM.

Skip-done by label (an OK run with the same label is reused), build retries (the build runs the
unit tests; one package is flaky), no new trial after --stop, waits until no other driver or
run_experiment.py uses the stack's cluster. Logs go to stdout and .apo/results/studyc-<stack>.log,
so it runs fine in tmux. Nothing is folded; the report says what would be.
"""

import argparse
import glob
import math
import os
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import finish_k  # noqa: E402  pylint: disable=wrong-import-position
import night  # noqa: E402  pylint: disable=wrong-import-position

ROOT, RES, BRAIN, GIT_REPOS = night.ROOT, night.RES, night.BRAIN, night.GIT_REPOS
rx = finish_k.rx

STACK = {
    "microvm": {
        # Fine ramp: same FINE_FROM as every v000 fine run; FINE_TO raised so a winner isn't capped.
        "fine": (4.0, 6.0, 0.25), "ceiling": 4,
        "v000_mean": 4.44,  # v000 fine ceilings 4.25 / 4.75 (prephase4), 4.5 / 4.25 (Stage A)
        # v000 fine runs at FINE_FROM=4: prephase4 x2, Stage A recheck #1 (tail-only logs), #2 (full logs).
        "v000_dirs": ["fineramp-v000-microvm-1", "fineramp-v000-microvm-2", "studyc-v000-microvm-1",
                      "studyc-v000-microvm-2"],
        # Stack CV from v000 x3 at fixed rate (resume p90 / suspend p90), in percent.
        "resume_cv_pct": 2.0, "suspend_cv_pct": 4.36,
        "layout": {"WORKER_COUNT": "8", "WORKER_CPU_REQUEST": "22"},
    },
    "gvisor": {
        "fine": (11.0, 15.0, 0.5), "ceiling": 11,
        "v000_mean": 12.0,  # 12.5 / 12.0 (prephase4), 12.0 / 11.5 (Stage A)
        "v000_dirs": ["fineramp-v000-gvisor-1", "fineramp-v000-gvisor-2", "studyc-v000-gvisor-1",
                      "studyc-v000-gvisor-2", "studyc-v000-gvisor-3"],
        "resume_cv_pct": 2.0, "suspend_cv_pct": 3.7,
    },
}
# Lever order = the recommended order (phase4_plan.md). flags: the build's ATE_APO_* switches;
# sets: run_experiment.py --set overrides (a lever with sets and no flags needs no code).
LEVERS = {
    "microvm": [
        {"id": "A", "name": "crng-reseed", "flags": ["ATE_APO_RESEED_FAST=1"]},
        # Only when listed in --levers: worth a run only if A2 shows the layout matters.
        {"id": "layout", "name": "worker-layout", "sets": {"WORKER_COUNT": "8", "WORKER_CPU_REQUEST": "22"},
         "optional": True},
        {"id": "2", "name": "skip-proc-scan", "flags": ["ATE_APO_SKIP_PROC_SCAN=1"]},
        {"id": "3", "name": "netns-pool-tap", "flags": ["ATE_APO_NETNS_POOL_TAP=1"]},
        {"id": "1", "name": "page-cache-drop", "flags": ["ATE_APO_DROP_CACHES=1"]},
    ],
    "gvisor": [
        {"id": "6", "name": "restore-oneshot", "flags": ["ATE_APO_RESTORE_ONESHOT=1"]},
        {"id": "5", "name": "teardown-pause-only", "flags": ["ATE_APO_TEARDOWN_PAUSE_ONLY=1"]},
        {"id": "7", "name": "early-restore", "flags": ["ATE_APO_EARLY_RESTORE=1"]},
        # Lever 4 (retained snapshot) is too big for Phase 4 (phase4_plan.md); not run.
    ],
}
RUNS_PER_LEVER = 2
ERR_TOL_PTS = 0.5
MAX_LEAKED = 5
RSS_FLAG_X = 1.5


def win_ceiling(v000_mean: float, step: float) -> float:
  """10% over the v000 mean, rounded up to the fine step (4.5 -> 5.0, 12.25 -> 13.5)."""
  return round(math.ceil(round(1.1 * v000_mean / step, 6)) * step, 6)


def branch_exists(branch: str) -> bool:
  return subprocess.run(["git", "-C", ROOT, "rev-parse", "--verify", "--quiet", branch],
                        capture_output=True, text=True, check=False).returncode == 0


def switch_problems(d: str, flags: List[str]) -> str:
  """'' when every worker of run d logged each of flags as on at startup (apo_switches.jsonl)."""
  if not flags:
    return ""
  path = os.path.join(d, "apo_switches.jsonl")
  recs = []
  if os.path.exists(path):
    with open(path, encoding="utf-8") as f:
      recs = [r for r in (rx.json_record(l) for l in f) if r]
  if not recs:
    return "no worker logged its APO lever switches (old build, or log not collected)"
  bad = [r.get("k8s.pod", "?") for r in recs if not set(flags) <= set((r.get("active") or "").split())]
  return f"switch {' '.join(flags)} not on in {len(bad)}/{len(recs)} workers: {bad}" if bad else ""


def parse_stop(s: str) -> datetime:
  for fmt in ("%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M:%SZ"):
    try:
      return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
    except ValueError:
      continue
  raise argparse.ArgumentTypeError(f"--stop wants e.g. 2026-10-12T15:00Z, got {s!r}")


class StudyC(finish_k.FinishK):
  def __init__(self, stack: str, dry_run: bool, plan: str):
    super().__init__(stack, dry_run)
    self.plan_name = plan
    self.sc = STACK[stack]
    self.log_path = os.path.join(RES, f"studyc-{plan}-{stack}.log")
    self.state_path = os.path.join(RES, f"studyc-{plan}-{stack}.state.json")
    self.skipped: Dict[str, str] = {}

  # --- trials -------------------------------------------------------------------

  def fine_args(self) -> List[str]:
    lo, hi, step = self.sc["fine"]
    return ["--mode", "fine-ramp", "--set", f"FINE_FROM={lo:g}", "--set", f"FINE_TO={hi:g}",
            "--set", f"FINE_STEP={step:g}", "--set", f"CEILING={self.sc['ceiling']}"]

  @staticmethod
  def lever_args(lever: Dict) -> List[str]:
    args = []
    if lever.get("flags"):
      args += ["--set", "APO_FLAGS=" + " ".join(lever["flags"])]
    for k, v in (lever.get("sets") or {}).items():
      args += ["--set", f"{k}={v}"]
    return args

  def worktree(self, lever_id: str) -> Tuple[Optional[str], str]:
    """(worktree path, why) for a lever's branch; (None, why) when there's no branch yet."""
    branch = f"apo/studyc-{self.stack}-{lever_id}"
    wt = os.path.join(GIT_REPOS, f"substrate-apo-studyc-{self.stack}-{lever_id}")
    if not branch_exists(branch):
      return None, f"no branch {branch} (lever code not written yet)"
    if not os.path.isdir(wt):
      if self.dry:
        self.plan.append(f"git worktree add {wt} {branch}")
      else:
        subprocess.run(["git", "-C", ROOT, "worktree", "add", wt, branch], check=True,
                       capture_output=True, text=True)
    return wt, branch

  def run_label(self, label: str, wt: str, args: List[str]) -> Optional[str]:
    """finish_k's trial() (skip-done, build retries) from worktree wt."""
    have = self.done_ok(label)
    if have:
      self.log(f"{label}: already done ({os.path.basename(have)})")
      return have
    self.agentsim_dir = finish_k.FINE_AGENTSIM
    d, rc = None, -1
    try:
      for attempt in range(1, finish_k.BUILD_ATTEMPTS + 1):
        d, rc = self.run_trial(label, wt, args)
        phase = night.load_json(os.path.join(d, "error.json")).get("phase") if d and rc else ""
        if phase != "PREFLIGHT_BUILD" or attempt == finish_k.BUILD_ATTEMPTS:
          break
        self.log(f"{label}: build failed (attempt {attempt}/{finish_k.BUILD_ATTEMPTS}); retrying")
    finally:
      self.agentsim_dir = ""
    return d if (d and rc == 0) else None

  def stopped(self, label: str) -> bool:
    if self.now() >= night.STOP_LAUNCH and not self.done_ok(label):
      self.stop_reason = self.stop_reason or f"stop time {night.STOP_LAUNCH:%Y-%m-%d %H:%M} UTC: no new trials"
      self.log(self.stop_reason)
      return True
    return False

  def run_stagea(self) -> None:
    self.log(f"Study C stage A start (dry_run={self.dry}, pid {os.getpid()})")
    try:
      self.wait_idle()
      steps = [(f"studyc-v000-{self.stack}-1", [])]
      if self.stack == "microvm":
        steps.append(("studyc-layout8-microvm-1", self.lever_args({"sets": self.sc["layout"]})))
      for label, extra in steps:
        if self.stopped(label):
          break
        self.run_label(label, ROOT, self.fine_args() + extra)
    except Exception as e:  # pylint: disable=broad-except
      self.stop_reason = f"driver error: {type(e).__name__}: {e}"
      self.log(self.stop_reason)
    finally:
      self.finish(self.write_stagea)

  def run_stageb(self, only: List[str]) -> None:
    levers = [l for l in LEVERS[self.stack] if l["id"] in only or (not only and not l.get("optional"))]
    for l in LEVERS[self.stack]:
      if l.get("optional") and l not in levers:
        self.skipped[l["id"]] = "optional: runs only when listed in --levers"
    self.log(f"Study C stage B start (dry_run={self.dry}, pid {os.getpid()}) levers "
             f"{[l['id'] for l in levers]}, stop {night.STOP_LAUNCH:%Y-%m-%d %H:%M} UTC")
    try:
      self.wait_idle()
      for lever in levers:
        if lever.get("flags"):
          wt, why = self.worktree(lever["id"])
          if not wt:
            self.skipped[lever["id"]] = why
            self.log(f"lever {lever['id']}: skipped: {why}")
            continue
        else:
          wt = ROOT  # no code: the base tree with overrides
        for i in range(1, RUNS_PER_LEVER + 1):
          label = f"studyc-{lever['id']}-{self.stack}-{i}"
          if self.stopped(label):
            return
          self.run_label(label, wt, self.fine_args() + self.lever_args(lever))
      winners = [l for l in levers if self.verdict(l)["win"]]
      if len(winners) >= 2:
        combo = self.combined(winners)
        wt, why = self.worktree("combined") if combo.get("flags") else (ROOT, "")
        if not wt:
          self.skipped["combined"] = why
        else:
          for i in range(1, RUNS_PER_LEVER + 1):
            label = f"studyc-combined-{self.stack}-{i}"
            if self.stopped(label):
              return
            self.run_label(label, wt, self.fine_args() + self.lever_args(combo))
      elif not self.dry:
        self.log(f"combined run skipped: {len(winners)} winner(s)")
    except Exception as e:  # pylint: disable=broad-except
      self.stop_reason = f"driver error: {type(e).__name__}: {e}"
      self.log(self.stop_reason)
    finally:
      self.finish(self.write_stageb)

  def combined(self, winners: List[Dict]) -> Dict:
    """The winners together; the code ones need branch apo/studyc-<stack>-combined (merge them)."""
    combo: Dict = {"id": "combined", "flags": [], "sets": {}}
    for l in winners:
      combo["flags"] += l.get("flags") or []
      combo["sets"].update(l.get("sets") or {})
    code = [l for l in winners if l.get("flags")]
    branch = f"apo/studyc-{self.stack}-combined"
    if len(code) >= 2 and not branch_exists(branch):
      if self.dry:
        self.plan.append(f"create {branch} = apo/sweperf-swap + merge "
                         + ", ".join(f"apo/studyc-{self.stack}-{l['id']}" for l in code))
      else:
        self.merge_branch(branch, [f"apo/studyc-{self.stack}-{l['id']}" for l in code])
    elif len(code) == 1 and not branch_exists(branch):
      if self.dry:
        self.plan.append(f"create {branch} = apo/studyc-{self.stack}-{code[0]['id']}")
      else:
        night.git(ROOT, "branch", branch, f"apo/studyc-{self.stack}-{code[0]['id']}")
    return combo

  def merge_branch(self, branch: str, parts: List[str]) -> None:
    wt = os.path.join(GIT_REPOS, f"substrate-apo-studyc-{self.stack}-combined")
    subprocess.run(["git", "-C", ROOT, "worktree", "add", "-b", branch, wt, "apo/sweperf-swap"],
                   check=True, capture_output=True, text=True)
    for p in parts:
      r = subprocess.run(["git", "-C", wt, "merge", "--no-edit", p], capture_output=True, text=True)
      if r.returncode != 0:
        subprocess.run(["git", "-C", wt, "merge", "--abort"], capture_output=True, text=True)
        raise RuntimeError(f"merging {p} into {branch} conflicts; combine by hand")

  def finish(self, writer) -> None:
    if self.dry:
      return
    self.set_state(step="done", trial="", stop_reason=self.stop_reason)
    try:
      writer()
    except Exception as e:  # pylint: disable=broad-except
      self.log(f"report failed: {type(e).__name__}: {e}")
    self.log("Study C driver done; node left up and idle")

  # --- comparison ---------------------------------------------------------------

  def v000_runs(self) -> List[str]:
    return [d for d in (self.done_ok(lbl) for lbl in self.sc["v000_dirs"]) if d]

  def v000_levels(self) -> Dict[float, List[Dict]]:
    per: Dict[float, List[Dict]] = {}
    for d in self.v000_runs():
      for r in self.levels(d):
        if r.get("verdict") == "clean":
          per.setdefault(float(r["target_per_s"]), []).append(r)
    return per

  def shared_level_problems(self, d: str) -> List[str]:
    """Clean levels of run d that v000 also ran clean: p90s within the stack CV, err+ref +0.5 pts."""
    per, bad = self.v000_levels(), []
    for r in self.levels(d):
      t = float(r["target_per_s"])
      if r.get("verdict") != "clean" or t not in per:
        continue
      def mean(k):
        xs = [x.get(k) for x in per[t] if x.get(k) is not None]
        return statistics.mean(xs) if xs else None
      for k, tol in (("resume_warm_p90_ms", self.sc["resume_cv_pct"]),
                     ("suspend_p90_ms", self.sc["suspend_cv_pct"])):
        a, b = r.get(k), mean(k)
        if a is not None and b and 100 * (a - b) / b > tol:
          bad.append(f"{k} at {t:g}/s {a} vs v000 {b:.1f} (+{100 * (a - b) / b:.1f}% > {tol}%)")
      a, b = r.get("error_refusal_pct") or 0, mean("error_refusal_pct") or 0
      if a - b > ERR_TOL_PTS:
        bad.append(f"err+ref at {t:g}/s {a}% vs v000 {b:.2f}% (> +{ERR_TOL_PTS} pts)")
    return bad

  def run_labels(self, lever: Dict) -> List[str]:
    """The lever's run labels: its own list, else studyc-<id>-<stack>-1..RUNS_PER_LEVER."""
    return lever.get("labels") or [f"studyc-{lever['id']}-{self.stack}-{i}"
                                   for i in range(1, RUNS_PER_LEVER + 1)]

  def verdict(self, lever: Dict) -> Dict:
    lo, hi, step = self.sc["fine"]
    need = win_ceiling(self.sc["v000_mean"], step)
    labels = self.run_labels(lever)
    runs = [self.done_ok(lbl) for lbl in labels]
    why, win = [], all(runs)
    if not all(runs):
      why.append(f"{sum(1 for r in runs if r)}/{len(labels)} OK runs")
    rows = []
    for d in runs:
      if not d:
        continue
      m = night.summary(d)
      c = m.get("fine_ceiling_per_s") or 0
      row = {"dir": d, "ceiling": c, "max_goodput": m.get("max_goodput_per_s"),
             "rss": m.get("snapshot_plugin_rss_mib"), "oom": m.get("node_oom_events"),
             "leaked": m.get("leaked_objects"), "crashed": m.get("crashed_actors"),
             "stalls": m.get("restore_stalls"), "refusals_504": m.get("refusals_status_504")}
      rows.append(row)
      tag = os.path.basename(d)
      if c < need - 1e-9:
        win = False
        why.append(f"{tag}: fine ceiling {c} < {need:g}")
      if row["oom"] != 0:  # missing counts as a miss
        win = False
        why.append(f"{tag}: Substrate OOM kills {row['oom']}")
      if row["leaked"] is None or row["leaked"] > MAX_LEAKED:
        win = False
        why.append(f"{tag}: leaked_objects {row['leaked']} > {MAX_LEAKED}")
      if row["crashed"]:
        win = False
        why.append(f"{tag}: crashed_actors {row['crashed']}")
      off = switch_problems(d, lever.get("flags") or [])
      if off:
        win = False
        why.append(f"{tag}: {off}")
      probs = self.shared_level_problems(d)
      if probs:
        win = False
        why += [f"{tag}: {p}" for p in probs]
    if win:
      why.append(f"both runs >= {need:g}/s; shared clean levels within noise; constraints hold")
    return {"id": lever["id"], "win": bool(win and rows), "need": need, "rows": rows, "why": why}

  # --- reports ------------------------------------------------------------------

  def level_table(self, d: Optional[str]) -> List[str]:
    out = ["| target /s | achieved /s | goodput /s | resume p90 ms | suspend p90 ms | err+ref % | "
           "refusals by status | verdict |", "|---|---|---|---|---|---|---|---|"]
    for r in self.levels(d):
      out.append(f"| {r.get('target_per_s')} | {r.get('achieved_per_s')} | {r.get('goodput_per_s')} | "
                 f"{r.get('resume_warm_p90_ms')} | {r.get('suspend_p90_ms')} | {r.get('error_refusal_pct')} | "
                 f"{r.get('refusal_status') or ''} | {r.get('verdict')} |")
    return out

  def run_row(self, label: str, d: Optional[str]) -> str:
    m = night.summary(d) if d else {}
    keys = ("fine_ceiling_per_s", "max_goodput_per_s", "restore_stalls", "restore_stall_bursts",
            "restore_stalls_in_reseed", "restore_stalls_in_probe", "reseed_retries",
            "refusals_status_503", "refusals_status_504", "snapshot_plugin_rss_mib",
            "leaked_objects", "node_oom_events", "crashed_actors")
    pv = night.load_json(os.path.join(d, "provenance.json")) if d else {}
    head = (pv.get("workspace_head") or "")[:8] + ("+dirty" if pv.get("workspace_dirty") else "")
    return (f"| {label} | " + " | ".join(str(m.get(k, "")) for k in keys) +
            f" | {head} | {os.path.basename(d) if d else 'missing'} |")

  RUN_HEADER = ("| run | fine ceiling /s | max goodput /s | stalls | bursts | in reseed | in probe | "
                "reseed retries | 503 | 504 | snapshot-plugin RSS MiB | leaked | OOM | crashed | commit | dir |")

  def write(self, name: str, lines: List[str]) -> str:
    text = "\n".join(lines) + "\n"
    for path in (os.path.join(RES, f"{name}-{self.stack}.md"), os.path.join(BRAIN, f"{name.replace('-', '_')}_{self.stack}.md")):
      with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    self.log(f"report written: {os.path.join(BRAIN, name.replace('-', '_') + '_' + self.stack + '.md')}")
    return text

  def write_stagea(self) -> str:
    lo, hi, step = self.sc["fine"]
    L = [f"# Study C stage A: stack-{self.stack}", "",
         f"_Written {self.now():%Y-%m-%d %H:%M UTC} by .apo/scripts/studyc.py --plan stagea._", "",
         f"**Stopped / finished because:** {self.stop_reason or 'all steps done'}", "",
         f"Fine ramp FINE_FROM={lo:g} FINE_TO={hi:g} FINE_STEP={step:g} (v000 reference mean "
         f"{self.sc['v000_mean']}/s, noise about 5%).", "", self.RUN_HEADER, "|" + "---|" * 16]
    for lbl in self.sc["v000_dirs"]:
      L.append(self.run_row(lbl, self.done_ok(lbl)))
    if self.stack == "microvm":
      L.append(self.run_row("A2 layout 8 x 22 CPU", self.done_ok("studyc-layout8-microvm-1")))
    for lbl in ["studyc-v000-%s-1" % self.stack] + (["studyc-layout8-microvm-1"] if self.stack == "microvm" else []):
      d = self.done_ok(lbl)
      L += ["", f"## {lbl} per level", ""] + self.level_table(d)
      st = night.load_json(os.path.join(d, "stalls.json")) if d else {}
      if st:
        L += ["", f"Stalls per worker: {st.get('per_pod')}; reseed retries per worker: "
                  f"{st.get('reseed_retries_per_pod')}; bursts: "
                  + "; ".join(f"{b['n']} stalls over {b['end'] - b['start']:.0f} s on {b['pods']}"
                              for b in st.get("bursts") or [])]
    return self.write("studyc-stagea-report", L)

  def write_stageb(self) -> str:
    lo, hi, step = self.sc["fine"]
    L = [f"# Study C stage B: stack-{self.stack}", "",
         f"_Written {self.now():%Y-%m-%d %H:%M UTC} by .apo/scripts/studyc.py._", "",
         f"**Stopped / finished because:** {self.stop_reason or 'all steps done'}", "",
         f"Fine ramp {lo:g}->{hi:g} step {step:g}. Win: fine ceiling >= "
         f"{win_ceiling(self.sc['v000_mean'], step):g}/s on both runs (10% over v000 {self.sc['v000_mean']}/s), "
         f"shared clean levels within resume +{self.sc['resume_cv_pct']}% / suspend +{self.sc['suspend_cv_pct']}% "
         f"/ err+ref +{ERR_TOL_PTS} pts of v000, 0 Substrate OOM kills, leaked <= {MAX_LEAKED}, 0 crashed.", "",
         self.RUN_HEADER, "|" + "---|" * 16]
    for lbl in self.sc["v000_dirs"]:
      L.append(self.run_row(lbl, self.done_ok(lbl)))
    rss0 = [night.summary(d).get("snapshot_plugin_rss_mib") for d in self.v000_runs()]
    rss0 = [x for x in rss0 if x]
    verdicts = []
    for lever in LEVERS[self.stack] + [{"id": "combined"}]:
      for i in range(1, RUNS_PER_LEVER + 1):
        lbl = f"studyc-{lever['id']}-{self.stack}-{i}"
        if self.done_ok(lbl) or self.dirs(lbl):
          L.append(self.run_row(lbl, self.done_ok(lbl)))
      if lever["id"] in self.skipped:
        L.append(f"| {lever['id']} | skipped: {self.skipped[lever['id']]} |" + " |" * 14)
      elif self.dirs(f"studyc-{lever['id']}-{self.stack}-1"):
        verdicts.append(self.verdict(lever))
    L += ["", "## Verdicts", ""]
    for v in verdicts:
      rss = [r["rss"] for r in v["rows"] if r["rss"]]
      x = round(statistics.mean(rss) / statistics.mean(rss0), 2) if rss and rss0 else None
      flag = f" (RSS {x}x v000{', flag' if x and x > RSS_FLAG_X else ''})" if x else ""
      L.append(f"- **{v['id']}: {'WIN' if v['win'] else 'no win'}**{flag}")
      L += [f"  - {w}" for w in v["why"]]
    L += ["", "Nothing was folded; folding needs your approval."]
    return self.write("studyc-report", L)


def main() -> int:
  ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  ap.add_argument("--stack", choices=sorted(STACK), required=True)
  ap.add_argument("--plan", choices=("stagea", "stageb"), default="stageb")
  ap.add_argument("--levers", default="", help="comma-separated lever ids (default: all, in order)")
  ap.add_argument("--stop", type=parse_stop, help="no new trial after this UTC time, e.g. 2026-10-12T15:00Z")
  ap.add_argument("--dry-run", action="store_true")
  ap.add_argument("--report-only", action="store_true")
  a = ap.parse_args()
  # finish_k lifts night's launch cut-off; --stop puts one back.
  night.STOP_LAUNCH = a.stop or datetime(2100, 1, 1, tzinfo=timezone.utc)
  s = StudyC(a.stack, a.dry_run, a.plan)
  if a.report_only:
    print(s.write_stagea() if a.plan == "stagea" else s.write_stageb())
    return 0
  if a.plan == "stagea":
    s.run_stagea()
  else:
    s.run_stageb([x for x in a.levers.split(",") if x])
  if a.dry_run:
    print(f"\nDRY RUN PLAN for stack-{a.stack} ({a.plan}; nothing launched):")
    for i, step in enumerate(s.plan, 1):
      print(f"  {i}. {step}")
    for lid, why in s.skipped.items():
      print(f"  - lever {lid} skipped: {why}")
  return 0


if __name__ == "__main__":
  sys.exit(main())
