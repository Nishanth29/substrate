#!/usr/bin/env python3
"""Finishes Phase 3 (Study K) on one stack: confirms the best knob sets, reports a fold verdict.

Usage: finish_k.py --stack microvm|gvisor [--plan finishk|prephase4] [--dry-run] [--report-only]

microVM: k010 x3 fixed-rate trials (RATE from tunables.microvm.env, same harness as v000),
then for k010 and k006 a coarse ramp plus a fine ramp near the ceiling.
gVisor: for k003 and k006 a coarse ramp plus a fine ramp near the ceiling.

One trial at a time on the stack's cluster, with the stack's env (night.py's Night). Knob
values come from .apo/results/studyk-<stack>-rows.json and are passed as --set KEY=VALUE.
Fine ramps use the fine-ramp agentsim (AGENTS_TCO_DIR=substrate-agents-tco-fineramp);
fixed and coarse ramps use the same agentsim as v000. Never folds anything: it writes
.apo/results/finishk-report-<stack>.md (and a brain copy) with the tables and the verdict.

--plan prephase4: v000 (default knobs) fine ramps x2 over exactly the same range as the k
fine ramps, then a per-k-set verdict against them in prephase4-report-<stack>.md. It first
waits until no other finish-K driver or run_experiment.py is busy on the stack's cluster, and
launches no trial after PREPHASE4_STOP.
"""

import argparse
import glob
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import night  # noqa: E402  pylint: disable=wrong-import-position

ROOT, RES, BRAIN, GIT_REPOS = night.ROOT, night.RES, night.BRAIN, night.GIT_REPOS
FINE_AGENTSIM = os.path.join(GIT_REPOS, "substrate-agents-tco-fineramp")
night.STOP_LAUNCH = datetime(2100, 1, 1, tzinfo=timezone.utc)  # no launch cut-off for this run
sys.path.insert(0, os.path.join(ROOT, ".apo"))
import run_experiment as rx  # noqa: E402  pylint: disable=wrong-import-position

PREPHASE4_STOP = datetime(2026, 10, 11, 23, 0, tzinfo=timezone.utc)  # no new trial after this
PREPHASE4 = {
    # Same FINE_FROM/TO/STEP as the k fine ramps: the wake/park p90 gates compare each level
    # with the first level of the same run, so fine ceilings only compare at the same FINE_FROM.
    "microvm": {"v000_runs": 2, "fine": (4, 5, 0.25), "k_sets": ["k010"],
                "resume_tol_pct": 2.0, "suspend_tol_pct": 4.36, "err_tol_pts": 0.5},
    "gvisor": {"v000_runs": 2, "fine": (11, 13, 0.5), "k_sets": ["k003", "k006"],
               "resume_tol_pct": 2.0, "suspend_tol_pct": 3.7, "err_tol_pts": 0.5},
}
RSS_FLAG_X = 1.5

PLAN = {
    # v000 references: coarse ramp ceilings and the v000 x3 fixed-rate means.
    # k006 ramps skipped (user, 18:52: k006 was -0.3% at fixed rate, not worth ~65 min).
    "microvm": {"fixed": "k010", "fixed_n": 3, "ramps": ["k010"], "ceiling": 4,
                "fine_span": 1.0, "fine_step": 0.25, "suspend_tol_pct": 4.36, "min_gain_pct": 3.0},
    "gvisor": {"fixed": "", "fixed_n": 0, "ramps": ["k003", "k006"], "ceiling": 11,
               # the coarse ramp jumps 11 -> 13 (x1.15), so the fine ramp spans 11..13
               "fine_span": 2.0, "fine_step": 0.5, "suspend_tol_pct": 3.7, "min_gain_pct": 3.0},
}
BUILD_ATTEMPTS = 3  # the build step runs the unit tests; one package is flaky
FIXED_KEYS = ["resume_warm_p90_ms", "suspend_p90_ms", "ttfe_accept_p95_ms", "error_refusal_pct",
              "achieved_ratio", "drain_s_max", "backlog_growth_excess", "leaked_objects",
              "node_oom_events", "node_oom_other", "crashed_actors", "windows_ok",
              "snapshot_plugin_rss_mib", "snapshot_plugin_peak_rss_mib", "mem_per_actor_mib"]


class FinishK(night.Night):
  def __init__(self, stack: str, dry_run: bool):
    super().__init__(stack, dry_run)
    self.log_path = os.path.join(RES, f"finishk-{stack}.log")
    self.state_path = os.path.join(RES, f"finishk-{stack}.state.json")
    self.p = PLAN[stack]
    self.agentsim_dir = ""
    rows = night.load_json(os.path.join(RES, f"studyk-{stack}-rows.json")) or []
    self.knobs: Dict[str, Dict] = {r["id"]: r["params"] for r in rows}
    self.objective = os.path.join(self.study_wt, ".apo", "objective.md")

  def env(self) -> Dict[str, str]:
    e = super().env()
    if self.agentsim_dir:
      e["AGENTS_TCO_DIR"] = self.agentsim_dir
    return e

  def sets(self, kid: str) -> List[str]:
    return [a for k, v in self.knobs[kid].items() for a in ("--set", f"{k}={v}")]

  def dirs(self, label: str) -> List[str]:
    return sorted(glob.glob(os.path.join(RES, f"{label}-2*")), key=os.path.getmtime)

  def done_ok(self, label: str) -> Optional[str]:
    ok = [d for d in self.dirs(label) if self.finished_ok(d)]
    return ok[-1] if ok else None

  def trial(self, label: str, args: List[str], fine: bool = False) -> Optional[str]:
    """Runs label unless an OK run already exists (so a restart doesn't redo work)."""
    have = self.done_ok(label)
    if have:
      self.log(f"{label}: already done ({os.path.basename(have)})")
      return have
    self.agentsim_dir = FINE_AGENTSIM if fine else ""
    try:
      for attempt in range(1, BUILD_ATTEMPTS + 1):
        d, rc = self.run_trial(label, ROOT, args)
        phase = night.load_json(os.path.join(d, "error.json")).get("phase") if d and rc else ""
        if phase != "PREFLIGHT_BUILD" or attempt == BUILD_ATTEMPTS:
          break
        self.log(f"{label}: build failed (attempt {attempt}/{BUILD_ATTEMPTS}, flaky unit test?); retrying")
    finally:
      self.agentsim_dir = ""
    return d if (d and rc == 0) else None

  def run(self) -> None:
    self.log(f"finish-K driver start (dry_run={self.dry}, pid {os.getpid()}) plan {self.p}")
    try:
      if self.p["fixed"]:
        kid = self.p["fixed"]
        for i in range(1, self.p["fixed_n"] + 1):
          self.trial(f"{kid}-{self.stack}-{i}", self.sets(kid))
      for kid in self.p["ramps"]:
        rd = self.trial(f"ramp-{kid}-{self.stack}", ["--mode", "ramp"] + self.sets(kid))
        coarse = (night.summary(rd).get("ceiling_per_s") if rd else None) or self.p["ceiling"]
        lo = max(float(self.p["ceiling"]), float(coarse))
        hi = lo + self.p["fine_span"]
        self.trial(f"fineramp-{kid}-{self.stack}",
                   ["--mode", "fine-ramp", "--set", f"FINE_FROM={lo:g}", "--set", f"FINE_TO={hi:g}",
                    "--set", f"FINE_STEP={self.p['fine_step']:g}", "--set", f"CEILING={self.p['ceiling']}"]
                   + self.sets(kid), fine=True)
    except Exception as e:  # pylint: disable=broad-except
      self.stop_reason = f"driver error: {type(e).__name__}: {e}"
      self.log(self.stop_reason)
    finally:
      if not self.dry:
        self.set_state(step="done", trial="", stop_reason=self.stop_reason)
        try:
          self.write_report()
        except Exception as e:  # pylint: disable=broad-except
          self.log(f"report failed: {type(e).__name__}: {e}")
        self.log("finish-K driver done; node left up and idle")

  # --- prephase4 ------------------------------------------------------------------

  def busy_pids(self) -> List[int]:
    """Other finish-K drivers for this stack, and run_experiment.py runs on this stack's cluster."""
    me, out = os.getpid(), []
    for p in glob.glob("/proc/[0-9]*/cmdline"):
      try:
        pid = int(p.split("/")[2])
        cmd = open(p, "rb").read().replace(b"\0", b" ").decode(errors="replace")
        if pid == me or not cmd.startswith("python3 "):
          continue
        if "finish_k.py" in cmd and f"--stack {self.stack}" in cmd and "prephase4" not in cmd:
          out.append(pid)
        elif "/.apo/run_experiment.py" in cmd:
          envb = open(f"/proc/{pid}/environ", "rb").read().split(b"\0")
          if f"CLUSTER_NAME={self.cfg['env']['CLUSTER_NAME']}".encode() in envb:
            out.append(pid)
      except (OSError, ValueError):
        continue
    return out

  def wait_idle(self) -> None:
    pids = self.busy_pids()
    if self.dry:
      self.plan.append(f"wait until the cluster is idle (busy now: {pids or 'none'})")
      return
    self.set_state(step="wait-idle", trial="")
    if pids:
      self.log(f"waiting for {pids} (other finish-K driver / trial on this cluster)")
    while self.busy_pids():
      time.sleep(60)
    # Trials whose driver was stopped get their DONE here (EXIT=0 iff summary.json and no error.json).
    for d in glob.glob(os.path.join(RES, f"*-{self.stack}-*")) + glob.glob(os.path.join(RES, f"*-{self.stack}-2*")):
      base = os.path.basename(d)
      if os.path.isdir(d) and not os.path.exists(os.path.join(d, "DONE")) and base.split("-")[0] in (
          "fineramp", "ramp", "k010", "k003", "k006") and os.path.exists(os.path.join(d, "provenance.json")):
        self.mark_adopted_done(d)

  def run_prephase4(self) -> None:
    pp = PREPHASE4[self.stack]
    lo, hi, step = pp["fine"]
    self.log(f"prephase4 driver start (dry_run={self.dry}, pid {os.getpid()}) plan {pp}")
    try:
      self.wait_idle()
      for i in range(1, pp["v000_runs"] + 1):
        if self.now() >= PREPHASE4_STOP and not self.done_ok(f"fineramp-v000-{self.stack}-{i}"):
          self.stop_reason = "23:00 UTC: no new trials"
          self.log(self.stop_reason)
          break
        self.trial(f"fineramp-v000-{self.stack}-{i}",
                   ["--mode", "fine-ramp", "--set", f"FINE_FROM={lo:g}", "--set", f"FINE_TO={hi:g}",
                    "--set", f"FINE_STEP={step:g}"], fine=True)
    except Exception as e:  # pylint: disable=broad-except
      self.stop_reason = f"driver error: {type(e).__name__}: {e}"
      self.log(self.stop_reason)
    finally:
      if not self.dry:
        self.set_state(step="done", trial="", stop_reason=self.stop_reason)
        try:
          self.write_prephase4()
          self.write_report()  # the finish-K report too (k006 microVM ramps skipped)
        except Exception as e:  # pylint: disable=broad-except
          self.log(f"report failed: {type(e).__name__}: {e}")
        self.log("prephase4 driver done; node left up and idle")

  def levels(self, d: Optional[str]) -> List[Dict]:
    if not d:
      return []
    lv = night.load_json(os.path.join(d, "levels.json")).get("levels")
    return lv if lv is not None else rx.level_rows(night.agentsim_result(d))

  def prephase4_verdicts(self) -> Tuple[Dict, List[Dict]]:
    pp = PREPHASE4[self.stack]
    step = pp["fine"][2]
    v0 = [d for i in range(1, pp["v000_runs"] + 1) for d in [self.done_ok(f"fineramp-v000-{self.stack}-{i}")] if d]
    v0c = [night.summary(d).get("fine_ceiling_per_s") or 0 for d in v0]
    v0rss = [night.summary(d).get("snapshot_plugin_rss_mib") for d in v0]
    v0rss = [x for x in v0rss if x]
    base = {"dirs": v0, "ceilings": v0c, "max": max(v0c) if v0c else None,
            "mean": round(statistics.mean(v0c), 3) if v0c else None,
            "spread": round(max(v0c) - min(v0c), 3) if v0c else None,
            "rss_mean": round(statistics.mean(v0rss), 1) if v0rss else None}
    # v000 mean per clean level, for the shared-level comparison
    per: Dict[float, List[Dict]] = {}
    for d in v0:
      for r in self.levels(d):
        if r.get("verdict") == "clean":
          per.setdefault(float(r["target_per_s"]), []).append(r)
    out = []
    for kid in pp["k_sets"]:
      d = self.done_ok(f"fineramp-{kid}-{self.stack}")
      m = night.summary(d) if d else {}
      c = m.get("fine_ceiling_per_s")
      why, ok = [], bool(d) and len(v0) == pp["v000_runs"]
      if not d:
        why.append(f"no OK fine ramp for {kid}")
      if len(v0) < pp["v000_runs"]:
        why.append(f"only {len(v0)}/{pp['v000_runs']} v000 fine ramps")
      if d and base["max"] is not None:
        need = base["max"] + step
        why.append(f"fine ceiling {c} vs v000 max {base['max']} (need >= {need:g}, one step above)")
        ok &= c is not None and c >= need - 1e-9
      worse = []
      for r in self.levels(d):
        t = float(r["target_per_s"])
        if r.get("verdict") != "clean" or t not in per:
          continue
        ref = per[t]
        def mean(k):
          xs = [x.get(k) for x in ref if x.get(k) is not None]
          return statistics.mean(xs) if xs else None
        for k, tol in (("resume_warm_p90_ms", pp["resume_tol_pct"]), ("suspend_p90_ms", pp["suspend_tol_pct"])):
          a, b = r.get(k), mean(k)
          if a is not None and b and 100 * (a - b) / b > tol:
            worse.append(f"{k} at {t:g}/s {a} vs v000 {b:.1f} (+{100 * (a - b) / b:.1f}% > {tol}%)")
        a, b = r.get("error_refusal_pct") or 0, mean("error_refusal_pct") or 0
        if a - b > pp["err_tol_pts"]:
          worse.append(f"err+ref at {t:g}/s {a}% vs v000 {b:.2f}% (> +{pp['err_tol_pts']} pts)")
      if worse:
        ok = False
        why += worse
      elif d:
        why.append("shared clean levels: resume/suspend p90 and err+ref not worse (within noise)")
      rss = m.get("snapshot_plugin_rss_mib")
      ratio = round(rss / base["rss_mean"], 2) if rss and base["rss_mean"] else None
      out.append({"set": kid, "dir": d, "ceiling": c, "max_goodput": m.get("max_goodput_per_s"),
                  "rss": rss, "rss_x": ratio, "rss_flag": bool(ratio and ratio > RSS_FLAG_X),
                  "verdict": "FOLD" if ok else "NO FOLD", "why": why})
    return base, out

  def write_prephase4(self) -> str:
    st, pp = self.stack, PREPHASE4[self.stack]
    lo, hi, step = pp["fine"]
    base, ks = self.prephase4_verdicts()
    L = [f"# Pre-Phase 4: stack-{st}", "",
         f"_Written {self.now().strftime('%Y-%m-%d %H:%M UTC')} by .apo/scripts/finish_k.py --plan prephase4._", "",
         f"**Stopped / finished because:** {self.stop_reason or 'all steps done'}", "",
         f"Fine ramps: FINE_FROM={lo:g} FINE_TO={hi:g} FINE_STEP={step:g}, one 3-minute window per level. "
         "The wake/park p90 gates (`--fail-rel-p90=2.5`) compare each level with the **first level of the same "
         f"run** (here {lo:g}/s; agentsim swap.go addLevel), so fine ceilings compare only at the same FINE_FROM.", "",
         "## Fine ceilings", "", "| run | fine ceiling /s | max goodput /s | snapshot-plugin RSS MiB | RSS vs v000 | dir |",
         "|---|---|---|---|---|---|"]
    for d, c in zip(base["dirs"], base["ceilings"]):
      m = night.summary(d)
      L.append(f"| v000 (defaults) | {c} | {m.get('max_goodput_per_s')} | {m.get('snapshot_plugin_rss_mib')} | | {os.path.basename(d)} |")
    L.append(f"| **v000 mean / spread** | **{base['mean']} / {base['spread']}** | | {base['rss_mean']} | 1.0x | |")
    for k in ks:
      flag = f" **> {RSS_FLAG_X}x, flag**" if k["rss_flag"] else ""
      L.append(f"| {k['set']} | {k['ceiling']} | {k['max_goodput']} | {k['rss']} | {k['rss_x']}x{flag} | "
               f"{os.path.basename(k['dir']) if k['dir'] else 'missing'} |")
    # coarse ramp RSS for context
    name, m0 = self.v000_ceiling()
    L += ["", f"Coarse ramps (context): v000 `{name}` ceiling {m0.get('ceiling_per_s')}/s, RSS {m0.get('snapshot_plugin_rss_mib')} MiB"]
    for kid in pp["k_sets"]:
      rd = self.done_ok(f"ramp-{kid}-{st}")
      if rd:
        mm = night.summary(rd)
        L.append(f"; {kid} `{os.path.basename(rd)}` ceiling {mm.get('ceiling_per_s')}/s, RSS {mm.get('snapshot_plugin_rss_mib')} MiB")
    L += ["", "## Per-level results", ""]
    runs = [(f"v000 run {i + 1}", d) for i, d in enumerate(base["dirs"])] + [(k["set"], k["dir"]) for k in ks]
    for label, d in runs:
      L += [f"### {label} ({os.path.basename(d) if d else 'missing'})", "",
            "| target /s | achieved /s | goodput /s | resume p90 ms | suspend p90 ms | err+ref % | drain s | verdict |",
            "|---|---|---|---|---|---|---|---|"]
      for r in self.levels(d):
        L.append(f"| {r.get('target_per_s')} | {r.get('achieved_per_s')} | {r.get('goodput_per_s')} | "
                 f"{r.get('resume_warm_p90_ms')} | {r.get('suspend_p90_ms')} | {r.get('error_refusal_pct')} | "
                 f"{r.get('drain_s')} | {r.get('verdict')} |")
      L.append("")
    L += ["## Verdict per k set", "",
          f"Rule: FOLD only if the k fine ceiling >= max(v000 fine ceilings) + one step ({step:g}/s) AND at the "
          f"shared clean levels resume p90 is within +{pp['resume_tol_pct']}%, suspend p90 within "
          f"+{pp['suspend_tol_pct']}% and err+ref within +{pp['err_tol_pts']} pts of the v000 mean. "
          f"RSS > {RSS_FLAG_X}x the v000 fine-ramp mean is flagged.", ""]
    for k in ks:
      L.append(f"- **{k['set']}: {k['verdict']}**" + (" (RSS flagged)" if k["rss_flag"] else ""))
      L += [f"  - {w}" for w in k["why"]]
    L += ["", "Nothing was folded; folding needs your approval."]
    text = "\n".join(L) + "\n"
    for path in (os.path.join(RES, f"prephase4-report-{st}.md"), os.path.join(BRAIN, f"prephase4_report_{st}.md")):
      with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    self.log(f"prephase4 report written: {os.path.join(BRAIN, f'prephase4_report_{st}.md')}; "
             + "; ".join(f"{k['set']} {k['verdict']}" for k in ks))
    return text

  # --- report and verdict --------------------------------------------------------

  def v000(self) -> List[Dict]:
    return [night.summary(d) for d in self.v000_dirs() if self.finished_ok(d)]

  def v000_ceiling(self) -> Tuple[str, Dict]:
    d = os.path.join(RES, self.cfg["ramp_dir"])
    return self.cfg["ramp_dir"], night.summary(d)

  def ramp_rows(self) -> List[Dict]:
    rows = []
    name, m = self.v000_ceiling()
    rows.append({"set": "v000 (defaults)", "coarse": m.get("ceiling_per_s"), "fine": None,
                 "dirs": name, "rss": m.get("snapshot_plugin_rss_mib"), "leaked": m.get("leaked_objects"),
                 "oom": m.get("node_oom_events"), "crashed": m.get("crashed_actors")})
    for kid in self.p["ramps"]:
      rd, fd = self.done_ok(f"ramp-{kid}-{self.stack}"), self.done_ok(f"fineramp-{kid}-{self.stack}")
      rm, fm = night.summary(rd) if rd else {}, night.summary(fd) if fd else {}
      rows.append({"set": kid, "coarse": rm.get("ceiling_per_s"), "fine": fm.get("fine_ceiling_per_s"),
                   "fine_levels": fm.get("fine_levels"),
                   "dirs": " / ".join(os.path.basename(x) for x in (rd, fd) if x) or "not run / failed",
                   "rss": max([x for x in (rm.get("snapshot_plugin_rss_mib"), fm.get("snapshot_plugin_rss_mib")) if x] or [None]),
                   "leaked": max([x for x in (rm.get("leaked_objects"), fm.get("leaked_objects")) if x is not None] or [None]),
                   "oom": max([x for x in (rm.get("node_oom_events"), fm.get("node_oom_events")) if x is not None] or [None]),
                   "crashed": max([x for x in (rm.get("crashed_actors"), fm.get("crashed_actors")) if x is not None] or [None])})
    return rows

  def verdict(self) -> Tuple[str, List[str]]:
    why: List[str] = []
    v0 = self.v000()
    v0r = statistics.mean(m["resume_warm_p90_ms"] for m in v0)
    v0s = statistics.mean(m["suspend_p90_ms"] for m in v0)
    ceil0 = float(self.p["ceiling"])
    ramps = {r["set"]: r for r in self.ramp_rows()[1:]}
    def ceiling(r: Dict) -> float:
      return max(float(r.get("fine") or 0), float(r.get("coarse") or 0))
    if self.stack == "microvm":
      kid = self.p["fixed"]
      ds = [d for i in range(1, self.p["fixed_n"] + 1) for d in [self.done_ok(f"{kid}-{self.stack}-{i}")] if d]
      ms = [night.summary(d) for d in ds]
      cons = self.constraints(self.objective)
      ok = len(ms) == self.p["fixed_n"]
      if not ok:
        why.append(f"only {len(ms)}/{self.p['fixed_n']} {kid} fixed-rate trials finished OK")
      if ms:
        r = statistics.mean(m["resume_warm_p90_ms"] for m in ms)
        s = statistics.mean(m["suspend_p90_ms"] for m in ms)
        gain = 100 * (v0r - r) / v0r
        ds_pct = 100 * (s - v0s) / v0s
        why.append(f"resume p90 mean {r:.1f} vs v000 {v0r:.1f} ms: {gain:+.1f}% better (need >= {self.p['min_gain_pct']}%)")
        why.append(f"suspend p90 mean {s:.1f} vs v000 {v0s:.1f} ms: {ds_pct:+.1f}% (need <= +{self.p['suspend_tol_pct']}%)")
        ok &= gain >= self.p["min_gain_pct"] and ds_pct <= self.p["suspend_tol_pct"]
        for d, m in zip(ds, ms):
          v = self.violations(m, cons)
          if v:
            why.append(f"{os.path.basename(d)} misses Phase 3 constraints: {v}")
            ok = False
      c = ceiling(ramps.get(kid, {}))
      why.append(f"{kid} ramp ceiling {c:g}/s vs v000 {ceil0:g}/s (need >=)")
      ok &= c >= ceil0
      return ("FOLD k010 into the microVM base (awaiting your approval)" if ok else
              "NO FOLD: keep the microVM defaults"), why
    best, bc = "", 0.0
    for kid, r in ramps.items():
      c = ceiling(r)
      why.append(f"{kid} ceiling {c:g}/s (coarse {r.get('coarse')}, fine {r.get('fine')}) vs v000 {ceil0:g}/s")
      if c > ceil0 and c > bc and not r.get("leaked") and not r.get("oom") and not r.get("crashed"):
        best, bc = kid, c
    return ((f"FOLD {best} into the gVisor base (ceiling {bc:g}/s > {ceil0:g}/s; awaiting your approval)" if best
             else "NO FOLD: close Study K on gVisor as \"keep defaults\""), why)

  def write_report(self) -> str:
    st = self.stack
    v0 = self.v000()
    L = [f"# Finish Study K: stack-{st}", "",
         f"_Written {self.now().strftime('%Y-%m-%d %H:%M UTC')} by .apo/scripts/finish_k.py._", "",
         f"**Stopped / finished because:** {self.stop_reason or 'all steps done'}", ""]
    if self.p["fixed"]:
      kid = self.p["fixed"]
      ds = [self.done_ok(f"{kid}-{st}-{i}") for i in range(1, self.p["fixed_n"] + 1)]
      ms = [night.summary(d) if d else {} for d in ds]
      L += [f"## {kid} x{self.p['fixed_n']} fixed rate vs v000 x3 (RATE from tunables.{st}.env)", "",
            f"Knobs: `{self.knobs.get(kid)}`", "",
            "| metric | v000 mean | " + " | ".join(os.path.basename(d) if d else "missing" for d in ds) +
            f" | {kid} mean | delta |", "|---|---|" + "---|" * len(ds) + "---|---|"]
      for k in FIXED_KEYS:
        a = [m.get(k) for m in v0 if isinstance(m.get(k), (int, float))]
        b = [m.get(k) for m in ms if isinstance(m.get(k), (int, float))]
        am = statistics.mean(a) if a else None
        bm = statistics.mean(b) if b else None
        dl = f"{100 * (bm - am) / am:+.1f}%" if am and bm is not None else ""
        L.append(f"| {k} | {round(am, 2) if am is not None else None} | " +
                 " | ".join(str(m.get(k)) for m in ms) + f" | {round(bm, 2) if bm is not None else None} | {dl} |")
      cons = self.constraints(self.objective)
      L += ["", "Phase 3 constraints (" + ", ".join(f"{c['metric']} {'>=' if 'min' in c else '<='} "
                                                    f"{c.get('min', c.get('max'))}" for c in cons) + "):"]
      for d, m in zip(ds, ms):
        if d:
          L.append(f"- {os.path.basename(d)}: {self.violations(m, cons) or 'all hold'}")
    L += ["", "## Ramp ceilings vs v000", "",
          "| knob set | coarse ceiling /s | fine ceiling /s | leaked | Substrate OOMs | crashed | snapshot-plugin RSS MiB | dirs |",
          "|---|---|---|---|---|---|---|---|"]
    for r in self.ramp_rows():
      L.append(f"| {r['set']} | {r['coarse']} | {r['fine']} | {r['leaked']} | {r['oom']} | {r['crashed']} | {r['rss']} | {r['dirs']} |")
    for kid in self.p["ramps"]:
      L.append(f"\n{kid} knobs: `{self.knobs.get(kid)}`")
      fd = self.done_ok(f"fineramp-{kid}-{st}")
      if fd:
        rr = night.agentsim_result(fd)
        lv = ", ".join(f"{x.get('target_per_s')}/s {x.get('verdict', '')[:20]}" for x in rr.get("levels") or [])
        L.append(f"fine levels ({os.path.basename(fd)}): {lv}")
    verdict, why = self.verdict()
    L += ["", "## Verdict", "", f"**{verdict}**", ""] + [f"- {w}" for w in why]
    L += ["", "Nothing was folded; folding needs your approval."]
    text = "\n".join(L) + "\n"
    for path in (os.path.join(RES, f"finishk-report-{st}.md"), os.path.join(BRAIN, f"finishk_report_{st}.md")):
      with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    self.log(f"finish-K report written: {os.path.join(BRAIN, f'finishk_report_{st}.md')}; verdict: {verdict}")
    return text


def main() -> int:
  ap = argparse.ArgumentParser()
  ap.add_argument("--stack", choices=sorted(PLAN), required=True)
  ap.add_argument("--dry-run", action="store_true")
  ap.add_argument("--report-only", action="store_true", help="only rewrite the report from existing results")
  ap.add_argument("--plan", choices=("finishk", "prephase4"), default="finishk")
  a = ap.parse_args()
  f = FinishK(a.stack, a.dry_run)
  if a.plan == "prephase4":
    f.log_path = os.path.join(RES, f"prephase4-{a.stack}.log")
    f.state_path = os.path.join(RES, f"prephase4-{a.stack}.state.json")
  if a.report_only:
    print(f.write_prephase4() if a.plan == "prephase4" else f.write_report())
    return 0
  if a.plan == "prephase4":
    f.run_prephase4()
  else:
    f.run()
  if a.dry_run:
    print(f"\nDRY RUN PLAN for stack-{a.stack} (nothing launched):")
    for i, step in enumerate(f.plan, 1):
      print(f"  {i}. {step}")
  return 0


if __name__ == "__main__":
  sys.exit(main())
