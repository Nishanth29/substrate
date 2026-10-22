#!/usr/bin/env python3
"""Unattended night driver for one APO stack: finish Phase 2, gate, then Study K.

Usage: night.py --stack microvm|gvisor [--dry-run] [--adopt-trial <results dir name>]

Adopts the stack's already-running job (waits for it, never restarts it), then runs
one trial at a time on that stack's cluster with that stack's env (own kubeconfig
and gcloud config via set-env.sh). Appends to .apo/results/night-<stack>.log, keeps
.apo/results/night-<stack>.state.json for the watchdog, and writes a morning report.
"""

import argparse
import glob
import json
import math
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import yaml

ROOT = "/usr/local/google/home/nishanthkotla/git-repos/substrate-apo"
GIT_REPOS = os.path.dirname(ROOT)
RES = os.path.join(ROOT, ".apo", "results")
BRAIN = ("/usr/local/google/home/nishanthkotla/.gemini/jetski/brain/"
         "1b60d1b5-e48a-4574-8172-89f49dfd07b3")
APO_BIN = os.path.expanduser("~/bin/apo")
PROJECT = "nishanth-gke-dev"

# 08:30 / 09:00 PDT on Oct 11, 2026.
STOP_LAUNCH = datetime(2026, 10, 11, 15, 30, tzinfo=timezone.utc)
HARD_STOP = datetime(2026, 10, 11, 16, 0, tzinfo=timezone.utc)

STACKS = {
    "microvm": {
        "env": {"CLUSTER_NAME": "substrate-metal", "COMPUTE_ZONE": "us-east4-c",
                "COMPUTE_REGION": "us-east4", "SANDBOX_CLASS": "microvm",
                "BUCKET_NAME": "snapshot-substrate-metal-nishanth-gke-dev",
                "WORKER_NODE_POOL": "microvm-metal", "ID": "stack-microvm"},
        "mig": "gke-substrate-metal-microvm-metal", "zone": "us-east4-c",
        # The running v000 batch (3 trials at RATE=3); its trials count toward v000 x3.
        "adopt": "run_batch.sh v000-microvm",
        "ramp_dir": "ramp-20261011-055118",
    },
    "gvisor": {
        "env": {"CLUSTER_NAME": "apo-gvisor", "COMPUTE_ZONE": "us-central1-a",
                "COMPUTE_REGION": "us-central1", "SANDBOX_CLASS": "gvisor",
                "BUCKET_NAME": "snapshot-apo-gvisor-nishanth-gke-dev",
                "WORKER_NODE_POOL": "apo-metal", "ID": "stack-gvisor"},
        "mig": "gke-apo-gvisor-apo-metal", "zone": "us-central1-a",
        # The running ramp; RATE for v000 comes from its ceiling.
        "adopt": "ramp-gvisor-20261011-062131",
        "ramp_dir": "ramp-gvisor-20261011-062131",
    },
}

STUDY_KNOBS = ["ATE_GCS_CLIENT_POOL", "ATE_GCS_RANGE_BYTES", "ATE_GCS_RANGE_INFLIGHT",
               "ATE_ZSTD_CHUNK_BYTES", "ATE_ZSTD_DECODER_CONCURRENCY", "NODE_TMPFS_ACTORS",
               "NODE_NEIGH_GC_THRESH3"]
V000_TARGET, V000_MAX_ATTEMPTS = 3, 5
# Plain main = the single base (upstream/main at d1b2371d); there is no apo/base branch.
BASE_REV = "d1b2371d"
STUDY_MAX_INFRA_FAILS = 3  # consecutive Study K trials with error.json (not constraint misses)


class Night:
  def __init__(self, stack: str, dry_run: bool):
    self.stack, self.cfg, self.dry = stack, dict(STACKS[stack]), dry_run
    self.log_path = os.path.join(RES, f"night-{stack}.log")
    self.state_path = os.path.join(RES, f"night-{stack}.state.json")
    self.state: Dict = {"stack": stack, "step": "start", "trial": "", "last_error": ""}
    self.base_wt = os.path.join(GIT_REPOS, f"substrate-apo-base-{stack}")
    self.study_wt = os.path.join(GIT_REPOS, f"substrate-apo-studyk-{stack}")
    self.plan: List[str] = []
    self.gate: Dict = {}
    self.phase3: Dict = {}
    self.study_rows: List[Dict] = []
    self.stop_reason = ""

  # --- bookkeeping -------------------------------------------------------------

  def log(self, msg: str) -> None:
    line = f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} [{self.stack}] {msg}"
    print(line, flush=True)
    if not self.dry:
      with open(self.log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

  def set_state(self, **kw) -> None:
    self.state.update(kw, updated=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    if not self.dry:
      with open(self.state_path, "w", encoding="utf-8") as f:
        json.dump(self.state, f, indent=2)

  def env(self) -> Dict[str, str]:
    e = dict(os.environ)
    for k in ("KUBECONFIG", "CLOUDSDK_CONFIG", "KUBECTL_CONTEXT"):  # set-env.sh sets them
      e.pop(k, None)
    e.update(self.cfg["env"])
    return e

  @staticmethod
  def now() -> datetime:
    return datetime.now(timezone.utc)

  # --- processes and trials ---------------------------------------------------

  @staticmethod
  def pids_matching(pattern: str) -> List[int]:
    """Pids whose command line contains pattern, except this driver's own tree and wrappers.

    The driver, its ancestors (the tmux wrapper's command line carries --adopt-trial <dir>)
    and its descendants never count, nor does any night_* script or tmux process.
    """
    me = os.getpid()
    procs: Dict[int, Tuple[int, str]] = {}
    for p in glob.glob("/proc/[0-9]*/cmdline"):
      try:
        pid = int(p.split("/")[2])
        cmd = open(p, "rb").read().replace(b"\0", b" ").decode(errors="replace")
        stat = open(f"/proc/{pid}/stat", encoding="utf-8", errors="replace").read()
        ppid = int(stat.rsplit(")", 1)[1].split()[1])
      except (OSError, ValueError, IndexError):
        continue
      procs[pid] = (ppid, cmd)
    own = {me}
    pid = me
    while pid in procs and procs[pid][0] > 1:  # ancestors
      pid = procs[pid][0]
      own.add(pid)
    def descends(pid: int) -> bool:
      seen = set()
      while pid in procs and pid not in seen and pid > 1:
        if pid == me:
          return True
        seen.add(pid)
        pid = procs[pid][0]
      return False
    out = []
    for pid, (_, cmd) in procs.items():
      if pid in own or descends(pid) or pattern not in cmd:
        continue
      if "night.py" in cmd or "night_" in cmd or "tmux" in cmd:
        continue
      out.append(pid)
    return out

  def wait_adopted(self) -> None:
    pat = self.cfg["adopt"]
    pids = self.pids_matching(pat)
    if self.dry:
      self.plan.append(f"adopt: wait for running job '{pat}' (pids now: {pids or 'none'})")
      return
    self.set_state(step="adopt-wait", trial=pat)
    self.log(f"adopting running job '{pat}' (pids {pids or 'none'}); waiting for it to finish")
    while self.pids_matching(pat):
      time.sleep(60)
    self.log(f"adopted job '{pat}' finished")
    if self.cfg.get("adopt_trial"):
      self.mark_adopted_done(os.path.join(RES, self.cfg["adopt_trial"]))

  def mark_adopted_done(self, d: str) -> None:
    """DONE for a trial launched by an earlier driver: EXIT=0 iff summary.json and no error.json."""
    if os.path.exists(os.path.join(d, "DONE")):
      return
    ok = os.path.exists(os.path.join(d, "summary.json")) and not os.path.exists(os.path.join(d, "error.json"))
    with open(os.path.join(d, "DONE"), "w", encoding="utf-8") as f:
      f.write(f"EXIT={0 if ok else 1}\n")
    err = load_json(os.path.join(d, "error.json"))
    self.log(f"adopted trial {os.path.basename(d)} done (inferred EXIT={0 if ok else 1})"
             + (f" error={err.get('phase')}" if err else ""))

  def run_trial(self, label: str, workdir: str, args: List[str]) -> Tuple[Optional[str], int]:
    """One trial on this stack's cluster; None if the launch window has closed."""
    if self.now() >= STOP_LAUNCH:
      self.stop_reason = self.stop_reason or "08:30 PDT: no new trials"
      self.log(f"not launching {label}: {self.stop_reason}")
      return None, -1
    d = os.path.join(RES, f"{label}-{self.now().strftime('%Y%m%d-%H%M%S')}")
    if self.dry:
      self.plan.append(f"trial {label}: {workdir} .apo/build.sh + run_experiment.sh {' '.join(args)}")
      return d, 0
    os.makedirs(d, exist_ok=True)
    self.set_state(step="trial", trial=os.path.basename(d))
    self.log(f"trial start {label} -> {d} (workdir {workdir}, args {args})")
    q = " ".join(args)
    cmd = (f"cd {workdir} && .apo/build.sh {d} > {d}/build_driver.log 2>&1 && "
           f".apo/run_experiment.sh {d} {q} > {d}/driver.log 2>&1")
    p = subprocess.Popen(["bash", "-c", cmd], env=self.env(), stdin=subprocess.DEVNULL,
                         start_new_session=True)
    rc = p.wait()
    with open(os.path.join(d, "DONE"), "w", encoding="utf-8") as f:
      f.write(f"EXIT={rc}\n")
    err = load_json(os.path.join(d, "error.json"))
    if rc != 0:
      self.set_state(last_error=f"{label}: {err.get('phase', 'build?')}: {err.get('message', '')[:200]}")
    self.log(f"trial end {label} EXIT={rc}" + (f" error={err.get('phase')}" if err else ""))
    return d, rc

  # --- metrics ------------------------------------------------------------------

  def constraints(self, objective_path: str = "") -> List[Dict]:
    fm = read_frontmatter(objective_path or os.path.join(ROOT, ".apo", "objective.md"))[0]
    return (fm.get("optimization") or {}).get("constraints") or []

  @staticmethod
  def violations(m: Dict, constraints: List[Dict]) -> List[str]:
    bad = []
    for c in constraints:
      v = m.get(c["metric"])
      if v is None:
        bad.append(f"{c['metric']} missing")
      elif "min" in c and v < c["min"]:
        bad.append(f"{c['metric']}={v} < {c['min']}")
      elif "max" in c and v > c["max"]:
        bad.append(f"{c['metric']}={v} > {c['max']}")
    return bad

  def v000_dirs(self) -> List[str]:
    return sorted(glob.glob(os.path.join(RES, f"v000-{self.stack}-*")),
                  key=lambda p: os.path.getmtime(p))

  @staticmethod
  def finished_ok(d: str) -> bool:
    done = open(os.path.join(d, "DONE")).read() if os.path.exists(os.path.join(d, "DONE")) else ""
    return "EXIT=0" in done and os.path.exists(os.path.join(d, "summary.json"))

  # --- steps --------------------------------------------------------------------

  def gvisor_rate(self) -> bool:
    d = os.path.join(RES, self.cfg["ramp_dir"])
    m = summary(d)
    ceiling = m.get("ceiling_per_s")
    if self.dry:
      self.plan.append(f"write tunables.gvisor.env RATE = floor(0.8 x ceiling) "
                       f"(ramp {self.cfg['ramp_dir']}: ceiling now {ceiling}) and commit it")
      return True
    if not ceiling:
      self.stop_reason = f"gVisor ramp gave no ceiling ({d}: {load_json(os.path.join(d, 'error.json'))})"
      return False
    rate = max(1, math.floor(0.8 * float(ceiling)))
    path = os.path.join(ROOT, ".apo", "manifests", "tunables.gvisor.env")
    text = ("# Per-stack overrides for stack-gvisor (sourced after tunables.env by "
            "run_experiment.py).\n"
            f"# RATE = floor(0.8 x ceiling): ceiling {ceiling}/s on main + Aditya at AWAKE=500\n"
            f"# ({self.cfg['ramp_dir']}).\n"
            f"export RATE={rate}\n")
    committed = subprocess.run(["git", "-C", ROOT, "show", "HEAD:.apo/manifests/tunables.gvisor.env"],
                               capture_output=True, text=True).stdout
    if committed == text:
      self.log(f"gVisor ceiling {ceiling}/s -> RATE={rate}; tunables.gvisor.env already committed")
      return True
    with open(path, "w", encoding="utf-8") as f:
      f.write(text)
    git(ROOT, "add", ".apo/manifests/tunables.gvisor.env")
    git(ROOT, "commit", "-q", "--amend", "--no-edit")
    self.log(f"gVisor ceiling {ceiling}/s -> RATE={rate}; committed tunables.gvisor.env "
             f"({git(ROOT, 'rev-parse', '--short', 'HEAD')})")
    return True

  def finish_v000(self) -> List[str]:
    dirs = self.v000_dirs()
    passed = [d for d in dirs if self.finished_ok(d)]
    attempts = len(dirs)
    if self.dry:
      running = bool(self.pids_matching(self.cfg["adopt"])) and self.stack == "microvm"
      self.plan.append(f"v000: {len(passed)} finished OK so far "
                       f"({[os.path.basename(d) for d in passed]})"
                       + ("; the adopted batch runs the rest of its 3 trials itself, then this script "
                          "counts again and runs only what is missing" if running else "")
                       + f" (target {V000_TARGET}, max {V000_MAX_ATTEMPTS} attempts incl. existing)")
      if not running:
        for i in range(len(passed), V000_TARGET):
          self.run_trial(f"v000-{self.stack}-{i + 1}", ROOT, [])
      return passed
    self.log(f"v000: {len(passed)}/{V000_TARGET} finished OK, {attempts} attempts so far")
    while len(passed) < V000_TARGET and attempts < V000_MAX_ATTEMPTS:
      d, rc = self.run_trial(f"v000-{self.stack}-{attempts + 1}", ROOT, [])
      if d is None:
        break
      attempts += 1
      if rc == 0 and self.finished_ok(d):
        passed.append(d)
    return passed

  def plain_main(self) -> Optional[str]:
    if self.dry:
      self.plan.append(f"plain main x1: worktree {self.base_wt} at {BASE_REV} (detached) + a copy "
                       f"of .apo (no results/scratch/configs), same cluster/RATE/AWAKE")
      self.run_trial(f"main-{self.stack}-1", self.base_wt, [])
      return None
    if not os.path.isdir(self.base_wt):
      git(ROOT, "worktree", "add", "--detach", self.base_wt, BASE_REV)
    copy_apo(os.path.join(ROOT, ".apo"), os.path.join(self.base_wt, ".apo"))
    self.log(f"plain main worktree {self.base_wt} at {git(self.base_wt, 'rev-parse', '--short', 'HEAD')}")
    d, _ = self.run_trial(f"main-{self.stack}-1", self.base_wt, [])
    return d

  def reparse_oom(self, dirs: List[str]) -> None:
    """Trials run before the ate-only OOM rule: fetch their kernel OOM lines and re-summarize."""
    for d in dirs:
      if os.path.exists(os.path.join(d, "node_oom.json")) or not os.path.exists(os.path.join(d, "summary.json")):
        continue
      cmd = f"cd {ROOT} && .apo/run_experiment.sh {d} --parse-only --fetch-oom > {d}/reparse_oom.log 2>&1"
      rc = subprocess.run(["bash", "-c", cmd], env=self.env(), stdin=subprocess.DEVNULL).returncode
      m = summary(d)
      self.log(f"re-parsed {os.path.basename(d)} with the ate-only OOM rule (rc={rc}): "
               f"node_oom_events={m.get('node_oom_events')} node_oom_other={m.get('node_oom_other')}")

  def evaluate_gate(self, passed: List[str]) -> bool:
    g: Dict = {"trials": [os.path.basename(d) for d in passed], "reasons": []}
    ms = [summary(d) for d in passed]
    # Read at gate time, so it can be changed while the night runs (default: strict).
    gate_cfg = load_json(os.path.join(ROOT, ".apo", "scripts", "night_gate.json"))
    skip = set(gate_cfg.get("exclude_constraints") or [])
    g["excluded_constraints"] = sorted(skip)
    # Per-metric bounds for the gate only, e.g. {"error_refusal_pct": {"max": 5}} (the spec limit).
    over = gate_cfg.get("constraint_overrides") or {}
    g["constraint_overrides"] = over
    cons = [dict(c, **over.get(c["metric"], {})) for c in self.constraints() if c["metric"] not in skip]
    if len(passed) < V000_TARGET:
      g["reasons"].append(f"only {len(passed)}/{V000_TARGET} v000 trials finished OK")
    for d, m in zip(passed, ms):
      v = self.violations(m, cons)
      if v:
        g["reasons"].append(f"{os.path.basename(d)} misses constraints: {v}")
      for k in ("leaked_objects", "node_oom_events", "crashed_actors"):
        if m.get(k) not in (0, 0.0):
          g["reasons"].append(f"{os.path.basename(d)} {k}={m.get(k)}")
    p90 = [m.get("resume_warm_p90_ms") for m in ms if m.get("resume_warm_p90_ms")]
    g["cv_resume_p90_pct"] = cv(p90)
    s90 = [m.get("suspend_p90_ms") for m in ms if m.get("suspend_p90_ms")]
    g["cv_suspend_p90_pct"] = cv(s90)
    if g["cv_resume_p90_pct"] is None or g["cv_resume_p90_pct"] >= 10:
      g["reasons"].append(f"resume_warm_p90 CV {g['cv_resume_p90_pct']}% (need < 10%)")
    # snapshot-plugin RSS growth is report-only: logged and in the morning report, never a gate reason.
    rss = [m.get("snapshot_plugin_rss_mib") for m in ms]
    g["snapshot_plugin_rss_mib"] = rss
    g["rss_notes"] = rss_notes(rss)
    for n in g["rss_notes"]:
      self.log(f"RSS (report only): {n}")
    g["passed"] = not g["reasons"]
    self.gate = g
    self.log(f"GATE {'PASS' if g['passed'] else 'FAIL'}: cv_resume={g['cv_resume_p90_pct']}% "
             f"cv_suspend={g['cv_suspend_p90_pct']}% rss={rss} excluded={g['excluded_constraints']} "
             f"overrides={over} "
             f"reasons={g['reasons']}")
    return g["passed"]

  def phase3_config(self, passed: List[str]) -> Dict:
    ms = [summary(d) for d in passed]
    def thresholds(c):
      c = c if c is not None else 10.0
      return {"min_improvement_pct": round(max(2.3 * c / math.sqrt(3), 3.0), 2),
              "noise_tolerance_pct": round(max(c, 2.0), 2)}
    worst_drain = max([(m.get("drain_s_max") or 0) for m in ms], default=0)
    worst_backlog = max([(m.get("backlog_growth_excess") or 0) for m in ms], default=0)
    worst_err = max([(m.get("error_refusal_pct") or 0) for m in ms], default=0)
    cfg = {
        "resume_warm_p90_ms": thresholds(self.gate.get("cv_resume_p90_pct")),
        "suspend_p90_ms": thresholds(self.gate.get("cv_suspend_p90_pct")),
        "constraints": {
            "drain_s_max": {"max": round(max(5.0, 2 * worst_drain), 2)},
            "leaked_objects": {"max": 5},
            "achieved_ratio": {"min": 0.97},
            "error_refusal_pct": {"max": round(min(5.0, max(0.5, 2 * worst_err)), 2)},
            "backlog_growth_excess": {"max": 0 if worst_backlog <= 0 else worst_backlog + 1},
        },
        "knobs": STUDY_KNOBS,
    }
    self.phase3 = cfg
    return cfg

  def setup_study(self, cfg: Dict, passed: List[str]) -> None:
    if not os.path.isdir(self.study_wt):
      git(ROOT, "worktree", "add", "-b", f"apo/studyk-{self.stack}", self.study_wt, "HEAD")
    tun = os.path.join(self.study_wt, ".apo", "manifests", "tunables.env")
    t = open(tun, encoding="utf-8").read()
    for k in STUDY_KNOBS:
      t = re.sub(rf"(# OPTUNA: name={k},.*?status=)locked", r"\1tuning", t)
    open(tun, "w", encoding="utf-8").write(t)
    obj = os.path.join(self.study_wt, ".apo", "objective.md")
    fm, body = read_frontmatter(obj)
    opt = fm["optimization"]
    for metric in opt["metrics"]:
      metric.update(cfg[metric["name"]])
    cons = {c["metric"]: c for c in opt["constraints"]}
    for name, bound in cfg["constraints"].items():
      cons.setdefault(name, {"metric": name}).clear()
      cons[name].update({"metric": name, **bound})
    opt["constraints"] = list(cons.values())
    opt["allowed_file_scope"] = ".apo/manifests/tunables.env\n"  # Study K: knobs only
    write_frontmatter(obj, fm, body)
    git(self.study_wt, "add", ".apo/manifests/tunables.env", ".apo/objective.md")
    git(self.study_wt, "commit", "-q", "-m", f"apo: Study K setup for stack-{self.stack} (knobs tuning, "
        "thresholds and constraints from v000)")
    for i, d in enumerate(passed, 1):
      self.apo_hpo("complete", f"v000-{i}", d)
    self.log(f"Study K set up in {self.study_wt} ({git(self.study_wt, 'rev-parse', '--short', 'HEAD')}); "
             f"v000 x{len(passed)} registered; config {json.dumps(cfg)}")

  def apo_hpo(self, mode: str, hid: str, results_dir: str = "") -> Dict:
    args = [APO_BIN, "hpo", "--workspace", self.study_wt, "--mode", mode, "--hypothesis-id", hid]
    if results_dir:
      args += ["--results-dir", results_dir]
    out = subprocess.run(args, cwd=self.study_wt, stdin=subprocess.DEVNULL, capture_output=True,
                         text=True, timeout=600, start_new_session=True)
    text = out.stdout or ""
    i = min([j for j in (text.find("["), text.find("{")) if j >= 0], default=-1)
    try:
      res = json.loads(text[i:]) if i >= 0 else {}
    except ValueError:
      res = {}
    if isinstance(res, list):
      res = res[0] if res else {}
    if out.returncode != 0 or res.get("status") == "FAILED":
      self.log(f"apo hpo {mode} {hid} rc={out.returncode}: {(out.stderr or text)[-300:]}")
    return res

  def study_k(self, passed: List[str]) -> None:
    cfg = self.phase3_config(passed)
    if self.dry:
      self.plan.append(f"Study K: worktree {self.study_wt} (branch apo/studyk-{self.stack}): 7 knobs "
                       f"status=tuning, scope .apo/manifests/tunables.env, thresholds/constraints from "
                       f"v000; register v000 x3; loop apo hpo suggest -> trial -> apo hpo complete until "
                       f"15:30 UTC (or {STUDY_MAX_INFRA_FAILS} infra failures in a row)")
      return
    self.setup_study(cfg, passed)
    base = statistics.mean(summary(d)["resume_warm_p90_ms"] for d in passed)
    base_s = statistics.mean(summary(d)["suspend_p90_ms"] for d in passed)
    cons = self.constraints(os.path.join(self.study_wt, ".apo", "objective.md"))
    n, infra_fails = 0, 0
    while self.now() < STOP_LAUNCH:
      n += 1
      hid = f"k{n:03d}"
      sug = self.apo_hpo("suggest", hid)
      params = sug.get("parameters") or {}
      if not params:
        self.stop_reason = f"apo hpo suggest gave no parameters ({sug})"
        break
      self.log(f"Study K {hid} params {params}")
      d, rc = self.run_trial(f"studyk-{self.stack}-{n:03d}", self.study_wt, [])
      if d is None:
        self.apo_hpo("stop", hid)
        break
      done = self.apo_hpo("complete", hid, d)
      m = summary(d)
      err = load_json(os.path.join(d, "error.json"))
      row = {"id": hid, "dir": os.path.basename(d), "params": params, "exit": rc,
             "status": done.get("status"), "error": err.get("phase", ""),
             "resume_warm_p90_ms": m.get("resume_warm_p90_ms"), "suspend_p90_ms": m.get("suspend_p90_ms"),
             "violations": self.violations(m, cons) if m else ["no summary"],
             "snapshot_plugin_rss_mib": m.get("snapshot_plugin_rss_mib")}
      if m.get("resume_warm_p90_ms"):
        row["d_resume_pct"] = round(100 * (m["resume_warm_p90_ms"] - base) / base, 1)
      if m.get("suspend_p90_ms"):
        row["d_suspend_pct"] = round(100 * (m["suspend_p90_ms"] - base_s) / base_s, 1)
      self.study_rows.append(row)
      json.dump(self.study_rows, open(os.path.join(RES, f"studyk-{self.stack}-rows.json"), "w"), indent=2)
      infra_fails = infra_fails + 1 if err else 0
      if infra_fails >= STUDY_MAX_INFRA_FAILS:
        self.stop_reason = f"{infra_fails} Study K trials in a row failed ({err.get('phase')})"
        break
    if not self.stop_reason and self.now() >= STOP_LAUNCH:
      self.stop_reason = "08:30 PDT: no new trials"

  # --- report -------------------------------------------------------------------

  def report(self) -> str:
    st = self.stack
    lines = [f"# Morning report: stack-{st}", "",
             f"_Written {self.now().strftime('%Y-%m-%d %H:%M UTC')} by .apo/scripts/night.py._", ""]
    lines += [f"**Stopped / finished because:** {self.stop_reason or 'all steps done'}", ""]
    # Ramp
    rd = os.path.join(RES, self.cfg["ramp_dir"])
    rr = agentsim_result(rd)
    rm = summary(rd)
    lines += ["## Ramp (main + Aditya, AWAKE=500)", "",
              f"`{self.cfg['ramp_dir']}`: ceiling_per_s **{rm.get('ceiling_per_s')}**, "
              f"spec_ceiling_per_s **{rm.get('spec_ceiling_per_s')}**", "",
              "| target/s | on time / wakes | resume p50 / p95 ms | suspend p95 ms | TTFE accept p95 | err+ref % | drain s | verdict |",
              "|---|---|---|---|---|---|---|---|"]
    for lv in (rr.get("levels") or []):
      w = (lv.get("resume_ms") or {}).get("warm") or {}
      t = ((lv.get("ttfe") or {}).get("accept") or {})
      lines.append(f"| {lv.get('target_per_s')} | {lv.get('on_time')}/{lv.get('wakes')} | {w.get('p50')} / {w.get('p95')} | "
                   f"{(lv.get('suspend_ms') or {}).get('p95')} | {t.get('p95')} | {lv.get('error_refusal_pct')} | "
                   f"{lv.get('drain_s')} | {lv.get('verdict')} |")
    # v000
    keys = ["resume_warm_p50_ms", "resume_warm_p90_ms", "resume_warm_p95_ms", "suspend_p90_ms", "suspend_p95_ms",
            "ttfe_accept_p95_ms", "achieved_ratio", "error_refusal_pct", "drain_s_max", "leaked_objects",
            "node_oom_events", "crashed_actors", "snapshot_plugin_rss_mib", "snapshot_plugin_peak_rss_mib",
            "atelet_rss_mib", "mem_per_actor_mib", "sandbox_procs", "node_mem_used_gib", "prewarm_s"]
    vd = self.v000_dirs()
    lines += ["", "## v000 (main + Aditya, fixed rate)", ""]
    if vd:
      lines += ["| metric | " + " | ".join(os.path.basename(d) + f" ({open(os.path.join(d, 'DONE')).read().strip() if os.path.exists(os.path.join(d, 'DONE')) else 'no DONE'})" for d in vd) + " |",
                "|---|" + "---|" * len(vd)]
      for k in keys:
        lines.append(f"| {k} | " + " | ".join(str(summary(d).get(k)) for d in vd) + " |")
    lines += ["", f"CV resume_warm_p90: **{self.gate.get('cv_resume_p90_pct')}%**, "
              f"CV suspend_p90: **{self.gate.get('cv_suspend_p90_pct')}%**"]
    # snapshot-plugin RSS per trial (report only, not a gate reason)
    lines += ["", "## snapshot-plugin RSS per trial (report only)", "",
              "| trial | snapshot-plugin RSS MiB (end) | peak MiB | atelet RSS MiB |", "|---|---|---|---|"]
    rd_m = summary(rd)
    lines.append(f"| {self.cfg['ramp_dir']} (ramp) | {rd_m.get('snapshot_plugin_rss_mib')} | "
                 f"{rd_m.get('snapshot_plugin_peak_rss_mib')} | {rd_m.get('atelet_rss_mib')} |")
    for d in vd:
      m = summary(d)
      lines.append(f"| {os.path.basename(d)} | {m.get('snapshot_plugin_rss_mib')} | "
                   f"{m.get('snapshot_plugin_peak_rss_mib')} | {m.get('atelet_rss_mib')} |")
    notes = self.gate.get("rss_notes") or rss_notes([summary(d).get("snapshot_plugin_rss_mib") for d in vd])
    lines += [""] + [f"- {n}" for n in notes]
    # what caused the worst drain in each v000 trial
    lines += ["", "## Drain cause per v000 trial (worst window)", ""]
    for d in vd:
      lines.append(f"- {os.path.basename(d)}: {drain_cause(d)}")
    # errors / refusals per v000 trial (the 503/504 retry tails)
    lines += ["", "## Errors and refusals per v000 trial (503/504 retry tails)", ""]
    for d in vd:
      lines.append(f"- {os.path.basename(d)}: {refusal_cause(d)}")
    # OOM kills: ate-related (gated) vs other (context, e.g. fluent-bit = logging pressure)
    lines += ["", "## Node OOM kills (ate-related are gated; other is context only)", "",
              "| trial | node_oom_events (ate) | node_oom_other | other tasks |", "|---|---|---|---|"]
    for d in [rd] + vd + sorted(glob.glob(os.path.join(RES, f"main-{st}-*"))):
      m = summary(d)
      oj = load_json(os.path.join(d, "node_oom.json"))
      lines.append(f"| {os.path.basename(d)} | {m.get('node_oom_events')} | {m.get('node_oom_other')} | "
                   f"{oj.get('other_tasks') or ''} |")
    fb = [os.path.basename(d) for d in vd
          if "fluent-bit" in (load_json(os.path.join(d, "node_oom.json")).get("other_tasks") or {})]
    if fb:
      lines.append(f"\nfluent-bit (GKE logging agent, kubepods memcg) was OOM-killed in {fb}: logging pressure "
                   "on the node, not Substrate. Not gated.")
    # main vs main + Aditya
    md = sorted(glob.glob(os.path.join(RES, f"main-{st}-*")))
    lines += ["", "## Plain main vs main + Aditya (same RATE / AWAKE)", ""]
    if md:
      mm = summary(md[-1])
      ok = [summary(d) for d in vd if self.finished_ok(d)]
      lines += ["| metric | plain main | main + Aditya (v000 mean) |", "|---|---|---|"]
      for k in keys[:12]:
        vals = [m.get(k) for m in ok if isinstance(m.get(k), (int, float))]
        lines.append(f"| {k} | {mm.get(k)} | {round(statistics.mean(vals), 2) if vals else None} |")
      if not mm:
        lines.append(f"\nplain main error: {load_json(os.path.join(md[-1], 'error.json'))}")
    else:
      lines.append("not run")
    # stage breakdown and leaks from the first OK v000
    ok_dirs = [d for d in vd if self.finished_ok(d)]
    if ok_dirs:
      m0 = summary(ok_dirs[0])
      lines += ["", f"## Stage breakdown ({os.path.basename(ok_dirs[0])}, p50 / p90 ms)", "",
                "| stage | p50 | p90 |", "|---|---|---|"]
      stages = sorted({k[6:-7] for k in m0 if k.startswith("stage_") and k.endswith("_p50_ms")},
                      key=lambda s: -(m0.get(f"stage_{s}_p50_ms") or 0))
      for s in stages[:14]:
        lines.append(f"| {s} | {m0.get(f'stage_{s}_p50_ms')} | {m0.get(f'stage_{s}_p90_ms')} |")
      lines += ["", "## Leaks (v000)", ""]
      for d in ok_dirs:
        lk = load_json(os.path.join(d, "leak.json"))
        lines.append(f"- {os.path.basename(d)}: leaked {lk.get('leaked', {}).get('leaked_objects')} "
                     f"(ate keys), actor dirs +{lk.get('leaked', {}).get('leaked_actor_entries')}")
      lines += ["", "## Provenance (v000)", ""]
      for d in ok_dirs:
        pv = load_json(os.path.join(d, "provenance.json"))
        lines.append(f"- {os.path.basename(d)}: workspace {pv.get('workspace_head', '')[:10]} "
                     f"dirty={pv.get('workspace_dirty')}, agentsim {pv.get('agentsim_commit', '')[:8]}, "
                     f"RATE={pv.get('effective', {}).get('RATE')} AWAKE={pv.get('effective', {}).get('AWAKE')}, "
                     f"{len(pv.get('images', {}))} images" if pv else f"- {os.path.basename(d)}: no provenance.json")
    # gate and Phase 3
    lines += ["", "## Gate", "", f"**{'PASS' if self.gate.get('passed') else 'FAIL'}**"]
    for r in self.gate.get("reasons", []):
      lines.append(f"- {r}")
    lines += ["", "## Phase 3 config applied", "", "```json", json.dumps(self.phase3, indent=2), "```"]
    # Study K
    lines += ["", f"## Study K trials ({len(self.study_rows)})", ""]
    if self.study_rows:
      lines += ["| id | " + " | ".join(STUDY_KNOBS) + " | resume p90 (Δ%) | suspend p90 (Δ%) | pass | note |",
                "|---|" + "---|" * (len(STUDY_KNOBS) + 4)]
      for r in self.study_rows:
        ok = not r["violations"] and not r["error"]
        lines.append(f"| {r['id']} | " + " | ".join(str(r['params'].get(k)) for k in STUDY_KNOBS) +
                     f" | {r['resume_warm_p90_ms']} ({r.get('d_resume_pct')}) | {r['suspend_p90_ms']} "
                     f"({r.get('d_suspend_pct')}) | {'yes' if ok else 'no'} | {r['error'] or '; '.join(r['violations'])[:80]} |")
      good = [r for r in self.study_rows if not r["violations"] and not r["error"] and r.get("d_resume_pct") is not None]
      if good:
        best = min(good, key=lambda r: r["d_resume_pct"])
        lines.append(f"\nBest vs v000 (resume p90): **{best['id']}** {best['d_resume_pct']}% "
                     f"(suspend {best.get('d_suspend_pct')}%), knobs {best['params']}. Not folded into the base.")
    # node state
    lines += ["", "## Node / pool state", "", "```", mig_state(self.cfg["mig"]), "```"]
    text = "\n".join(lines) + "\n"
    for path in (os.path.join(RES, f"morning-report-{st}.md"), os.path.join(BRAIN, f"morning_report_{st}.md")):
      with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    self.log(f"morning report written: {os.path.join(BRAIN, f'morning_report_{st}.md')}")
    add_findings(st)
    return text

  # --- main ---------------------------------------------------------------------

  def run(self) -> None:
    self.log(f"night driver start (dry_run={self.dry}, pid {os.getpid()})")
    try:
      self.wait_adopted()
      if self.stack == "gvisor":
        self.set_state(step="rate")
        if not self.gvisor_rate():
          raise StopStack()
      self.set_state(step="v000")
      passed = self.finish_v000()
      self.set_state(step="plain-main")
      self.plain_main()
      self.set_state(step="gate")
      if self.dry:
        self.plan.append("gate: 3/3 v000 pass all constraints; resume p90 CV < 10%; leaked/oom/crashed = 0; "
                         "constraints in night_gate.json exclude_constraints are skipped (read at gate time); "
                         "snapshot-plugin RSS growth is report only. FAIL -> stop, node idle, report")
        self.study_k(passed)
        return
      self.reparse_oom(passed + sorted(glob.glob(os.path.join(RES, f"main-{self.stack}-*"))))
      if not self.evaluate_gate(passed):
        self.stop_reason = self.stop_reason or "gate failed: " + "; ".join(self.gate["reasons"])
        raise StopStack()
      self.set_state(step="study-k")
      self.study_k(passed)
    except StopStack:
      pass
    except Exception as e:  # pylint: disable=broad-except
      self.stop_reason = f"driver error: {type(e).__name__}: {e}"
      self.log(self.stop_reason)
    finally:
      if not self.dry:
        self.set_state(step="done", trial="", stop_reason=self.stop_reason)
        try:
          self.report()
        except Exception as e:  # pylint: disable=broad-except
          self.log(f"report failed: {type(e).__name__}: {e}")
        self.log("night driver done; node left up and idle")


class StopStack(Exception):
  pass


# --- helpers ------------------------------------------------------------------


def load_json(path: str) -> Dict:
  try:
    with open(path, encoding="utf-8") as f:
      return json.load(f)
  except (OSError, ValueError):
    return {}


def summary(d: str) -> Dict:
  return load_json(os.path.join(d, "summary.json")).get("metrics") or {}


def agentsim_result(d: str) -> Dict:
  r: Dict = {}
  try:
    for line in open(os.path.join(d, "agentsim.log"), encoding="utf-8", errors="replace"):
      if line.startswith("AGENTSIM_RESULT_JSON "):
        r = json.loads(line[len("AGENTSIM_RESULT_JSON "):])
  except OSError:
    pass
  return r


def rss_notes(rss: List[Optional[float]]) -> List[str]:
  """Trial-over-trial snapshot-plugin RSS growth (> 20% = possible leak). Report only."""
  if not rss:
    return ["snapshot-plugin RSS: no trials"]
  if any(r is None for r in rss):
    return [f"snapshot-plugin RSS missing in some trials: {rss}"]
  out = []
  for a, b in zip(rss, rss[1:]):
    if a and b > 1.2 * a:
      out.append(f"snapshot-plugin RSS grew {a} -> {b} MiB (+{round(100 * (b - a) / a)}%, > 20%): possible leak")
  return out or [f"snapshot-plugin RSS within 20% trial over trial: {rss}"]


def _ts_ms(s: str) -> float:
  """RFC3339 with nanoseconds -> unix ms."""
  base, _, frac = s.rstrip("Z").partition(".")
  t = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
  return 1000 * (t + float("0." + (frac or "0")))


def drain_cause(d: str) -> str:
  """Which window had the worst drain, and which wakes were still running when it closed.

  agentsim's per-swap CSV has the swap start (unix_ms) and the wake time (first_req_ms);
  park times are only in the per-window percentiles, so if no wake ran past the close the
  drain is put on the parks.
  """
  r = agentsim_result(d)
  levels = [lv for lv in (r.get("levels") or []) if not lv.get("warmup")]
  if not levels:
    return "no agentsim result"
  lv = max(levels, key=lambda x: x.get("drain_s") or 0)
  starts: Dict[str, float] = {}
  header: List[str] = []
  rows: List[List[str]] = []
  try:
    for line in open(os.path.join(d, "agentsim.log"), encoding="utf-8", errors="replace"):
      if line.startswith("{") and '"msg":"swap window"' in line:
        try:
          j = json.loads(line)
          starts[j.get("tag", "")] = _ts_ms(j["time"])
        except (ValueError, KeyError):
          pass
      elif line.startswith("unix_ms,agent,kind"):
        header = line.strip().split(",")
      elif header and line[:1].isdigit():
        p = line.strip().split(",")
        if len(p) == len(header):
          rows.append(p)
  except OSError:
    return "no agentsim.log"
  tag = lv.get("tag", "")
  park = lv.get("suspend_ms") or {}
  head = (f"window {tag}: drain {lv.get('drain_s')} s, backlog {lv.get('backlog')}, "
          f"on time {lv.get('on_time')}/{lv.get('wakes')}, refusals {lv.get('refusals')}, "
          f"park p99 {park.get('p99')} ms")
  if tag not in starts or not header:
    return head + "; no per-swap data to attribute it"
  ws = starts[tag]
  close = ws + 1000 * float(lv.get("window_s") or 0)
  ix = {k: i for i, k in enumerate(header)}
  late = []
  for p in rows:
    try:
      t0, wake = float(p[ix["unix_ms"]]), float(p[ix["first_req_ms"]] or 0)
    except (ValueError, KeyError):
      continue
    if ws <= t0 < close and t0 + wake > close:
      late.append((t0 + wake - close, p))
  if not late:
    return head + "; no wake ran past the window close, so the drain was slow parks"
  late.sort(key=lambda x: -x[0])
  parts = []
  for over, p in late[:3]:
    parts.append(f"agent {p[ix['agent']]} wake started {round((close - float(p[ix['unix_ms']])) / 1000, 1)} s "
                 f"before close, took {float(p[ix['first_req_ms']]):.0f} ms with {p[ix['refusals']]} refusal(s) "
                 f"and {p[ix['errors']]} error(s), finished {round(over / 1000, 1)} s after close")
  return head + f"; slow wake(s) ({len(late)} past close): " + "; ".join(parts)


FINDINGS_START, FINDINGS_END = "<!-- gate-findings:start -->", "<!-- gate-findings:end -->"


def findings_section(stack: str) -> str:
  """Gate overrides in effect, and what passed only because of them (issues to investigate)."""
  gate_cfg = load_json(os.path.join(ROOT, ".apo", "scripts", "night_gate.json"))
  skip = gate_cfg.get("exclude_constraints") or []
  over = gate_cfg.get("constraint_overrides") or {}
  orig = {c["metric"]: c for c in read_frontmatter(os.path.join(ROOT, ".apo", "objective.md"))[0]
          .get("optimization", {}).get("constraints", [])}
  vd = sorted(glob.glob(os.path.join(RES, f"v000-{stack}-*")), key=os.path.getmtime)
  ms = {os.path.basename(d): summary(d) for d in vd if Night.finished_ok(d)}
  lines = [FINDINGS_START, "## Gate overrides in effect and issues to investigate", "",
           "_From `.apo/scripts/night_gate.json` (read at gate time). Anything below passed the gate "
           "only because of an override: treat it as an open issue, not a pass._", "",
           "| metric | objective.md | gate | v000 values |", "|---|---|---|---|"]
  def bound(c):
    return ", ".join(f"{k} {v}" for k, v in c.items() if k in ("min", "max")) or "-"
  for k in list(skip) + [k for k in over if k not in skip]:
    gate = "excluded" if k in skip else bound(over[k])
    vals = ", ".join(f"{n.split('-20')[0]}={m.get(k)}" for n, m in ms.items())
    lines.append(f"| {k} | {bound(orig.get(k, {}))} | {gate} | {vals or 'none yet'} |")
  issues = []
  for k in list(skip) + [k for k in over if k not in skip]:
    c = orig.get(k, {})
    for n, m in ms.items():
      v = Night.violations(m, [c]) if c else []
      if v and m.get(k) is not None:
        issues.append(f"- **{k}** in {n}: {m.get(k)} breaks the objective limit ({bound(c)}); passed only "
                      f"via the override ({'excluded' if k in skip else bound(over[k])}). Investigate "
                      "(503/504 retry tail).")
  rss = [(n, m.get("snapshot_plugin_rss_mib")) for n, m in ms.items()]
  for (n1, a), (n2, b) in zip(rss, rss[1:]):
    if a and b and b > 1.2 * a:
      issues.append(f"- **snapshot-plugin RSS** {n1} -> {n2}: {a} -> {b} MiB ({a / 1024:.1f} -> {b / 1024:.1f} GiB, "
                    f"+{round(100 * (b - a) / a)}%): possible leak. Report-only in the gate; investigate.")
  for n, m in ms.items():
    if m.get("node_oom_other"):
      oj = load_json(os.path.join(RES, n, "node_oom.json"))
      issues.append(f"- node_oom_other in {n}: {m.get('node_oom_other')} {oj.get('other_tasks') or ''} "
                    "(not Substrate; context only, e.g. fluent-bit = logging pressure)")
  lines += ["", "### Possible issues to investigate (not passes)", ""] + (issues or ["- none"])
  lines += ["", FINDINGS_END]
  return "\n".join(lines) + "\n"


def add_findings(stack: str) -> bool:
  """Puts the findings section into both morning report copies (replaces an older one)."""
  done = False
  for path in (os.path.join(RES, f"morning-report-{stack}.md"), os.path.join(BRAIN, f"morning_report_{stack}.md")):
    if not os.path.exists(path):
      continue
    text = open(path, encoding="utf-8").read()
    if FINDINGS_START in text:
      text = text[:text.index(FINDINGS_START)] + text[text.index(FINDINGS_END) + len(FINDINGS_END) + 1:]
    sec = findings_section(stack)
    anchor = "## Phase 3 config applied"
    text = text.replace(anchor, sec + "\n" + anchor, 1) if anchor in text else text + "\n" + sec
    with open(path, "w", encoding="utf-8") as f:
      f.write(text)
    done = True
  return done


def refusal_cause(d: str) -> str:
  """Per-window errors/refusals and the swaps that needed 503/504 retries (from the per-swap CSV)."""
  r = agentsim_result(d)
  levels = [lv for lv in (r.get("levels") or []) if not lv.get("warmup")]
  if not levels:
    return "no agentsim result"
  per = ", ".join(f"{lv.get('tag')} {lv.get('error_refusal_pct')}% ({lv.get('errors')} err / "
                  f"{lv.get('refusals')} ref)" for lv in levels)
  header: List[str] = []
  retried: List[Tuple[float, str, str, str]] = []
  try:
    for line in open(os.path.join(d, "agentsim.log"), encoding="utf-8", errors="replace"):
      if line.startswith("unix_ms,agent,kind"):
        header = line.strip().split(",")
      elif header and line[:1].isdigit():
        p = line.strip().split(",")
        if len(p) != len(header):
          continue
        ix = {k: i for i, k in enumerate(header)}
        try:
          ref, err = int(float(p[ix["refusals"]] or 0)), int(float(p[ix["errors"]] or 0))
          if ref or err:
            retried.append((float(p[ix["first_req_ms"]] or 0), p[ix["agent"]], str(ref), str(err)))
        except (ValueError, KeyError):
          continue
  except OSError:
    pass
  if not retried:
    return per + "; no swap needed a retry"
  ms = sorted(x[0] for x in retried)
  slow = sorted(retried, reverse=True)[:3]
  return (per + f"; {len(retried)} swaps hit 503/504 or errors (whole run), their wake took "
          f"p50 {ms[len(ms) // 2]:.0f} ms / max {ms[-1]:.0f} ms; slowest: "
          + ", ".join(f"agent {a} {t:.0f} ms ({rf} ref, {e} err)" for t, a, rf, e in slow))


def cv(vals: List[float]) -> Optional[float]:
  vals = [v for v in vals if v]
  if len(vals) < 2:
    return None
  return round(100 * statistics.stdev(vals) / statistics.mean(vals), 2)


def git(cwd: str, *args: str) -> str:
  out = subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True, check=True)
  return out.stdout.strip()


def read_frontmatter(path: str) -> Tuple[Dict, str]:
  text = open(path, encoding="utf-8").read()
  parts = text.split("---\n", 2)
  return yaml.safe_load(parts[1]) or {}, parts[2]


def write_frontmatter(path: str, fm: Dict, body: str) -> None:
  with open(path, "w", encoding="utf-8") as f:
    f.write("---\n" + yaml.safe_dump(fm, sort_keys=False, width=100) + "---\n" + body)


def copy_apo(src: str, dst: str) -> None:
  """The harness for a plain-main worktree: .apo without results, scratch or credentials."""
  skip = {"results", "scratch", ".worktrees", "__pycache__", "optuna_study.db"}
  for name in os.listdir(src):
    if name in skip or name.startswith((".kubeconfig", ".gcloud-config")) or (
        name.startswith("stack") and name.endswith(".env")):
      continue
    s, d = os.path.join(src, name), os.path.join(dst, name)
    if os.path.isdir(s):
      shutil.copytree(s, d, dirs_exist_ok=True)
    else:
      os.makedirs(dst, exist_ok=True)
      shutil.copy2(s, d)


def mig_state(name_prefix: str) -> str:
  out = subprocess.run(["gcloud", "compute", "instance-groups", "managed", "list", "--project", PROJECT,
                        f"--filter=name~{name_prefix}", "--format=table(name,zone.basename(),targetSize,size)"],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
  return (out.stdout or out.stderr).strip()


def main() -> int:
  ap = argparse.ArgumentParser()
  ap.add_argument("--stack", choices=sorted(STACKS), required=True)
  ap.add_argument("--dry-run", action="store_true")
  ap.add_argument("--adopt-trial", default="",
                  help="adopt this running trial (a results dir name) instead of the stack's default job; "
                       "writes its DONE when it ends")
  ap.add_argument("--findings", action="store_true",
                  help="only (re)write the gate-overrides/issues section into the stack's morning report")
  a = ap.parse_args()
  if a.findings:
    print(f"findings {'written' if add_findings(a.stack) else 'skipped (no report yet)'} for stack-{a.stack}")
    return 0
  n = Night(a.stack, a.dry_run)
  if a.adopt_trial:
    n.cfg["adopt"] = n.cfg["adopt_trial"] = a.adopt_trial
  signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
  n.run()
  if a.dry_run:
    print(f"\nDRY RUN PLAN for stack-{a.stack} (nothing launched):")
    for i, step in enumerate(n.plan, 1):
      print(f"  {i}. {step}")
  return 0


if __name__ == "__main__":
  sys.exit(main())
