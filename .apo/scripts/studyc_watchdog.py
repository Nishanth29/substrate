#!/usr/bin/env python3
"""Study C overnight watchdog: logs both stacks every minute and enforces the stop times.

Usage: studyc_watchdog.py --stop 2026-10-12T15:00Z --hard-stop 2026-10-12T15:30Z

- Never launches trials and never resizes node pools (metal nodes stay up).
- The drivers (studyc_night.py) launch nothing after --stop themselves.
- At --hard-stop: SIGTERM, once, to every running run_experiment.py. The harness turns that into a
  clean stop (agentsim Job deleted, workers to 0, actors purged, lever flags cleared, leak check),
  error.json phase STOPPED.
- 15 min later, anything still running gets SIGKILL, and each stack is cleaned by hand with its own
  kubeconfig and context: agentsim Jobs deleted, WorkerPool to 0.
- Once no driver or trial is left (or 25 min after the hard stop), it rewrites both morning
  reports and the summary, then exits.
Log: .apo/results/studyc-watchdog.log.
"""

import argparse
import glob
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import studyc  # noqa: E402  pylint: disable=wrong-import-position

ROOT = "/usr/local/google/home/nishanthkotla/git-repos/substrate-apo"
RES = os.path.join(ROOT, ".apo", "results")
LOG = os.path.join(RES, "studyc-watchdog.log")
SCRIPTS = os.path.join(ROOT, ".apo", "scripts")
STACKS = {
    "microvm": {"kubeconfig": os.path.join(ROOT, ".apo", ".kubeconfig-substrate-metal"),
                "context": "gke_nishanth-gke-dev_us-east4-c_substrate-metal", "cluster": "substrate-metal"},
    "gvisor": {"kubeconfig": os.path.join(ROOT, ".apo", ".kubeconfig-apo-gvisor"),
               "context": "gke_nishanth-gke-dev_us-central1-a_apo-gvisor", "cluster": "apo-gvisor"},
}


def now() -> datetime:
  return datetime.now(timezone.utc)


def log(msg: str) -> None:
  line = f"{now():%Y-%m-%dT%H:%M:%SZ} [watchdog] {msg}"
  print(line, flush=True)
  with open(LOG, "a", encoding="utf-8") as f:
    f.write(line + "\n")


def procs() -> List[Dict]:
  out = []
  for p in glob.glob("/proc/[0-9]*/cmdline"):
    try:
      pid = int(p.split("/")[2])
      cmd = open(p, "rb").read().replace(b"\0", b" ").decode(errors="replace")
      env = open(f"/proc/{pid}/environ", "rb").read().split(b"\0")
    except (OSError, ValueError):
      continue
    cluster = next((e.split(b"=", 1)[1].decode() for e in env if e.startswith(b"CLUSTER_NAME=")), "")
    out.append({"pid": pid, "cmd": cmd, "cluster": cluster})
  return out


def trials(ps: List[Dict]) -> List[Dict]:
  return [p for p in ps if "/.apo/run_experiment.py" in p["cmd"] and p["cmd"].startswith("python3 ")]


def drivers(ps: List[Dict]) -> List[Dict]:
  return [p for p in ps if "studyc_night.py" in p["cmd"] and "--stack" in p["cmd"]
          and "--report-only" not in p["cmd"] and p["pid"] != os.getpid()]


def state_line(ps: List[Dict]) -> str:
  parts = []
  for stack, cfg in STACKS.items():
    st = studyc.night.load_json(os.path.join(RES, f"studyc-night-{stack}.state.json"))
    alive = any(f"--stack {stack}" in d["cmd"] for d in drivers(ps))
    running = [t["pid"] for t in trials(ps) if t["cluster"] == cfg["cluster"]]
    parts.append(f"{stack}: driver {'up' if alive else 'down'}, step {st.get('step', '?')} "
                 f"{st.get('trial', '')}, trial pids {running or 'none'}")
  return "; ".join(parts)


def kubectl(stack: str, *args: str) -> str:
  cfg = STACKS[stack]
  r = subprocess.run(["kubectl", "--context", cfg["context"], *args], capture_output=True, text=True,
                     env=dict(os.environ, KUBECONFIG=cfg["kubeconfig"]), check=False, timeout=600)
  return (r.stdout + r.stderr).strip()


def hand_cleanup(stack: str) -> None:
  log(f"{stack}: cleanup by hand: " +
      kubectl(stack, "-n", "agent-sim", "delete", "job", "-l", "app=agentsim", "--ignore-not-found",
              "--wait=true", "--timeout=5m")[-200:] + " | " +
      kubectl(stack, "-n", "benchmark-workloads", "patch", "workerpool", "benchmark-ateom", "--type=merge",
              "-p", json.dumps({"spec": {"replicas": 0}}))[-200:])


def reports() -> None:
  for stack in STACKS:
    r = subprocess.run([sys.executable, os.path.join(SCRIPTS, "studyc_night.py"), "--stack", stack,
                        "--report-only"], capture_output=True, text=True, check=False)
    log(f"{stack} report: exit {r.returncode}")
  r = subprocess.run([sys.executable, os.path.join(SCRIPTS, "studyc_night.py"), "--summary"],
                     capture_output=True, text=True, check=False)
  log(f"summary: exit {r.returncode}")


def main() -> int:
  ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  ap.add_argument("--stop", type=studyc.parse_stop, required=True)
  ap.add_argument("--hard-stop", type=studyc.parse_stop, required=True)
  ap.add_argument("--interval", type=int, default=60)
  a = ap.parse_args()
  kill_at, end_at = a.hard_stop + timedelta(minutes=15), a.hard_stop + timedelta(minutes=25)
  log(f"start (pid {os.getpid()}): no new trials after {a.stop:%H:%M}, SIGTERM at {a.hard_stop:%H:%M}, "
      f"SIGKILL + hand cleanup at {kill_at:%H:%M}, reports by {end_at:%H:%M} UTC")
  termed, killed, last = False, False, 0.0
  while True:
    ps = procs()
    if time.time() - last >= 300 or now() >= a.stop:
      log(state_line(ps))
      last = time.time()
    if now() >= a.hard_stop and not termed:
      for t in trials(ps):
        log(f"hard stop: SIGTERM run_experiment pid {t['pid']} ({t['cluster']})")
        try:
          os.kill(t["pid"], signal.SIGTERM)
        except OSError as e:
          log(f"  SIGTERM failed: {e}")
      termed = True
    if now() >= kill_at and not killed:
      left = trials(ps)
      for t in left:
        log(f"still running 15 min after SIGTERM: SIGKILL pid {t['pid']} ({t['cluster']})")
        try:
          os.kill(t["pid"], signal.SIGKILL)
        except OSError:
          pass
      for stack, cfg in STACKS.items():
        if any(t["cluster"] == cfg["cluster"] for t in left):
          hand_cleanup(stack)
      killed = True
    if now() >= a.hard_stop and ((not trials(ps) and not drivers(ps)) or now() >= end_at):
      log(f"done: {state_line(ps)}")
      reports()
      log("exit; nodes left up")
      return 0
    time.sleep(a.interval)


if __name__ == "__main__":
  sys.exit(main())
