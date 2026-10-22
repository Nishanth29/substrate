#!/usr/bin/env python3
"""Night watchdog: every 5 min log both stacks' state; at 09:00 PDT stop running trials.

Appends to .apo/results/night-watchdog.log. Never launches trials and never resizes
pools (metal nodes stay up). When a stack's morning report has no gate-findings section
(drivers started before it existed), it adds one with `night.py --findings`. At 16:00 UTC it sends SIGTERM once to every running
run_experiment.py; the harness turns that into a clean stop (cleanup, workers to 0,
actors purged, leak check) and writes error.json phase STOPPED.
"""

import glob
import json
import os
import signal
import subprocess
import time
from datetime import datetime, timezone

ROOT = "/usr/local/google/home/nishanthkotla/git-repos/substrate-apo"
RES = os.path.join(ROOT, ".apo", "results")
LOG = os.path.join(RES, "night-watchdog.log")
HARD_STOP = datetime(2026, 10, 11, 16, 0, tzinfo=timezone.utc)
END = datetime(2026, 10, 11, 16, 45, tzinfo=timezone.utc)
MIGS = {"microvm": ("gke-substrate-metal-microvm-metal", "us-east4-c"),
        "gvisor": ("gke-apo-gvisor-apo-metal", "us-central1-a")}


def now() -> datetime:
  return datetime.now(timezone.utc)


def log(msg: str) -> None:
  line = f"{now().strftime('%Y-%m-%dT%H:%M:%SZ')} {msg}"
  print(line, flush=True)
  with open(LOG, "a", encoding="utf-8") as f:
    f.write(line + "\n")


def trial_pids():
  out = []
  for p in glob.glob("/proc/[0-9]*/cmdline"):
    try:
      cmd = open(p, "rb").read().replace(b"\0", b" ").decode(errors="replace")
    except OSError:
      continue
    if cmd.startswith("python3 ") and "/.apo/run_experiment.py" in cmd:
      out.append((int(p.split("/")[2]), cmd))
  return out


def mig(prefix: str) -> str:
  out = subprocess.run(["gcloud", "compute", "instance-groups", "managed", "list", "--project",
                        "nishanth-gke-dev", f"--filter=name~{prefix}", "--format=value(targetSize,size)"],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=120)
  return (out.stdout.strip() or out.stderr.strip()[:80]).replace("\t", "/")


def state(stack: str) -> dict:
  try:
    return json.load(open(os.path.join(RES, f"night-{stack}.state.json"), encoding="utf-8"))
  except (OSError, ValueError):
    return {}


def ensure_findings(stack: str) -> None:
  """Adds the gate overrides / issues section to a finished stack's morning report."""
  path = os.path.join(RES, f"morning-report-{stack}.md")
  if state(stack).get("step") != "done" or not os.path.exists(path):
    return
  if "<!-- gate-findings:start -->" in open(path, encoding="utf-8").read():
    return
  out = subprocess.run(["python3", os.path.join(ROOT, ".apo", "scripts", "night.py"), "--stack", stack,
                        "--findings"], capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=120)
  log(f"[{stack}] {(out.stdout or out.stderr).strip()[:200]}")


def main() -> None:
  stopped = False
  log("watchdog start")
  while now() < END:
    for stack, (prefix, _) in MIGS.items():
      s = state(stack)
      log(f"[{stack}] step={s.get('step')} trial={s.get('trial')} node(target/size)={mig(prefix)} "
          f"last_error={s.get('last_error', '')[:160]!r} stop_reason={s.get('stop_reason', '')[:120]!r} "
          f"updated={s.get('updated')}")
      try:
        ensure_findings(stack)
      except Exception as e:  # pylint: disable=broad-except
        log(f"[{stack}] findings failed: {type(e).__name__}: {e}")
    pids = trial_pids()
    log(f"running trials: {[(p, c.split()[2].split('/')[-1] if len(c.split()) > 2 else c) for p, c in pids]}")
    if now() >= HARD_STOP and not stopped:
      for pid, cmd in pids:
        try:
          os.kill(pid, signal.SIGTERM)
          log(f"09:00 PDT: SIGTERM -> {pid} ({cmd[:160]})")
        except OSError as e:
          log(f"SIGTERM {pid} failed: {e}")
      stopped = True
    time.sleep(300)
  log("watchdog end")


if __name__ == "__main__":
  main()
