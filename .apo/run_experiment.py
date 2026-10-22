#!/usr/bin/env python3
"""APO harness: sweperf activation throughput (ResumeActor/s at N active actors).

Run through .apo/run_experiment.sh, which sources .apo/set-env.sh first.

One trial (--mode fixed, the default; what APO runs):
  1. Installs the trial's Substrate build (hack/install-ate.sh) and recreates
     the sweperf template with benchmarking/workloads/deploy.sh, so its golden
     snapshot comes from this build.
  2. Applies node knobs (tmpfs for the actor directory, neighbour-table
     sysctls) through a privileged node shell, and object-storage env knobs.
  3. Purges every simulated actor, then runs agentsim (substrate-agents-tco,
     --sweperf) as a Job: register REGISTERED actors, pre-warm each with one
     sweperf step, keep AWAKE active and turn over RATE per second for one
     warm-up window plus WINDOWS measurement windows.
  4. Reads agentsim's result JSON, the atelet/worker "timing breakdown" logs
     and the node's OOM-kill count, and writes <RESULTS_DIR>/summary.json as
     {"metrics": {...}}, or error.json. The trial window's WARN/ERROR lines,
     router and Envoy logs are saved too (collect_breakdown), and stalled
     restores are counted per worker and burst (stalls.json).
  5. Always (also on failure) deletes the agentsim Job, scales the workers to
     0 and purges the actors. Our node objects (ate cgroups, actor netns,
     tap/veth links, actor mounts, runtime processes) are counted before the
     run and after this cleanup (leak.json); leaked_objects is a constraint.
     Residue from earlier trials is cleaned at the start; if it won't go, the
     trial errors (recycle the node).

--mode ramp raises the rate level by level (RAMP_START x RAMP_MULT) until a
gate fails and reports the last clean rate (`ceiling_per_s`): the acceptance
test for a champion and how v000 sets RATE (80% of the ceiling).

--mode fine-ramp steps a fractional rate (agentsim --swap-rate) from FINE_FROM
(default: the coarse ceiling, CEILING or RATE / 0.8) by FINE_STEP (0.25) up to
FINE_TO (FINE_FROM + 1), one WINDOW per level, with the ramp's gates; it reports
`fine_ceiling_per_s` and `fine_spec_ceiling_per_s`, fine enough to confirm a
10-20% throughput win.

Offline checks (no cluster):
  run_experiment.py --render-only DIR   render the agentsim Job to DIR/job.yaml
  run_experiment.py --parse-only DIR    summarize DIR/agentsim.log (+ breakdown)
"""

import argparse
import calendar
import json
import math
import os
import re
import shlex
import signal
import statistics
import string
import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

APO_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(APO_DIR, ".."))
MANIFESTS = os.path.join(APO_DIR, "manifests")
TUNABLES = os.path.join(MANIFESTS, "tunables.env")

SIM_ATESPACE = "agents-sim"
RESULT_PREFIX = "AGENTSIM_RESULT_JSON "
POOL = ("-n", "benchmark-workloads", "workerpool", "benchmark-ateom")
# Pod states that mean the agentsim pod will never start on its own.
START_FAIL_REASONS = ("ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError")
PENDING_LIMIT_S = 600


def log(*args, component: str = "run_experiment") -> None:
  """Log line format from Mauricio's APO harness: ISO-8601 UTC timestamp + component."""
  ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
  print(f"{ts} [{component}]", *args, flush=True)


def source_env_file(path: str, env: Dict[str, str]) -> Dict[str, str]:
  """Returns env updated with the variables a bash `source` of path exports."""
  if not os.path.exists(path):
    return env
  out = subprocess.run(
      ["bash", "-c", f"set -a; source {shlex.quote(path)} >/dev/null; env -0"],
      env=env, capture_output=True, check=True,
  ).stdout
  new_env = dict(env)
  for item in out.split(b"\0"):
    if b"=" in item:
      k, v = item.split(b"=", 1)
      new_env[k.decode()] = v.decode()
  return new_env


def run(cmd: List[str], env: Dict[str, str], check: bool = True, quiet: bool = False, **kw):
  if not quiet:  # quiet: the trial-log poller's reads, every LOG_POLL_S
    log("$", " ".join(shlex.quote(c) for c in cmd))
  return subprocess.run(cmd, env=env, cwd=ROOT, check=check, **kw)


def kubectl(env: Dict[str, str], *args: str, check: bool = True, **kw):
  return run(["kubectl", "--context", env["KUBECTL_CONTEXT"], *args], env, check=check, **kw)


def render(path: str, values: Dict[str, str]) -> str:
  with open(path, encoding="utf-8") as f:
    return string.Template(f.read()).substitute(values)


def duration_s(d: str) -> int:
  m = re.fullmatch(r"(\d+)([smh]?)", d.strip())
  if not m:
    raise ValueError(f"bad duration {d!r}")
  return int(m.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600}[m.group(2)]


# --- Deploy -------------------------------------------------------------------


def context_problem(current: str, want: str, kubeconfig: str) -> Optional[str]:
  """deploy.sh, ko and kubectl-ate use the current context, not KUBECTL_CONTEXT."""
  if current != want:
    return (f"current-context of {kubeconfig or '(default kubeconfig)'} is {current or '(none)'!s}, "
            f"not {want}: another stack's set-env.sh may share this kubeconfig")
  return None


class ContextError(Exception):
  """The stack's kubeconfig points at another cluster."""


def check_context(env: Dict[str, str]) -> Optional[str]:
  got = run(["kubectl", "config", "current-context"], env, check=False,
            capture_output=True, text=True)
  return context_problem((got.stdout or "").strip(), env.get("KUBECTL_CONTEXT", ""),
                         env.get("KUBECONFIG", ""))

# SandboxConfig each class's templates name (benchmarking/workloads/deploy.sh).
SANDBOX_CONFIG_NAMES = {"gvisor": "gvisor-default", "microvm": "microvm"}


def sandboxconfig_problem(cfg: Optional[dict], name: str) -> Optional[str]:
  """Returns why SandboxConfig cfg can't serve actors, or None if it can."""
  if cfg is None:
    return (f"SandboxConfig {name} not found (gvisor-default: install-ate.sh "
            "--deploy-ate-system; microvm: hack/install-microvm-deps.sh --install)")
  spec = cfg.get("spec") or {}
  default = spec.get("defaultVersion")
  if not default:
    return f"SandboxConfig {name} has no defaultVersion (old format? re-apply it)"
  for v in spec.get("versions") or []:
    if v.get("name") == default:
      if v.get("state") != "Enabled":
        return f"SandboxConfig {name} default version {default} is {v.get('state')}"
      return None
  return f"SandboxConfig {name} has no version named {default}"


def check_sandboxconfig(env: Dict[str, str]) -> Optional[str]:
  name = SANDBOX_CONFIG_NAMES.get(env.get("SANDBOX_CLASS", "gvisor"), "gvisor-default")
  got = kubectl(env, "get", "sandboxconfig", name, "-o", "json", check=False,
                capture_output=True, text=True)
  if got.returncode != 0:
    if "NotFound" in (got.stderr or ""):
      return sandboxconfig_problem(None, name)
    return f"could not read SandboxConfig {name}: {(got.stderr or '').strip()[:300]}"
  try:
    return sandboxconfig_problem(json.loads(got.stdout), name)
  except ValueError:
    return f"could not parse SandboxConfig {name}"


def pin_workers(env: Dict[str, str]) -> bool:
  """Patches the worker pool placement and size. False if no pool yet."""
  patch: Dict = {}
  if env.get("WORKER_NODE_SELECTOR"):
    k, v = env["WORKER_NODE_SELECTOR"].split("=", 1)
    patch["nodeSelector"] = {k: v}
  if env.get("WORKER_TOLERATION"):
    key_val, effect = env["WORKER_TOLERATION"].split(":", 1)
    k, v = key_val.split("=", 1)
    patch["tolerations"] = [{"key": k, "operator": "Equal", "value": v, "effect": effect}]
  if env.get("WORKER_CPU_REQUEST"):
    patch["resources"] = {"requests": {"cpu": env["WORKER_CPU_REQUEST"]}}
  got = kubectl(env, "get", *POOL, "-o", "jsonpath={.spec.template.resources.requests.cpu}",
                check=False, capture_output=True, text=True)
  if got.returncode != 0:
    return False
  want = env.get("WORKER_CPU_REQUEST")
  # Same value deploy.sh applies; a pool from before the field was required
  # fails validation on any template patch without it.
  classes = [{"name": env.get("SANDBOX_CLASS", "gvisor")}]
  if want and got.stdout.strip() not in ("", want):
    # A new pod size can't roll out in place with no surge: switch the
    # template at zero replicas, then scale the new pods up.
    kubectl(env, "patch", *POOL, "--type=merge", "-p",
            json.dumps({"spec": {"replicas": 0, "sandboxClasses": classes,
                                 "template": patch}}))
    kubectl(env, "-n", "benchmark-workloads", "wait", "--for=delete", "pod",
            "-l", "ate.dev/worker-pool=benchmark-ateom", "--timeout=10m")
  spec = {"replicas": int(env["WORKER_COUNT"]), "sandboxClasses": classes, "template": patch}
  kubectl(env, "patch", *POOL, "--type=merge", "-p", json.dumps({"spec": spec}))
  return True


TEMPLATE_ATESPACE = "benchmark-workloads"
GOLDEN_ATESPACE = "ate-golden"  # golden actors: name = the template's UID


class TemplateError(Exception):
  """The old sweperf template can't be removed before deploy.sh recreates it."""


def kubectl_ate(env: Dict[str, str], *args: str):
  return run([env["KUBECTL_ATE_BIN"], "--context", env["KUBECTL_CONTEXT"], *args], env,
             check=False, capture_output=True, text=True)


def build_kubectl_ate(env: Dict[str, str]) -> None:
  # Per cluster: two stacks' trials may build it at the same time from this tree.
  out = os.path.join(APO_DIR, "scratch", "bin", env.get("CLUSTER_NAME", "default"), "kubectl-ate")
  run(["go", "build", "-o", out, "./cmd/kubectl-ate"], env)
  env["KUBECTL_ATE_BIN"] = out


def purge_golden_actor(env: Dict[str, str], template_uid: str) -> None:
  """Deletes the template's golden actor rows (same SQL as purge_actors)."""
  if not re.fullmatch(r"[0-9a-zA-Z-]+", template_uid):
    raise TemplateError(f"unexpected template uid {template_uid!r}")
  where = f"atespace = '{GOLDEN_ATESPACE}' AND name = '{template_uid}'"
  psql(env, f"DELETE FROM worker_assignments WHERE actor_uid IN "
            f"(SELECT uid FROM actors WHERE {where})")
  n = psql(env, f"WITH d AS (DELETE FROM actors WHERE {where} RETURNING 1) "
                "SELECT count(*) FROM d")
  log(f"purged {n} golden actor(s) of template uid {template_uid}")


def remove_template(env: Dict[str, str], name: str) -> None:
  """Deletes the template (and its golden actor) so deploy.sh can recreate it.

  deploy.sh ignores delete errors; a golden actor assigned to a node that no
  longer exists (pool scaled to 0) makes the delete fail and the create then
  hits AlreadyExists. Purge that actor and retry once.
  """
  get_args = ("get", "actor-template", name, "-a", TEMPLATE_ATESPACE, "-o", "json")
  got = kubectl_ate(env, *get_args)
  if got.returncode != 0:
    if "NotFound" in (got.stderr or ""):
      return
    raise TemplateError(f"get actor-template {name}: {(got.stderr or '').strip()[:400]}")
  try:
    uid = (json.loads(got.stdout).get("metadata") or {}).get("uid") or ""
  except ValueError:
    uid = ""
  errors = []
  for attempt in (1, 2):
    d = kubectl_ate(env, "delete", "actor-template", name, "-a", TEMPLATE_ATESPACE)
    if d.returncode == 0:
      gone = kubectl_ate(env, *get_args)
      if gone.returncode != 0 and "NotFound" in (gone.stderr or ""):
        return
      errors.append(f"attempt {attempt}: still present after delete")
    else:
      errors.append(f"attempt {attempt}: {(d.stderr or d.stdout or '').strip()[:400]}")
    log(f"template delete failed ({errors[-1]})")
    if attempt == 1:
      if not uid:
        break
      purge_golden_actor(env, uid)
  raise TemplateError(f"can't remove actor-template {TEMPLATE_ATESPACE}/{name}: "
                      + "; ".join(errors))


def deploy(env: Dict[str, str]) -> None:
  """Installs the trial build and recreates the sweperf template (new golden)."""
  install_flags = shlex.split(env.get("INSTALL_FLAGS", ""))
  run(["./hack/install-ate.sh", *install_flags,
       "--deploy-atelet", "--deploy-ate-apiserver", "--deploy-atenet"], env)
  build_kubectl_ate(env)
  remove_template(env, env["TEMPLATE"])
  problem = check_context(env)  # deploy.sh below uses the current context
  if problem:
    raise ContextError(problem)
  deploy_env = dict(env, WORKLOAD_TEMPLATES=env["TEMPLATE"])
  deploy_cmd = ["./benchmarking/workloads/deploy.sh", "--deploy",
                f"--worker-count={env['WORKER_COUNT']}",
                f"--sandbox-class={env['SANDBOX_CLASS']}",
                f"--actor-memory={env['ACTOR_MEMORY']}"]
  # Pin first so the golden is taken on the metal node (a snapshot from another
  # CPU family won't restore). ko apply is client-side, so the pin survives.
  pinned = pin_workers(env)
  run(deploy_cmd, env=deploy_env)
  if not pinned and pin_workers(env):
    run(deploy_cmd, env=deploy_env)  # first deploy: redo the template on the pinned pool
  kubectl(env, "-n", "benchmark-workloads", "rollout", "status",
          "deployment/benchmark-ateom", "--timeout=20m")


# --- Node and object-storage knobs --------------------------------------------


def node_sh(env: Dict[str, str], script: str, check: bool = True) -> str:
  """Runs script in the privileged node shell on the metal node."""
  out = kubectl(env, "-n", "kube-system", "exec", "apo-nodeshell", "--", "sh", "-c", script,
                check=check, capture_output=True, text=True)
  return (out.stdout or "").strip()


def ensure_nodeshell(env: Dict[str, str]) -> None:
  manifest = render(os.path.join(MANIFESTS, "nodeshell.yaml.tmpl"), env)
  kubectl(env, "apply", "-f", "-", input=manifest, text=True)
  kubectl(env, "-n", "kube-system", "wait", "--for=condition=Ready", "pod/apo-nodeshell",
          "--timeout=5m")


def restart_atelet(env: Dict[str, str]) -> None:
  ds = kubectl(env, "-n", "ate-system", "get", "ds", "-o", "name", capture_output=True,
               text=True).stdout.split()
  for d in (x for x in ds if "atelet" in x):
    kubectl(env, "-n", "ate-system", "rollout", "restart", d)
    kubectl(env, "-n", "ate-system", "rollout", "status", d, "--timeout=10m")


def apply_node_knobs(env: Dict[str, str]) -> Dict[str, str]:
  """Applies NODE_* knobs; the worker pool must be at zero for the tmpfs switch."""
  ensure_nodeshell(env)
  thresh3 = int(env.get("NODE_NEIGH_GC_THRESH3", "1024"))
  # The nodeshell image (debian slim) has no sysctl: use the host's.
  node_sh(env, "for f in ipv4 ipv6; do nsenter -t 1 -m -n -- "
               f"sysctl -qw net.$f.neigh.default.gc_thresh3={thresh3} "
               f"net.$f.neigh.default.gc_thresh2={max(thresh3 // 2, 512)} "
               f"net.$f.neigh.default.gc_thresh1={max(thresh3 // 8, 128)}; done")
  want_tmpfs = env.get("NODE_TMPFS_ACTORS", "false") == "true"
  mounted = node_sh(env, "grep -c ' /var/lib/ate/actors tmpfs' /proc/1/mounts || true") not in ("", "0")
  if want_tmpfs != mounted:
    if want_tmpfs:
      node_sh(env, "set -e; d=/host/var/lib/ate; mkdir -p $d/actors; "
                   "nsenter -t 1 -m -- mount -t tmpfs -o size=400G,mode=0700 tmpfs /var/lib/ate/actors")
    else:
      node_sh(env, "nsenter -t 1 -m -- umount /var/lib/ate/actors")
    restart_atelet(env)  # so atelet and the workers see the new mount
  return {"node_tmpfs_actors": str(want_tmpfs).lower(), "node_neigh_gc_thresh3": str(thresh3),
          "apo_flags": write_apo_flags(env)}


# Lever switches (Phase 4): WorkerPool.spec.template has no env, but every worker pod mounts
# the host's /var/lib/ate, so a lever build reads its ATE_APO_* switch from the environment or
# from this file. Rewritten on every trial (empty when APO_FLAGS is unset) while the workers
# are at zero, so a lever's flag can never leak into a v000 trial.
APO_FLAGS_FILE = "/var/lib/ate/apo-flags.env"
APO_SWITCHES_MSG = "APO lever switches"  # logged once by a lever build's ateom at startup
APO_EVENT_PREFIX = "APO "  # a lever's own records, e.g. "APO reseed answered"
APO_FLAG_RE = re.compile(r"ATE_APO_[A-Z0-9_]+=[A-Za-z0-9._-]*")


def apo_flag_lines(value: str) -> List[str]:
  """The KEY=VALUE switches of APO_FLAGS (space-separated); ValueError on anything else."""
  flags = value.split()
  bad = [f for f in flags if not APO_FLAG_RE.fullmatch(f)]
  if bad:
    raise ValueError(f"APO_FLAGS entries must look like ATE_APO_NAME=value: {bad}")
  return flags


def write_apo_flags(env: Dict[str, str]) -> str:
  flags = apo_flag_lines(env.get("APO_FLAGS", ""))
  body = "".join(f + "\n" for f in flags)
  node_sh(env, f"printf %s {shlex.quote(body)} > /host{APO_FLAGS_FILE}")
  return " ".join(flags)


def apply_storage_knobs(env: Dict[str, str]) -> Dict[str, str]:
  """Sets the object-storage env knobs (STORAGE_KNOBS names) on STORAGE_KNOB_TARGETS.

  STORAGE_KNOBS lists the env var names (from the env-knob patch); an unset or
  empty value removes the variable, so the build default applies.
  STORAGE_KNOB_TARGETS is a space-separated list of namespace/kind/name[:container].
  """
  names = env.get("STORAGE_KNOBS", "").split()
  targets = env.get("STORAGE_KNOB_TARGETS", "").split()
  if not names or not targets:
    return {}
  sets = [f"{n}={env[n]}" if env.get(n) else f"{n}-" for n in names]
  for t in targets:
    obj, _, container = t.partition(":")
    ns, kind_name = obj.split("/", 1)
    if kind_name.endswith("/ATELET_DS"):  # versioned name, e.g. daemonset/atelet-apo
      names_ds = kubectl(env, "-n", ns, "get", "ds", "-o", "name", capture_output=True,
                         text=True).stdout.split()
      kind_name = next(d for d in names_ds if d.split("/")[-1].startswith("atelet"))
    extra = ["-c", container] if container else []
    kubectl(env, "-n", ns, "set", "env", kind_name, *extra, *sets)
    kubectl(env, "-n", ns, "rollout", "status", kind_name, "--timeout=10m", check=False)
  return {n: env.get(n, "") for n in names}


# --- Fleet --------------------------------------------------------------------


def psql(env: Dict[str, str], sql: str) -> str:
  # ateapi's schema (ATE_API_POSTGRES_SCHEMA); without it, unqualified names
  # resolve through "$user", public, which can hold stale tables.
  schema = env.get("PG_SCHEMA", "substrate")
  out = kubectl(env, "-n", "ate-system", "exec", "postgres-0", "-c", "postgres", "--",
                "env", f"PGOPTIONS=-c search_path={schema}",
                "psql", "-U", "postgres", "-d", "atepg", "-At", "-c", sql,
                capture_output=True, text=True)
  return out.stdout.strip()


def scale_workers(env: Dict[str, str], n: int) -> None:
  kubectl(env, "patch", *POOL, "--type=merge", "-p", json.dumps({"spec": {"replicas": n}}))
  if n == 0:
    kubectl(env, "-n", "benchmark-workloads", "wait", "--for=delete", "pod",
            "-l", "ate.dev/worker-pool=benchmark-ateom", "--timeout=15m")
  else:
    kubectl(env, "-n", "benchmark-workloads", "rollout", "status",
            "deployment/benchmark-ateom", "--timeout=20m")


# Workers register with ate-api-server after their pod is Ready; a ramp that starts before
# all of them have registered runs on fewer workers than WORKER_COUNT says.
REGISTER_TIMEOUT_S = 600
REGISTER_SETTLE_S = 20


def registered_workers(env: Dict[str, str]) -> Optional[int]:
  got = psql(env, "SELECT count(*) FROM workers")
  try:
    return int(got.split()[-1])
  except (ValueError, IndexError):
    return None


class WorkersNotRegistered(Exception):
  """Not every worker registered with the control plane before the ramp."""


def wait_workers_registered(env: Dict[str, str], n: int, timeout_s: int = REGISTER_TIMEOUT_S) -> int:
  """Waits until n workers are registered (then REGISTER_SETTLE_S more); returns the count."""
  deadline = time.time() + timeout_s
  got = None
  while time.time() < deadline:
    got = registered_workers(env)
    if got is not None and got >= n:
      log(f"{got}/{n} workers registered")
      time.sleep(REGISTER_SETTLE_S)
      PROVENANCE["registered_workers"] = got
      return got
    time.sleep(5)
  raise WorkersNotRegistered(f"only {got}/{n} workers registered after {timeout_s}s")


def purge_actors(env: Dict[str, str]) -> None:
  """Deletes every simulated actor; restarts the workers so no sandbox survives."""
  for sql in (f"DELETE FROM worker_assignments WHERE actor_uid IN "
              f"(SELECT uid FROM actors WHERE atespace = '{SIM_ATESPACE}')",):
    psql(env, sql)
  n = psql(env, f"WITH d AS (DELETE FROM actors WHERE atespace = '{SIM_ATESPACE}' "
                "RETURNING 1) SELECT count(*) FROM d")
  log(f"deleted {n} simulated actors")


# --- agentsim -----------------------------------------------------------------


def coarse_ceiling(env: Dict[str, str]) -> float:
  """The stack's coarse ramp ceiling: CEILING, else RATE / 0.8 rounded up (RATE = 80% of it)."""
  if env.get("CEILING"):
    return float(env["CEILING"])
  return float(math.ceil(float(env["RATE"]) / 0.8))


def fine_plan(env: Dict[str, str]) -> Tuple[float, float, float]:
  """Returns the fine ramp's (from, to, step) in swaps/s (FINE_FROM, FINE_TO, FINE_STEP)."""
  lo = float(env.get("FINE_FROM") or coarse_ceiling(env))
  hi = float(env.get("FINE_TO") or lo + 1)
  return lo, hi, float(env.get("FINE_STEP") or 0.25)


def fine_levels(env: Dict[str, str]) -> int:
  lo, hi, step = fine_plan(env)
  return 1 if step <= 0 or hi < lo else int((hi - lo) / step + 1e-9) + 1


def window_plan(env: Dict[str, str], mode: str):
  """Returns (wave_interval_s, warmup_windows, total_duration_s)."""
  win = duration_s(env["WINDOW"])
  if mode == "ramp":
    return win, 0, duration_s(env.get("RAMP_DURATION", "60m"))
  if mode == "fine-ramp":  # one window per level, 30 s per level for the drain
    return win, 0, (win + 30) * fine_levels(env)
  warm = math.ceil(duration_s(env["WARMUP"]) / win)
  return win, warm, win * (warm + int(env["WINDOWS"]))


def agentsim_args(env: Dict[str, str], mode: str, prefix: str) -> List[str]:
  win, warm, total = window_plan(env, mode)
  # fine-ramp: agentsim ignores --swap-start/--swap-mult once --swap-rate is set.
  start, mult = (env["RAMP_START"], env["RAMP_MULT"]) if mode == "ramp" else (env["RATE"], "1")
  args = [
      "--sweperf",
      f"--atespace={SIM_ATESPACE}",
      f"--template={env['TEMPLATE']}",
      f"--actor-prefix={prefix}",
      f"--agents={env['REGISTERED']}",
      f"--swap-fill={env['AWAKE']}",
      f"--swap-start={start}",
      f"--swap-mult={mult}",
      f"--swap-every={env['SWAP_EVERY']}",
      f"--wave-interval={win}s",
      # Upper bound only: a fixed-rate run ends after its windows.
      f"--duration={total + 300}s",
      f"--setup-concurrency={env['SETUP_CONCURRENCY']}",
      f"--prewarm-steps={env['PREWARM_STEPS']}",
      f"--ttfe-sample-pct={env['TTFE_SAMPLE_PCT']}",
      f"--fail-error-pct={env['FAIL_ERROR_PCT']}",
      f"--fail-refusal-pct={env['FAIL_REFUSAL_PCT']}",
      f"--fail-crashed={env['FAIL_CRASHED']}",
      "--lifecycle-mode=suspend",
      "--suspender-url=",
      "--result-json=-",
  ]
  if mode == "ramp":
    args.append(f"--fail-rel-p90={env.get('FAIL_REL_P90', '2.5')}")
  elif mode == "fine-ramp":
    lo, hi, step = fine_plan(env)
    args += [f"--swap-rate={lo:g}", f"--swap-rate-step={step:g}", f"--swap-rate-max={hi:g}",
             f"--fail-rel-p90={env.get('FAIL_REL_P90', '2.5')}"]
  else:
    args += [f"--swap-windows={env['WINDOWS']}", f"--warmup-windows={warm}"]
  return args + shlex.split(env.get("AGENTSIM_EXTRA_ARGS", ""))


def render_job(env: Dict[str, str], mode: str, trial_id: str, prefix: str) -> str:
  _, _, total = window_plan(env, mode)
  # Setup (registration + pre-warm) plus the run, with room to spare.
  deadline = total + int(env.get("SETUP_BUDGET_S", "3600"))
  values = dict(env, JOB_NAME=f"agentsim-{trial_id}", TRIAL_ID=trial_id,
                JOB_DEADLINE_S=str(deadline),
                AGENTSIM_IMAGE=env.get("AGENTSIM_IMAGE", "AGENTSIM_IMAGE_UNSET"),
                AGENTSIM_ARGS=json.dumps(agentsim_args(env, mode, prefix)))
  return render(os.path.join(MANIFESTS, "agentsim-job.yaml.tmpl"), values)


class AgentsimStartError(Exception):
  """The agentsim pod can't start (image pull, pod config, or stuck Pending)."""


def pod_created_epoch(pod: dict) -> Optional[float]:
  ts = (pod.get("metadata") or {}).get("creationTimestamp")
  try:
    return float(calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")))
  except (TypeError, ValueError):
    return None


def agentsim_start_problem(pods: dict, applied_at: float, now: float) -> Optional[str]:
  """Returns why the Job's pod can't start, or None while it still may."""
  items = pods.get("items") or []
  if not items and now - applied_at > PENDING_LIMIT_S:
    return f"no agentsim pod {PENDING_LIMIT_S // 60} min after the Job was applied"
  for pod in items:
    name = (pod.get("metadata") or {}).get("name", "?")
    st = pod.get("status") or {}
    for cs in (st.get("initContainerStatuses") or []) + (st.get("containerStatuses") or []):
      waiting = (cs.get("state") or {}).get("waiting") or {}
      if waiting.get("reason") in START_FAIL_REASONS:
        return f"pod {name}: {waiting['reason']}: {waiting.get('message', '')}"[:500]
    if st.get("phase") == "Pending":
      # Never earlier than PENDING_LIMIT_S after our own apply (clock skew).
      since = max(pod_created_epoch(pod) or applied_at, applied_at)
      if now - since > PENDING_LIMIT_S:
        return f"pod {name} Pending for more than {PENDING_LIMIT_S // 60} min"
  return None


def run_agentsim(env: Dict[str, str], mode: str, trial_id: str, out_dir: str) -> str:
  """Runs the agentsim Job to completion; returns the path of its saved log."""
  prefix = "apo" + trial_id.replace("-", "")[-10:]
  job = render_job(env, mode, trial_id, prefix)
  with open(os.path.join(out_dir, "job.yaml"), "w", encoding="utf-8") as f:
    f.write(job)
  kubectl(env, "-n", "agent-sim", "delete", "job", "-l", "app=agentsim", "--wait=true",
          check=False)
  kubectl(env, "apply", "-f", "-", input=job, text=True)
  applied_at = time.time()
  name = f"job/agentsim-{trial_id}"
  selector = f"job-name=agentsim-{trial_id}"
  _, _, total = window_plan(env, mode)
  deadline = time.time() + total + int(env.get("SETUP_BUDGET_S", "3600")) + 300
  while time.time() < deadline:
    st = kubectl(env, "-n", "agent-sim", "get", name, "-o",
                 "jsonpath={.status.succeeded}/{.status.failed}", check=False,
                 capture_output=True, text=True).stdout.strip()
    if st.startswith("1/") or st.endswith("/1"):
      break
    pods = kubectl(env, "-n", "agent-sim", "get", "pods", "-l", selector, "-o", "json",
                   check=False, capture_output=True, text=True)
    try:
      problem = (agentsim_start_problem(json.loads(pods.stdout), applied_at, time.time())
                 if pods.returncode == 0 else None)
    except ValueError:
      problem = None
    if problem:
      with open(os.path.join(out_dir, "agentsim_pod.txt"), "w", encoding="utf-8") as f:
        kubectl(env, "-n", "agent-sim", "describe", "pods", "-l", selector, stdout=f,
                stderr=subprocess.STDOUT, check=False)
      raise AgentsimStartError(problem)
    time.sleep(30)
  ids = kubectl(env, "-n", "agent-sim", "get", "pods", "-l", selector, "-o",
                "jsonpath={.items[*].status.containerStatuses[*].imageID}", check=False,
                capture_output=True, text=True)
  PROVENANCE["agentsim_image_id"] = (ids.stdout or "").strip()
  log_path = os.path.join(out_dir, "agentsim.log")
  with open(log_path, "w", encoding="utf-8") as f:
    kubectl(env, "-n", "agent-sim", "logs", name, "--tail=-1", stdout=f, check=False)
  return log_path


def parse_result(log_path: str) -> Optional[dict]:
  result = None
  with open(log_path, encoding="utf-8", errors="replace") as f:
    for line in f:
      if line.startswith(RESULT_PREFIX):
        result = json.loads(line[len(RESULT_PREFIX):])
  return result


# --- Stage breakdown and node OOM ---------------------------------------------


def json_record(line: str) -> Optional[dict]:
  """The JSON object of a log line (kubectl may prefix it), or None."""
  i = line.find("{")
  if i < 0:
    return None
  try:
    rec = json.loads(line[i:])
  except ValueError:
    return None
  return rec if isinstance(rec, dict) else None


# Envoy access-log lines for the sweperf polling calls; a 2xx one carries nothing a
# trial needs, and they outnumber the wakes many times over.
ENVOY_POLL_RE = re.compile(r'"(GET /status|POST /execute) HTTP/[0-9.]+" 2\d\d ')


ENVOY_ACCESS_RE = re.compile(r'\[\d{4}-\d\d-\d\dT[^\]]*Z\] "')
ENVOY_OWN_BAD_RE = re.compile(r"\]\[(warning|error|critical)\]\[")


def keep_envoy_line(line: str) -> bool:
  """Envoy lines worth keeping: every access line but the 2xx polls (so every wake, GET
  /health, and any non-2xx), and Envoy's own warning/error lines."""
  if ENVOY_ACCESS_RE.match(line):
    return not ENVOY_POLL_RE.search(line)
  return bool(ENVOY_OWN_BAD_RE.search(line))


# The kubelet rotates container logs (about 10 MiB per file) and the logs API reads only the
# current file, so one read at the end of a trial sees only its last minute on a busy worker.
# The trial's logs are therefore polled while it runs, this often, from where the last poll ended.
LOG_POLL_S = 20


def log_ts_key(ts: str) -> Optional[Tuple[int, int]]:
  """(seconds, nanoseconds) of a kubelet --timestamps prefix such as 2026-10-12T01:48:39.12Z."""
  m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z$", ts)
  if not m:
    return None
  return (calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")),
          int((m.group(2) or "0").ljust(9, "0")[:9]))


class TrialLogs:
  """The trial window's logs from the node daemons, workers and router, read as the trial runs.

  breakdown.jsonl: the atelet and worker 'timing breakdown' records, each with the pod it came
  from (k8s.pod). warn.jsonl: their WARN/ERROR records (for example "guest CRNG reseed failed;
  retrying"), also with k8s.pod. router.log: the atenet router's WARN/ERROR lines.
  envoy_access.log: the router Envoy's access log without the successful /status and /execute
  polls (so one line per wake, with status, duration and upstream worker IP). pods.json: every
  worker pod seen -> IP and node, to map Envoy's upstream address to a worker.
  apo_switches.jsonl: each worker's startup "APO lever switches" record (the ATE_APO_* switches
  that were on), with k8s.pod. apo_events.jsonl: a lever's own "APO ..." records, with k8s.pod.

  poll() reads every container from where its last poll ended (--since-time has one-second
  resolution, so lines are deduplicated by their kubelet timestamp); start() polls every
  LOG_POLL_S in a thread; finish() stops it, polls a last time and closes the files.
  """

  FILES = ("breakdown.jsonl", "warn.jsonl", "router.log", "envoy_access.log", "apo_switches.jsonl",
           "apo_events.jsonl")

  def __init__(self, env: Dict[str, str], since: str, out_dir: str):
    self.env, self.since, self.out_dir = env, since, out_dir
    self.last: Dict[Tuple[str, str], Tuple[int, int]] = {}
    self.pods_info: Dict[str, Dict[str, str]] = {}
    self.out = {f: open(os.path.join(out_dir, f), "w", encoding="utf-8") for f in self.FILES}
    self.mu = threading.Lock()
    self.stopped = threading.Event()
    self.thread: Optional[threading.Thread] = None

  def start(self) -> "TrialLogs":
    def loop():
      while not self.stopped.wait(LOG_POLL_S):
        try:
          self.poll()
        except Exception as e:  # pylint: disable=broad-except
          log(f"log poll failed: {type(e).__name__}: {e}")  # the next poll catches up
    self.thread = threading.Thread(target=loop, name="trial-logs", daemon=True)
    self.thread.start()
    return self

  def stop(self) -> None:
    self.stopped.set()
    if self.thread:
      self.thread.join(timeout=120)

  def finish(self) -> str:
    self.stop()
    try:
      self.poll()
    finally:
      for f in self.out.values():
        f.close()
      write_json(os.path.join(self.out_dir, "pods.json"), self.pods_info)
    return os.path.join(self.out_dir, "breakdown.jsonl")

  def poll(self) -> None:
    with self.mu:
      for ns, sel in (("ate-system", None), ("benchmark-workloads", "ate.dev/worker-pool=benchmark-ateom")):
        got = kubectl(self.env, "-n", ns, "get", "pods", "-o", "json", *(["-l", sel] if sel else []),
                      capture_output=True, text=True, check=False, quiet=True)
        try:
          items = json.loads(got.stdout or "{}").get("items") or []
        except ValueError:
          items = []
        for pod in items:
          name = (pod.get("metadata") or {}).get("name", "")
          if ns == "ate-system" and "atelet" not in name and "atenet-router" not in name:
            continue
          if sel:
            self.pods_info[name] = {"ip": (pod.get("status") or {}).get("podIP", ""),
                                    "node": (pod.get("spec") or {}).get("nodeName", "")}
          for c in (pod.get("spec") or {}).get("containers") or []:
            self.read(ns, name, c.get("name", ""))
      for f in self.out.values():
        f.flush()

  def read(self, ns: str, pod: str, container: str) -> None:
    key = (pod, container)
    last = self.last.get(key)
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(last[0])) if last else self.since
    text = kubectl(self.env, "-n", ns, "logs", pod, "-c", container, "--timestamps",
                   f"--since-time={since}", check=False, capture_output=True, text=True,
                   quiet=True).stdout or ""
    for line in text.splitlines():
      ts, _, body = line.partition(" ")
      k = log_ts_key(ts)
      if k is None or (last is not None and k <= last):
        continue  # not a log line, or already read by an earlier poll
      self.last[key] = last = k
      self.route(pod, container, body)

  def route(self, pod: str, container: str, line: str) -> None:
    if "atenet-router" in pod:
      if container == "envoy":
        if keep_envoy_line(line):
          self.out["envoy_access.log"].write(line + "\n")
      else:
        rec = json_record(line)
        if rec and rec.get("level") in ("WARN", "ERROR"):
          self.out["router.log"].write(line + "\n")
      return
    rec = json_record(line)
    if rec is None:
      return
    tagged = json.dumps(dict(rec, **{"k8s.pod": pod})) + "\n"
    if "timing breakdown" in line:
      self.out["breakdown.jsonl"].write(tagged)
    elif rec.get("msg") == APO_SWITCHES_MSG:
      self.out["apo_switches.jsonl"].write(tagged)
    elif str(rec.get("msg", "")).startswith(APO_EVENT_PREFIX):
      self.out["apo_events.jsonl"].write(tagged)
    elif rec.get("level") in ("WARN", "ERROR"):
      self.out["warn.jsonl"].write(tagged)


def start_trial_logs(env: Dict[str, str], since: str, out_dir: str) -> TrialLogs:
  """Starts reading the trial's logs as it runs (collect_breakdown finishes it)."""
  return TrialLogs(env, since, out_dir).start()


def collect_breakdown(env: Dict[str, str], since: str, out_dir: str,
                      logs: Optional[TrialLogs] = None) -> str:
  """Finishes the trial's log collection (one read now when none was started); see TrialLogs."""
  return (logs or TrialLogs(env, since, out_dir)).finish()


def summarize_breakdown(path: str) -> Dict[str, float]:
  """p50/p90 ms per stage of the simulated actors' restore and checkpoint lines."""
  stages: Dict[str, List[float]] = {}
  if not os.path.exists(path):
    return {}
  with open(path, encoding="utf-8") as f:
    for line in f:
      try:
        rec = json.loads(line)
      except ValueError:
        continue
      if rec.get("ate.atespace") != SIM_ATESPACE:
        continue
      for k, v in rec.items():
        if ".duration." in k and isinstance(v, (int, float)):
          stage = k.split(".actor.", 1)[-1].replace(".duration.", "_")
          stages.setdefault(stage, []).append(v * 1000)
  out: Dict[str, float] = {}
  for stage, vals in sorted(stages.items()):
    vals.sort()
    out[f"stage_{stage}_p50_ms"] = round(vals[len(vals) // 2], 1)
    out[f"stage_{stage}_p90_ms"] = round(vals[min(len(vals) - 1, int(len(vals) * 0.9))], 1)
  return out


# A restore whose wakeup_probe (resume until the wakeup probes pass) takes this long is a
# stall: past the router's 5 s park budget, on its way to the 10 s ext_proc timeout.
STALL_S = 5.0
BURST_GAP_S = 30.0  # stalls closer than this belong to one burst
RESEED_RETRY_MSG = "guest CRNG reseed failed; retrying"


def rfc3339_epoch(ts: str) -> Optional[float]:
  """Seconds since the epoch of a log time such as 2026-10-12T01:03:32.143725428Z."""
  m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?Z$", ts or "")
  if not m:
    return None
  return calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")) + float(m.group(2) or 0)


def summarize_stalls(out_dir: str) -> Tuple[Dict[str, float], Dict]:
  """Stalled restores of the simulated actors: how many, which part of wakeup_probe, which workers.

  Reads breakdown.jsonl (the ateom-microvm restore records carry wakeup_probe and, from the
  Phase 4 instrumentation on, its parts agent_dial, reseed and probe) and warn.jsonl (reseed
  retries). Returns (metrics, detail): detail goes to stalls.json, per worker and per burst.
  """
  key = "ateom.actor.restore.duration."
  restores, stalls, split = 0, [], False
  path = os.path.join(out_dir, "breakdown.jsonl")
  if os.path.exists(path):
    with open(path, encoding="utf-8") as f:
      for line in f:
        rec = json_record(line)
        if not rec or rec.get("ate.atespace") != SIM_ATESPACE or (key + "wakeup_probe") not in rec:
          continue
        restores += 1
        split |= (key + "reseed") in rec
        if rec[key + "wakeup_probe"] >= STALL_S:
          stalls.append({"t": rfc3339_epoch(rec.get("time", "")), "pod": rec.get("k8s.pod", ""),
                         "wakeup_probe_s": rec[key + "wakeup_probe"],
                         **{p + "_s": rec.get(key + p) for p in ("agent_dial", "reseed", "probe")}})
  retries: Dict[str, int] = {}
  wpath = os.path.join(out_dir, "warn.jsonl")
  if os.path.exists(wpath):
    with open(wpath, encoding="utf-8") as f:
      for line in f:
        rec = json_record(line)
        if rec and rec.get("msg") == RESEED_RETRY_MSG:
          retries[rec.get("k8s.pod", "")] = retries.get(rec.get("k8s.pod", ""), 0) + 1
  if not restores and not retries:
    return {}, {}
  timed = sorted((s for s in stalls if s["t"] is not None), key=lambda s: s["t"])
  bursts: List[Dict] = []
  for s in timed:
    if bursts and s["t"] - bursts[-1]["end"] <= BURST_GAP_S:
      b = bursts[-1]
      b["end"], b["n"] = s["t"], b["n"] + 1
      b["pods"][s["pod"]] = b["pods"].get(s["pod"], 0) + 1
    else:
      bursts.append({"start": s["t"], "end": s["t"], "n": 1, "pods": {s["pod"]: 1}})
  per_pod: Dict[str, int] = {}
  for s in stalls:
    per_pod[s["pod"]] = per_pod.get(s["pod"], 0) + 1
  def part(name: str) -> int:  # stalls whose time went mostly into one part of wakeup_probe
    return sum(1 for s in stalls if s.get(name + "_s") is not None
               and s[name + "_s"] >= 0.5 * s["wakeup_probe_s"])
  m = {"restore_records": restores, "restore_stalls": len(stalls),
       "restore_stall_bursts": len(bursts), "restore_stall_pods": len(per_pod),
       "reseed_retries": sum(retries.values())}
  if split:  # records from before the instrumentation have wakeup_probe only
    m.update(restore_stalls_in_reseed=part("reseed"), restore_stalls_in_probe=part("probe"),
             restore_stalls_in_agent_dial=part("agent_dial"))
  detail = {"stall_s": STALL_S, "burst_gap_s": BURST_GAP_S, "per_pod": per_pod,
            "reseed_retries_per_pod": retries, "bursts": bursts, "stalls": stalls}
  return m, detail


# Stages whose time is spent outside the allowed scope (runsc, cloud-hypervisor, the
# kernel). Changes in scope can only reduce how often or how concurrently they run.
BLACK_BOX = {
    "restore_app_restore": "gVisor sentry restore inside runsc",
    "restore_pause_restore": "gVisor sentry restore inside runsc",
    "restore_vm_restore": "cloud-hypervisor vm.restore",
    "checkpoint_checkpoint": "runsc checkpoint / cloud-hypervisor vm.snapshot",
}


def write_monitor(results_dir: str, breakdown: Dict[str, float], metrics: Dict) -> None:
  """Writes monitor/README.md: a readable per-trial digest for the evolve agent."""
  mon = os.path.join(results_dir, "monitor")
  os.makedirs(mon, exist_ok=True)
  rows = sorted({k[len("stage_"):-len("_p50_ms")] for k in breakdown if k.endswith("_p50_ms")},
                key=lambda st: -breakdown.get(f"stage_{st}_p50_ms", 0))
  lines = [
      "# Trial digest (sweperf activation throughput)", "",
      "Where the time of one restore and one checkpoint goes on the node, from the",
      "atelet and worker `timing breakdown` log lines of the simulated actors",
      "(`breakdown.jsonl` has the raw lines). Sorted by p50, largest first.", "",
      "| stage | p50 ms | p90 ms | note |", "|---|---|---|---|",
  ]
  for st in rows:
    note = BLACK_BOX.get(st, "")
    note = f"black box: {note}" if note else ""
    lines.append(f"| {st} | {breakdown.get(f'stage_{st}_p50_ms')} | "
                 f"{breakdown.get(f'stage_{st}_p90_ms')} | {note} |")
  lines += ["", "Headline metrics:", ""]
  for k in ("resume_warm_p90_ms", "suspend_p90_ms", "achieved_ratio", "error_refusal_pct",
            "ttfe_accept_p95_ms", "ceiling_per_s", "spec_ceiling_per_s", "fine_ceiling_per_s",
            "fine_spec_ceiling_per_s", "restore_stalls", "restore_stall_bursts",
            "restore_stalls_in_reseed", "restore_stalls_in_probe", "reseed_retries",
            "refusals_status_503", "refusals_status_504"):
    if k in metrics:
      lines.append(f"- `{k}`: {metrics[k]}")
  with open(os.path.join(mon, "README.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")


def node_oom_kills(env: Dict[str, str], since_epoch: int) -> Optional[int]:
  """Count of every kernel oom-kill since since_epoch (kept for reference; see node_oom_lines)."""
  got = node_sh(env, f"nsenter -t 1 -m -- journalctl -k --since=@{since_epoch} --no-pager "
                     "| grep -c 'oom-kill' || true", check=False)
  try:
    return int(got.splitlines()[-1])
  except (ValueError, IndexError):
    return None


def node_oom_lines(env: Dict[str, str], since_epoch: int, until_epoch: int = 0) -> Optional[str]:
  """The node's kernel oom-kill lines in the window; None if the kernel log can't be read."""
  until = f" --until=@{until_epoch}" if until_epoch else ""
  got = node_sh(env, f"nsenter -t 1 -m -- journalctl -k --since=@{since_epoch}{until} --no-pager "
                     "&& echo __JOURNAL_OK__", check=False)
  if "__JOURNAL_OK__" not in got:
    return None
  return "\n".join(l for l in got.splitlines() if "oom-kill" in l)


# OOM kills that count: Substrate and its actors. Kernel task names are cut at 15 chars
# (cloud-hyperviso). Sandboxed workload processes (sweperf etc.) are matched by their actor
# cgroup (task_memcg), same leaves as the leak check.
_ATE_OOM_TASK_RE = re.compile(r"^(runsc.*|cloud-hyperviso.*|virtiofsd.*|kata.*|ateom.*|atelet.*|"
                              r"snapshot-plugi.*|agentsim.*|sweperf.*)$")


def classify_oom(text: str) -> Dict:
  """Splits oom-kill lines into ate-related (gated) and other (context only)."""
  ate, other = [], []
  for line in (text or "").splitlines():
    if "oom-kill:" not in line:  # one per kill; "invoked oom-killer" lines name the invoker
      continue
    task = re.search(r"[ ,]task=([^,]+)", line)
    memcg = re.search(r"task_memcg=([^,]+)", line)
    name = task.group(1).strip() if task else "?"
    is_ate = bool(_ATE_OOM_TASK_RE.match(name)) or bool(
        memcg and re.fullmatch(_ATE_CGROUP_RE, memcg.group(1).strip()))
    (ate if is_ate else other).append(name)
  tally = lambda names: dict(sorted({n: names.count(n) for n in set(names)}.items()))
  return {"node_oom_events": len(ate), "node_oom_other": len(other),
          "ate_tasks": tally(ate), "other_tasks": tally(other)}


# --- Node leak check ------------------------------------------------------------
# Counted on the metal node with workers at 0 and actors purged: before the
# trial (after the node knobs) and after the end-of-trial cleanup. Runs in the
# host's mount and net namespaces, so it uses the host's tools and /proc.

# Actor cgroup leaves: gVisor <actorUID>-<container> and the ateom leaf
# (internal/ocispec, internal/ateomcgroup), microVM /ateomchv/<cid> (kata).
_UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_ATE_CGROUP_RE = f".*/(ateom|ateomchv|{_UUID}-[^/]*)(/.*)?"
_RUNTIME_PROCS = "runsc*|cloud-hypervisor*|virtiofsd*|kata*"

NODE_SNAPSHOT_SH = f"""\
cg=/sys/fs/cgroup
echo cgroups=$(find $cg -mindepth 1 -type d 2>/dev/null | wc -l)
echo cgroups_ate=$(find $cg -mindepth 1 -type d -regextype posix-extended \\
  -regex '{_ATE_CGROUP_RE}' 2>/dev/null | wc -l)
echo netns=$( {{ for f in /run/netns/*; do [ -e "$f" ] && stat -L -c 'net:[%i]' "$f"; done
  for p in /proc/[0-9]*; do readlink $p/ns/net; done; }} 2>/dev/null | grep '^net:' | sort -u | wc -l)
echo netns_named=$(ls -A /run/netns 2>/dev/null | wc -l)
echo netns_actor=$(ls -A /run/netns 2>/dev/null | grep -c '^ateom-actor:')
echo netns_lsns=$(lsns -n -t net 2>/dev/null | wc -l)
lh=$(ip -o link 2>/dev/null | wc -l)
ln=$(for f in /run/netns/*; do [ -e "$f" ] && ip -n "${{f##*/}}" -o link 2>/dev/null; done | wc -l)
echo links=$((lh + ln))
echo links_host=$lh
echo links_tap_veth=$( {{ ip -o -d link
  for f in /run/netns/*; do [ -e "$f" ] && ip -n "${{f##*/}}" -o -d link; done; }} 2>/dev/null \\
  | grep -cE ' (veth|tun) ')
echo mounts=$(grep -cE '^([^ ]+ ){{4}}/var/lib/ate(/| )' /proc/1/mountinfo)
echo mounts_actors=$(grep -cE '^([^ ]+ ){{4}}/var/lib/ate/actors/' /proc/1/mountinfo)
echo actor_entries=$(find /var/lib/ate/actors -mindepth 1 2>/dev/null | wc -l)
echo actor_kb=$( (du -sxk /var/lib/ate/actors 2>/dev/null || echo 0) | cut -f1)
n=0
for p in /proc/[0-9]*; do e=$(readlink $p/exe 2>/dev/null)
  case "${{e##*/}}" in {_RUNTIME_PROCS}) n=$((n + 1));; esac; done
echo processes=$n
echo neigh=$(ip neigh 2>/dev/null | wc -l)
echo snapshot_ok=1
"""

# Only run with workers at 0 and actors purged: nothing of ours should be left.
# $KEEP lists actor UIDs still in the DB (e.g. the template's); their dirs stay,
# since actor dirs also root images in atelet's image cache.
NODE_CLEAN_SH = f"""\
grep -E '^([^ ]+ ){{4}}/var/lib/ate/actors/' /proc/1/mountinfo | cut -d' ' -f5 | sort -r \\
  | while read -r m; do umount -l "$m"; done
for p in /proc/[0-9]*; do e=$(readlink $p/exe 2>/dev/null)
  case "${{e##*/}}" in {_RUNTIME_PROCS}) kill -9 "${{p#/proc/}}";; esac; done
sleep 2
for f in /run/netns/ateom-actor:*; do [ -e "$f" ] && {{ umount "$f"; rm -f "$f"; }}; done
find /sys/fs/cgroup -mindepth 1 -depth -type d -regextype posix-extended \\
  -regex '{_ATE_CGROUP_RE}' -exec rmdir {{}} \\;
for d in /var/lib/ate/actors/*; do [ -e "$d" ] || continue
  case " $KEEP " in *" ${{d##*/}} "*) ;; *) rm -rf --one-file-system "$d";; esac; done
true
"""

# Summed into leaked_objects (the constraint): ate-specific counts only. The
# node-wide totals (cgroups, netns, links, mounts) move with unrelated pod
# churn, so they are reported as leaked_* context only.
LEAK_KEYS = ("cgroups_ate", "netns_actor", "links_tap_veth", "mounts_actors", "processes")
# Left over from earlier trials; cleaned before the next one.
RESIDUE_KEYS = ("cgroups_ate", "netns_actor", "processes", "mounts_actors", "actor_entries")
# Residue still there after a clean means the node needs recycling (never automatic).
NODE_RESIDUE_MAX = 5


class LeakCheckError(Exception):
  """The node leak check couldn't read the node or the node has stuck residue."""


def host_sh(script: str) -> str:
  """Wraps script to run in the host's mount and net namespaces."""
  return "nsenter -t 1 -m -n -- sh -c " + shlex.quote(script)


def parse_node_snapshot(text: str) -> Optional[Dict[str, int]]:
  """Parses NODE_SNAPSHOT_SH output; None unless it finished with every required key."""
  out: Dict[str, int] = {}
  for line in text.splitlines():
    k, sep, v = line.strip().partition("=")
    if sep and re.fullmatch(r"[a-z_]+", k):
      try:
        out[k] = int(v.strip())
      except ValueError:
        pass
  required = LEAK_KEYS + ("cgroups", "netns", "links", "mounts")
  if out.pop("snapshot_ok", None) != 1 or any(k not in out for k in required):
    return None
  return out


def residue_objects(snap: Dict[str, int]) -> int:
  """Our objects on an idle node (actor dirs not counted: plain files)."""
  return sum(snap.get(k, 0) for k in RESIDUE_KEYS if k != "actor_entries")


def leak_metrics(before: Dict[str, int], after: Dict[str, int]) -> Dict[str, int]:
  m = {f"leaked_{k}": after[k] - before[k] for k in sorted(after) if k in before}
  m["leaked_objects"] = sum(m[f"leaked_{k}"] for k in LEAK_KEYS)
  return m


LAST_SNAPSHOT_RAW = {"text": ""}  # raw NODE_SNAPSHOT_SH output, saved in leak.json

# Node daemons whose memory we watch (context only): name -> metric prefix.
RSS_PROCS = {"snapshot-plugin": "snapshot_plugin", "atelet": "atelet"}
NODE_RSS_SH = """\
n=0; t=0
for p in /proc/[0-9]*; do e=$(readlink $p/exe 2>/dev/null); e=${e##*/}
  case "$e" in snapshot-plugin|atelet)
    echo "$e $(awk '/^VmRSS:/{print $2}' $p/status 2>/dev/null)" \\
      "$(awk '/^VmHWM:/{print $2}' $p/status 2>/dev/null)";;
  runsc*|cloud-hypervisor*|virtiofsd*)
    r=$(awk '/^VmRSS:/{print $2}' $p/status 2>/dev/null); n=$((n + 1)); t=$((t + ${r:-0}));;
  esac; done
echo "sandbox_procs $n $t"
awk '/^MemTotal:/{tt=$2} /^MemAvailable:/{a=$2} END{print "node_mem", tt, a}' /proc/meminfo
"""


def parse_node_rss(text: str) -> Dict[str, float]:
  """Sums VmRSS / VmHWM (kB) per daemon into <prefix>_rss_mib / _peak_rss_mib."""
  sums: Dict[str, List[int]] = {}
  for line in text.splitlines():
    parts = line.split()
    if len(parts) == 3 and parts[0] in RSS_PROCS and parts[1].isdigit() and parts[2].isdigit():
      acc = sums.setdefault(parts[0], [0, 0])
      acc[0] += int(parts[1])
      acc[1] += int(parts[2])
  out: Dict[str, float] = {}
  for name, (rss, hwm) in sums.items():
    out[f"{RSS_PROCS[name]}_rss_mib"] = round(rss / 1024, 1)
    out[f"{RSS_PROCS[name]}_peak_rss_mib"] = round(hwm / 1024, 1)
  for line in text.splitlines():
    parts = line.split()
    if len(parts) == 3 and parts[0] == "sandbox_procs" and parts[1].isdigit() and parts[2].isdigit():
      out["sandbox_procs"] = int(parts[1])  # runsc / cloud-hypervisor / virtiofsd still running
      out["sandbox_rss_mib"] = round(int(parts[2]) / 1024, 1)
    if len(parts) == 3 and parts[0] == "node_mem" and parts[1].isdigit() and parts[2].isdigit():
      out["node_mem_used_gib"] = round((int(parts[1]) - int(parts[2])) / 1048576, 1)
  return out


def node_rss(env: Dict[str, str]) -> Dict[str, float]:
  return parse_node_rss(node_sh(env, host_sh(NODE_RSS_SH), check=False))


def node_snapshot(env: Dict[str, str]) -> Dict[str, int]:
  ensure_nodeshell(env)
  raw = node_sh(env, host_sh(NODE_SNAPSHOT_SH), check=False)
  LAST_SNAPSHOT_RAW["text"] = raw
  snap = parse_node_snapshot(raw)
  if snap is None:
    raise LeakCheckError("could not count objects on the node (see the node_sh output)")
  return snap


def clean_node_residue(env: Dict[str, str], leak: Dict) -> None:
  """Cleans what earlier trials left on the idle node; errors if it won't go."""
  start = node_snapshot(env)
  leak["start"] = start
  leak["node_residue_objects"] = residue_objects(start)
  if not any(start.get(k, 0) for k in RESIDUE_KEYS):
    return
  log("node residue: " + ", ".join(f"{k}={start.get(k)}" for k in RESIDUE_KEYS))
  keep = " ".join(u for u in psql(env, "SELECT uid FROM actors").split()
                  if re.fullmatch(r"[0-9a-zA-Z-]+", u))
  node_sh(env, host_sh(f"KEEP={shlex.quote(keep)}\n" + NODE_CLEAN_SH), check=False)
  cleaned = node_snapshot(env)
  leak["start_after_clean"] = cleaned
  if residue_objects(cleaned) > NODE_RESIDUE_MAX:
    raise LeakCheckError(
        f"{residue_objects(cleaned)} objects left on the node after cleaning "
        f"({', '.join(f'{k}={cleaned.get(k)}' for k in RESIDUE_KEYS)}); recycle the node")


# --- Summary ------------------------------------------------------------------


def pct(block: Optional[dict], key: str) -> Optional[float]:
  return None if not block else block.get(key)


def level_meets_spec(w: dict) -> bool:
  """The spec's SLOs on one level: resume and suspend p95 <= 500 ms, failures <= 5%,
  and TTFE p95 <= 1 s when the level has TTFE samples."""
  rp95 = pct((w.get("resume_ms") or {}).get("warm"), "p95")
  sp95 = pct(w.get("suspend_ms"), "p95")
  err = w.get("error_refusal_pct")
  ttfe = (w.get("ttfe") or {}).get("accept") or {}
  if None in (rp95, sp95, err) or rp95 > 500 or sp95 > 500 or err > 5:
    return False
  return not ttfe.get("n") or (ttfe.get("p95") is not None and ttfe["p95"] <= 1000)


def spec_ceiling(levels: List[dict]) -> Optional[dict]:
  """Highest level where it and every lower level meet the spec (ramp order)."""
  best = None
  for w in sorted(levels, key=lambda w: w.get("target_per_s") or 0):
    if not level_meets_spec(w):
      break
    best = w
  return best


def goodput(w: Dict) -> Optional[float]:
  """Swaps/s that succeeded: achieved_per_s x (1 - error_refusal_pct/100)."""
  a = w.get("achieved_per_s")
  if a is None:
    return None
  return round(a * (1 - (w.get("error_refusal_pct") or 0) / 100), 3)


def level_rows(result: dict) -> List[Dict]:
  """One row per measured ramp level (hold levels after a failure excluded), with goodput.

  The wake/park p90 gates (agentsim --fail-rel-p90) compare each level with the first
  measured level of the same run, so fine ceilings are only comparable at the same FINE_FROM.
  """
  rows = []
  for w in result.get("levels") or []:
    if w.get("warmup") or w.get("tag"):
      continue
    rows.append({"target_per_s": w.get("target_per_s"), "achieved_per_s": w.get("achieved_per_s"),
                 "goodput_per_s": goodput(w),
                 "resume_warm_p90_ms": pct((w.get("resume_ms") or {}).get("warm"), "p90"),
                 "suspend_p90_ms": pct(w.get("suspend_ms"), "p90"),
                 "error_refusal_pct": w.get("error_refusal_pct"), "drain_s": w.get("drain_s"),
                 "on_time": w.get("on_time"), "wakes": w.get("wakes"), "verdict": w.get("verdict"),
                 "refusal_status": w.get("refusal_status") or {},
                 "error_status": w.get("error_status") or {}})
  return rows


def summarize(result: dict, breakdown: Dict[str, float], extra: Dict, mode: str,
              rate: float = 0) -> Dict:
  m: Dict = {}
  windows = result.get("windows") or result.get("levels") or []
  merged = result.get("merged") or {}
  overall = result.get("overall") or {}
  measured = [w for w in windows if not w.get("warmup")]
  if mode in ("ramp", "fine-ramp"):
    m["ceiling_per_s"] = overall.get("last_clean_achieved_per_s")
    m["ceiling_target_per_s"] = overall.get("last_clean_target_per_s")
    best = spec_ceiling(measured)
    m["spec_ceiling_per_s"] = best.get("achieved_per_s") if best else 0
    m["spec_ceiling_target_per_s"] = best.get("target_per_s") if best else 0
    # Per-level goodput is in levels.json; the max over clean levels is the metric.
    clean = [r["goodput_per_s"] for r in level_rows(result)
             if r["verdict"] == "clean" and r["goodput_per_s"] is not None]
    m["max_goodput_per_s"] = max(clean) if clean else 0
  if mode == "fine-ramp":
    # Ramp levels only (no hold): the last clean one before the first failure.
    levels = [w for w in measured if not w.get("tag")]
    last = None
    for w in levels:
      if w.get("verdict") != "clean":
        break
      last = w
    m["fine_ceiling_per_s"] = last.get("target_per_s") if last else 0
    m["fine_ceiling_achieved_per_s"] = last.get("achieved_per_s") if last else 0
    best = spec_ceiling(levels)
    m["fine_spec_ceiling_per_s"] = best.get("target_per_s") if best else 0
    m["fine_spec_ceiling_achieved_per_s"] = best.get("achieved_per_s") if best else 0
    m["fine_levels"] = len(levels)
  resume = (merged.get("resume_ms") or {})
  m["resume_warm_p50_ms"] = pct(resume.get("warm"), "p50")
  m["resume_warm_p90_ms"] = pct(resume.get("warm"), "p90")
  m["resume_warm_p95_ms"] = pct(resume.get("warm"), "p95")
  m["resume_warm_p99_ms"] = pct(resume.get("warm"), "p99")
  m["resume_cold_p90_ms"] = pct(resume.get("cold"), "p90")
  m["suspend_p90_ms"] = pct(merged.get("suspend_ms"), "p90")
  m["suspend_p95_ms"] = pct(merged.get("suspend_ms"), "p95")
  m["achieved_per_s"] = merged.get("achieved_per_s")
  m["achieved_ratio"] = merged.get("achieved_ratio")
  m["error_refusal_pct"] = merged.get("error_refusal_pct")
  m["warm_pct"] = merged.get("warm_pct")
  m["crashed_actors"] = max([w.get("crashed", 0) or 0 for w in windows] or [0])
  # Refused wake attempts by HTTP status (504: the router's ext_proc timeout; 503: no
  # capacity), and failed wakes by their last status (agentsim: client_timeout, none = no reply).
  for code, n in sorted((merged.get("refusal_status") or {}).items()):
    m[f"refusals_status_{code}"] = n
  for code, n in sorted((merged.get("error_status") or {}).items()):
    m[f"errors_status_{code}"] = n
  ttfe = result.get("ttfe") or merged.get("ttfe") or {}
  m["ttfe_accept_p50_ms"] = pct(ttfe.get("accept"), "p50")
  m["ttfe_accept_p95_ms"] = pct(ttfe.get("accept"), "p95")
  # Context only: includes the sweperf step's own runtime (spec check uses accept).
  m["ttfe_complete_p50_ms"] = pct(ttfe.get("complete"), "p50")
  m["ttfe_complete_p95_ms"] = pct(ttfe.get("complete"), "p95")
  m["prewarm_s"] = (result.get("prewarm") or {}).get("duration_s")
  m["prewarm_failures"] = (result.get("prewarm") or {}).get("failures")
  if mode == "fixed":  # per-window metrics; in ramp mode the ceilings are the result
    m["goodput_per_s"] = goodput(merged)  # over the merged measured windows
    backlogs = [w.get("backlog", 0) or 0 for w in measured]
    m["backlog_growth"] = max(0, backlogs[-1] - backlogs[0]) if len(backlogs) > 1 else 0
    # Up to one tick's worth (RATE) of growth is normal jitter; the gate is on the excess.
    m["backlog_growth_excess"] = max(0, m["backlog_growth"] - rate)
    m["windows_ok"] = merged.get("windows", len(measured))
    m["windows_clean"] = merged.get("windows_clean")
    # Each window drains its in-flight wakes before the next starts, so the backlog resets;
    # a long drain is the sign of wakes outrunning the window.
    drains = [w["drain_s"] for w in measured if w.get("drain_s") is not None]
    m["drain_s_max"] = max(drains) if drains else merged.get("drain_s")
    p90s = [pct((w.get("resume_ms") or {}).get("warm"), "p90") for w in measured]
    p90s = [x for x in p90s if x]
    if len(p90s) > 1:
      m["resume_warm_p90_ms_cv_pct"] = round(100 * statistics.pstdev(p90s) / statistics.mean(p90s), 2)
    # Spec view (go/agent-substrate-perf-benchmark-specs), reported, not optimized.
    def le(v, limit):  # 1/0 so every metric is numeric; unknown counts as a miss
      return int(v is not None and v <= limit)
    m["spec_resume_p95_le_500ms"] = le(m["resume_warm_p95_ms"], 500)
    m["spec_suspend_p95_le_500ms"] = le(m["suspend_p95_ms"], 500)
    m["spec_failure_le_5pct"] = le(m["error_refusal_pct"], 5)
    m["spec_ttfe_p95_le_1s"] = le(m["ttfe_accept_p95_ms"], 1000)  # TTFE = step accepted
  m.update(breakdown)
  m.update(extra)
  return {k: v for k, v in m.items() if v is not None}


def cleanup_trial(env: Dict[str, str], trial_id: str) -> None:
  """Stops the trial's load so no resident actor keeps the node busy afterwards."""
  steps = (
      ("delete agentsim Job", lambda: kubectl(
          env, "-n", "agent-sim", "delete", "job", f"agentsim-{trial_id}",
          "--ignore-not-found", "--cascade=foreground", "--wait=true", "--timeout=5m")),
      ("scale workers to 0", lambda: scale_workers(env, 0)),
      ("clear lever flags", lambda: write_apo_flags(dict(env, APO_FLAGS=""))),
      ("purge actors", lambda: purge_actors(env)),
  )
  for what, step in steps:
    try:
      step()
    except Exception as e:  # pylint: disable=broad-except
      # The next trial's start-of-trial purge still runs.
      log(f"cleanup: {what} failed: {type(e).__name__}: {e}")


# --- Provenance ---------------------------------------------------------------

PROVENANCE: Dict = {}  # filled during the trial, written to provenance.json


def git_out(cwd: str, *args: str) -> str:
  out = subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True, check=False)
  return (out.stdout or "").strip() if out.returncode == 0 else ""


def provenance_static(env: Dict[str, str], mode: str, trial_id: str) -> Dict:
  """What this trial ran: code, images, stack and the effective load settings."""
  tco = env.get("AGENTS_TCO_DIR", "")
  prov = {
      "trial_id": trial_id, "mode": mode, "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
      "workspace_head": git_out(ROOT, "rev-parse", "HEAD"),
      "workspace_dirty": bool(git_out(ROOT, "status", "--porcelain", "--untracked-files=no")),
      "agentsim_commit": git_out(tco, "rev-parse", "HEAD") if tco else "",
      "agentsim_dirty": bool(git_out(tco, "status", "--porcelain", "--untracked-files=no", "--",
                                     "service")) if tco else False,
      "agentsim_image": env.get("AGENTSIM_IMAGE", ""),
      "stack_id": env.get("ID") or f"stack-{env.get('SANDBOX_CLASS', 'gvisor')}",
      "cluster": env.get("CLUSTER_NAME", ""), "kubectl_context": env.get("KUBECTL_CONTEXT", ""),
      "sandbox_class": env.get("SANDBOX_CLASS", ""), "worker_node_pool": env.get("WORKER_NODE_POOL", ""),
      "bucket": env.get("BUCKET_NAME", ""),
      "effective": {k: env.get(k) for k in ("RATE", "AWAKE", "REGISTERED", "WINDOW", "WINDOWS",
                                            "WARMUP", "WORKER_COUNT", "WORKER_CPU_REQUEST",
                                            "ACTOR_MEMORY", "RAMP_START", "RAMP_MULT",
                                            "APO_FLAGS")},
  }
  if mode == "fine-ramp":
    lo, hi, step = fine_plan(env)
    prov["effective"].update(FINE_FROM=lo, FINE_TO=hi, FINE_STEP=step)
  return prov


def cluster_images(env: Dict[str, str]) -> Dict[str, str]:
  """Substrate images as deployed (ko-resolved digests): workload name -> images."""
  images: Dict[str, str] = {}
  for ns in ("ate-system", "benchmark-workloads"):
    got = kubectl(env, "-n", ns, "get", "daemonsets,deployments", "-o",
                  "jsonpath={range .items[*]}{.kind}/{.metadata.name}={.spec.template.spec."
                  "containers[*].image}{'\\n'}{end}", check=False, capture_output=True, text=True)
    for line in (got.stdout or "").splitlines():
      name, sep, imgs = line.partition("=")
      if sep and name:
        images[f"{ns}/{name}"] = imgs.strip()
  return images


def load_json_file(path: str) -> Dict:
  try:
    with open(path, encoding="utf-8") as f:
      return json.load(f)
  except (OSError, ValueError):
    return {}


def write_json(path: str, data: dict) -> None:
  with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")


def error(results_dir: str, phase: str, message: str) -> int:
  write_json(os.path.join(results_dir, "error.json"),
             {"phase": phase, "error_type": "BENCHMARK_FAILURE", "message": message})
  log(f"FAILED [{phase}]: {message}")
  return 1


def main() -> int:
  p = argparse.ArgumentParser(description=__doc__,
                              formatter_class=argparse.RawDescriptionHelpFormatter)
  p.add_argument("results_dir", nargs="?", default=os.path.join(APO_DIR, "results", "scratch"))
  p.add_argument("--mode", choices=("fixed", "ramp", "fine-ramp"), default="fixed")
  p.add_argument("--render-only", action="store_true", help="render the agentsim Job and exit")
  p.add_argument("--parse-only", action="store_true", help="summarize an existing results dir")
  p.add_argument("--fetch-oom", action="store_true",
                 help="with --parse-only and no node_oom.log: read the node's kernel log for the "
                      "trial's window (provenance start_utc .. end_utc + 5 min) and classify it")
  p.add_argument("--skip-deploy", action="store_true", help="reuse the installed build/template")
  p.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                 help="override a tunable (repeatable; APO trials don't use it)")
  args = p.parse_args()

  def _stop(signum, frame):  # SIGTERM: stop like Ctrl-C, so the finally cleanup runs
    raise KeyboardInterrupt
  signal.signal(signal.SIGTERM, _stop)
  results_dir = os.path.abspath(args.results_dir)
  os.makedirs(results_dir, exist_ok=True)

  env = source_env_file(TUNABLES, dict(os.environ))
  # Per-stack overrides (RATE is set from each stack's own ceiling).
  env = source_env_file(os.path.join(MANIFESTS, f"tunables.{env.get('SANDBOX_CLASS', 'gvisor')}.env"),
                        env)
  art = os.path.join(results_dir, "build_artifacts.env")
  env = source_env_file(art, env)
  for kv in args.set:
    k, v = kv.split("=", 1)
    env[k] = v
  trial_id = time.strftime("%Y%m%d-%H%M%S")

  if args.render_only:
    with open(os.path.join(results_dir, "job.yaml"), "w", encoding="utf-8") as f:
      f.write(render_job(env, args.mode, trial_id, "apo" + trial_id.replace("-", "")[-10:]))
    log(f"rendered {results_dir}/job.yaml")
    return 0

  log_path = os.path.join(results_dir, "agentsim.log")
  leak_path = os.path.join(results_dir, "leak.json")
  extra: Dict = {}
  if not args.parse_only:
    if not env.get("AGENTSIM_IMAGE"):
      return error(results_dir, "PREFLIGHT", "AGENTSIM_IMAGE unset (run .apo/build.sh first)")
    problem = check_context(env)
    if problem:
      return error(results_dir, "PREFLIGHT_CONTEXT", problem)
    problem = check_sandboxconfig(env)
    if problem:
      return error(results_dir, "PREFLIGHT_SANDBOXCONFIG", problem)
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    since_epoch = int(time.time())
    leak: Dict = {}
    trial_logs: Optional[TrialLogs] = None
    PROVENANCE.update(provenance_static(env, args.mode, trial_id))
    write_json(os.path.join(results_dir, "provenance.json"), PROVENANCE)
    try:
      try:
        if not args.skip_deploy:
          deploy(env)
        scale_workers(env, 0)
        purge_actors(env)
        clean_node_residue(env, leak)  # what earlier trials left, workers at 0
        knobs = {**apply_node_knobs(env), **apply_storage_knobs(env)}
        write_json(os.path.join(results_dir, "knobs.json"), knobs)  # strings, not metrics
        leak["before"] = node_snapshot(env)
        leak["raw_before"] = LAST_SNAPSHOT_RAW["text"]
        scale_workers(env, int(env["WORKER_COUNT"]))
        trial_logs = start_trial_logs(env, since, results_dir)
        wait_workers_registered(env, int(env["WORKER_COUNT"]))
        log_path = run_agentsim(env, args.mode, trial_id, results_dir)
      except KeyboardInterrupt:
        return error(results_dir, "STOPPED", "stopped by a signal; cleanup still runs")
      except AgentsimStartError as e:
        return error(results_dir, "AGENTSIM_START", str(e))
      except TemplateError as e:
        return error(results_dir, "PREFLIGHT_TEMPLATE", str(e))
      except ContextError as e:
        return error(results_dir, "PREFLIGHT_CONTEXT", str(e))
      except LeakCheckError as e:
        return error(results_dir, "LEAK_CHECK", str(e))
      except WorkersNotRegistered as e:
        return error(results_dir, "WORKERS_REGISTER", str(e))
      except subprocess.CalledProcessError as e:
        return error(results_dir, "DEPLOY_OR_RUN", f"{e.cmd[:6]} exited {e.returncode}")
      except Exception as e:  # pylint: disable=broad-except
        return error(results_dir, "DEPLOY_OR_RUN", f"{type(e).__name__}: {e}")
      try:
        collect_breakdown(env, since, results_dir, trial_logs)
      except Exception as e:  # pylint: disable=broad-except
        log(f"breakdown not collected: {type(e).__name__}: {e}")  # context only
      try:
        oom_text = node_oom_lines(env, since_epoch)
      except Exception as e:  # pylint: disable=broad-except
        oom_text = None
        log(f"OOM check failed: {type(e).__name__}: {e}")
      if oom_text is None:
        return error(results_dir, "OOM_CHECK",
                     "could not read the node's kernel log for OOM kills")
      with open(os.path.join(results_dir, "node_oom.log"), "w", encoding="utf-8") as f:
        f.write(oom_text + "\n")
      oom = classify_oom(oom_text)
      write_json(os.path.join(results_dir, "node_oom.json"), oom)
      extra["node_oom_events"], extra["node_oom_other"] = oom["node_oom_events"], oom["node_oom_other"]
      try:
        extra.update(node_rss(env))  # before cleanup: the daemons as the run left them
        if extra.get("sandbox_rss_mib") and int(env.get("AWAKE", "0")) > 0:
          # agentsim leaves ~AWAKE actors resident, so this is per active actor
          extra["mem_per_actor_mib"] = round(extra["sandbox_rss_mib"] / int(env["AWAKE"]), 1)
      except Exception as e:  # pylint: disable=broad-except
        log(f"daemon RSS not read: {type(e).__name__}: {e}")  # context only
    finally:
      if trial_logs:
        trial_logs.stop()  # a run that failed before collect_breakdown
      # agentsim leaves ~AWAKE actors running; stop them before the next trial.
      try:
        PROVENANCE["images"] = cluster_images(env)
      except Exception as e:  # pylint: disable=broad-except
        log(f"images not read: {type(e).__name__}: {e}")  # context only
      PROVENANCE["end_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
      write_json(os.path.join(results_dir, "provenance.json"), PROVENANCE)
      cleanup_trial(env, trial_id)
      if "before" in leak:
        time.sleep(15)  # let kubelet and the CNI finish tearing the workers down
        try:
          leak["after"] = node_snapshot(env)
          leak["raw_after"] = LAST_SNAPSHOT_RAW["text"]
          leak["leaked"] = leak_metrics(leak["before"], leak["after"])
        except Exception as e:  # pylint: disable=broad-except
          log(f"leak check after cleanup failed: {type(e).__name__}: {e}")
      if leak:
        write_json(leak_path, leak)
    if not os.path.exists(leak_path):
      return error(results_dir, "LEAK_CHECK", "no leak.json written")

  if os.path.exists(leak_path):  # always after a trial; optional for --parse-only
    with open(leak_path, encoding="utf-8") as f:
      leak = json.load(f)
    if not leak.get("before") or not leak.get("after"):
      return error(results_dir, "LEAK_CHECK",
                   "no node counts after the cleanup (see leak.json)")
    extra.update(leak_metrics(leak["before"], leak["after"]))
    extra["node_residue_objects"] = leak.get("node_residue_objects")

  if args.parse_only:
    # Live-only metrics (node RSS, OOM counts) can't be recomputed: keep them from the old summary,
    # except OOM counts, which are reclassified from node_oom.log (fetched with --fetch-oom).
    old = (load_json_file(os.path.join(results_dir, "summary.json")).get("metrics") or {})
    oom_log = os.path.join(results_dir, "node_oom.log")
    if not os.path.exists(oom_log) and args.fetch_oom:
      pv = load_json_file(os.path.join(results_dir, "provenance.json"))
      try:
        t0 = calendar.timegm(time.strptime(pv["start_utc"], "%Y-%m-%dT%H:%M:%SZ"))
        t1 = calendar.timegm(time.strptime(pv["end_utc"], "%Y-%m-%dT%H:%M:%SZ")) + 300
      except (KeyError, ValueError):
        return error(results_dir, "OOM_CHECK", "no start_utc/end_utc in provenance.json for --fetch-oom")
      ensure_nodeshell(env)
      text = node_oom_lines(env, t0, t1)
      if text is None:
        return error(results_dir, "OOM_CHECK", "could not read the node's kernel log for OOM kills")
      with open(oom_log, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    if os.path.exists(oom_log):
      oom = classify_oom(open(oom_log, encoding="utf-8").read())
      write_json(os.path.join(results_dir, "node_oom.json"), oom)
      extra["node_oom_events"], extra["node_oom_other"] = oom["node_oom_events"], oom["node_oom_other"]
    for k, v in old.items():
      if k not in extra and (k.startswith(("node_oom", "snapshot_plugin_", "atelet_", "sandbox_"))
                             or k in ("mem_per_actor_mib", "node_mem_used_gib", "sandbox_procs")):
        extra[k] = v

  if not os.path.exists(log_path):
    return error(results_dir, "PARSE", f"no agentsim log at {log_path}")
  result = parse_result(log_path)
  if not result:
    return error(results_dir, "PARSE", "agentsim printed no result JSON (see agentsim.log)")
  prewarm = result.get("prewarm") or {}
  failures = prewarm.get("failures") or 0
  registered, awake = int(env["REGISTERED"]), int(env["AWAKE"])
  left = (prewarm.get("actors") or registered) - failures
  if failures > 0.01 * registered or left < awake:
    return error(results_dir, "PREWARM",
                 f"{failures} pre-warm failures of {registered} registered; {left} actors left "
                 f"for {awake} awake")
  breakdown = summarize_breakdown(os.path.join(results_dir, "breakdown.jsonl"))
  stall_metrics, stall_detail = summarize_stalls(results_dir)
  if stall_detail:
    write_json(os.path.join(results_dir, "stalls.json"), stall_detail)
  extra.update(stall_metrics)
  rate = float(env["RATE"]) if args.mode == "fixed" else 0.0
  metrics = summarize(result, breakdown, extra, args.mode, rate)
  write_json(os.path.join(results_dir, "summary.json"), {"metrics": metrics})
  if args.mode in ("ramp", "fine-ramp"):
    write_json(os.path.join(results_dir, "levels.json"), {"levels": level_rows(result)})
  write_monitor(results_dir, breakdown, metrics)
  log(f"wrote {results_dir}/summary.json: "
      + ", ".join(f"{k}={metrics.get(k)}" for k in
                  ("resume_warm_p90_ms", "suspend_p90_ms", "achieved_ratio",
                   "error_refusal_pct", "ceiling_per_s", "fine_ceiling_per_s",
                   "fine_spec_ceiling_per_s") if k in metrics))
  return 0


if __name__ == "__main__":
  sys.exit(main())
