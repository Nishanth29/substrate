#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Task goodput for benchmark runs: good tasks per hour, per node/vCPU/GB, and why tasks were bad.

The benchmark only records facts; this script decides good or bad from them, so
a rule change is a config edit and a rerun. See README.md.

  goodput.py compute <run dir | gs://.../run_ts=.../run_tag=.../> [--out DIR]
                     [--baseline FILE | --baseline-run <1-user run>...]   (the run to score goes first)
  goodput.py backfill <gs://.../runs/sweperf_*> --since YYYY-MM-DD --out DIR [--jobs N] [--baseline FILE]
  goodput.py baseline <1-user run dir | gs path>...
"""

import argparse
import concurrent.futures
import json
import re
import statistics
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import classify  # noqa: E402
import metrics  # noqa: E402
import parse  # noqa: E402

DEFAULT_CONFIG = Path(__file__).resolve().parent / "config.json"
BASELINE_VERSION = 1
RUN_FILES = ("logs.txt", "stats.jsonl", "server_summary.json", "stats_history.csv")
RUN_PATH = re.compile(r"/runs/(?P<test>[^/]+)/run_date=(?P<date>[^/]+)/run_ts=(?P<ts>\d+)/run_tag=(?P<tag>[^/]+)/?$")


def load_config(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        config = json.load(f)
    try:
        classify.validate_config(config)
    except ValueError as e:
        raise ValueError(f"{path}: {e}") from e
    return config


def runtime_of(test_name: str, trial: dict[str, Any]) -> str:
    """trial_summary's sandbox_class, else from the test name (microVM tests end in _microvm or _uvm)."""
    sc = str(trial.get("sandbox_class") or "").lower()
    if sc:
        return sc
    t = test_name.lower()
    return "microvm" if t.endswith(("_microvm", "_uvm")) else "gvisor"


def steady_window(meta: dict[str, Any], run_dir: Path, log_ts: list[float],
                  cut: Optional[float]) -> tuple[tuple[float, float], str]:
    """server_summary's steady window, else the runner's rule on stats_history.csv,
    else the run's start..end, else the log's first..last line before the cut-off.
    The window ends at the cut-off at the latest."""
    lo, hi = parse.num(meta.get("steady_start_ts")), parse.num(meta.get("steady_end_ts"))
    start, end = parse.num(meta.get("start_ts")) or 0.0, parse.num(meta.get("end_ts")) or 0.0
    if lo and hi and hi > lo:
        window, source = (lo, hi), "server_summary"
    elif hist := parse.steady_from_history(run_dir / "stats_history.csv", start, end):
        window, source = hist, "stats_history"
    elif end > start > 0:
        window, source = (start, end), "run"
    else:
        ts = [t for t in log_ts if cut is None or t <= cut]
        window, source = ((min(ts), max(ts)) if ts else (0.0, 0.0)), "log"
    if cut is not None and cut > window[0]:
        window = (window[0], min(window[1], cut))
    return window, source


@dataclass
class Run:
    """One run directory, read and split into tasks: what compute and baseline_refs share."""
    log: parse.RunLog
    meta: dict[str, Any]
    trial_row: dict[str, Any]
    trial: dict[str, Any]
    cut: Optional[float]
    cut_source: str
    tasks: list[classify.Task]
    startup: list[parse.Event]


def load_run(run_dir: Path) -> Run:
    """Reads a run and builds its tasks up to the cut-off. Raises for a run without task records."""
    log = parse.read_run(run_dir / "logs.txt")
    if not log.records:
        raise ValueError(f"no '{parse.MSG_TASK_RECORD}' lines in {run_dir / 'logs.txt'}; "
                         "runs before the task record (#2537) aren't supported")
    stats = parse.read_stats(run_dir / "stats.jsonl")
    meta = parse.read_server_metadata(run_dir / "server_summary.json")
    trial_row = stats.get("trial_summary") or {}
    trial = trial_row.get("measurements") or {}
    cut, cut_source = classify.cutoff(log.events, parse.epoch(trial.get("run_end")), parse.num(meta.get("end_ts")))
    tasks, startup = classify.build_tasks(log, cut)
    return Run(log, meta, trial_row, trial, cut, cut_source, tasks, startup)


def compute(run_dir: Path, config: dict[str, Any], identity: Optional[dict[str, str]] = None,
            baseline: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """goodput.json content for one run directory. identity (from a gs:// run path) wins over the files.

    baseline is {"source": ..., "refs": {field: value}} from --baseline or
    --baseline-run; without one, baseline_ratio checks are skipped.
    """
    run = load_run(run_dir)
    log, meta, trial_row, trial, cut, tasks, startup = (run.log, run.meta, run.trial_row, run.trial, run.cut,
                                                        run.tasks, run.startup)
    ident = identity or {}
    test_name = ident.get("test_name") or meta.get("test_name") or trial_row.get("test_name", "")
    runtime = runtime_of(test_name, trial)
    machine_type = str(trial.get("machine_type") or "")
    templates = {r.template for r in log.records if r.template}
    template = str(trial.get("template") or (templates.pop() if len(templates) == 1 else ""))

    startup_ts = [s.t for s in startup]
    episode_ts = classify.startup_episodes(startup, log.records)
    log_ts = [r.t for r in log.records] + [ev.t for ev in log.events]
    steady, steady_source = steady_window(meta, run_dir, log_ts, cut)
    whole = (parse.num(meta.get("start_ts")) or min(log_ts), cut if cut is not None else max(log_ts))
    capacity = {k: parse.num(trial.get(k)) for k in ("node_count", "allocatable_cores", "allocatable_ram_gb")}
    refs = dict((baseline or {}).get("refs") or {})

    task_rows = []
    for t in tasks:
        r = t.record
        task_rows.append({"actor": t.actor, "user": r.user if r else None, "end": t.end,
                          "wall_ms": r.wall_ms if r else None, "cel_ms": r.cel_ms if r else None,
                          "resume_max_ms": r.resume_max_ms if r else None, "ttfe_max_ms": r.ttfe_max_ms if r else None,
                          "retries": r.retries if r else None, "failed": r.failed if r else True,
                          "first_failure_t": t.first_failure_t, "fail_step": t.fail_step, "fail_cycle": t.fail_cycle,
                          "status_by_mode": {}, "reasons_by_mode": {}})
    modes: dict[str, Any] = {}
    for mode_name, mode in sorted(config["modes"].items()):
        rows = []
        for t, row in zip(tasks, task_rows):
            status, reasons = classify.classify(t, mode, refs)
            row["status_by_mode"][mode_name] = status
            row["reasons_by_mode"][mode_name] = reasons
            values = {f: getattr(t.record, f) for f in classify.CHECK_FIELDS} if t.record else {}
            rows.append({**row, "status": status, "reasons": reasons, "values": values})
        applied, skipped = classify.split_checks(mode, refs)
        fields = classify.ratio_fields(mode, refs)
        modes[mode_name] = {
            "checks_applied": applied,
            "checks_skipped": skipped,
            "steady": metrics.window_metrics(rows, startup_ts, episode_ts, steady, capacity, refs, fields),
            "whole": metrics.window_metrics(rows, startup_ts, episode_ts, whole, capacity, refs, fields),
        }

    finished = sum(1 for t in tasks if t.finished)
    return {
        "version": 2,
        "run": {"test_name": test_name, "tag": ident.get("tag") or meta.get("tag") or trial_row.get("tag", ""),
                "run_ts": str(ident.get("run_ts") or meta.get("start_ts") or ""),
                "run_date": ident.get("run_date", ""), "data_timestamp": meta.get("data_timestamp", ""),
                "runtime": runtime, "machine_type": machine_type, "template": template,
                "record_version": parse.RECORD_VERSION, **capacity},
        "cutoff": {"ts": cut, "source": run.cut_source},
        "steady_window": {"start_ts": steady[0], "end_ts": steady[1], "source": steady_source},
        "baseline": {"source": (baseline or {}).get("source"), "refs": refs},
        "modes": modes,
        "totals": {"tasks_finished": finished, "tasks_unfinished_bad": len(tasks) - finished,
                   "startup_failure_attempts": len(startup), "startup_failure_episodes": len(episode_ts),
                   "skipped_records": log.skipped_records, "actors": len({t.actor for t in tasks})},
        "tasks": task_rows,
    }


# --- Baselines ------------------------------------------------------------------

def baseline_refs(run_dirs: list[Path]) -> tuple[dict[str, float], int]:
    """Reference values from 1-user runs, and how many tasks they come from.

    Pools the finished, non-failed tasks of all the runs (built like compute
    builds them, up to the cut-off) and takes, for each check field, the median
    over the tasks that logged it. A field no task logged is left out. Applies
    no checks. Raises when the runs have no clean task.
    """
    clean = [t.record for d in run_dirs for t in load_run(d).tasks if t.finished and not t.record.failed]
    if not clean:
        raise ValueError("no finished task without failures in the baseline runs")
    refs = {}
    for f in classify.CHECK_FIELDS:
        values = [getattr(r, f) for r in clean if getattr(r, f) is not None]
        if values:
            refs[f] = round(float(statistics.median(values)), 3)
    return refs, len(clean)


def load_baseline(path: Path) -> dict[str, Any]:
    """A saved baseline (from the baseline command) as {"source", "refs"}."""
    with open(path, encoding="utf-8") as f:
        j = json.load(f)
    refs = j.get("refs") if isinstance(j, dict) else None
    if not isinstance(j, dict) or j.get("version") != BASELINE_VERSION or not isinstance(refs, dict):
        raise ValueError(f"{path}: not a version {BASELINE_VERSION} baseline with a 'refs' object")
    for k, v in refs.items():
        if k not in classify.CHECK_FIELDS or not parse.is_number(v):
            raise ValueError(f"{path}: refs.{k} must be a check field with a number")
    return {"source": f"file:{path}", "refs": refs}


def baseline_from_runs(args: list[str], tmp: Path) -> dict[str, Any]:
    """{"source", "refs"} from paired 1-user runs (directories or gs:// paths)."""
    dirs = [resolve_run(arg, tmp / f"baseline{i}")[0] for i, arg in enumerate(args)]
    refs, _ = baseline_refs(dirs)
    return {"source": "run:" + ",".join(args), "refs": refs}


def jsonl_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    """stats.jsonl-shaped rows (string measurements), one per mode; whole-run values get a whole_ prefix."""
    run = result["run"]
    rows = []
    for mode_name, m in sorted(result["modes"].items()):
        meas = {"checks_applied": ",".join(m["checks_applied"]), "checks_skipped": ",".join(m["checks_skipped"]),
                "baseline_source": result["baseline"]["source"] or "", "runtime": run["runtime"],
                "machine_type": run["machine_type"], "template": run["template"]}
        for win, prefix in (("steady", ""), ("whole", "whole_")):
            for k, v in m[win].items():
                if k == "bad_reasons":
                    meas.update({f"{prefix}bad_{reason}": str(n) for reason, n in v.items()})
                elif v is not None:
                    meas[prefix + k] = str(v)
        rows.append({"timestamp": run["data_timestamp"], "tag": run["tag"], "test_name": run["test_name"],
                     "metric": f"goodput_{mode_name}", "measurements": meas})
    return rows


def write_outputs(result: dict[str, Any], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "goodput.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)
        f.write("\n")
    with open(out / "goodput.jsonl", "w", encoding="utf-8") as f:
        f.writelines(json.dumps(row, sort_keys=True) + "\n" for row in jsonl_rows(result))


def summary_line(result: dict[str, Any]) -> str:
    run = result["run"]
    parts = [f"{run['test_name'] or '?'} {run['run_date']} {run['runtime']} {run['machine_type']}"]
    for mode_name, m in sorted(result["modes"].items()):
        s = m["steady"]
        parts.append(f"{mode_name}: good {s['tasks_good']}/{s['tasks_done']} done (+{s['tasks_unfinished_bad']} "
                     f"unfinished bad), {s['good_tasks_per_hr']} good/h, ratio {s['goodput_ratio_pct']}%")
    return " | ".join(parts)


# --- GCS (through gcloud) -------------------------------------------------------

def gcloud(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["gcloud", "storage", *args], capture_output=True, text=True, check=False)


def fetch_run(gs_dir: str, dest: Path) -> Path:
    """Copies a run's input files from GCS; only logs.txt is required."""
    dest.mkdir(parents=True, exist_ok=True)
    base = gs_dir.rstrip("/") + "/"
    for name in RUN_FILES:
        gcloud("cp", "-q", base + name, str(dest / name))
    if not (dest / "logs.txt").exists():
        raise FileNotFoundError(f"no logs.txt under {base}")
    return dest


def identity_from_path(path: str) -> dict[str, str]:
    """test_name, run_date, run_ts and tag from a .../runs/<test>/run_date=/run_ts=/run_tag= path."""
    m = RUN_PATH.search(path.rstrip("/"))
    if not m:
        return {}
    return {"test_name": m["test"], "run_date": m["date"], "run_ts": m["ts"], "tag": m["tag"]}


def resolve_run(arg: str, tmp: Path) -> tuple[Path, dict[str, str]]:
    if arg.startswith("gs://"):
        return fetch_run(arg, tmp / "run"), identity_from_path(arg)
    return Path(arg), identity_from_path(str(Path(arg).resolve()))


def list_runs(prefix: str, since: str) -> list[str]:
    """Run directories under prefix (may contain *) with run_date >= since."""
    res = gcloud("ls", prefix.rstrip("/") + "/**/logs.txt")
    if res.returncode != 0:
        raise RuntimeError(f"gcloud storage ls failed: {res.stderr.strip()[:300]}")
    runs = set()
    for line in res.stdout.split():
        d = line[: -len("logs.txt")]
        ident = identity_from_path(d)
        if ident and ident["run_date"] >= since:
            runs.add(d)
    return sorted(runs)


# --- Commands -------------------------------------------------------------------

def cmd_compute(a: argparse.Namespace) -> int:
    config = load_config(Path(a.config))
    with tempfile.TemporaryDirectory() as tmp:
        try:
            baseline = None
            if a.baseline:
                baseline = load_baseline(Path(a.baseline))
            elif a.baseline_run:
                baseline = baseline_from_runs(a.baseline_run, Path(tmp))
        except (OSError, ValueError) as e:
            print(f"baseline: {e}", file=sys.stderr)
            return 1
        run_dir, ident = resolve_run(a.run, Path(tmp))
        result = compute(run_dir, config, ident, baseline)
        out = Path(a.out) if a.out else (Path("goodput-out") if a.run.startswith("gs://") else run_dir)
        write_outputs(result, out)
    print(summary_line(result))
    print(f"wrote {out / 'goodput.json'} and {out / 'goodput.jsonl'}")
    return 0


def backfill_one(gs_dir: str, config: dict[str, Any], out_root: Path,
                 baseline: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Computes one run under out_root (mirroring the input layout); returns a summary row."""
    ident = identity_from_path(gs_dir)
    out = (out_root / ident["test_name"] / f"run_date={ident['run_date']}" / f"run_ts={ident['run_ts']}"
           / f"run_tag={ident['tag']}")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            result = compute(fetch_run(gs_dir, Path(tmp)), config, ident, baseline)
        write_outputs(result, out)
    except Exception as e:  # pylint: disable=broad-except
        return {"run": gs_dir, "ok": False, "error": f"{type(e).__name__}: {e}"}
    return {"run": gs_dir, "ok": True, "line": summary_line(result),
            **{f"{k}_steady": v["steady"] for k, v in result["modes"].items()}}


def cmd_backfill(a: argparse.Namespace) -> int:
    config = load_config(Path(a.config))
    try:
        baseline = load_baseline(Path(a.baseline)) if a.baseline else None
    except (OSError, ValueError) as e:
        print(f"baseline: {e}", file=sys.stderr)
        return 1
    out_root = Path(a.out)
    runs = list_runs(a.prefix, a.since)
    print(f"{len(runs)} runs since {a.since}", flush=True)
    out_root.mkdir(parents=True, exist_ok=True)
    failed = 0
    with open(out_root / "backfill_summary.jsonl", "w", encoding="utf-8") as summary, \
            concurrent.futures.ThreadPoolExecutor(max_workers=max(1, a.jobs)) as pool:
        for row in pool.map(lambda d: backfill_one(d, config, out_root, baseline), runs):
            if row["ok"]:
                print(row.pop("line"), flush=True)
            else:
                failed += 1
                print(f"FAILED {row['run']}: {row['error']}", file=sys.stderr, flush=True)
            summary.write(json.dumps(row, sort_keys=True) + "\n")
    print(f"done: {len(runs) - failed} ok, {failed} failed; outputs under {out_root}")
    return 1 if failed else 0


def cmd_baseline(a: argparse.Namespace) -> int:
    """Prints a baseline file from 1-user runs: per check field, the median over
    the clean tasks of all the runs pooled (see baseline_refs)."""
    with tempfile.TemporaryDirectory() as tmp:
        try:
            dirs = [resolve_run(arg, Path(tmp) / str(i))[0] for i, arg in enumerate(a.runs)]
            refs, tasks = baseline_refs(dirs)
        except (OSError, ValueError) as e:
            print(f"baseline: {e}", file=sys.stderr)
            return 1
    print(json.dumps({"version": BASELINE_VERSION, "refs": refs, "runs": len(a.runs), "tasks": tasks,
                      "sources": a.runs}, indent=2, sort_keys=True))
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=str(DEFAULT_CONFIG), help="modes and their checks (default: %(default)s)")
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("compute", help="one run: write goodput.json and goodput.jsonl")
    c.add_argument("run", help="run directory or gs:// run path")
    c.add_argument("--out", help="output directory (default: the run directory; ./goodput-out for gs://)")
    cb = c.add_mutually_exclusive_group()
    cb.add_argument("--baseline", help="saved baseline file (from the baseline command)")
    cb.add_argument("--baseline-run", nargs="+", metavar="RUN",
                    help="paired 1-user run directories or gs:// paths, pooled into one baseline")
    b = sub.add_parser("backfill", help="every run under a gs:// prefix since a date (local output only)")
    b.add_argument("prefix", help="e.g. gs://<bucket>/<prefix>/runs/sweperf_*")
    b.add_argument("--since", required=True, help="first run_date to include, YYYY-MM-DD")
    b.add_argument("--out", required=True, help="local output root (mirrors the input layout)")
    b.add_argument("--jobs", type=int, default=8, help="runs fetched and computed in parallel")
    b.add_argument("--baseline", help="saved baseline file applied to every run")
    s = sub.add_parser("baseline", help="print a baseline file from 1-user runs")
    s.add_argument("runs", nargs="+", help="run directories or gs:// run paths")
    a = p.parse_args(argv)
    return {"compute": cmd_compute, "backfill": cmd_backfill, "baseline": cmd_baseline}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
