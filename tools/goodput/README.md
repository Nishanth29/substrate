# goodput

Task goodput for benchmark runs: how many **good** tasks the platform completes
per hour, per node, per vCPU and per GB of RAM, and why the bad ones were bad.
The task logs it reads are currently the sweperf benchmark's.

The benchmark only records facts; this script decides good or bad from them. A
rule change is an edit to `config.json` and a rerun, with no new benchmark run.
Follow-up work is tracked in #2837.

Python 3.10+, standard library only. GCS access goes through `gcloud storage`.

## Commands

```sh
# One run, local directory or a run path in GCS.
tools/goodput/goodput.py compute <run dir | gs://.../run_date=.../run_ts=.../run_tag=.../> [--out DIR]

# Every run under a prefix since a date. Writes locally only, mirroring the input layout.
tools/goodput/goodput.py backfill 'gs://<bucket>/<prefix>/runs/sweperf_*' --since 2026-10-17 --out /tmp/goodput

# A baselines[] entry from 1-user runs (median wall time of tasks without failures, task CEL).
tools/goodput/goodput.py baseline <1-user run>...
```

`--config` selects a different rules file (default `config.json` next to the script).

## Running it

- **Automated runs** will get goodput from a step after each test (not from the runner); that step is a follow-up (#2838).
- **Runs on your own cluster:** `tools/goodput/goodput.py compute <dest>/runs/<name>/run_date=<YYYY-MM-DD>/run_ts=<ts>/run_tag=<tag>/`,
  the directory `runner.py` writes `logs.txt`, `stats.jsonl` and `server_summary.json` to.

## Inputs

From the run directory the runner uploads:

| file | used for |
|---|---|
| `logs.txt` | task records, failure lines, the shutdown marker, startup failures |
| `stats.jsonl` | `trial_summary` (machine type, node count, allocatable cores and RAM) |
| `server_summary.json` | `metadata`: steady window, run start/end, test name, tag |
| `stats_history.csv` | steady window fallback, same rule as the runner |

The benchmark logs one `sweperf task record` line per finished task
(`benchmarking/README.md`, "Sweperf Log Lines"). Runs from before it (#2537)
aren't supported: `compute` fails on a `logs.txt` without task records.

## Rules

**Tasks.** Each task runs on a fresh actor.
- A finished task is one task record. Its wall time, in-sandbox time, slowest
  resume and TTFE, retries and first failure come from the record.
- A task that never finished is an actor with a failure line ("ResumeActor
  failed in step", "execute steps failed" or "SuspendActor failed") before the
  cut-off and no task record.
- Task records of another version, or missing a required field, are skipped and
  counted in `skipped_records`.

**Cut-off.** This is when the load stopped. Failures after it are teardown, not
part of a user's task. The first available of:
1. the benchmark's shutdown marker ("sweperf shutdown started");
2. boomer's "Recv quit message from master" line;
3. `trial_summary.run_end`;
4. `server_summary` `end_ts`.

Interrupted tasks log no record, so a record logged just after the cut-off still
counts; its end is taken as the cut-off.

**Good or bad.** A bad task gets the reasons of the first gate it fails.
- **G1, no failure.** A record with `failed: true` is bad (`resume_failed`,
  `execute_failed`, `suspend_failed` from its `fail_step`, else `failed`). A
  resume that failed and was retried successfully still makes the task bad, the
  same rule as the benchmark's `TaskCEL`/`TaskWallClock` failures. A task that
  never finished is always bad, named by its first failure line.
- **G2, responsiveness (strict mode).** The record's `resume_max_ms` ≤
  `resume_max_s` (`slow_resume`) and `ttfe_max_ms` ≤ `ttfe_max_s` (`slow_ttfe`).
  A task without `ttfe_max_ms` (no cycle got an asynchronous ack) is only held
  to the resume bound.
- **G3, slowness.** Wall time ÷ the baseline's `wall_ref_ms` ≤ `s_max` (`slow`).
  It only applies when `config.json` has a baseline for the run's runtime and
  machine type (`g3_basis: wall`); otherwise it's `skipped`.
- **Startup failures** ("sweperf on_start failed") are reported as attempts and
  as episodes: a user's consecutive startup failures until its next task record.
  They stay out of the ratio.

**Windows.**
- A finished task counts in the window it **ended** in. An unfinished task counts
  in the window of its first failure.
- `steady` is the steady window from `server_summary.json`, falling back to
  `stats_history.csv`, then the run's start/end, then the log's first/last line.
  It ends at the cut-off at the latest.
- `whole` runs from the run start to the cut-off.

## Outputs

`goodput.json`:
- `run`, `cutoff`, `steady_window`, `baseline`;
- per mode (`strict`, `relaxed`): `g2` (`applied` or `off`), `g3_basis`, and per
  window (`steady`, `whole`):
  - `tasks_done`, `tasks_good`, `tasks_bad`, `tasks_unfinished_bad`, `bad_reasons`,
    `startup_failure_attempts`, `startup_failure_episodes`;
  - `good_tasks_per_hr`, and per node/vCPU/GB-RAM hour;
  - `goodput_ratio_pct` = good ÷ (done + unfinished bad);
  - wall-time percentiles of good tasks;
  - with a baseline: wall ratio percentiles and `eta_infra` (reference CEL ÷ median wall);
- `totals` over the whole log;
- `tasks`: one row per task (actor, user, end, wall and CEL time, slowest resume
  and TTFE, retries, first failure, status and reasons per mode).

`goodput.jsonl`: one row per mode, shaped like `stats.jsonl` (`metric` =
`goodput_strict` / `goodput_relaxed`, string `measurements`). Whole-run values
carry a `whole_` prefix.

## Config (`config.json`)

- `modes`: thresholds per mode, placeholders until calibrated.
  - strict: G2 on, `ttfe_max_s` 1.0, `resume_max_s` 1.5, `s_max` 1.5;
  - relaxed: G2 off, `s_max` 2.0.
- `baselines`: `{runtime, machine_type, template, wall_ref_ms, task_cel_ref_ms}`
  entries, printed by `goodput.py baseline`. `template` is matched only when both
  the baseline and the run name one; the run's template comes from
  `trial_summary`, else from its task records.

## Tests

```sh
python3 tools/goodput/test_goodput.py
```

The tests build stand-in run directories and log lines inline (including copies
of real task log lines). They never contact GCS or a cluster.
