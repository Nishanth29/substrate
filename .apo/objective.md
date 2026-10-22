---
optimization:
  backend: optuna
  metrics:
    # Placeholder thresholds; set from the v000 3-run CV (min_improvement >= 2.3 x CV / sqrt(N)).
    - name: resume_warm_p90_ms
      goal: minimize
      min_improvement_pct: 5.0
      noise_tolerance_pct: 3.0
    - name: suspend_p90_ms
      goal: minimize
      min_improvement_pct: 5.0
      noise_tolerance_pct: 3.0
  constraints:
    - metric: achieved_ratio
      min: 0.98
    - metric: error_refusal_pct
      max: 0.5
    - metric: crashed_actors
      max: 0
    - metric: node_oom_events
      max: 0
    - metric: backlog_growth_excess
      max: 0
    - metric: windows_ok
      min: 3
    # Node objects left after the end-of-trial cleanup; start at 5, tune after v000.
    - metric: leaked_objects
      max: 5
    # Worst per-window drain after the close (wakes outrunning the window); tune after v000.
    - metric: drain_s_max
      max: 5
  allowed_file_scope: |
    pkg/objectstorage/**
    cmd/atelet/**
    cmd/snapshot-plugin/**
    cmd/ateom-gvisor/**
    cmd/ateom-microvm/**
    internal/ateomnet/**
    internal/ateomcgroup/**
    internal/tarutil/**
    !**/*_test.go
    !**/*.pb.go
    !benchmarking/**
    !.apo/**
---

# Objective: sweperf activation throughput (ResumeActor/s at N active actors)

Raise how many sweperf actors per second one bare-metal node can resume (and
suspend, one for one) while N actors stay active, with every gate met. This is
the throughput benchmark of
[go/agent-substrate-perf-benchmark-specs](http://go/agent-substrate-perf-benchmark-specs):
ResumeActor/s and SuspendActor/s >= 30, resume/suspend p95 <= 500 ms,
failures <= 5%, TTFE p95 <= 1 s. "Swap" in the load generator means one
suspend plus one resume (actor turnover), not memory swap.

## Workload

- **Load generator:** agentsim swap mode (aditya-shantanu/substrate-agents-tco,
  `--sweperf`): it keeps `AWAKE` actors resident and every 1-s tick parks k and
  wakes k, so ResumeActor/s = SuspendActor/s = k per second. A wake is
  `GET /health` through the router (no work in the actor). No script steps run
  in the actors.
- **Fleet:** `REGISTERED` sweperf actors (astropy-7336, 37 MB compressed / 61 MB
  uncompressed snapshot), `AWAKE` of them active. A pre-warm pass runs one
  sweperf step in every actor and parks it, so every measured wake restores
  the actor's own snapshot (warm), not the template's golden snapshot.
- **Rate:** a fixed rate `RATE` per second, 80% of this stack's v000 ceiling,
  held for `WINDOWS` measurement windows after a warm-up. The full ramp
  (`--mode ramp`) measures the ceiling itself; it is the acceptance test for a
  champion, not the trial metric.
- **TTFE:** about `TTFE_SAMPLE_PCT`% of wakes also run one sweperf step. TTFE
  is **step accepted**: wake start -> `/execute` returns 202. This is our
  reading of the spec's TTFE: it measures what Substrate controls (wake ->
  actor ready -> first command running). Step completion also includes the
  sweperf workload's own runtime, so it is reported as context
  (`ttfe_complete_*`) but not used for the spec check. Reported, not
  optimized, and kept out of the load.
- **Isolation:** each trial installs its build, purges every actor, recreates
  the template (fresh golden from this build) and refills the fleet. A
  snapshot written by a different build is never restored.

## Metrics

- `resume_warm_p90_ms`: warm-wake ResumeActor p90 (through the router) at
  `RATE`, from the merged samples of all windows. Primary.
- `suspend_p90_ms`: SuspendActor p90 at `RATE`.
- Constraints: `achieved_ratio` (achieved / target rate) >= 0.98,
  `error_refusal_pct` < 0.5, no crashed actors, no node OOM kills, no backlog
  growth beyond one tick (`backlog_growth_excess`), all windows measured, no
  more than 1% pre-warm failures (else the trial errors out), and no node leak:
  `leaked_objects` (ate cgroups + actor netns + tap/veth links + actor mounts +
  runtime processes on the metal node after the end-of-trial cleanup, minus
  before the run) <= 5, so a change that leaks fails instead of looking fast.
  Node-wide totals, actor-dir files/KB and neighbour entries are reported as
  `leaked_*` context (and in `leak.json`).
- Reported for context, not optimized: cold-wake resume p90, warm-wake share,
  TTFE p50/p95 (accept; `ttfe_complete_*` as context), plugin and worker stage
  medians (restore prep, network, VMM or sandbox create, restore, teardown,
  snapshot download and upload), pre-warm time, snapshot-plugin and atelet RSS
  and peak RSS at the end of the run, and the spec view (resume and
  suspend p95 vs 500 ms, failures vs 5%, TTFE accept p95 vs 1 s).

## Allowed changes

The node data plane on the activation path: the snapshot plugin and object
storage (`pkg/objectstorage`, `cmd/snapshot-plugin`), atelet, the gVisor and
microVM workers, worker networking (`internal/ateomnet`), cgroups and tar
handling. The benchmark, the load generator, the cluster manifests and
`.apo/` are out of scope, so throughput can't be "improved" by changing the
load or the hardware. The router's parked-request budget stays at its default:
raising it turns refusals into slow resumes and only hides the problem.

Each winning change must be small and behind a flag or env knob, so it can
become one upstream PR.

## Where the time goes today (start here)

From the Oct 9 activation campaign (agents-tco, nano agents, same node type);
the base build already has that campaign's patches (pooled GCS clients and
buffers, 4 MiB x 12 download ranges, CRNG reseed retry, no tar fsync, gVisor
netns pool, `--shared-root`, lean gVisor teardown):

- **microVM** breaks at ~20/s with the CPUs ~35% busy: every per-VM step slows
  at once (tap setup 2 -> 1,300 ms, VM restore 140 -> 1,500, restore prep
  10 -> 1,000, teardown 230 -> 1,460). Candidates:
  - `internal/ateomnet` / `cmd/ateom-microvm/net.go`: keep the tap with a
    pooled network namespace (5-6 netlink round trips per tap today).
  - `cmd/ateom-microvm` cleanup (`kata/cleanup_linux.go`): scans every
    `/proc/*/cmdline` and mountinfo 3 times per swap; skip when the actor's
    cgroup was killed cleanly.
  - drop the guest page cache before `vm.snapshot` (the 128 MiB image is
    mostly page cache): fewer bytes everywhere.
  - asynchronous, batched teardown after the snapshot is listed.
- **gVisor** passes ~9/s: `pause_create` grows 90 -> 700 ms (mount-namespace
  lock in runsc's chroot setup), cgroup v2 attach writes, the sentry restore.
  Candidates: fewer runsc execs per teardown (kill/wait/state/delete per
  container), fewer mounts per sandbox.
- **Both:** same-node resume from a retained local copy of the last uploaded
  snapshot (no download), a zstd dictionary trained on the golden image,
  remaining allocation churn in `pkg/objectstorage`.

Already tried and rejected in that campaign (don't retry): composite uploads
in 8 MiB parts, a denser zstd level, GOGC/GOMEMLIMIT tuning,
`runsc --ignore-cgroups`, reusable cgroup slots.
