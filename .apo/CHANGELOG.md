# Unified Historical Ledger

## [2026-10-11T02:40:00Z] [INITIALIZATION]

Initialized the sweperf activation-throughput workspace (ResumeActor/s at N
active actors; agentsim swap mode on the sweperf astropy template) with two
stacks: microVM on substrate-metal (us-east4-c) and gVisor on apo-gvisor
(us-central1), one c3-standard-192-metal node each. Base build: upstream main
plus the agents-tco Oct 9 campaign patches and the object-storage env knobs.
The baseline `v000` is not recorded yet. Supersedes the glutton throughput
workspace on `apo/throughput`.

## [2026-10-11T05:10:00Z] [BOOTSTRAP]

Phase 1 bootstrap of stack-microvm (substrate-metal, us-east4-c). Use this as
the checklist for a cluster that already ran an older Substrate (and for the
gVisor cluster):

- Node pool: c3-standard-192-metal was stocked out in us-east4-c (Oct 11), so
  stack-microvm uses the existing c3-highmem-192-metal pool `microvm-metal`.
  Metal pools carry the taint `ate.dev/sandboxClass=<class>:NoSchedule`.
- `ATE_CREDENTIAL_PROVIDER='{"name":"k8s.io"}'` (set-env.sh, as in CI): required
  by `--deploy-ate-system` and `--deploy-atenet`.
- Postgres schema owner: a bundled Postgres from before the separate owner and
  read/write roles has schema `substrate` owned by `postgres`; setup refuses it.
  Back up (pg_dump), then `DROP SCHEMA substrate CASCADE`; the install recreates
  the roles and schema. The harness pins psql to that schema (`PG_SCHEMA`),
  since an old `public.actors` can shadow it.
- Pod-certificate controller: the per-trial install (atelet, apiserver, atenet)
  doesn't update it, and an old one never issues the `postgres.podcert.ate.dev`
  certs. Run `hack/install-ate.sh $INSTALL_FLAGS --deploy-ate-system` once (a
  first SandboxConfig admission denial right after the policy update passed on
  retry).
- WorkerPool: an old-format `benchmark-ateom` (no `spec.sandboxClasses`) fails
  validation; delete it and let deploy.sh recreate it. pin_workers also sets
  `sandboxClasses`.
- SandboxConfig: an old-format `microvm` config (no `defaultVersion`) leaves the
  golden waiting; replace it with `manifests/microvm/sandboxconfig-microvm.yaml.tmpl`
  rendered for the stack bucket (kata-assets sha256 checked). run_experiment.py
  now fails fast with PREFLIGHT_SANDBOXCONFIG.
- gVisor (apo-gvisor, fresh cluster, Oct 11): no stale objects, so only the
  create flags (Workload Identity, Dataplane V2, the pod-certificate beta APIs
  at creation), `setup-gcp create bucket` + `create iam`, the lifecycle rule and
  one `--deploy-ate-system` were needed; the smoke run passed first time. Run
  every gVisor command with the gVisor env (its own kubeconfig and gcloud
  config); see the 05:50 incident.

## [2026-10-11T05:20:00Z] [KNOWN_ISSUE]

Possible upstream bug (report later): actors assigned to a node that no longer
exists can't be deleted, so the template delete fails. Seen on substrate-metal
(d1b2371d + patches):

1. Create an ActorTemplate whose golden actor gets assigned to node A (here the
   golden crashed first: GoldenActorCrashed).
2. Scale A's node pool to 0, so node A is deleted.
3. `kubectl ate delete actor-template <name> -a <atespace>` fails with
   `workflow failed at step DeleteGoldenActor ... CallAteletTerminate ... no
   atelet pod found on node "<A>"`. A later create of the same name fails with
   AlreadyExists (deploy.sh hides the delete error).

Expected: terminating an actor whose node is gone succeeds (nothing to stop).
Workaround in run_experiment.py (remove_template): delete the template with
errors visible; on failure purge its golden actor rows (atespace ate-golden,
name = template UID) and retry once; else error.json phase PREFLIGHT_TEMPLATE.

## [2026-10-11T05:50:00Z] [INCIDENT]

Both stacks shared `.apo/.kubeconfig`. Sourcing set-env.sh for the gVisor
bootstrap switched its current-context to apo-gvisor while the microVM ramp ran;
deploy.sh (ko apply, kubectl-ate and kubectl without a context) then created the
ramp's WorkerPool on apo-gvisor. The ramp was killed and the WorkerPool removed
(no template or actors reached apo-gvisor). Fix: set-env.sh uses one kubeconfig
and one gcloud config per stack (`.kubeconfig-<cluster>`,
`.gcloud-config-<cluster>`), and run_experiment.py checks the current-context
against KUBECTL_CONTEXT at preflight and before deploy.sh (PREFLIGHT_CONTEXT).
Rule: never run two stacks' commands from the same kubeconfig.
## [2026-10-22T03:50:00Z] [PORT]

Ported the workspace from the old base (`d1b2371d` + Aditya's patches) to plain
upstream main `3e2797bc` (316 commits; branch `apo/mvm-throughput`). Each item
was checked against the code:

- Renames (#2254, #2540, #2981): `cmd/ateom-{gvisor,microvm}` ->
  `cmd/ateworker-*`, `cmd/snapshot-plugin` -> `cmd/objectstore-plugin` (atelet
  sidecar container and process `objectstore-plugin`), `internal/ateomnet` ->
  `internal/ateworker/actornet` + `internal/netns`, `internal/ateomcgroup` ->
  `internal/ateworker/cgroup`. objective.md `allowed_file_scope` and build.sh
  follow; build.sh now fails (SCOPE_PACKAGE_MISSING) instead of silently
  skipping a missing scope package, and its test-retry grep follows the new
  module path `go.ate.dev/substrate` (#2181).
- Install (#2718): `hack/install-ate.sh` is only `go run ./cmd/ate-setup "$@"`.
  Trials run `install-ate.sh $INSTALL_FLAGS deploy {atelet,apiserver,atenet}`
  (INSTALL_FLAGS `--cluster-size=size0 --podcert-workers-per-signer=16` are
  still global ate-setup flags). ate-setup reads its settings from the env, so
  set-env.sh now exports SNAPSHOT_LOCATION=`gs://$BUCKET_NAME/` (required unless
  the PerTemplateSnapshotLocation preview gate is on, #2544; the template's
  storageLocation is ignored), SNAPSHOT_TRANSFER_GSA_NAME=ate-snapshot-transfer
  (#3093), ATE_API_AUTHZ_BOOTSTRAP_OWNERS (ate-client + agentsim, authz is
  enforced, #2591/#2942), ATE_INGRESS_ALLOWED_SPIFFE_IDS (agentsim; the router
  is mTLS-only on 443, #2977) and NO_DEV_ENV=1. VERSION=apo is gone: the
  substrate-version label and atelet-<version> DaemonSets were removed (#1812);
  atelet is DaemonSet `atelet` on every node.
- deploy.sh: the WorkerPool and its Deployment are `benchmark-worker` (pods
  `ate.dev/worker-pool=benchmark-worker`); atecontroller adds the
  `ate.dev/sandboxClass=<class>` toleration itself. The sweperf template moved
  to `sweperf-astropy-7336` (generated) with fixed limits 2 CPU / 4Gi; actor
  limits are now mandatory (#2285, #2769) and booked against worker capacity,
  so 4Gi x AWAKE=500 can't fit the node. The harness now uses deploy.sh only
  for the pool (`WORKLOAD_TEMPLATES=" "`) and creates the template itself from
  `manifests/sweperf-template.yaml.tmpl` (same image/command as upstream) with
  ACTOR_CPU=1 / ACTOR_MEMORY=2Gi, which mirror the old template's
  limit-less defaults (kata 2 GiB guest), then waits for the golden.
- Golden actors live in the template's atespace (#2543), not ate-golden
  (remove_template's purge fallback follows).
- Logs: worker breakdown keys are `ateworker.actor.restore.duration.*`
  (phaselog); upstream's reseed time is only in the worker's "Actor restore
  phases" record (now saved as restore_phases.jsonl and used for the stall
  split when the breakdown carries no parts); lever builds' parts are
  agent_dial/reseed_rpc/probe; the "guest CRNG reseed failed; retrying" record
  is gone (single 60 s call, #2743), so reseed_retries counts lever A's
  "APO reseed attempt" records with attempt > 1. The worker's own total is
  `stage_restore_worker_total_*` (it used to merge into atelet's total).
  atelet restore records carry `ate.snapshot.kind` (local / latest / boot):
  the summary counts restores per kind (`restores_kind_*`,
  `restore_local_pct`) with download/manifest_fetch/total p50/p90 per kind.
- Node checks: actor netns `worker-actor:<uid>` (was `ateom-actor:`), worker
  cgroup leaf `worker` (was `ateom`/`ateomchv`; the cgroup regex is now
  anchored under kubepods), OOM task names `objectstore-plu*`/`ateworker*`,
  RSS of `objectstore-plugin`; `/var/lib/ate/snapshots` dirs/KB reported as
  context (per-actor local snapshots, #2746).
- Lifecycle: agentsim keeps `--lifecycle-mode=suspend` explicit (now
  LIFECYCLE_MODE, locked). SuspendActor with no options is asynchronous
  upstream (#2805), pause is gone (#2813), paused/suspended resume paths are
  merged (#2967); the mode is recorded in summary.json `metadata`.
- agentsim: fine-ramp ported on local branch `fine-ramp-ate-main` of
  substrate-agents-tco-fineramp (7abf9d9: build against main, drop the pause
  lifecycle, Resources limits; c4ef15c: dial atenet-router over SPIFFE mTLS on
  https://atenet-router.ate-system.svc:443 with the podidentity bundle and the
  servicedns CA, as upstream PR #3089 does). The Job's namespace carries
  `ate.dev/api-access=true` (ate-system denies ingress by default, #2522).
- Storage knobs: the Study K env knobs (ATE_GCS_*, ATE_ZSTD_*, GOGC) only
  existed in Aditya's patch; upstream has none, so STORAGE_KNOBS is empty.
- SandboxConfig preflight also fails when the microvm assets are not in
  `<BUCKET_NAME>-assets` (#3037).
- RATE=3 (microvm) is provisional, from the old base: re-derive it from a v000
  ramp on this base.

## [2026-10-22T03:52:00Z] [ENGINE_CONTRACT]

Aligned .apo/ with the gke_apo engine as implemented (src/apo, not only the
README, which disagrees with the code in places):

- stacks.json: `{"stacks": [{"id": ...}]}` is the only shape the loader
  accepts; status in stacks.json is not used for scheduling and there is no
  disabled status (only `draining` in results/state.json). stack-microvm is the
  only entry in `stacks`; stack-gvisor (apo-gvisor, stocked out) moved to a
  `disabled_stacks` list the engine never reads. Entries carry the engine's
  `zone` and our `compute_zone`/`compute_region`/`project_id`, plus `stack_id`
  (the engine does not export `id` to stack.env). Values avoid `"`/`$`, since
  stack.env is written unescaped.
- stack.env: the engine writes it at the trial worktree root (every key
  uppercased); set-env.sh sources `<root>/stack.env`, then `.apo/stack.env`,
  and maps ZONE to COMPUTE_ZONE. build.sh and run_experiment.sh source
  set-env.sh themselves, since the merge gate and the evolve evaluator run them
  without it.
- summary.json: `{"metrics": {flat numbers}, "metadata": {...}}`; the engine
  reads only `metrics`, metadata records lifecycle/durability mode, agentsim
  commit/branch/image, workspace head and upstream base, stack, template and
  actor limits.
- error.json: adds `reason` (what the engine reads, then `message`) and
  `status: FAILED`; phase/error_type stay for us.
- objective.md frontmatter passes the ObjectiveParser rules (thresholds stay
  placeholders until the v000 3-run CV). Baseline: run
  `.apo/run_experiment.sh .apo/results/raw/v000`, then
  `apo hpo complete --hypothesis-id v000 --workspace . --results-dir .apo/results/raw/v000`
  (the Optuna DB is .apo/optuna_study.db; `orchestrator init` also seeds v000
  if that dir exists).

## [2026-10-22T03:43:00Z] [SETUP_GCP]

stack-microvm (project nishanth-gke-dev, number 643986858598; cluster
substrate-metal, us-east4-c; GKE 1.35.6-gke.1250001 with Workload Identity
and the podcertificate/clustertrustbundle beta APIs enabled at creation; works
although setup-gcp docs now ask for 1.36+). Run with
PROJECT_ID/PROJECT_NUMBER/GCE_REGION=us-east4/BUCKET_NAME/SNAPSHOT_TRANSFER_GSA_NAME
set and the stack's gcloud config:

- `go run ./tools/setup-gcp create bucket`: snapshot bucket unchanged (US-EAST4,
  uniform access); created `gs://snapshot-substrate-metal-nishanth-gke-dev-assets`
  (US-EAST4, uniform bucket-level access) for micro-VM assets (#3037).
- `go run ./tools/setup-gcp create iam`:
  - created GSA `ate-snapshot-transfer@nishanth-gke-dev.iam.gserviceaccount.com`
    (#3093) and granted `roles/iam.serviceAccountTokenCreator` on it to the WI
    principal `.../subject/ns/ate-system/sa/ate-api-server`;
  - on `gs://snapshot-substrate-metal-nishanth-gke-dev`: transfer GSA
    `roles/storage.objectAdmin` + `roles/storage.bucketViewer`;
  - on the `-assets` bucket: atelet WI principal `roles/storage.objectViewer`;
  - project `roles/artifactregistry.reader` for the atelet WI principal and
    `643986858598-compute@developer.gserviceaccount.com` (already present).
- Not removed (setup-gcp only adds; removal is optional hardening, see open
  issues): atelet WI principal objectAdmin + bucketViewer, the compute SA's
  objectAdmin and benchmarking/benchmark-runner's objectAdmin on the snapshot
  bucket.

## [2026-10-22T03:45:00Z] [SETUP_CLUSTER]

The old install (d1b2371d) can't be upgraded in place (#1812: version label
removed; #2888: proto fields renumbered, so stored rows are unreadable).
Reinstalled in place without recreating the cluster:

1. Backups: `.apo/results/backups/atepg-substrate-metal-20261022T034450Z.sql.gz`
   (pg_dumpall) and `.apo/results/backups/k8s-substrate-metal-20261022T0345Z/`
   (SandboxConfigs, WorkerPools, ate-system workloads/ConfigMaps).
2. Deleted WorkerPool `benchmark-workloads/benchmark-ateom` (replicas 0) and
   DaemonSet `ate-system/atelet-apo`; with no atelet on the metal node, wiped
   `/var/lib/ate/*` there (old image cache and sandbox asset cache indexed
   from the snapshot bucket; ~620 MB).
3. `DROP SCHEMA substrate CASCADE` in atepg (old schema_migrations + 17
   tables, no simulated actors left); stale `public.*` tables left alone.
4. `hack/install-ate.sh --cluster-size=size0 --podcert-workers-per-signer=16
   --rollout-timeout=10m deploy ate-system` (set-env.sh env): new CRDs,
   podcertificate-controller, ate-idp-server, atelet DaemonSet `atelet`,
   schema recreated by ateapi.
   - Incident: the new ate-api-server crash-looped on `json: unknown field
     "actorIdentityJWTProvider"`: the install kept the old
     `ate-api-authentication` ConfigMap (field dropped in #2597). Fixed with
     `install-ate.sh ... create api-authentication-config`; the deploy then
     finished (ate-api-server rollout 84 s).
5. `hack/install-microvm-deps.sh --install`: staged kata 4.1.0 assets to
   `gs://snapshot-substrate-metal-nishanth-gke-dev-assets/kata-assets/` and
   re-applied SandboxConfig `microvm` (v1, now pointing at the asset bucket;
   preflight checks this).
6. The harness itself creates WorkerPool `benchmark-workloads/benchmark-worker`,
   atespace benchmark-workloads, template sweperf-astropy-7336 and namespace
   agent-sim (label ate.dev/api-access=true) on each trial.

Pre-flight: build.sh passed (agentsim image
`us-docker.pkg.dev/nishanth-gke-dev/gcr.io/ate-images/agentsim:c4ef15ca7110-276b621489cb`),
`--render-only` passed, run_experiment.py preflight (context, SandboxConfig)
passed.

## [2026-10-22T04:13:00Z] [INCIDENT] smoke attempt 1: node disk full

`.apo/run_experiment.sh .apo/results/scratch/smoke-mvm-tput --set WINDOWS=1`
(kept as `results/scratch/smoke-mvm-tput-attempt1`): deploy, golden and the 4
workers were fine, then agentsim setup (REGISTERED=2000) filled the metal
node's 193 GiB boot disk: `/var/lib/ate/snapshots` reached 180 GB = 1991
actors x ~114 MB `local-checkpoint`. Upstream atelet keeps each suspended
actor's latest snapshot locally for `--snapshot-detach-delay` (default 30m;
upload after 5m; #2733/#2789/#2804); the old base deleted it. DiskPressure
evicted the workers, atelet and the node shell: 350/2000 setup failures (no
space left on device, worker.sock gone, virtiofsd socket timeouts, vm.snapshot
500s), error.json OOM_CHECK (node shell evicted). Recovery: node shell now
`priorityClassName: system-node-critical`; deleted the purged actors' dirs
under /var/lib/ate/{snapshots,actors} (DB had no actors left), DiskPressure
cleared 04:18Z, evicted atelet pod deleted and recreated.

Harness changes (user decision for Phase 1: two arms on REGISTERED=1000):
- REGISTERED=1000 (AWAKE=500) in tunables.env.
- New locked tunable ATELET_SNAPSHOT_DETACH_DELAY (empty = upstream default
  30m, arm "local"; 2m = arm "gcs"), applied to the atelet DaemonSet each
  trial and recorded in summary metadata.
- check_node_disk (PREFLIGHT_DISK): fails fast when REGISTERED x
  SNAPSHOT_MB_PER_ACTOR (140; 133 MB measured) > free /var/lib/ate space - NODE_DISK_RESERVE_MB
  (30000).
- Node residue cleanup also removes purged actors' /var/lib/ate/snapshots
  dirs; end-of-run metrics node_local_snapshots, node_local_snapshot_gib,
  local_snapshot_mb_per_actor, node_disk_used_pct.

## [2026-10-22T04:35:00Z] [SMOKE] smoke attempt 2: passed

`.apo/run_experiment.sh .apo/results/scratch/smoke-mvm-tput --set WINDOWS=1
--set REGISTERED=1000` (AWAKE=500, RATE=3, upstream atelet defaults, i.e.
detach delay 30m; workspace 957b5ee4 + the uncommitted fleet/disk-guard
changes; agentsim c4ef15c). summary.json written, no error.json.

- achieved_ratio 1.0 (3.0/s), error_refusal_pct 0.2 (one 504), crashed 0,
  windows_ok 1, backlog_growth_excess 0, drain_s_max 0, prewarm 108 s with 0
  failures, Substrate OOM kills 0 (one fluent-bit OOM kill, context only).
- resume warm p50/p90/p95/p99 382/407/412/471 ms; suspend p90/p95 310/316 ms
  (spec resume/suspend p95 <= 500 ms both met); TTFE accept p50 397 ms, p95
  5222 ms (spec TTFE missed; few samples at TTFE_SAMPLE_PCT=5).
- leaked_objects 2 (links_tap_veth 2) <= 5; 500 purged actors' local snapshot
  dirs (68 GB) remain after the trial (atelet keeps them up to 30 min; the next
  trial's residue clean removes them).
- Restore source: the 1000 first restores during setup were `latest`
  (downloaded from GCS: download p50/p90 54/67 ms, manifest_fetch 0.2/0.3 ms,
  restore total 313/720 ms); every later restore, including all 2460
  pre-warm/fill/warm-up/measured wakes, was `local` (no download;
  manifest_fetch 0.1/0.1 ms, restore total 343/616 ms) -> restore_local_pct
  71.1 overall, 100% in the measured window. Worker: vm_restore 243/311 ms,
  wakeup_probe 9/26 ms, worker total 312/388 ms; 3 restores stalled >= 5 s,
  all in the reseed.
- Disk: 1000 local snapshots = 129.9 GiB (133 MB per actor), node disk 73.5%
  used at the end of the run. objectstore-plugin RSS 680 MiB (peak 721),
  atelet 103 MiB, 155 MiB per active actor.
- Restore phase logs were collected the whole run: breakdown.jsonl 10,847
  records, restore_phases.jsonl 2,340, envoy_access.log 3,615 lines.

Not comparable with the old base's numbers (2Gi/1-vCPU template, local
restores, mTLS router, async suspend). Next: v000 ramps for both Phase 1 arms
(local: default detach delay; gcs: ATELET_SNAPSHOT_DETACH_DELAY=2m), then
RATE and the thresholds from the 3-run CV.

