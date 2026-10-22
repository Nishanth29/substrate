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
