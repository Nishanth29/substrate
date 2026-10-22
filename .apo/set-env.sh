#!/bin/bash
# APO environment for the Agent Substrate throughput workload (GKE, isolated
# gcloud and kubectl config). Assembled from the gke_apo engine's set-env
# templates (base.sh + gke-isolated.sh + registry-auth.sh), with one gcloud
# config and one kubeconfig per stack (see CHANGELOG 2026-10-11T05:50).
#
# Sourced by .apo/build.sh and .apo/run_experiment.sh themselves, so the
# engine's merge gate and evolve evaluator (which run them without sourcing
# this file) get the same environment.

export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
export WORKLOAD_ROOT="$(cd "${SCRIPT_DIR}/.." &> /dev/null && pwd)"

# --- Python -------------------------------------------------------------------
# run_experiment.py only needs PyYAML; the engine ships Optuna in its own
# blaze binary, so no virtualenv is needed.
REAL_USER=$(whoami)
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
export PYTHONPATH="/usr/local/google/home/${REAL_USER}/.local/lib/python${PYTHON_VERSION}/site-packages:${PYTHONPATH:-}"
if ! python3 -c "import yaml" &>/dev/null; then
  echo "set-env.sh: python3 needs PyYAML (pip install --user pyyaml)" >&2
fi

# --- Leased stack -------------------------------------------------------------
# The engine writes stack.env at the worktree root (stack_leases.write_stack_env)
# with the stacks.json keys uppercased. .apo/stack.env is accepted for manual runs.
for f in "${WORKLOAD_ROOT}/stack.env" "${SCRIPT_DIR}/stack.env"; do
  if [ -f "$f" ]; then
    source "$f"
    break
  fi
done

export SCRATCH_DIR="${SCRIPT_DIR}/scratch"
mkdir -p "${SCRATCH_DIR}"

# --- Target project and cluster (stack.env values win) ------------------------
# Manual runs without a lease default to stack-microvm (the only active stack
# in stacks.json). Never point this at substrate-poc or apo-gvisor.
export PROJECT_ID="${PROJECT_ID:-nishanth-gke-dev}"
export CLUSTER_NAME="${CLUSTER_NAME:-substrate-metal}"
export COMPUTE_REGION="${COMPUTE_REGION:-us-east4}"
export COMPUTE_ZONE="${COMPUTE_ZONE:-us-east4-c}"
export SANDBOX_CLASS="${SANDBOX_CLASS:-microvm}"
export GCE_REGION="${COMPUTE_REGION}"
export CLUSTER_LOCATION="${COMPUTE_ZONE}"
export KUBECTL_CONTEXT="gke_${PROJECT_ID}_${COMPUTE_ZONE}_${CLUSTER_NAME}"
export BUCKET_NAME="${BUCKET_NAME:-snapshot-${CLUSTER_NAME}-${PROJECT_ID}}"
export RESULTS_BUCKET_PREFIX="${RESULTS_BUCKET_PREFIX:-gs://${BUCKET_NAME}/apo-sweperf}"
export KO_DOCKER_REPO="${KO_DOCKER_REPO:-gcr.io/${PROJECT_ID}/ate-images}"
export KO_DEFAULTPLATFORMS="${KO_DEFAULTPLATFORMS:-linux/amd64}"
# ate-setup reads its settings from the environment; never pick up a
# developer .ate-dev-env.sh from the tree.
export NO_DEV_ENV=1

# --- Install settings (hack/install-ate.sh = `go run ./cmd/ate-setup`) ---------
# Cluster-wide snapshot location (required unless the PerTemplateSnapshotLocation
# preview gate is on; the sweperf template's storageLocation is ignored without
# it, upstream #2544).
export SNAPSHOT_LOCATION="${SNAPSHOT_LOCATION:-gs://${BUCKET_NAME}/}"
# GSA snapshot transfers run as (upstream #3093); created by
# `go run ./tools/setup-gcp create iam`, required to deploy ate-api-server on GKE.
export SNAPSHOT_TRANSFER_GSA_NAME="${SNAPSHOT_TRANSFER_GSA_NAME:-ate-snapshot-transfer}"
# agentsim runs as ServiceAccount agent-sim/default and authenticates to both
# ate-api-server and the ingress router with its podidentity certificate.
export AGENTSIM_NAMESPACE="${AGENTSIM_NAMESPACE:-agent-sim}"
export AGENTSIM_SPIFFE_ID="${AGENTSIM_SPIFFE_ID:-spiffe://cluster.local/ns/${AGENTSIM_NAMESPACE}/sa/default}"
# ate-api-server enforces authorization (upstream #2591): kubectl-ate's
# principal and agentsim's hold owner on global:root for the install's life,
# as benchmarking/automation/orchestrator.py does for its runner.
export ATE_API_AUTHZ_BOOTSTRAP_OWNERS="${ATE_API_AUTHZ_BOOTSTRAP_OWNERS:-system:serviceaccount:ate-system:ate-client,${AGENTSIM_SPIFFE_ID}}"
# The Envoy ingress router only admits these client SPIFFE IDs (mTLS on 443;
# the plain-HTTP mode is gone, upstream #2977).
export ATE_INGRESS_ALLOWED_SPIFFE_IDS="${ATE_INGRESS_ALLOWED_SPIFFE_IDS:-${AGENTSIM_SPIFFE_ID}}"
export ROUTER_URL="${ROUTER_URL:-https://atenet-router.ate-system.svc:443}"

# Global ate-setup flags passed on every trial's install, so the control plane
# sizing is identical across trials.
export INSTALL_FLAGS="${INSTALL_FLAGS:---cluster-size=size0 --podcert-workers-per-signer=16}"

# Required by `deploy ate-system` and `deploy atenet`: the bundled Kubernetes
# Secrets provider, as in CI (.github/workflows/pr-workflow.yaml).
if [ -z "${ATE_CREDENTIAL_PROVIDER:-}" ]; then
  export ATE_CREDENTIAL_PROVIDER='{"name":"k8s.io"}'
fi

# agentsim (aditya-shantanu/substrate-agents-tco) is the load generator, on
# branch fine-ramp-ate-main (fine-ramp ported to upstream main: no pause
# lifecycle, mTLS to the router). build.sh builds its image once per commit.
export AGENTS_TCO_DIR="${AGENTS_TCO_DIR:-${HOME}/git-repos/substrate-agents-tco-fineramp}"
export AGENTSIM_IMAGE_REPO="${AGENTSIM_IMAGE_REPO:-us-docker.pkg.dev/${PROJECT_ID}/gcr.io/ate-images/agentsim}"

# Pins benchmark workers to the stack's metal node pool. atecontroller adds the
# ate.dev/sandboxClass=<class> toleration itself, so no WORKER_TOLERATION.
export WORKER_NODE_POOL="${WORKER_NODE_POOL:-microvm-metal}"
export WORKER_NODE_SELECTOR="${WORKER_NODE_SELECTOR:-cloud.google.com/gke-nodepool=${WORKER_NODE_POOL}}"
export WORKER_TOLERATION="${WORKER_TOLERATION:-}"

# --- Isolated gcloud and kubectl config ---------------------------------------
# One gcloud config and one kubeconfig per stack (cluster). deploy.sh, ko and
# kubectl-ate use the current context, and `use-context` below changes it, so
# two stacks sharing a kubeconfig would send one stack's workloads to the other.
ACTIVE_ACCOUNT=$(CLOUDSDK_CONFIG="${HOME}/.config/gcloud" gcloud config get-value account 2>/dev/null || true)
export CLOUDSDK_CONFIG="${SCRIPT_DIR}/.gcloud-config-${CLUSTER_NAME}"
mkdir -p "${CLOUDSDK_CONFIG}"
for pair in "application_default_credentials.json" "credentials.db" "access_tokens.db"; do
  if [ -f "${HOME}/.config/gcloud/${pair}" ] && [ ! -f "${CLOUDSDK_CONFIG}/${pair}" ]; then
    cp "${HOME}/.config/gcloud/${pair}" "${CLOUDSDK_CONFIG}/${pair}"
  fi
done
export KUBECONFIG="${SCRIPT_DIR}/.kubeconfig-${CLUSTER_NAME}"

gcloud config set project "${PROJECT_ID}" &>/dev/null || true
gcloud config set compute/zone "${COMPUTE_ZONE}" &>/dev/null || true
gcloud config set compute/region "${COMPUTE_REGION}" &>/dev/null || true
if [ -n "$ACTIVE_ACCOUNT" ]; then
  gcloud config set account "${ACTIVE_ACCOUNT}" &>/dev/null || true
fi

if ! kubectl --context "${KUBECTL_CONTEXT}" config get-contexts "${KUBECTL_CONTEXT}" &>/dev/null; then
  gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --zone="${COMPUTE_ZONE}" --project="${PROJECT_ID}" &>/dev/null || true
fi
kubectl --context "${KUBECTL_CONTEXT}" config use-context "${KUBECTL_CONTEXT}" &>/dev/null || true

# --- Registry auth --------------------------------------------------------------
gcloud auth configure-docker "gcr.io,us-docker.pkg.dev,${COMPUTE_REGION}-docker.pkg.dev" --quiet &>/dev/null || true
