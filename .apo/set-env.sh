#!/bin/bash
# APO environment for the Agent Substrate throughput workload (GKE, isolated
# gcloud and kubectl config). Assembled from the engine's set-env templates
# (base.sh + gke-isolated.sh + registry-auth.sh).

export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
export WORKLOAD_ROOT="$(cd "${SCRIPT_DIR}/.." &> /dev/null && pwd)"

# --- Python -------------------------------------------------------------------
# run_experiment.py only needs PyYAML; the engine ships Optuna in its own
# blaze binary, so no virtualenv is needed.
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
# Manual runs without a lease default to the microVM stack (stack-microvm in
# stacks.json). Never point this at substrate-poc.
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

# agentsim (aditya-shantanu/substrate-agents-tco, branch sweperf-adapter) is
# the load generator. build.sh builds its image once per commit of that repo.
export AGENTS_TCO_DIR="${AGENTS_TCO_DIR:-${HOME}/git-repos/substrate-agents-tco}"
export AGENTSIM_IMAGE_REPO="${AGENTSIM_IMAGE_REPO:-us-docker.pkg.dev/${PROJECT_ID}/gcr.io/ate-images/agentsim}"

# Fixed Substrate version label: every trial replaces atelet-apo in place
# instead of adding a new versioned DaemonSet (see docs/upgrade.md). The
# worker nodes must carry ate.dev/substrate-version=apo.
export VERSION="${VERSION:-apo}"

# Flags passed to hack/install-ate.sh on every trial, so the control plane
# sizing is identical across trials.
export INSTALL_FLAGS="${INSTALL_FLAGS:---cluster-size=size0 --podcert-workers-per-signer=16}"

# Required by --deploy-ate-system and --deploy-atenet: the bundled Kubernetes
# Secrets provider, as in CI (.github/workflows/pr-workflow.yaml).
if [ -z "${ATE_CREDENTIAL_PROVIDER:-}" ]; then
  export ATE_CREDENTIAL_PROVIDER='{"name":"k8s.io"}'
fi

# Pins benchmark workers to the stack's metal node pool. Manual runs default to
# stack-microvm's pool (microvm-metal: c3-standard-192-metal was stocked out).
export WORKER_NODE_POOL="${WORKER_NODE_POOL:-microvm-metal}"
export WORKER_NODE_SELECTOR="${WORKER_NODE_SELECTOR:-cloud.google.com/gke-nodepool=${WORKER_NODE_POOL}}"
export WORKER_TOLERATION="${WORKER_TOLERATION:-}"

# --- Isolated gcloud and kubectl config ---------------------------------------
# One gcloud config and one kubeconfig per stack (cluster). deploy.sh, ko and
# kubectl-ate use the current context, and `use-context` below changes it, so
# two stacks sharing a kubeconfig would send one stack's workloads to the other.
ACTIVE_ACCOUNT=$(gcloud config get-value account 2>/dev/null || true)
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

if ! kubectl config get-contexts "${KUBECTL_CONTEXT}" &>/dev/null; then
  gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --zone="${COMPUTE_ZONE}" --project="${PROJECT_ID}" &>/dev/null || true
fi
kubectl config use-context "${KUBECTL_CONTEXT}" &>/dev/null || true

# --- Registry auth --------------------------------------------------------------
gcloud auth configure-docker "gcr.io,us-docker.pkg.dev,${COMPUTE_REGION}-docker.pkg.dev" --quiet &>/dev/null || true
