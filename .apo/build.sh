#!/usr/bin/env bash
# Pre-flight gate for an APO trial, run inside the trial worktree:
#   1. go build and go vet for the packages the agent may change,
#   2. their unit tests (-short),
#   3. the agentsim load-generator image (built once per substrate-agents-tco
#      commit, reused across trials).
# Substrate images are built and deployed by run_experiment.py through
# hack/install-ate.sh and benchmarking/workloads/deploy.sh.
#
# Usage: .apo/build.sh [RESULTS_DIR]

set -euo pipefail

RESULTS_DIR="${1:-.apo/results/scratch}"
mkdir -p "${RESULTS_DIR}"
RESULTS_DIR="$(cd "${RESULTS_DIR}" && pwd)"

APO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${APO_DIR}/.." && pwd)"
cd "${ROOT}"

# shellcheck disable=SC1091
source "${APO_DIR}/set-env.sh" >/dev/null

LOG="${RESULTS_DIR}/build_output.log"
: > "${LOG}"

# The allowed file scope (objective.md), as Go packages. Upstream renamed
# cmd/ateom-* to cmd/ateworker-*, cmd/snapshot-plugin to cmd/objectstore-plugin,
# internal/ateomnet to internal/ateworker/actornet + internal/netns and
# internal/ateomcgroup to internal/ateworker/cgroup (see CHANGELOG 2026-10-22).
PKGS=(
  ./pkg/objectstorage/...
  ./cmd/atelet/...
  ./cmd/objectstore-plugin/...
  ./cmd/ateworker-gvisor/...
  ./cmd/ateworker-microvm/...
  ./internal/ateworker/actornet/...
  ./internal/ateworker/cgroup/...
  ./internal/netns/...
  ./internal/tarutil/...
)
# Lever branches only (ATE_APO_* switches); absent on the base.
OPTIONAL_PKGS=(
  ./internal/apoflag/...
)
EXISTING=()
MISSING=()
for p in "${PKGS[@]}"; do
  if [ -d "${p%/...}" ]; then EXISTING+=("$p"); else MISSING+=("$p"); fi
done
for p in "${OPTIONAL_PKGS[@]}"; do
  [ -d "${p%/...}" ] && EXISTING+=("$p")
done

fail() {
  local error_type="$1" message="$2"
  cat "${LOG}" >&2 || true
  cat > "${RESULTS_DIR}/error.json" <<EOF
{
  "phase": "PREFLIGHT_BUILD",
  "error_type": "${error_type}",
  "message": "${message} See build_output.log."
}
EOF
  echo "Build validation failed: ${message}" >&2
  exit 1
}

if [ "${#MISSING[@]}" -gt 0 ]; then
  echo "allowed-scope packages missing on this base: ${MISSING[*]}" >> "${LOG}"
  fail "SCOPE_PACKAGE_MISSING" "allowed-scope packages missing: ${MISSING[*]} (update build.sh and objective.md)."
fi

echo ">>> go build" | tee -a "${LOG}"
go build "${EXISTING[@]}" >> "${LOG}" 2>&1 || fail "COMPILE_FAILURE" "go build failed."

echo ">>> go vet" | tee -a "${LOG}"
go vet "${EXISTING[@]}" >> "${LOG}" 2>&1 || fail "VET_FAILURE" "go vet failed."

echo ">>> go test -short" | tee -a "${LOG}"
if ! go test -short -count=1 "${EXISTING[@]}" >> "${LOG}" 2>&1; then
  # Retry only the failed packages once: a flake (e.g. cmd/atelet
  # TestSandboxAssetPrewarmDownloads under full parallel load) shouldn't DISCARD a trial.
  FAILED=$(grep -E '^FAIL\s+go\.ate\.dev/' "${LOG}" | awk '{print $2}' | sort -u)
  [ -n "${FAILED}" ] || fail "UNIT_TEST_FAILURE" "Unit tests failed."
  echo ">>> retrying failed packages once: ${FAILED}" | tee -a "${LOG}"
  # shellcheck disable=SC2086
  go test -short -count=1 ${FAILED} >> "${LOG}" 2>&1 || fail "UNIT_TEST_FAILURE" "Unit tests failed twice."
fi

# agentsim depends only on substrate-agents-tco (outside the allowed scope), so
# tag it by that repo's commit: every trial reuses one image.
if [ -z "${AGENTSIM_IMAGE:-}" ]; then
  SIM_REV="$(git -C "${AGENTS_TCO_DIR}" rev-parse --short=12 HEAD)"
  if ! git -C "${AGENTS_TCO_DIR}" diff --quiet HEAD -- service; then
    fail "IMAGE_BUILD_FAILURE" "substrate-agents-tco/service has uncommitted changes; commit them first."
  fi
  AGENTSIM_IMAGE="${AGENTSIM_IMAGE_REPO}:${SIM_REV}-$(git -C "${ROOT}" rev-parse --short=12 HEAD)"
  if gcloud artifacts docker images describe "${AGENTSIM_IMAGE}" >/dev/null 2>&1; then
    echo ">>> Reusing agentsim image ${AGENTSIM_IMAGE}" | tee -a "${LOG}"
  else
    echo ">>> Building agentsim image ${AGENTSIM_IMAGE}" | tee -a "${LOG}"
    # agentsim's go.mod replaces substrate with ../../substrate; build it against
    # this tree instead (go.work replaces win), so client and server share a base.
    GOWORK_FILE="${RESULTS_DIR}/agentsim.go.work"
    printf 'go 1.27.0\n\nuse %s/service\n\nreplace github.com/agent-substrate/substrate => %s\n' \
      "${AGENTS_TCO_DIR}" "${ROOT}" > "${GOWORK_FILE}"
    # run-tool.sh finds its repo from the CWD, so resolve ko here before the cd.
    KO_BIN="$(cd "${ROOT}" && hack/run-tool.sh --print-bin-path ko)" \
      || fail "IMAGE_BUILD_FAILURE" "ko not found."
    (cd "${AGENTS_TCO_DIR}/service" && GOWORK="${GOWORK_FILE}" \
      KO_DOCKER_REPO="${AGENTSIM_IMAGE_REPO%/*}" KO_DEFAULTPLATFORMS=linux/amd64 \
      "${KO_BIN}" build --base-import-paths --tags="${AGENTSIM_IMAGE##*:}" ./cmd/agentsim) >> "${LOG}" 2>&1 \
      || fail "IMAGE_BUILD_FAILURE" "agentsim image build failed."
  fi
fi

{
  echo "AGENTSIM_IMAGE=${AGENTSIM_IMAGE}"
  echo "AGENTSIM_COMMIT=$(git -C "${AGENTS_TCO_DIR}" rev-parse HEAD)"
  echo "AGENTSIM_BRANCH=$(git -C "${AGENTS_TCO_DIR}" rev-parse --abbrev-ref HEAD)"
} > "${RESULTS_DIR}/build_artifacts.env"
echo ">>> Build validation succeeded. Artifacts: ${RESULTS_DIR}/build_artifacts.env" | tee -a "${LOG}"
