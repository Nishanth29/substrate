#!/bin/bash
# APO benchmark entry point: .apo/run_experiment.sh <RESULTS_DIR>
# Writes <RESULTS_DIR>/summary.json on success, <RESULTS_DIR>/error.json on failure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/set-env.sh"

exec python3 "${SCRIPT_DIR}/run_experiment.py" "$@"
