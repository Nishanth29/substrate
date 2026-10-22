#!/bin/bash
# Night driver for stack-gvisor (runs in tmux session apo-night, window gvisor).
# The stack env (cluster, zone, kubeconfig, gcloud config) is set by night.py per trial.
cd /usr/local/google/home/nishanthkotla/git-repos/substrate-apo || exit 1
exec python3 .apo/scripts/night.py --stack gvisor "$@"
