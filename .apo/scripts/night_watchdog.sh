#!/bin/bash
# Night watchdog (tmux session apo-night, window watchdog).
cd /usr/local/google/home/nishanthkotla/git-repos/substrate-apo || exit 1
exec python3 .apo/scripts/night_watchdog.py
