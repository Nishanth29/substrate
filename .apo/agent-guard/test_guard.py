#!/usr/bin/env python3
"""Self-test for apo_guard.sh: feeds sample commands and checks deny/allow."""
import json, os, subprocess, sys, tempfile

GUARD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "apo_guard.sh")
PUSH = "git " + "push"
CASES = [
    (f"{PUSH} origin apo/mvm-throughput", "deny"),
    (f"{PUSH} origin apo/mvm-throughput-results", "ask"),
    (f"git add -A && git commit -m x && {PUSH} origin apo/mvm-throughput-results", "ask"),
    (f"{PUSH} upstream apo/mvm-throughput-results", "deny"),
    (f"{PUSH} --force origin apo/mvm-throughput-results", "deny"),
    ("kubectl get pods", "deny"),
    ("kubectl --context gke_x_apo-" + "gvisor get pods", "deny"),
    ("kubectl --context gke_nishanth-gke-dev_us-east4-c_substrate-metal get pods -A", "ask"),
    ("gcloud container clusters " + "resize substrate-metal --node-pool microvm-metal --num-nodes 0", "deny"),
    ("gcloud container node-pools " + "create x", "deny"),
    (".apo/run_experiment.sh .apo/results/phase1/v000-local-1 --mode fine-ramp", "ask"),
    ("rm -" + "rf .apo/results", "deny"),
    ("gcloud storage " + "rm gs://x/y", "deny"),
]

home = tempfile.mkdtemp()
os.makedirs(os.path.join(home, ".gemini/config"))
bad = 0
for cmd, want in CASES:
    r = subprocess.run([sys.executable, GUARD], input=json.dumps({"toolCall": {"args": {"CommandLine": cmd}}}),
                       capture_output=True, text=True, env=dict(os.environ, HOME=home))
    got = json.loads(r.stdout)["decision"]
    ok = got == want
    bad += not ok
    print(("ok  " if ok else "FAIL"), f"{got:5}", cmd)
print("all passed" if not bad else f"{bad} failed")
sys.exit(1 if bad else 0)
