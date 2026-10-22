#!/usr/bin/env python3
"""apo-guard: PreToolUse hook for run_command. Blocks risky commands during APO runs.

Install (on the machine that runs the agent):
  mkdir -p ~/.gemini/config
  cp .apo/agent-guard/apo_guard.sh ~/.gemini/config/apo_guard.sh && chmod +x ~/.gemini/config/apo_guard.sh
  cp .apo/agent-guard/hooks.json ~/.gemini/config/hooks.json   # merge by hand if one exists
Denied commands are logged to ~/.gemini/config/apo_guard.log.
"""
import datetime, json, os, re, sys

HOME = os.path.expanduser("~")
LOG = os.path.join(HOME, ".gemini/config/apo_guard.log")
PASS = "ask"  # anything not matched falls through to the normal permission flow

# The one push allowed: the Phase 1 report branch to the user's fork.
RESULTS_PUSH = (r"^git\s+push\s+(-u\s+)?(origin|git@github\.com:Nishanth29/substrate\.git|"
                r"https://github\.com/Nishanth29/substrate\.git)\s+apo/mvm-throughput-results$")


def out(decision, reason=""):
    if decision != PASS:
        try:
            with open(LOG, "a") as f:
                f.write(f"{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} "
                        f"{decision} [{reason}] {CMD[:300]}\n")
        except OSError:
            pass
    print(json.dumps({"decision": decision, "reason": f"apo-guard: {reason}" if reason else ""}))
    sys.exit(0)


try:
    data = json.load(sys.stdin)
except Exception:
    CMD = ""
    out("ask", "unreadable hook input")
CMD = ((data.get("toolCall") or {}).get("args") or {}).get("CommandLine", "") or ""
c = " ".join(CMD.split())
segs = [s.strip() for s in re.split(r"&&|\|\||;|\||\n|\$\(|`", CMD) if s.strip()]
home = re.escape(HOME)

DENY = [
    (r"substrate-poc", "never touch substrate-poc"),
    (r"apo-gvisor|us-central1", "the gVisor stack is off-limits for this campaign"),
    (r"\bgh\s+pr\s+(create|merge|close)\b", "no PR actions"),
    (r"\bgit\s+reset\s+--hard\b|\bgit\s+clean\s+-\w*f", "destructive git"),
    (r"\bgit\s+branch\s+-D\b", "branch force-delete"),
    (r"\bgcloud\s+container\s+(clusters|node-pools)\s+(delete|create|resize|update)\b", "no cluster/pool changes"),
    (r"\bgcloud\s+compute\s+instance-groups\s+managed\s+resize\b", "no node scaling"),
    (r"\bgcloud\s+storage\s+(rm|buckets\s+delete)\b|\bgsutil\s+(-m\s+)?(rm|rb)\b", "no GCS deletes"),
    (r"\bgcloud\s+\S+(\s+\S+)?\s+(add|remove)-iam-policy-binding\b", "no IAM changes"),
    (rf"\brm\s+-\w*r\w*\s+(/|~|\$HOME|{home})/?(\s|$)", "rm -r on / or home"),
    (r"\brm\s+-\w*r\w*\s+\S*git-repos/(substrate[\w-]*|sweperf)/?(\s|$)", "rm -r of a repo"),
    (r"\brm\s+-\w*r\w*\s+\S*\.apo/results/?(\s|$)", "rm -r of .apo/results"),
    (r"\bkubectl\b.*\bdelete\s+(ns|namespace|namespaces|node|nodes)\b", "no namespace/node deletes"),
]

for pat, why in DENY:
    if re.search(pat, c):
        out("deny", why)

# git push: only the results branch to the user's fork.
for s in segs:
    if re.search(r"\bgit\s+push\b", s) and not re.match(RESULTS_PUSH, " ".join(s.split())):
        out("deny", "no pushes (only apo/mvm-throughput-results to Nishanth29/substrate)")

# Prometheus in the benchmark has no volume: never restart/scale/delete it.
if re.search(r"\bkubectl\b", c) and "prometheus" in c and re.search(r"\b(delete|scale|patch|rollout\s+restart)\b", c):
    out("deny", "prometheus has no volume")

# Every kubectl call must name the substrate-metal cluster explicitly.
for s in segs:
    if re.match(r"^(\w+=\S*\s+)*(timeout\s+\d+\s+)?kubectl\b", s):
        if re.search(r"\bkubectl\s+(config|version|help|--help)\b", s):
            continue
        if "--context" not in s:
            out("deny", "kubectl without --context")
        if "substrate-metal" not in s:
            out("deny", "kubectl --context must be substrate-metal")

out(PASS)
