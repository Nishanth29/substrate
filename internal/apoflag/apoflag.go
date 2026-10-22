// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package apoflag holds the switches of the APO Phase 4 levers: ATE_APO_* settings
// that turn an experimental code path on in a build whose default is the unchanged
// path, so one build serves the baseline and the lever.
//
// The WorkerPool pod template has no env, so the benchmark harness writes the
// switches to File on the node (every worker mounts /var/lib/ate) before the
// workers start. Init reads that file once at startup, lets the process
// environment override it, and logs what is on.
package apoflag

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
)

// File is where the harness writes the switches, one KEY=VALUE per line.
const File = "/var/lib/ate/apo-flags.env"

// Prefix is the name prefix every switch carries; other names are ignored.
const Prefix = "ATE_APO_"

var (
	once     sync.Once
	switches map[string]string
)

// Init reads the switches from File and the environment, once, and logs the active
// ones. Call it at startup, after logging is set up; later calls do nothing.
func Init(ctx context.Context) {
	once.Do(func() {
		switches = Load(File, os.Environ())
		slog.InfoContext(ctx, "APO lever switches", slog.String("active", Describe(switches)))
	})
}

// On reports whether the switch name is set to a true value (1, true, on, yes).
func On(name string) bool {
	switch strings.ToLower(Value(name)) {
	case "1", "true", "on", "yes":
		return true
	}
	return false
}

// Value returns the switch's value, or "" when it isn't set. Without an Init call
// it loads the switches on first use, without logging.
func Value(name string) string {
	once.Do(func() { switches = Load(File, os.Environ()) })
	return switches[name]
}

// Load returns the ATE_APO_* settings of the file at path (missing file: none),
// overridden by those of environ (os.Environ format).
func Load(path string, environ []string) map[string]string {
	out := map[string]string{}
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			put(out, strings.TrimSpace(sc.Text()))
		}
		_ = f.Close()
	}
	for _, kv := range environ {
		put(out, kv)
	}
	return out
}

func put(m map[string]string, kv string) {
	k, v, ok := strings.Cut(kv, "=")
	if ok && strings.HasPrefix(k, Prefix) && len(k) > len(Prefix) {
		m[k] = v
	}
}

// Describe lists the switches as sorted KEY=VALUE pairs, or "none".
func Describe(m map[string]string) string {
	if len(m) == 0 {
		return "none"
	}
	kvs := make([]string, 0, len(m))
	for k, v := range m {
		kvs = append(kvs, k+"="+v)
	}
	sort.Strings(kvs)
	return strings.Join(kvs, " ")
}
