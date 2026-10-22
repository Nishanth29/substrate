//go:build linux

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

package main

import (
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ateworker/phaselog"
)

func TestWakeupPhasesSplitTheProbeWindow(t *testing.T) {
	tResume := time.Unix(1000, 0)
	tAgentDial := tResume.Add(20 * time.Millisecond)
	tReseeded := tAgentDial.Add(15 * time.Second) // a reseed that stalled
	tProbe := tReseeded.Add(300 * time.Millisecond)

	got := wakeupPhases(tResume, tAgentDial, tReseeded, tProbe)
	want := []phaselog.Phase{
		{Name: phaseWakeupProbe, D: 15*time.Second + 320*time.Millisecond},
		{Name: phaseAgentDial, D: 20 * time.Millisecond},
		{Name: phaseReseedRPC, D: 15 * time.Second},
		{Name: phaseProbe, D: 300 * time.Millisecond},
	}
	if len(got) != len(want) {
		t.Fatalf("wakeupPhases returned %d phases, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("phase %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if sum := got[1].D + got[2].D + got[3].D; sum != got[0].D {
		t.Errorf("agent_dial + reseed_rpc + probe = %v, want wakeup_probe %v", sum, got[0].D)
	}
}
