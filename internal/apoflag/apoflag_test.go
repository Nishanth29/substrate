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

package apoflag

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFileAndEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "apo-flags.env")
	body := "ATE_APO_RESEED_FAST=1\n\n  ATE_APO_X=on  \nOTHER=1\nATE_APO_=bad\nnot a setting\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got := Load(path, []string{"ATE_APO_X=off", "PATH=/bin", "ATE_APO_ENV_ONLY=yes"})
	want := map[string]string{"ATE_APO_RESEED_FAST": "1", "ATE_APO_X": "off", "ATE_APO_ENV_ONLY": "yes"}
	if len(got) != len(want) {
		t.Fatalf("Load = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Load[%s] = %q, want %q (env overrides the file)", k, got[k], v)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	if got := Load(filepath.Join(t.TempDir(), "absent"), nil); len(got) != 0 {
		t.Errorf("Load of a missing file = %v, want none", got)
	}
}

func TestDescribe(t *testing.T) {
	if got := Describe(nil); got != "none" {
		t.Errorf("Describe(nil) = %q, want none", got)
	}
	got := Describe(map[string]string{"ATE_APO_B": "1", "ATE_APO_A": "on"})
	if got != "ATE_APO_A=on ATE_APO_B=1" {
		t.Errorf("Describe = %q, want sorted pairs", got)
	}
}

func TestOn(t *testing.T) {
	once.Do(func() {}) // keep Value from reading the real node file
	switches = map[string]string{"ATE_APO_A": "1", "ATE_APO_B": "TRUE", "ATE_APO_C": "0", "ATE_APO_D": ""}
	for name, want := range map[string]bool{"ATE_APO_A": true, "ATE_APO_B": true, "ATE_APO_C": false,
		"ATE_APO_D": false, "ATE_APO_UNSET": false} {
		if got := On(name); got != want {
			t.Errorf("On(%s) = %v, want %v", name, got, want)
		}
	}
}
