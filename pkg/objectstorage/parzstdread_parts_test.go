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

package objectstorage

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestSparsePartFramesDecodeInParallel checks every frame the upload parts write is
// one parZstdReader will hand to a parallel worker, i.e. it declares a size within
// parZstdMaxFrame, so a restore never falls back to decoding serially.
func TestSparsePartFramesDecodeInParallel(t *testing.T) {
	const logical, populated, extent = 80 << 20, 40 << 20, 4 << 20
	src := writeSnapshotSource(t, t.TempDir(), logical, populated, extent)
	exts, pop, err := sparseExtents(src, logical)
	if err != nil {
		t.Fatal(err)
	}
	stream := encodeSnapshot(t, src, logical, planSparseParts(exts, pop, 2))
	r := bytes.NewReader(stream[len(sparseMagic)+4:])
	frames := 0
	for {
		frame, size, _, ok, err := readZstdFrame(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("frame %d: %v", frames, err)
		}
		if !ok {
			t.Fatalf("frame %d would be decoded serially", frames)
		}
		if size < 0 || size > parZstdMaxFrame {
			t.Fatalf("frame %d declares size %d, want 0..%d", frames, size, parZstdMaxFrame)
		}
		if len(frame) == 0 {
			t.Fatalf("frame %d is empty", frames)
		}
		frames++
	}
	if frames < 5 {
		t.Errorf("only %d frames for %d MiB", frames, pop>>20)
	}
}
