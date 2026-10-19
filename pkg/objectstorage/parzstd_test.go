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
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func randomBytes(seed int64, n int) []byte {
	b := make([]byte, n)
	rng := rand.New(rand.NewSource(seed))
	fillSnapshotData(rng, b)
	return b
}

// writeInPieces feeds data to w in uneven writes, so chunk boundaries fall inside them.
func writeInPieces(t *testing.T, w io.Writer, data []byte) {
	t.Helper()
	rng := rand.New(rand.NewSource(int64(len(data))))
	for len(data) > 0 {
		n := min(len(data), 1+rng.Intn(3<<20))
		if _, err := w.Write(data[:n]); err != nil {
			t.Fatal(err)
		}
		data = data[n:]
	}
}

func zstdDecodeAll(t *testing.T, stream []byte) []byte {
	t.Helper()
	zr, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	out, err := zr.DecodeAll(stream, nil)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestFramedZstdWritesSizedFrames checks framedZstd round-trips and cuts its output
// into frames of at most parZstdChunk that each declare their size, which is what
// lets a restore decode them in parallel.
func TestFramedZstdWritesSizedFrames(t *testing.T) {
	for _, n := range []int{0, 1, parZstdChunk - 1, parZstdChunk, parZstdChunk + 1, 3*parZstdChunk + 4321} {
		data := randomBytes(int64(n), n)
		var out bytes.Buffer
		zw := newFramedZstd(&out)
		writeInPieces(t, zw, data)
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		if got := zstdDecodeAll(t, out.Bytes()); !bytes.Equal(got, data) {
			t.Fatalf("n=%d: round trip differs (%d vs %d bytes)", n, len(got), len(data))
		}
		frames, sized, maxSize := zstdFrameStats(t, out.Bytes())
		if want := (n + parZstdChunk - 1) / parZstdChunk; frames != want {
			t.Errorf("n=%d: %d frames, want %d", n, frames, want)
		}
		if sized != frames {
			t.Errorf("n=%d: %d of %d frames declare their size", n, sized, frames)
		}
		if maxSize > parZstdChunk {
			t.Errorf("n=%d: frame of %d bytes, want at most %d", n, maxSize, parZstdChunk)
		}
	}
}

// TestPooledEncodersDoNotLeakData runs uploads back to back and side by side with
// different data through both encoders. Pooled buffers are reused, so a
// later upload must never carry bytes from an earlier one.
func TestPooledEncodersDoNotLeakData(t *testing.T) {
	encoders := map[string]func(io.Writer) io.WriteCloser{
		"framed": func(w io.Writer) io.WriteCloser { return newFramedZstd(w) },
		"par":    func(w io.Writer) io.WriteCloser { return newParZstd(w, 2) },
	}
	for name, newEnc := range encoders {
		t.Run(name, func(t *testing.T) {
			upload := func(t *testing.T, seed int64, n int) {
				data := randomBytes(seed, n)
				var out bytes.Buffer
				zw := newEnc(&out)
				writeInPieces(t, zw, data)
				if err := zw.Close(); err != nil {
					t.Error(err)
					return
				}
				if got := zstdDecodeAll(t, out.Bytes()); !bytes.Equal(got, data) {
					t.Errorf("seed %d: decoded %d bytes, want %d matching the input", seed, len(got), len(data))
				}
			}
			// A big upload fills the pooled buffers; a smaller one after it must not
			// pick up the rest of them.
			upload(t, 1, 3*parZstdChunk+parZstdChunk/2)
			upload(t, 2, parZstdChunk+parZstdChunk/4)
			upload(t, 3, 100)
			var wg sync.WaitGroup
			for i := range 6 {
				wg.Go(func() { upload(t, int64(10+i), (i+1)*parZstdChunk/2) })
			}
			wg.Wait()
		})
	}
}

type failAfter struct {
	n   int
	err error
}

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n == 0 {
		return 0, f.err
	}
	f.n--
	return len(p), nil
}

// TestFramedZstdReportsWriteErrors checks a failing destination stops the encoder
// and surfaces from Write and Close, and that Close is safe to call twice.
func TestFramedZstdReportsWriteErrors(t *testing.T) {
	boom := errors.New("boom")
	zw := newFramedZstd(&failAfter{n: 1, err: boom})
	data := randomBytes(5, 3*parZstdChunk)
	var werr error
	for len(data) > 0 && werr == nil {
		n := min(len(data), 1<<20)
		_, werr = zw.Write(data[:n])
		data = data[n:]
	}
	if !errors.Is(werr, boom) {
		t.Errorf("Write error = %v, want %v", werr, boom)
	}
	if err := zw.Close(); !errors.Is(err, boom) {
		t.Errorf("Close error = %v, want %v", err, boom)
	}
	if err := zw.Close(); !errors.Is(err, boom) {
		t.Errorf("second Close error = %v, want %v", err, boom)
	}
}

// TestSparsePartsWriteSizedFrames checks the upload parts are made of sized frames
// of at most parZstdChunk, several per part, rather than one streaming frame each.
func TestSparsePartsWriteSizedFrames(t *testing.T) {
	const logical, populated, extent = 80 << 20, 40 << 20, 4 << 20
	src := writeSnapshotSource(t, t.TempDir(), logical, populated, extent)
	exts, pop, err := sparseExtents(src, logical)
	if err != nil {
		t.Fatal(err)
	}
	groups := planSparseParts(exts, pop, 2)
	frames, sized, maxSize := zstdFrameStats(t, encodeSnapshot(t, src, logical, groups))
	if frames <= len(groups) {
		t.Errorf("%d frames for %d parts of %d MiB, want several per part", frames, len(groups), pop>>20/int64(len(groups)))
	}
	if sized != frames {
		t.Errorf("%d of %d frames declare their size", sized, frames)
	}
	if maxSize > parZstdChunk {
		t.Errorf("frame of %d bytes, want at most %d", maxSize, parZstdChunk)
	}
}

func TestWriteAfterClose(t *testing.T) {
	fz := newFramedZstd(io.Discard)
	if err := fz.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fz.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("framedZstd Write after Close = %v, want os.ErrClosed", err)
	}
	pz := newParZstd(io.Discard, 1)
	if err := pz.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pz.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("parZstd Write after Close = %v, want os.ErrClosed", err)
	}
}
