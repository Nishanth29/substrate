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
	"io"
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstd's streaming encoder compresses one block at a time however many cores it is
// given — WithEncoderConcurrency only lets the next block be filled while the
// current one encodes. That caps a snapshot upload at one core of compression
// (measured on a GKE worker node: ~190 MB/s of compressed output, below what the
// parallel part upload underneath it can absorb, so the uploader starved).
//
// parZstd instead cuts the stream into chunks and compresses each as its own zstd
// FRAME on its own core. Concatenated frames decode as one stream, so the output
// still reads back through a plain zstd reader and the snapshot format is unchanged.
// Independent chunks cost a little ratio (no matches across a boundary), which
// parZstdChunk is sized to keep negligible.
const (
	// parZstdChunk is the per-frame chunk size.
	parZstdChunk = 8 << 20
	// parZstdQueue is how many chunks may be outstanding per worker, so the producer
	// can run ahead while workers compress.
	parZstdQueue = 2
	// parZstdFrameCap is the capacity of a pooled output buffer: one chunk's frame
	// even when the chunk does not compress.
	parZstdFrameCap = parZstdChunk + parZstdChunk/16
)

// Chunk and frame buffers are pooled across uploads rather than allocated for each
// one: a snapshot otherwise allocates tens of MiB of fresh buffers per upload, and a
// node checkpointing many actors at once spends its time page-faulting them in. Only
// buffers of the pooled capacity go back, so a pool never hands out something smaller
// than its callers expect. Encoders are not pooled: an Encoder makes an internal
// channel on first use, which ties it to the synctest bubble (in tests) that first
// used it.
var (
	parZstdChunkPool = sync.Pool{New: func() any { b := make([]byte, 0, parZstdChunk); return &b }}
	parZstdFramePool = sync.Pool{New: func() any { b := make([]byte, 0, parZstdFrameCap); return &b }}
)

// newChunkEncoder returns an encoder for EncodeAll use. Single-segment frames always
// declare their content size, even the short last frame of a stream (zstd otherwise
// leaves it out under 1 KiB), so every frame can be decoded independently; a chunk is
// at most parZstdChunk, so the frame never needs a larger window than that.
func newChunkEncoder() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithSingleSegment(true))
	if err != nil {
		// NewWriter only fails on bad options, which are compile-time constants here.
		panic(err)
	}
	return enc
}

func getChunk() []byte { return (*parZstdChunkPool.Get().(*[]byte))[:0] }
func getFrame() []byte { return (*parZstdFramePool.Get().(*[]byte))[:0] }

func putChunk(b []byte) {
	if cap(b) == parZstdChunk {
		b = b[:0]
		parZstdChunkPool.Put(&b)
	}
}

func putFrame(b []byte) {
	if cap(b) == parZstdFrameCap {
		b = b[:0]
		parZstdFramePool.Put(&b)
	}
}

// parZstd is an io.WriteCloser that compresses what it is given as parallel zstd
// frames, written to dst in order. Close flushes the tail and reports the first
// error from any worker or from dst.
type parZstd struct {
	dst     io.Writer
	workers int

	buf      []byte
	free     chan []byte
	jobs     chan parZstdJob
	ordered  chan chan []byte
	done     chan struct{}
	err      error
	workerWG sync.WaitGroup
}

type parZstdJob struct {
	buf []byte
	out chan []byte
}

// newParZstd starts the worker pool, capped at GOMAXPROCS (workers <= 0 asks for it).
func newParZstd(dst io.Writer, workers int) *parZstd {
	if workers <= 0 || workers > runtime.GOMAXPROCS(0) {
		workers = runtime.GOMAXPROCS(0)
	}
	p := &parZstd{
		dst:     dst,
		workers: workers,
		free:    make(chan []byte, workers*parZstdQueue),
		jobs:    make(chan parZstdJob, workers),
		ordered: make(chan chan []byte, workers*parZstdQueue),
		done:    make(chan struct{}),
	}
	for range workers * parZstdQueue {
		p.free <- getChunk()
	}
	for range workers {
		p.workerWG.Go(p.worker)
	}
	go p.writer()
	p.buf = <-p.free
	return p
}

// worker compresses whole chunks. Each holds its own encoder: the encoders are
// single-shot EncodeAll users, so one per worker keeps their state private.
func (p *parZstd) worker() {
	enc := newChunkEncoder()
	defer enc.Close()
	for j := range p.jobs {
		// Only a full chunk takes a pooled frame. A partial one (a small file, or
		// the tail of a stream) gets a buffer its own size, so many small uploads
		// at once don't each hold a full-size frame.
		var dst []byte
		if len(j.buf) == parZstdChunk {
			dst = getFrame()
		} else {
			dst = make([]byte, 0, len(j.buf)+len(j.buf)/16)
		}
		j.out <- enc.EncodeAll(j.buf, dst)
		p.free <- j.buf[:0]
	}
}

// writer drains the per-chunk result channels in dispatch order, so the frames land
// in the same order the bytes arrived.
func (p *parZstd) writer() {
	defer close(p.done)
	for out := range p.ordered {
		frame := <-out
		if p.err == nil {
			_, p.err = p.dst.Write(frame)
		}
		putFrame(frame)
	}
}

func (p *parZstd) Write(b []byte) (int, error) {
	total := len(b)
	for len(b) > 0 {
		n := min(cap(p.buf)-len(p.buf), len(b))
		p.buf = append(p.buf, b[:n]...)
		b = b[n:]
		if len(p.buf) == cap(p.buf) {
			p.flush()
		}
	}
	return total, nil
}

// flush dispatches the current chunk and takes a fresh buffer.
func (p *parZstd) flush() {
	if len(p.buf) == 0 {
		return
	}
	out := make(chan []byte, 1)
	p.ordered <- out
	p.jobs <- parZstdJob{buf: p.buf, out: out}
	p.buf = <-p.free
}

func (p *parZstd) Close() error {
	p.flush()
	close(p.jobs)
	close(p.ordered)
	<-p.done
	p.workerWG.Wait()
	// Every chunk is idle now: the one in hand and the rest back in free.
	putChunk(p.buf)
	p.buf = nil
	for {
		select {
		case b := <-p.free:
			putChunk(b)
		default:
			return p.err
		}
	}
}

// framedZstd compresses on the caller's goroutine into independent frames of at most
// parZstdChunk input bytes, each declaring its decompressed size. It is for callers
// that already run one encoder per parallel unit of work (an upload part), so it
// needs no workers of its own. Unlike one streaming frame per part, sized frames
// can be split and decoded in parallel on restore. It holds one pooled chunk, one
// pooled frame and one encoder, which keeps a part within serialWorkerMemory.
type framedZstd struct {
	dst   io.Writer
	enc   *zstd.Encoder
	buf   []byte
	frame []byte
	err   error
}

func newFramedZstd(dst io.Writer) *framedZstd {
	return &framedZstd{
		dst:   dst,
		enc:   newChunkEncoder(),
		buf:   getChunk(),
		frame: getFrame(),
	}
}

func (z *framedZstd) Write(b []byte) (int, error) {
	total := len(b)
	for len(b) > 0 && z.err == nil {
		n := min(cap(z.buf)-len(z.buf), len(b))
		z.buf = append(z.buf, b[:n]...)
		b = b[n:]
		if len(z.buf) == cap(z.buf) {
			z.flush()
		}
	}
	if z.err != nil {
		return total - len(b), z.err
	}
	return total, nil
}

// flush compresses the buffered chunk into one frame and writes it.
func (z *framedZstd) flush() {
	if len(z.buf) == 0 || z.err != nil {
		return
	}
	frame := z.enc.EncodeAll(z.buf, z.frame[:0])
	_, z.err = z.dst.Write(frame)
	// Keep the frame buffer EncodeAll returned: if it had to grow, reuse the
	// grown one for the next chunk instead of growing again.
	z.frame = frame
	z.buf = z.buf[:0]
}

// Close writes the last, partial frame, returns the buffers to their pools and
// releases the encoder. It reports the first error from dst.
func (z *framedZstd) Close() error {
	if z.enc == nil {
		return z.err
	}
	z.flush()
	z.enc.Close()
	putChunk(z.buf)
	putFrame(z.frame)
	z.enc, z.buf, z.frame = nil, nil, nil
	return z.err
}
