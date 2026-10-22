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
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// fakeAgentConn answers a reseed after delay (never when delay < 0), with err.
type fakeAgentConn struct {
	delay time.Duration
	err   error

	mu        sync.Mutex
	got       [][]byte
	deadlines []time.Duration // each call's deadline, relative to the call
	closed    bool
}

func (f *fakeAgentConn) ReseedRandomDev(ctx context.Context, data []byte) error {
	f.mu.Lock()
	f.got = append(f.got, data)
	if d, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, time.Until(d))
	}
	f.mu.Unlock()
	if f.delay < 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-time.After(f.delay):
		return f.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeAgentConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeAgentConn) nonces() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.got...)
}

func (f *fakeAgentConn) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// fakeDialer hands out conns in order (then ones that never answer), fails the dials
// errs names, counts dials and records every connection it handed out.
type fakeDialer struct {
	conns []*fakeAgentConn
	errs  []error
	n     int
	made  []*fakeAgentConn
}

func (d *fakeDialer) dial(context.Context) (agentReseeder, error) {
	i := d.n
	d.n++
	if i < len(d.errs) && d.errs[i] != nil {
		return nil, d.errs[i]
	}
	c := &fakeAgentConn{delay: -1}
	if i < len(d.conns) && d.conns[i] != nil {
		c = d.conns[i]
	}
	d.made = append(d.made, c)
	return c, nil
}

func allClosed(t *testing.T, cs []*fakeAgentConn) {
	t.Helper()
	for i, c := range cs {
		if !c.isClosed() {
			t.Errorf("fresh connection %d left open", i)
		}
	}
}

var testNonce = []byte("0123456789abcdef0123456789abcdef")

func TestReseedFastFirstAnswerNoRedial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := &fakeAgentConn{delay: 5 * time.Millisecond}
		d := &fakeDialer{}
		start := time.Now()
		if err := reseedGuestCRNGFast(t.Context(), first, d.dial, testNonce, reseedAttemptTimeout, reseedBudget); err != nil {
			t.Fatalf("reseed: %v", err)
		}
		if d.n != 0 {
			t.Errorf("dialed %d fresh connections, want 0", d.n)
		}
		if got := time.Since(start); got != 5*time.Millisecond {
			t.Errorf("took %v, want 5ms", got)
		}
		if first.isClosed() {
			t.Error("closed the activation's connection")
		}
		if dl := first.deadlines; len(dl) != 1 || dl[0] != reseedAttemptTimeout {
			t.Errorf("attempt deadlines = %v, want [%v]", dl, reseedAttemptTimeout)
		}
	})
}

func TestReseedFastStalledFirstFreshConnectionWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := &fakeAgentConn{delay: 16 * time.Second} // waits for the node-wide tick
		fresh := &fakeAgentConn{delay: 3 * time.Millisecond}
		d := &fakeDialer{conns: []*fakeAgentConn{fresh}}
		start := time.Now()
		if err := reseedGuestCRNGFast(t.Context(), first, d.dial, testNonce, reseedAttemptTimeout, reseedBudget); err != nil {
			t.Fatalf("reseed: %v", err)
		}
		if got, want := time.Since(start), reseedAttemptTimeout+3*time.Millisecond; got != want {
			t.Errorf("took %v, want %v (attempt 1 times out, the fresh connection answers in 3ms)", got, want)
		}
		if d.n != 1 {
			t.Errorf("dialed %d fresh connections, want 1", d.n)
		}
		if !fresh.isClosed() || first.isClosed() {
			t.Errorf("closed: fresh %v (want true), first %v (want false)", fresh.isClosed(), first.isClosed())
		}
		if fg, ff := fresh.nonces(), first.nonces(); len(fg) != 1 || len(ff) != 1 ||
			!bytes.Equal(fg[0], testNonce) || !bytes.Equal(ff[0], testNonce) {
			t.Error("both attempts must carry the same nonce")
		}
	})
}

func TestReseedFastNoAnswerFailsAfterBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := &fakeAgentConn{delay: -1}
		d := &fakeDialer{}
		start := time.Now()
		err := reseedGuestCRNGFast(t.Context(), first, d.dial, testNonce, reseedAttemptTimeout, reseedBudget)
		if err == nil {
			t.Fatal("reseed succeeded with no answer")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want a deadline", err)
		}
		if got := time.Since(start); got != reseedBudget {
			t.Errorf("gave up after %v, want the %v budget", got, reseedBudget)
		}
		// Attempts start at 0, 1.5, 3, 4.5, 6 and 7.5 s; the last is cut to 0.5 s.
		if d.n != 5 {
			t.Errorf("dialed %d fresh connections, want 5", d.n)
		}
		for i, c := range d.made {
			want := reseedAttemptTimeout
			if i == len(d.made)-1 {
				want = reseedBudget - 5*reseedAttemptTimeout
			}
			if len(c.deadlines) != 1 || c.deadlines[0] != want {
				t.Errorf("fresh connection %d: deadlines %v, want [%v]", i, c.deadlines, want)
			}
		}
		allClosed(t, d.made)
		if first.isClosed() {
			t.Error("closed the activation's connection")
		}
	})
}

func TestReseedFastErrorsThenFreshSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := &fakeAgentConn{delay: time.Millisecond, err: errors.New("agent error")}
		fresh := &fakeAgentConn{delay: time.Millisecond}
		d := &fakeDialer{errs: []error{errors.New("dial refused")}, conns: []*fakeAgentConn{nil, fresh}}
		start := time.Now()
		if err := reseedGuestCRNGFast(t.Context(), first, d.dial, testNonce, reseedAttemptTimeout, reseedBudget); err != nil {
			t.Fatalf("reseed: %v", err)
		}
		if d.n != 2 {
			t.Errorf("dials = %d, want 2 (a refused one, then one that answers)", d.n)
		}
		// Each fast failure waits out reseedRetryPause before the next attempt.
		if got, want := time.Since(start), 2*reseedRetryPause+time.Millisecond; got != want {
			t.Errorf("took %v, want %v", got, want)
		}
		allClosed(t, d.made)
	})
}

func TestReseedFastGuestStoppedFailsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := &fakeAgentConn{delay: -1}
		d := &fakeDialer{errs: []error{errGuestStopped}}
		start := time.Now()
		err := reseedGuestCRNGFast(t.Context(), first, d.dial, testNonce, reseedAttemptTimeout, reseedBudget)
		if !errors.Is(err, errGuestStopped) {
			t.Errorf("err = %v, want %v", err, errGuestStopped)
		}
		if got := time.Since(start); got != reseedAttemptTimeout {
			t.Errorf("gave up after %v, want %v (no attempt after the VMM is gone)", got, reseedAttemptTimeout)
		}
		if d.n != 1 {
			t.Errorf("dials = %d, want 1", d.n)
		}
	})
}

func TestReseedFastCallerCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		first := &fakeAgentConn{delay: -1}
		d := &fakeDialer{}
		go func() {
			time.Sleep(2 * time.Second)
			cancel()
		}()
		start := time.Now()
		if err := reseedGuestCRNGFast(ctx, first, d.dial, testNonce, reseedAttemptTimeout, reseedBudget); !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want canceled", err)
		}
		if got := time.Since(start); got != 2*time.Second {
			t.Errorf("returned after %v, want 2s", got)
		}
		allClosed(t, d.made)
	})
}

func TestReseedFastFitsTheRouterTimeout(t *testing.T) {
	if reseedAttemptTimeout >= reseedBudget {
		t.Errorf("attempt timeout %v must be below the budget %v", reseedAttemptTimeout, reseedBudget)
	}
	// Envoy's ext_proc limit is 10 s; the rest of a restore needs some of it.
	if reseedBudget >= 10*time.Second {
		t.Errorf("budget %v must stay inside Envoy's 10s ext_proc timeout", reseedBudget)
	}
}

func TestUseFastReseedOffByDefault(t *testing.T) {
	// The test binary has no ATE_APO_RESEED_FAST in its env or node file.
	t.Setenv(reseedFastSwitch, "")
	if useFastReseed() {
		t.Error("lever A on without its switch")
	}
}
