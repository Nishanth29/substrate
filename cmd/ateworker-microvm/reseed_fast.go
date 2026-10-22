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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.ate.dev/substrate/internal/apoflag"
)

// APO lever A. Under a dense restore load a restored guest sometimes leaves the reseed
// request unanswered for 5-16 s, and the stalled requests of different guests all
// complete together on a node-wide tick, while a new connection to the same agent is
// answered within milliseconds. With the switch on, each reseed attempt gets
// reseedAttemptTimeout, and every attempt after the first goes out on a fresh kata-agent
// connection, until one succeeds or reseedBudget runs out. The guarantee is unchanged:
// the restore only goes on (and the actor only becomes reachable) after a successful
// reseed, and a reseed that never succeeds within the budget fails the restore. With the
// switch off, reseedGuestCRNG's single reseedTimeout call runs as before.
const (
	reseedFastSwitch     = "ATE_APO_RESEED_FAST"
	reseedAttemptTimeout = 1500 * time.Millisecond
	// Inside the router's 10 s ext_proc limit, with room for the rest of the restore.
	reseedBudget = 8 * time.Second
	// The least time between two attempts, so an attempt that fails at once (an agent
	// error, a refused dial) does not turn the budget into a busy loop.
	reseedRetryPause = 100 * time.Millisecond
)

// agentReseeder is a kata-agent connection a fresh reseed attempt dials and closes.
type agentReseeder interface {
	randomDevReseeder
	Close() error
}

// Outcomes of one reseed attempt, as logged.
const (
	reseedOutcomeOK        = "ok"
	reseedOutcomeTimeout   = "timeout"
	reseedOutcomeError     = "error"
	reseedOutcomeDialError = "dial_error"
	reseedOutcomeCanceled  = "canceled"
)

// useFastReseed reports whether lever A's switch is on.
func useFastReseed() bool {
	return apoflag.On(reseedFastSwitch)
}

// reseedGuestCRNGFast sends nonce to the guest's kata-agent in attempts of at most
// attemptTimeout each until one succeeds or budget runs out. Attempt 1 goes out on
// first, the activation's connection, which the caller owns and which stays open (it
// serves log forwarding and guest stats). Every later attempt dials a fresh connection
// with dial, inside the same attemptTimeout, and closes it when the attempt ends.
// Re-sending is safe: mixing more entropy into the CRNG is idempotent, and a late answer
// to an abandoned attempt is ignored.
func reseedGuestCRNGFast(ctx context.Context, first randomDevReseeder, dial func(context.Context) (agentReseeder, error),
	nonce []byte, attemptTimeout, budget time.Duration) error {
	bctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	start := time.Now()
	var lastErr error
	for attempt := 1; ; attempt++ {
		sent := time.Now()
		outcome, err := reseedAttempt(bctx, attempt, first, dial, nonce, attemptTimeout)
		took := time.Since(sent)
		attrs := []any{slog.Int("attempt", attempt), slog.Float64("attempt_ms", reseedMS(took)),
			slog.String("outcome", outcome)}
		if err != nil {
			attrs = append(attrs, slog.Any("err", err))
		}
		slog.InfoContext(ctx, "APO reseed attempt", attrs...)
		if err == nil {
			slog.InfoContext(ctx, "APO reseed answered", slog.Int("attempt", attempt), slog.Int("sent", attempt),
				slog.Float64("total_ms", reseedMS(time.Since(start))), slog.Float64("attempt_ms", reseedMS(took)))
			return nil
		}
		lastErr = err
		switch {
		case ctx.Err() != nil:
			// The caller gave up; that is not the guest's failure.
			return fmt.Errorf("agent ReseedRandomDev: %w", errors.Join(ctx.Err(), lastErr))
		case errors.Is(err, errGuestStopped):
			// The VMM is gone: no later attempt can succeed.
			return fmt.Errorf("agent ReseedRandomDev: %w", err)
		}
		if took < reseedRetryPause {
			select {
			case <-bctx.Done():
			case <-time.After(reseedRetryPause - took):
			}
		}
		if bctx.Err() != nil {
			err := errors.Join(bctx.Err(), lastErr)
			slog.WarnContext(ctx, "APO reseed failed", slog.Int("sent", attempt),
				slog.Float64("total_ms", reseedMS(time.Since(start))), slog.Any("err", err))
			return fmt.Errorf("agent ReseedRandomDev: no successful answer in %v over %d attempts: %w", budget, attempt, err)
		}
	}
}

// reseedAttempt makes one attempt, bounded by attemptTimeout and by ctx: on first for
// attempt 1, otherwise on a connection it dials and closes.
func reseedAttempt(ctx context.Context, attempt int, first randomDevReseeder,
	dial func(context.Context) (agentReseeder, error), nonce []byte, attemptTimeout time.Duration) (string, error) {
	actx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	r := first
	if attempt > 1 {
		c, err := dial(actx)
		if err != nil {
			return attemptOutcome(ctx, actx, reseedOutcomeDialError),
				fmt.Errorf("dialing a fresh kata-agent connection: %w", err)
		}
		defer func() { _ = c.Close() }()
		r = c
	}
	if err := r.ReseedRandomDev(actx, nonce); err != nil {
		return attemptOutcome(ctx, actx, reseedOutcomeError), err
	}
	return reseedOutcomeOK, nil
}

// attemptOutcome names a failed attempt: canceled when the caller's ctx ended, timeout
// when only the attempt's own deadline did, otherwise failed.
func attemptOutcome(ctx, actx context.Context, failed string) string {
	switch {
	case ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded):
		return reseedOutcomeCanceled
	case actx.Err() != nil:
		return reseedOutcomeTimeout
	default:
		return failed
	}
}

func reseedMS(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
