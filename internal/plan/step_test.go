/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plan

import (
	"testing"
	"time"
)

const (
	prev    = "sha256:previous"
	desired = "sha256:desired"
	foreign = "sha256:foreign"
)

var (
	t0     = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	timing = Timing{}.WithDefaults()
	open   = Gate{Open: true}
	closed = Gate{Reason: "etcd has a learner"}
)

func tryStep(name string) Step {
	return Step{Name: name, Tried: true, PreviousHash: prev, DesiredHash: desired, Since: t0, Deadline: t0.Add(timing.TryTimeout)}
}

func plainStep(name string) Step {
	return Step{Name: name, PreviousHash: prev, DesiredHash: desired, Since: t0}
}

func on(active, persistent string) Node {
	return Node{Reachable: true, ActiveHash: active, PersistentHash: persistent}
}

var unreachable = Node{}

func TestVerifyingANodeThatNeedsNoChange(t *testing.T) {
	soaked := t0.Add(-timing.Soak)

	tests := []struct {
		name         string
		node         Node
		checks       Gate
		healthySince time.Time
		after        time.Duration
		want         ActionKind
		reason       string
	}{
		{"starts the soak once healthy", on(desired, desired), open, time.Time{}, 0, MarkHealthy, ""},
		{"a node booted from disk has no persistent config and is settled", on(desired, ""), open, time.Time{}, 0, MarkHealthy, ""},
		{"soaks", on(desired, desired), open, t0, time.Minute, Wait, ""},
		{"completes after a full soak", on(desired, desired), open, soaked, 0, Complete, ""},
		{"a failing check restarts the soak", on(desired, desired), closed, t0, time.Minute, MarkUnhealthy, ""},
		{"waits for a staged config to settle", on(desired, prev), open, time.Time{}, 0, Wait, ""},
		{"waits for health", on(desired, desired), closed, time.Time{}, time.Minute, Wait, ""},
		{"unhealthy for too long fails", on(desired, desired), closed, time.Time{}, timing.StepTimeout, Fail, FailTimeout},
		{"a config from somewhere else halts", on(foreign, foreign), open, soaked, 0, Fail, FailConfigChanged},
		{"an unreachable node times out", unreachable, open, time.Time{}, timing.StepTimeout, Fail, FailTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := plainStep(StepVerifying)
			step.HealthySince = tt.healthySince

			got := NextStep(step, tt.node, open, tt.checks, timing, t0.Add(tt.after))
			if got.Kind != tt.want || got.Reason != tt.reason {
				t.Fatalf("got %s reason=%q (%s)", got.Kind, got.Reason, got.Message)
			}
		})
	}
}

func TestWaitMessagesAreStable(t *testing.T) {
	// A message that changed with the clock would rewrite status on every reconcile.
	step := tryStep(StepVerifying)
	step.HealthySince = t0

	first := NextStep(step, on(desired, ""), open, open, timing, t0.Add(10*time.Second))
	second := NextStep(step, on(desired, ""), open, open, timing, t0.Add(50*time.Second))

	if first.Kind != Wait || first.Message == "" || first.Message != second.Message {
		t.Fatalf("soak messages must not change over time: %q vs %q", first.Message, second.Message)
	}
}

func TestUntouched(t *testing.T) {
	step := tryStep(StepTrying)

	if !Untouched(step, on(prev, ""), t0) {
		t.Fatal("a step that still waits to try, on a node on its previous config, is untouched")
	}

	for name, touched := range map[string]bool{
		"unreachable":        Untouched(step, unreachable, t0),
		"tried":              Untouched(step, on(desired, ""), t0),
		"past the deadline":  Untouched(step, on(prev, ""), step.Deadline),
		"already verifying":  Untouched(tryStep(StepVerifying), on(prev, ""), t0),
		"already confirming": Untouched(tryStep(StepConfirming), on(prev, ""), t0),
	} {
		if touched {
			t.Errorf("%s: must not count as untouched", name)
		}
	}
}

func TestTryProtocolHappyPath(t *testing.T) {
	step := tryStep(StepTrying)

	// Persisted, node still on the previous config: send the try.
	if got := NextStep(step, on(prev, ""), open, open, timing, t0); got.Kind != Try {
		t.Fatalf("expected Try, got %s", got.Kind)
	}

	// The try is active. Never send it again: observe and move on.
	if got := NextStep(step, on(desired, ""), open, open, timing, t0.Add(5*time.Second)); got.Kind != Advance || got.Step != StepVerifying {
		t.Fatalf("expected Advance to Verifying, got %+v", got)
	}

	step.Name, step.Since = StepVerifying, t0.Add(5*time.Second)

	if got := NextStep(step, on(desired, ""), open, open, timing, t0.Add(10*time.Second)); got.Kind != MarkHealthy {
		t.Fatalf("expected MarkHealthy, got %s", got.Kind)
	}

	step.HealthySince = t0.Add(10 * time.Second)

	if got := NextStep(step, on(desired, ""), open, open, timing, t0.Add(time.Minute)); got.Kind != Wait {
		t.Fatalf("expected to soak, got %s", got.Kind)
	}

	// A check failing during the soak restarts it.
	if got := NextStep(step, on(desired, ""), open, closed, timing, t0.Add(time.Minute)); got.Kind != MarkUnhealthy {
		t.Fatalf("expected MarkUnhealthy, got %s", got.Kind)
	}

	got := NextStep(step, on(desired, ""), open, open, timing, step.HealthySince.Add(timing.Soak))
	if got.Kind != Advance || got.Step != StepConfirming {
		t.Fatalf("expected Advance to Confirming, got %+v", got)
	}

	step.Name = StepConfirming

	if got := NextStep(step, on(desired, ""), open, open, timing, t0.Add(3*time.Minute)); got.Kind != Confirm {
		t.Fatalf("expected Confirm, got %s", got.Kind)
	}

	if got := NextStep(step, on(desired, desired), open, open, timing, t0.Add(3*time.Minute)); got.Kind != Complete {
		t.Fatalf("expected Complete, got %s", got.Kind)
	}
}

func TestTryThatCutsTheNodeOffIsNeverConfirmed(t *testing.T) {
	step := tryStep(StepVerifying)

	// The node is gone for the whole window.
	for _, after := range []time.Duration{10 * time.Second, 2 * time.Minute, 4 * time.Minute} {
		if got := NextStep(step, unreachable, open, open, timing, t0.Add(after)); got.Kind != Wait {
			t.Fatalf("after %s: expected Wait, got %s", after, got.Kind)
		}
	}

	gaveUp := NextStep(step, unreachable, open, open, timing, step.Deadline.Add(-ConfirmMargin))
	if got := gaveUp; got.Kind != Advance || got.Step != StepRollingBack {
		t.Fatalf("expected Advance to RollingBack, got %+v", got)
	}

	step.Name = StepRollingBack

	if got := NextStep(step, on(prev, ""), open, open, timing, step.Deadline); got.Kind != Wait {
		t.Fatalf("expected to wait out the grace period, got %s", got.Kind)
	}

	got := NextStep(step, on(prev, ""), open, open, timing, step.Deadline.Add(RollbackGrace))
	if got.Kind != Fail || got.Reason != FailRolledBack {
		t.Fatalf("expected RolledBack, got %+v", got)
	}

	// The node never returns: this needs a human.
	got = NextStep(step, unreachable, open, open, timing, step.Deadline.Add(RollbackGrace+timing.StepTimeout))
	if got.Kind != Fail || got.Reason != FailNodeUnreachableAfterRollback {
		t.Fatalf("expected NodeUnreachableAfterRollback, got %+v", got)
	}
}

func TestTryingRearmsOnlyWhileNoTryCanBePending(t *testing.T) {
	step := tryStep(StepTrying)
	late := step.Deadline.Add(-time.Minute)

	// Gates held the try back until too little of the window is left. The node is still on its
	// previous config before the deadline, so nothing was tried and a fresh deadline is safe.
	if got := NextStep(step, on(prev, ""), closed, open, timing, late); got.Kind != Rearm {
		t.Fatalf("expected Rearm, got %s", got.Kind)
	}

	// Past the deadline the same observation may be a reverted try. It is a rollback, never a retry.
	if got := NextStep(step, on(prev, ""), open, open, timing, step.Deadline); got.Kind != Advance || got.Step != StepRollingBack {
		t.Fatalf("expected Advance to RollingBack, got %+v", got)
	}

	// Waiting on gates is bounded like every other wait.
	step.Deadline = t0.Add(timing.StepTimeout + timing.TryTimeout)

	got := NextStep(step, on(prev, ""), closed, open, timing, t0.Add(timing.StepTimeout))
	if got.Kind != Fail || got.Reason != FailTimeout {
		t.Fatalf("expected Timeout, got %+v", got)
	}
}

func TestLostConfirmIsRecognised(t *testing.T) {
	step := tryStep(StepRollingBack)

	// The confirm landed but its answer was lost, and the deadline passed meanwhile.
	if got := NextStep(step, on(desired, desired), open, open, timing, step.Deadline.Add(time.Minute)); got.Kind != Complete {
		t.Fatalf("expected Complete, got %s", got.Kind)
	}
}

// brokenRule names the rule of the try protocol that action breaks in the given situation, or
// returns "".
func brokenRule(step Step, node Node, pre, checks Gate, now time.Time, action Action) string {
	onPrevious := node.Reachable && node.ActiveHash == prev
	tried := node.Reachable && node.ActiveHash == desired
	beforeDeadline := now.Before(step.Deadline)
	withMargin := now.Before(step.Deadline.Add(-ConfirmMargin))

	switch action.Kind { //nolint:exhaustive // only the dangerous actions carry rules
	case Try:
		switch {
		case !onPrevious:
			return "a try must only be sent to a node on its previous config"
		case !pre.Open:
			return "a try must not be sent while a gate is closed"
		case !beforeDeadline:
			return "a try must not be sent after its deadline"
		}
	case Rearm:
		if !onPrevious || !beforeDeadline {
			return "the deadline may only move while no try can be pending"
		}
	case Confirm:
		if step.Name != StepConfirming || !tried || !withMargin {
			return "a confirm needs the tried config active, with margin before the deadline"
		}
	case Complete:
		if !tried || node.PersistentHash != desired {
			return "complete needs the desired config active and persistent"
		}
	case Advance:
		if action.Step == StepConfirming && (!checks.Open || !tried || now.Sub(step.HealthySince) < timing.Soak || !withMargin) {
			return "confirming needs a full soak of passing checks, with margin before the deadline"
		}
	}

	return ""
}

// The rules of the try protocol, checked over every combination of step, observation, gate and
// time rather than a handful of chosen cases.
func TestTryProtocolInvariants(t *testing.T) {
	nodes := map[string]Node{
		"unreachable":           unreachable,
		"previous":              on(prev, ""),
		"previous, persisted":   on(prev, prev),
		"tried":                 on(desired, ""),
		"tried over persistent": on(desired, prev),
		"confirmed":             on(desired, desired),
		"foreign":               on(foreign, foreign),
	}

	offsets := []time.Duration{
		0, time.Second, timing.Soak, timing.TryTimeout - ConfirmMargin - time.Second, timing.TryTimeout - ConfirmMargin,
		timing.TryTimeout - time.Second, timing.TryTimeout, timing.TryTimeout + RollbackGrace, timing.TryTimeout + time.Hour,
	}

	gates := []Gate{open, closed}
	checked := 0

	for _, name := range []string{StepTrying, StepVerifying, StepConfirming, StepRollingBack} {
		for nodeName, node := range nodes {
			for _, offset := range offsets {
				for _, healthyFor := range []time.Duration{-1, 0, timing.Soak} {
					step := tryStep(name)
					now := t0.Add(offset)

					if healthyFor >= 0 {
						step.HealthySince = now.Add(-healthyFor)
					}

					for _, pre := range gates {
						for _, checks := range gates {
							checked++

							if rule := brokenRule(step, node, pre, checks, now, NextStep(step, node, pre, checks, timing, now)); rule != "" {
								t.Errorf("%s / %s / +%s: %s", name, nodeName, offset, rule)
							}
						}
					}
				}
			}
		}
	}

	if want := 4 * len(nodes) * len(offsets) * 3 * 4; checked != want {
		t.Fatalf("checked %d situations, expected %d", checked, want)
	}
}

func TestAFailedConfirmIsNotRepeatedCloseToTheDeadline(t *testing.T) {
	step := tryStep(StepConfirming)

	if got := NextStep(step, on(desired, ""), open, open, timing, step.Deadline.Add(-ConfirmMargin-time.Second)); got.Kind != Confirm {
		t.Fatalf("expected Confirm while the margin holds, got %s", got.Kind)
	}

	got := NextStep(step, on(desired, ""), open, open, timing, step.Deadline.Add(-ConfirmMargin))
	if got.Kind != Advance || got.Step != StepRollingBack {
		t.Fatalf("expected Advance to RollingBack inside the margin, got %+v", got)
	}

	// A confirm that landed is still recognised there.
	if got := NextStep(step, on(desired, desired), open, open, timing, step.Deadline.Add(-time.Second)); got.Kind != Complete {
		t.Fatalf("expected Complete, got %s", got.Kind)
	}
}

func TestTimingValidation(t *testing.T) {
	if err := timing.Validate(); err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}

	if err := (Timing{TryTimeout: 3 * time.Minute, Soak: 2 * time.Minute, StepTimeout: time.Minute}).Validate(); err == nil {
		t.Fatal("a try window that leaves no time to verify must be refused")
	}

	if err := (Timing{TryTimeout: time.Hour, Soak: -time.Second, StepTimeout: time.Minute}).Validate(); err == nil {
		t.Fatal("a negative soak must be refused")
	}

	if err := (Timing{TryTimeout: time.Hour, Soak: 10 * time.Minute, StepTimeout: 10 * time.Minute}).Validate(); err == nil {
		t.Fatal("a step timeout that the soak alone exhausts must be refused")
	}

	if FirstStep(true) != StepTrying || FirstStep(false) != StepVerifying {
		t.Fatal("a change begins with a try; a node that needs none is only verified")
	}
}
