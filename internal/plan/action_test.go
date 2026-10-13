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
	"strings"
	"testing"
	"time"
)

// Every kind of action has a name of its own. Two kinds that shared a value would be carried out
// as each other, and one without a name would be logged as nothing.
func TestActionKindsAreDistinctAndNamed(t *testing.T) {
	seen := map[string]ActionKind{}

	for k := range actionKinds {
		name := k.String()
		if name == "" {
			t.Fatalf("action kind %d has no name", int(k))
		}

		if other, taken := seen[name]; taken {
			t.Fatalf("action kinds %d and %d are both called %s", int(other), int(k), name)
		}

		seen[name] = k
	}

	if got := actionKinds.String(); !strings.HasPrefix(got, "ActionKind(") {
		t.Fatalf("a value that is no kind is not given the name of one: %q", got)
	}
}

func TestDoWaitsForTheNodeAndTheGate(t *testing.T) {
	step := Step{Name: StepPulling, Since: t0}
	soon, late := t0.Add(time.Minute), t0.Add(timing.StepTimeout)

	tests := []struct {
		name    string
		there   bool
		pre     Gate
		now     time.Time
		want    ActionKind
		message string
	}{
		{"acts once the node is there and the gate is open", true, open, soon, Pull, ""},
		{"waits for the node first", false, closed, soon, Wait, notReachable},
		{"waits for the gate", true, closed, soon, Wait, closed.Reason},
		// A gate is closed by not being open. One that gives no reason keeps the action back all
		// the same.
		{"a closed gate that says nothing is still closed", true, Gate{}, soon, Wait, "a gate is closed"},
		{"fails with what it waited for: the node", false, closed, late, Fail, notReachable},
		{"fails with what it waited for: the gate", true, closed, late, Fail, closed.Reason},
		{"fails when nothing held it up and it still was not done", true, open, late, Fail, notSent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := step.do(Pull, "pulled", tt.there, notReachable, tt.pre, timing, tt.now)
			if got.Kind != tt.want || !strings.Contains(got.Message, tt.message) {
				t.Fatalf("got %s %q, want %s with %q", got.Kind, got.Message, tt.want, tt.message)
			}

			if got.Kind == Fail && got.Reason != FailTimeout {
				t.Fatalf("a step that was not done in time fails as a timeout, not %s", got.Reason)
			}
		})
	}
}

func TestDrainCordonsOnlyBehindAnOpenGate(t *testing.T) {
	step := Step{Name: StepDraining, Since: t0}
	now := t0.Add(time.Minute)

	// Whatever is known about the node, a closed gate keeps it from being cordoned.
	for _, cordonRecorded := range []bool{false, true} {
		step.Cordoned = cordonRecorded

		for _, gate := range []Gate{closed, {}} {
			action, draining := step.drain(true, false, -1, gate, timing, now)
			if !draining || action.Kind != Wait {
				t.Fatalf("a node is not cordoned while the gate is closed: got %s (recorded %v)", action.Kind, cordonRecorded)
			}
		}
	}

	step.Cordoned = false

	order := []struct {
		name     string
		recorded bool
		known    bool
		cordoned bool
		podsLeft int
		want     ActionKind
		draining bool
	}{
		{"the Node was not read", false, false, false, -1, Wait, true},
		{"the decision to cordon is recorded before the cordon", false, true, false, -1, MarkCordoned, true},
		{"then the node is cordoned", true, true, false, -1, Cordon, true},
		{"pods that were not counted are not assumed gone", true, true, true, -1, Wait, true},
		{"pods that are left are evicted", true, true, true, 3, Evict, true},
		{"a node with nothing left on it is drained", true, true, true, 0, Wait, false},
	}

	for _, tt := range order {
		t.Run(tt.name, func(t *testing.T) {
			step.Cordoned = tt.recorded

			action, draining := step.drain(tt.known, tt.cordoned, tt.podsLeft, open, timing, now)
			if action.Kind != tt.want || draining != tt.draining {
				t.Fatalf("got %s, draining %v", action.Kind, draining)
			}
		})
	}

	late, _ := step.drain(true, true, 3, open, timing, t0.Add(timing.DrainTimeout))
	if late.Kind != Fail || late.Reason != FailDrainTimeout {
		t.Fatalf("a drain that does not end fails as one: %+v", late)
	}
}

func TestFirstBlockIsTheMostFundamental(t *testing.T) {
	if reason, _ := firstBlock(block{}, block{}); reason != "" {
		t.Fatalf("nothing blocks when nothing is in the way, got %s", reason)
	}

	reason, message := firstBlock(block{}, staleBlock([]string{"w-1"}), abortedBlock(true, "this"), gateBlock(closed))
	if reason != BlockNotCurrent || !strings.Contains(message, "w-1") {
		t.Fatalf("got %s: %s", reason, message)
	}

	// A gate that closed for something particular says so; any other closed gate is a closed gate.
	if reason, _ := firstBlock(gateBlock(Gate{Reason: "x", block: BlockQuorumAtRisk})); reason != BlockQuorumAtRisk {
		t.Fatalf("got %s", reason)
	}

	if reason, _ := firstBlock(gateBlock(Gate{})); reason != BlockGateClosed {
		t.Fatalf("a closed gate blocks whatever it says, got %q", reason)
	}
}

func TestPolicyBlocksInOrder(t *testing.T) {
	everything := Policy{Paused: true, Invalid: "spec.rollout: no", OutsideWindow: "outside", GateClosed: "gate"}

	if got := everything.block(); got.reason != BlockPaused {
		t.Fatalf("paused comes first, got %s", got.reason)
	}

	everything.Paused = false
	if got := everything.block(); got.reason != BlockInvalid || got.message != everything.Invalid {
		t.Fatalf("timings that cannot be used come before the window, got %+v", got)
	}

	everything.Invalid = ""
	if got := everything.block(); got.reason != BlockOutsideWindow {
		t.Fatalf("got %s", got.reason)
	}

	// Timings that cannot be used start nothing. They do not stop a node that is being changed
	// from being finished, and so do not close the hold of an operation in flight.
	if !(Policy{Invalid: "spec.rollout: no"}).Hold().Open {
		t.Fatal("invalid timings must not hold an operation in flight")
	}
}
