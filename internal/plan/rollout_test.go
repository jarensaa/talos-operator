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
	"slices"
	"strings"
	"testing"

	"github.com/jarensaa/talos-operator/internal/classify"
)

// Machines of the cluster the tests plan for.
const (
	w1  = "w-1"
	w2  = "w-2"
	cp1 = "cp-1"
)

// inSync is a machine running its rendered config; changed one whose spec renders something new.
func inSync(name string, controlPlane bool) Machine {
	return Machine{Name: name, ControlPlane: controlPlane, Current: true, LiveHash: "a", DesiredHash: "a", AppliedHash: "a"}
}

func changed(name string, controlPlane bool, class classify.Class) Machine {
	return Machine{
		Name: name, ControlPlane: controlPlane, Current: true, LiveHash: "a", DesiredHash: "b", AppliedHash: "a",
		Class: class, Reasons: []string{"v1alpha1/Example"},
	}
}

// drifted is a machine whose node was changed by hand: the spec still renders what was applied.
func drifted(name string) Machine {
	return Machine{Name: name, Current: true, LiveHash: "x", DesiredHash: "a", AppliedHash: "a", Class: classify.LiveSafe}
}

func TestPlanRolloutOrdersWorkersBeforeControlPlanes(t *testing.T) {
	got := PlanRollout([]Machine{
		changed("cp-2", true, classify.LiveSafe), changed("worker-b", false, classify.LiveSafe),
		changed(cp1, true, classify.LiveSafe), changed("worker-a", false, classify.LiveSafe),
		inSync("cp-3", true),
	}, Settings{DriftPolicy: DriftReport}, open)

	if want := []string{"worker-a", "worker-b", cp1, "cp-2"}; !slices.Equal(got.Pending, want) {
		t.Fatalf("order: got %v, want %v", got.Pending, want)
	}

	if !got.MayStart() || got.Targets[cp1] != "b" || got.Bases[cp1] != "a" || len(got.Targets) != 4 || len(got.Bases) != 4 {
		t.Fatalf("expected a startable rollout with what each machine leaves and reaches, got %+v", got)
	}
}

func TestPlanRolloutNothingToDo(t *testing.T) {
	got := PlanRollout([]Machine{inSync(cp1, true), inSync(w1, false)}, Settings{}, closed)
	if got.MayStart() || len(got.Pending) != 0 || got.BlockReason != "" {
		t.Fatalf("in-sync machines need no rollout and report no block: %+v", got)
	}
}

func TestPlanRolloutDriftPolicy(t *testing.T) {
	machines := []Machine{drifted(w1), inSync(cp1, true)}

	report := PlanRollout(machines, Settings{DriftPolicy: DriftReport}, open)
	if report.MayStart() || !slices.Equal(report.Drifted, []string{w1}) {
		t.Fatalf("Report must leave a drifted node alone and name it: %+v", report)
	}

	enforce := PlanRollout(machines, Settings{DriftPolicy: DriftEnforce}, open)
	if !enforce.MayStart() || !slices.Equal(enforce.Pending, []string{w1}) || len(enforce.Drifted) != 0 {
		t.Fatalf("Enforce must roll a drifted node back: %+v", enforce)
	}

	// A spec change is rolled out whatever the drift policy says.
	spec := PlanRollout([]Machine{changed(w1, false, classify.LiveSafe)}, Settings{DriftPolicy: DriftReport}, open)
	if !spec.MayStart() {
		t.Fatalf("a spec change must roll out under Report: %+v", spec)
	}
}

func TestPlanRolloutBlocks(t *testing.T) {
	change := []Machine{changed(w1, false, classify.LiveSafe), inSync(cp1, true)}

	stale := changed(w2, false, classify.LiveSafe)
	stale.Current = false

	tests := []struct {
		name     string
		machines []Machine
		settings Settings
		gate     Gate
		reason   string
		message  string
	}{
		{"paused", change, Settings{Paused: true}, open, BlockPaused, "spec.paused"},
		{"a closed gate", change, Settings{}, closed, BlockGateClosed, closed.Reason},
		{"an aborted change is not started again", change, Settings{AbortedTargets: map[string]string{w1: "b", "w-9": "b"}}, open,
			BlockAborted, "aborted"},
		{"stale status", append(slices.Clone(change), stale), Settings{}, open, BlockNotCurrent, w2},
		{"a protected field", []Machine{changed(cp1, true, classify.Protected)}, Settings{}, open, BlockProtected, "cp-1 (v1alpha1/Example)"},
		{"a boot-time change", []Machine{changed(w1, false, classify.BootTime)}, Settings{}, open, BlockRequiresReinstall, w1},
		// One blocked machine blocks the rollout for all of them.
		{"protected on one machine", []Machine{changed(w1, false, classify.LiveSafe), changed(cp1, true, classify.Protected)},
			Settings{}, open, BlockProtected, cp1},
		// Protected outranks everything a human could lift.
		{"protected while paused", []Machine{changed(cp1, true, classify.Protected)}, Settings{Paused: true}, closed, BlockProtected, cp1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PlanRollout(tt.machines, tt.settings, tt.gate)
			if got.MayStart() || got.BlockReason != tt.reason || !strings.Contains(got.BlockMessage, tt.message) {
				t.Fatalf("got reason=%q message=%q, want %q containing %q", got.BlockReason, got.BlockMessage, tt.reason, tt.message)
			}

			if len(got.Pending) == 0 {
				t.Fatal("a blocked rollout still reports what is pending")
			}
		})
	}
}

func TestPlanRolloutAbort(t *testing.T) {
	// The aborted operation was taking w-1 and w-2 to "b"; w-1 was done before the abort.
	aborted := Settings{AbortedTargets: map[string]string{w1: "b", w2: "b"}}

	done := inSync(w1, false)
	done.LiveHash, done.DesiredHash, done.AppliedHash = "b", "b", "b"

	rest := PlanRollout([]Machine{done, changed(w2, false, classify.LiveSafe)}, aborted, open)
	if rest.MayStart() || rest.BlockReason != BlockAborted || !rest.AbortStands {
		t.Fatalf("what the aborted rollout had left must not start again: %+v", rest)
	}

	// The abort does not depend on what the nodes run, only on what would be rolled out.
	moved := changed(w2, false, classify.LiveSafe)
	moved.LiveHash = "x"

	if got := PlanRollout([]Machine{done, moved}, aborted, open); got.BlockReason != BlockAborted {
		t.Fatalf("a node that drifted does not lift an abort: %+v", got)
	}

	// Reverting the spec is a new change: it rolls the finished machine back.
	reverted := done
	reverted.DesiredHash = "a"

	if got := PlanRollout([]Machine{reverted, inSync(w2, false)}, aborted, open); !got.MayStart() || got.AbortStands {
		t.Fatalf("a reverted spec must roll out and spend the abort: %+v", got)
	}

	// So is rendering something else.
	next := changed(w2, false, classify.LiveSafe)
	next.DesiredHash = "c"

	if got := PlanRollout([]Machine{done, next}, aborted, open); !got.MayStart() || got.AbortStands {
		t.Fatalf("a new config must roll out and spend the abort: %+v", got)
	}

	// Nothing pending and everything known: the abort is spent.
	if got := PlanRollout([]Machine{done, inSync(w2, false)}, aborted, open); got.AbortStands {
		t.Fatalf("an abort with nothing left to block is spent: %+v", got)
	}

	// A machine that cannot be judged keeps the abort, whatever else is pending.
	unknown := changed(w2, false, classify.LiveSafe)
	unknown.Current = false

	if got := PlanRollout([]Machine{done, unknown}, aborted, open); !got.AbortStands {
		t.Fatalf("an abort must outlive a machine without current status: %+v", got)
	}

	if got := PlanRollout([]Machine{inSync(w1, false)}, Settings{}, open); got.AbortStands {
		t.Fatal("no abort, nothing stands")
	}
}

func TestPlanRolloutApproval(t *testing.T) {
	machines := []Machine{changed(w1, false, classify.Storage), changed(w2, false, classify.LiveSafe)}

	waiting := PlanRollout(machines, Settings{}, open)
	if waiting.MayStart() || waiting.BlockReason != BlockAwaitingApproval || !slices.Equal(waiting.NeedsApproval, []string{w1}) {
		t.Fatalf("a storage change needs approval: %+v", waiting)
	}

	if !strings.Contains(waiting.BlockMessage, waiting.ChangeHash) {
		t.Fatalf("the message must carry the hash to approve: %q", waiting.BlockMessage)
	}

	approved := PlanRollout(machines, Settings{ApprovedChange: waiting.ChangeHash}, open)
	if !approved.MayStart() {
		t.Fatalf("an approved change may start: %+v", approved)
	}

	// The approval names one exact change. Rendering something else needs a new one.
	machines[0].DesiredHash = "c"

	replayed := PlanRollout(machines, Settings{ApprovedChange: waiting.ChangeHash}, open)
	if replayed.MayStart() || replayed.ChangeHash == waiting.ChangeHash {
		t.Fatalf("an approval must not carry over to a different change: %+v", replayed)
	}
}

func TestPlanRolloutHeldMachines(t *testing.T) {
	held := changed(w1, false, classify.LiveSafe)
	held.Hold = true

	skipped := changed(w2, false, classify.LiveSafe)
	skipped.SkippedHash = skipped.DesiredHash

	got := PlanRollout([]Machine{held, skipped, changed("w-3", false, classify.LiveSafe)}, Settings{}, open)
	if !slices.Equal(got.Pending, []string{"w-3"}) || !slices.Equal(got.Held, []string{w1, w2}) {
		t.Fatalf("held and skipped machines are left out and named: %+v", got)
	}

	// A skip is for one rendered config only.
	skipped.DesiredHash = "c"

	again := PlanRollout([]Machine{skipped}, Settings{}, open)
	if !slices.Equal(again.Pending, []string{w2}) {
		t.Fatalf("a new config rolls out to a previously skipped machine: %+v", again)
	}
}

func TestChangeHashIdentifiesTheChange(t *testing.T) {
	a := PlanRollout([]Machine{changed(w1, false, classify.LiveSafe), changed(w2, false, classify.LiveSafe)}, Settings{}, open)
	b := PlanRollout([]Machine{changed(w2, false, classify.LiveSafe), changed(w1, false, classify.LiveSafe)}, Settings{}, open)

	if a.ChangeHash == "" || a.ChangeHash != b.ChangeHash {
		t.Fatalf("the hash must not depend on order: %q vs %q", a.ChangeHash, b.ChangeHash)
	}

	fewer := PlanRollout([]Machine{changed(w1, false, classify.LiveSafe)}, Settings{}, open)
	if fewer.ChangeHash == a.ChangeHash {
		t.Fatal("a different set of machines is a different change")
	}
}
