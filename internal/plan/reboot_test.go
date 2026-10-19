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

// A reboot a human asked for keeps the rules of an upgrade's reboot: not drained or rebooted while
// the gate is closed, rebooted only on the boot ID recorded, back on the system it ran, uncordoned
// only if the operator cordoned it, and verified before the operation halts again.
func TestARebootAHumanAskedFor(t *testing.T) {

	// The node runs the system it ran: Installed is that, for a reboot.
	running := with(nodeBefore(), func(n *UpgradeNode) { n.Installed = true })
	back := with(running, func(n *UpgradeNode) { n.BootID = bootAfter })
	elsewhere := with(back, func(n *UpgradeNode) { n.Installed = false })

	draining := upgradeStep(StepDraining)

	if got := NextRebootStep(draining, running, closed, closed, timing, t0.Add(time.Minute)); got.Kind != Wait {
		t.Fatalf("no node is cordoned while the gate is closed, got %+v", got)
	}

	if got := NextRebootStep(draining, running, open, closed, timing, t0.Add(time.Minute)); got.Kind != MarkCordoned {
		t.Fatalf("the cordon is recorded before it is made, got %+v", got)
	}

	cordoned := with(running, func(n *UpgradeNode) { n.Unschedulable = true })

	drained := draining
	drained.Cordoned = true

	if got := NextRebootStep(drained, cordoned, open, closed, timing, t0.Add(time.Minute)); got.Kind != Advance || got.Step != StepRebooting {
		t.Fatalf("a drained node is rebooted next, got %+v", got)
	}

	reboot := Step{Name: StepRebooting, Since: t0, BootID: bootBefore, Cordoned: true}

	if got := NextRebootStep(reboot, cordoned, closed, closed, timing, t0.Add(time.Minute)); got.Kind != Wait {
		t.Fatalf("no node is rebooted while the gate is closed, got %+v", got)
	}

	if got := NextRebootStep(reboot, cordoned, open, closed, timing, t0.Add(time.Minute)); got.Kind != Reboot {
		t.Fatalf("the node is rebooted while it has the recorded boot ID, got %+v", got)
	}

	if got := NextRebootStep(reboot, with(back, func(n *UpgradeNode) { n.Unschedulable = true }), open, closed, timing,
		t0.Add(time.Minute)); got.Kind != Advance || got.Step != StepBooting {
		t.Fatalf("a node that rebooted already is not rebooted again, got %+v", got)
	}

	booting := reboot
	booting.Name = StepBooting

	if got := NextRebootStep(booting, elsewhere, open, closed, timing, t0.Add(time.Minute)); got.Kind != Fail || got.Reason != FailFellBack {
		t.Fatalf("a node back on another system than it ran is a failure, got %+v", got)
	}

	if got := NextRebootStep(booting, back, open, closed, timing, t0.Add(time.Minute)); got.Kind != Advance || got.Step != StepUncordoning {
		t.Fatalf("a node back on its system is uncordoned next, got %+v", got)
	}

	// Someone else's cordon is theirs to undo.
	uncordoning := upgradeStep(StepUncordoning)
	if got := NextRebootStep(uncordoning, with(back, func(n *UpgradeNode) { n.Unschedulable = true }), open, closed, timing,
		t0.Add(time.Minute)); got.Kind != Advance || got.Step != StepVerifying {
		t.Fatalf("a node the operator did not cordon is left cordoned, got %+v", got)
	}

	verifying := Step{Name: StepVerifying, Since: t0, HealthySince: t0}
	if got := NextRebootStep(verifying, back, open, open, timing, t0.Add(timing.Soak)); got.Kind != Complete {
		t.Fatalf("a node healthy for the soak is done, got %+v", got)
	}

	if got := NextRebootStep(Step{Name: StepTrying, Since: t0}, back, open, open, timing, t0); got.Kind != Fail ||
		got.Reason != FailUnknownStep {
		t.Fatalf("a step a reboot does not have is unknown, got %+v", got)
	}
}
