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

import "time"

// A reboot a human asked for after an operation halted at a machine (docs/scope.md §8.3): a node
// whose change broke it in a way its revert did not undo, as a kubelet Talos lost, is drained,
// rebooted, and verified like an upgraded node, and the operation then halts again for the human
// to say what follows. It is the upgrade's sequence without the install: the node is to come back
// on the system it ran.

const (
	// FailRebooted is the halt after a reboot a human asked for: the node rebooted and is healthy,
	// and the step it had failed at waits for what the human says next. It is not a failure of the
	// node.
	FailRebooted = "Rebooted"
	// FailRebootNotAllowed: a reboot in flight may not go on, because upgrades were disabled or the
	// machine is no longer a member. Nothing more is sent.
	FailRebootNotAllowed = "RebootNotAllowed"
)

// NextRebootStep decides what to do for the step in flight of a reboot. node.Installed says
// whether the node runs the system it ran when the reboot was asked of it.
//
// It keeps every rule of NextUpgradeStep: the node is not cordoned, and not rebooted, while pre is
// closed; it is rebooted only while it still has the boot ID that was recorded; a node that comes
// back on another system is a failure; and it is uncordoned only if the operator cordoned it.
func NextRebootStep(step Step, node UpgradeNode, pre, checks Gate, t Timing, now time.Time) Action {
	switch step.Name {
	case StepDraining:
		return nextDraining(step, node, pre, t, now)
	case StepRebooting:
		return nextRebooting(step, node, pre, t, now)
	case StepBooting:
		return nextBooting(step, node, t, now)
	case StepUncordoning:
		return nextUncordoning(step, node, t, now)
	case StepVerifying:
		return nextSettling(step, node, checks, t, now)
	default:
		return unknownStep(step.Name)
	}
}
