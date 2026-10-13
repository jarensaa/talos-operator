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

package talos

import (
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
	"github.com/siderolabs/talos/pkg/machinery/resources/runtime"
)

// What Talos calls the things the operator compares by name. No machinery type leaves this
// package, so a machine's type, its stage and a volume's phase cross it as strings; these are the
// values of those strings the rest of the operator asks about. Each is what machinery itself
// says, so it cannot drift from what a node reports, and TestNamesAreWhatTheOperatorReliesOn pins
// what status, the exported metrics and the planners rely on them being.
//
// They are variables only because machinery's names are not constants. Nothing assigns to them.
var (
	// MachineTypeControlPlane and MachineTypeWorker are what Reader.MachineType returns and
	// Member.MachineType holds.
	MachineTypeControlPlane = machine.TypeControlPlane.String()
	MachineTypeWorker       = machine.TypeWorker.String()

	// StageRunning is the stage of a booted, configured machine; StageMaintenance that of one
	// that has no config and waits for one (MachineStatus.Stage, Candidate.Stage).
	StageRunning     = runtime.MachineStageRunning.String()
	StageMaintenance = runtime.MachineStageMaintenance.String()

	// VolumeReady is the phase of a volume that is in order (Volume.Phase).
	VolumeReady = block.VolumePhaseReady.String()
)
