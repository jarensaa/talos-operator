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

import "testing"

// The names come from machinery, and the rest of the operator relies on what they are: a
// TalosMachine's status holds a stage and the phases of its volumes, the exporter labels a metric
// with a phase, and planners and tests compare all of them. A machinery that calls one of them
// something else fails here, before it changes what a status that was written yesterday means.
func TestNamesAreWhatTheOperatorReliesOn(t *testing.T) {
	for name, tt := range map[string]struct{ got, want string }{
		"MachineTypeControlPlane": {MachineTypeControlPlane, "controlplane"},
		"MachineTypeWorker":       {MachineTypeWorker, "worker"},
		"StageRunning":            {StageRunning, "running"},
		"StageMaintenance":        {StageMaintenance, "maintenance"},
		"VolumeReady":             {VolumeReady, "ready"},
	} {
		if tt.got != tt.want {
			t.Errorf("%s is %q in this machinery; the operator relies on %q", name, tt.got, tt.want)
		}
	}
}
