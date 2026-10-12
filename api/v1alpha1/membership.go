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

package v1alpha1

// IsMember reports whether the machine counts as a node of its cluster: it was found to be one
// (status.joinedAt), and has not been taken out since (status.removedAt). A machine that is not
// is one to be added, or one that has left; no rollout, no upgrade and no proof waits for it
// (docs/scope.md §7.6).
func (tm *TalosMachine) IsMember() bool {
	return tm.Status.JoinedAt != nil && tm.Status.RemovedAt == nil
}
