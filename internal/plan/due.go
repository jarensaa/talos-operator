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
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// What every planner of an operation answers: what is due, in which order, and whether it may
// start. Each kind decides for itself what is due and what stands in its way; how that is said
// is the same for all of them.

// Due is an operation that is due, and whether it may start.
type Due struct {
	// Pending lists the turns of the operation, in order. Empty when nothing is due.
	Pending []string
	// Targets maps each turn to what it is to reach: the hash of a config, an installer image,
	// the image of a component. Nil for a kind whose turns all reach the same thing. An operation
	// does exactly what was planned, so this is what its turns are held against.
	Targets map[string]string
	// Change identifies exactly this operation: what is pending, with what it leaves and reaches.
	// Approvals and aborts refer to it, so neither can be replayed onto another change.
	Change string
	// Held lists machines that are due and are excluded: on hold, or left out by a human.
	Held []string
	// AbortStands is true while an earlier abort still covers everything that is pending, or that
	// cannot be told yet. Once it is false the abort is spent and can be forgotten.
	AbortStands bool
	// BlockReason and BlockMessage are set when Pending is not empty but the operation may not
	// start.
	BlockReason  string
	BlockMessage string
}

// MayStart reports whether the operation is due and nothing blocks it.
func (d Due) MayStart() bool { return len(d.Pending) > 0 && d.BlockReason == "" }

// Reasons an operation that is due may not start. They are the reasons of the RolloutBlocked
// condition, and of the condition of the kind that waits.
const (
	BlockPaused            = talosv1alpha1.ReasonPaused
	BlockNotCurrent        = talosv1alpha1.ReasonAwaitingStatus
	BlockProtected         = talosv1alpha1.ReasonProtectedChange
	BlockRequiresReinstall = talosv1alpha1.ReasonRequiresReinstall
	BlockAwaitingUpgrade   = talosv1alpha1.ReasonAwaitingUpgrade
	// BlockAwaitingKubernetes: a Kubernetes upgrade has to change the config first.
	BlockAwaitingKubernetes = talosv1alpha1.ReasonAwaitingKubernetesUpgrade
	BlockAborted            = talosv1alpha1.ReasonAborted
	BlockAwaitingApproval   = talosv1alpha1.ReasonAwaitingApproval
	BlockGateClosed         = talosv1alpha1.ReasonGateClosed
	// BlockOutsideWindow: no maintenance window is open.
	BlockOutsideWindow = talosv1alpha1.ReasonOutsideWindow
	// BlockInvalid: spec.rollout cannot be understood.
	BlockInvalid = talosv1alpha1.ReasonInvalidRollout
	// BlockIncompatible: a node cannot be taken to the version or installer its spec asks for.
	BlockIncompatible = talosv1alpha1.ReasonIncompatible
	// BlockNotAtRest: the cluster does not run what its spec asks for, so a machine that is added
	// would join on something else than its peers run.
	BlockNotAtRest = talosv1alpha1.ReasonNotAtRest
	// BlockHostnameInUse: the config gives the machine the hostname of a member. Talos removes
	// the etcd member of that name when a control plane joins.
	BlockHostnameInUse = talosv1alpha1.ReasonHostnameInUse
	// BlockQuorumAtRisk: taking the control plane out would leave etcd with fewer than three
	// voting members.
	BlockQuorumAtRisk = talosv1alpha1.ReasonQuorumAtRisk
	// BlockLastControlPlane: the machine is the cluster's only control plane.
	BlockLastControlPlane = talosv1alpha1.ReasonLastControlPlane
	// BlockTooSoon: etcd was defragmented less than the interval ago.
	BlockTooSoon = talosv1alpha1.ReasonTooSoon
)

// block is one reason an operation that is due may not start; the zero value is none.
type block struct{ reason, message string }

// firstBlock is the first of blocks that is one. A planner lists what may stand in the way of
// its operation, most fundamental first.
func firstBlock(blocks ...block) (string, string) {
	for _, b := range blocks {
		if b.reason != "" {
			return b.reason, b.message
		}
	}

	return "", ""
}

// staleBlock: nothing starts while a machine cannot be judged.
func staleBlock(stale []string) block {
	if len(stale) == 0 {
		return block{}
	}

	return block{BlockNotCurrent, "waiting for current status of " + strings.Join(stale, ", ")}
}

// abortedBlock: what a human aborted is not started again as it is. what names it.
func abortedBlock(stands bool, what string) block {
	if !stands {
		return block{}
	}

	return block{BlockAborted, what + " was aborted; change the spec, or revert it, to continue"}
}

// gateBlock: the gate of the first turn is closed. A gate that closed for something more
// particular than the cluster's health says so itself.
func gateBlock(gate Gate) block {
	if gate.Open {
		return block{}
	}

	return block{cmp.Or(gate.block, BlockGateClosed), gate.Reason}
}

// abortStands reports whether an abort still covers what is pending. It does while any machine
// cannot be judged, and while every pending turn is to reach what the aborted operation was
// taking it to. A pending turn with any other target is a change the human who aborted has not
// seen: the spec moved on, and so does the operation.
func abortStands(aborted map[string]string, stale []string, targets map[string]string) bool {
	if len(aborted) == 0 {
		return false
	}

	if len(stale) > 0 {
		return true
	}

	for turn, target := range targets {
		if aborted[turn] != target {
			return false
		}
	}

	return len(targets) > 0
}

// fingerprint identifies a change by the lines that describe it, whatever their order.
func fingerprint(lines []string) string {
	lines = slices.Clone(lines)
	slices.Sort(lines)

	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))

	return hex.EncodeToString(sum[:8])
}

func rank(b bool) int {
	if b {
		return 1
	}

	return 0
}

// ails reports whether the node of that name is known to be unhealthy.
func ails(p Pass, node string) bool {
	if p.Cluster == nil {
		return false
	}

	member, known := p.Cluster.member(node)

	return known && member.ails()
}
