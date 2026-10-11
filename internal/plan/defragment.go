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
	"fmt"
	"slices"
	"strings"
	"time"
)

// Defragmenting etcd (docs/scope.md §14.6). A member's database file only grows: what compaction
// and deletion free inside it stays allocated until the member is defragmented. Left alone, the
// file reaches etcd's quota, etcd raises NOSPACE and refuses every write, and the Kubernetes API
// with it. That is also the end of what an operator inside the cluster can do, since it can no
// longer write a step down. So the upkeep is to never get there: defragment while there is room.
//
// It is an operation like the others: the one lock, one member at a time, followers first and
// the leader last, each only while the whole cluster is healthy and verified before the next. A
// member does not answer while it is defragmented.

// StepDefragmenting defragments the member's database.
const StepDefragmenting = "Defragmenting"

// Defaults of spec.maintenance.etcd.
const (
	DefaultDefragmentFreePercent = 50
	DefaultDefragmentMinimumSize = 100 << 20
	DefaultDefragmentInterval    = 24 * time.Hour
	blockNotDue                  = ""
	// BlockTooSoon: etcd was defragmented less than the interval ago.
	BlockTooSoon = "TooSoon"
)

// Defrag is the action of StepDefragmenting.
const Defrag ActionKind = iota + 300

// DefragMember is what the planner needs to know about a control plane's etcd member.
type DefragMember struct {
	Name string
	// Node is the hostname of the machine's live node.
	Node string
	// Known is true when the member's sizes were read in this pass. Size is its database file,
	// InUse what of it holds data.
	Known       bool
	Size, InUse int64
	// Leader is true for the member that leads etcd, which goes last.
	Leader bool
}

// free is how much of the member's file is free space, in percent.
func (m DefragMember) free() int64 {
	if m.Size <= 0 || m.InUse > m.Size {
		return 0
	}

	return (m.Size - m.InUse) * 100 / m.Size
}

// DefragSettings is the cluster's side of defragmenting.
type DefragSettings struct {
	Paused bool
	Policy Policy
	// FreePercent, MinimumSize and Interval are spec.maintenance.etcd, defaults filled in.
	FreePercent int64
	MinimumSize int64
	Interval    time.Duration
	// Last is when etcd was last defragmented; zero if never. Now is the time of this pass.
	Last, Now time.Time
}

// Defragment is the planner's answer to "is etcd to be defragmented, and may that start?".
type Defragment struct {
	// Pending lists the members to defragment: every control plane, because what fragments one
	// fragments them all, followers first by name and the leader last.
	Pending []string
	// Fragmented says which members are over the threshold, with their numbers.
	Fragmented []string
	// Change identifies this defragmentation.
	Change string
	// BlockReason and BlockMessage are set when Pending is not empty but it may not start.
	BlockReason  string
	BlockMessage string
}

// MayStart reports whether etcd is to be defragmented and nothing blocks it.
func (d Defragment) MayStart() bool { return len(d.Pending) > 0 && d.BlockReason == "" }

// PlanDefragment decides whether etcd is to be defragmented and whether that may start.
func PlanDefragment(members []DefragMember, s DefragSettings, p Pass) Defragment {
	var out Defragment

	for _, m := range members {
		if m.Known && m.Size >= s.MinimumSize && m.free() >= s.FreePercent {
			out.Fragmented = append(out.Fragmented, fmt.Sprintf("%s (%d%% of %d MiB is free)", m.Name, m.free(), m.Size>>20))
		}
	}

	slices.Sort(out.Fragmented)

	if len(out.Fragmented) == 0 {
		return out
	}

	ordered := slices.Clone(members)
	slices.SortFunc(ordered, func(a, b DefragMember) int {
		return cmp.Or(cmp.Compare(rank(a.Leader), rank(b.Leader)), strings.Compare(a.Name, b.Name))
	})

	for _, m := range ordered {
		out.Pending = append(out.Pending, m.Name)
	}

	sum := sha256.Sum256([]byte(strings.Join(out.Fragmented, "\n")))
	out.Change = hex.EncodeToString(sum[:8])

	switch reason, message := s.Policy.block(s.Paused); {
	case reason != blockNotDue:
		out.BlockReason, out.BlockMessage = reason, message
	case !s.Last.IsZero() && s.Now.Sub(s.Last) < s.Interval:
		out.BlockReason = BlockTooSoon
		out.BlockMessage = fmt.Sprintf("etcd was defragmented at %s, and is not again before %s have passed",
			s.Last.UTC().Format(time.RFC3339), s.Interval)
	default:
		// A member that is being defragmented answers nobody. That is only asked of a cluster in
		// which everything else is in order, and of which every member's numbers are known.
		for _, m := range members {
			if !m.Known {
				out.BlockReason, out.BlockMessage = BlockNotCurrent, "the size of the etcd database of "+m.Name+" was not read"

				return out
			}
		}

		if gate := TouchGate(p, "").And(s.Policy.Gates()); !gate.Open {
			out.BlockReason, out.BlockMessage = BlockGateClosed, gate.Reason
		}
	}

	return out
}

// DefragStep is the persisted state of the step of a member that is being defragmented.
type DefragStep struct {
	Name string
	// Since is when this step began.
	Since time.Time
	// HealthySince is when verification first passed without interruption; zero if it does not.
	HealthySince time.Time
}

// NextDefragStep decides what to do for the member that is being defragmented. reachable is
// whether the member's node answered in this pass; pre is the gate for defragmenting it (the
// whole cluster healthy, and nothing pausing the operation), checks the verification afterwards.
//
// A member is never defragmented while the gate is closed: not while another member is
// unhealthy, and not while the member before it has not been verified.
func NextDefragStep(step DefragStep, reachable bool, pre, checks Gate, t Timing, now time.Time) Action {
	switch step.Name {
	case StepDefragmenting:
		switch {
		case timedOut(Step{Since: step.Since}, t, now):
			return fail(FailTimeout, "not defragmented within %s: %s", t.StepTimeout, cmp.Or(pre.Reason, notReachable))
		case !reachable:
			return wait(notReachable)
		case !pre.Open:
			return wait("%s", pre.Reason)
		default:
			return Action{Kind: Defrag}
		}
	case StepVerifying:
		reason := checks.Reason
		if !reachable {
			reason = notReachable
		}

		return settle(reachable && checks.Open, reason, step.Since, step.HealthySince, t, now)
	default:
		return fail(FailTimeout, "unknown step %q", step.Name)
	}
}
