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
)

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
	Policy Policy
	// FreePercent, MinimumSize and Interval are spec.maintenance.etcd, defaults filled in.
	FreePercent int64
	MinimumSize int64
	Interval    time.Duration
	// Last is when etcd was last defragmented; zero if never. Now is the time of this pass.
	Last, Now time.Time
}

// Defragment is the planner's answer to "is etcd to be defragmented, and may that start?". Its
// turns are the members to defragment: every control plane, because what fragments one fragments
// them all, followers first by name and the leader last.
type Defragment struct {
	Due
	// Fragmented names the members that are over the threshold, and Detail says the same with
	// their numbers. The numbers change with every write to the cluster, so they are for a log
	// and not for a condition.
	Fragmented []string
	Detail     []string
}

// PlanDefragment decides whether etcd is to be defragmented and whether that may start.
func PlanDefragment(members []DefragMember, s DefragSettings, p Pass) Defragment {
	var (
		out     Defragment
		unknown block
	)

	for _, m := range members {
		if m.Known && m.Size >= s.MinimumSize && m.free() >= s.FreePercent {
			out.Fragmented = append(out.Fragmented, m.Name)
			out.Detail = append(out.Detail, fmt.Sprintf("%s (%d%% of %d MiB is free)", m.Name, m.free(), m.Size>>20))
		}

		if !m.Known && unknown.reason == "" {
			unknown = block{BlockNotCurrent, "the size of the etcd database of " + m.Name + " was not read"}
		}
	}

	slices.Sort(out.Fragmented)
	slices.Sort(out.Detail)

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

	var tooSoon block
	if !s.Last.IsZero() && s.Now.Sub(s.Last) < s.Interval {
		tooSoon = block{BlockTooSoon, fmt.Sprintf("etcd was defragmented at %s, and is not again before %s have passed",
			s.Last.UTC().Format(time.RFC3339), s.Interval)}
	}

	out.Change = fingerprint(out.Fragmented)
	// A member that is being defragmented answers nobody. That is only asked of a cluster in which
	// everything else is in order, and of which every member's numbers are known.
	out.BlockReason, out.BlockMessage = firstBlock(
		s.Policy.block(),
		tooSoon,
		unknown,
		gateBlock(TouchGate(p, "").And(s.Policy.Gates())),
	)

	return out
}

// NextDefragStep decides what to do for the member that is being defragmented. reachable is
// whether the member's node answered in this pass; pre is the gate for defragmenting it (the
// whole cluster healthy, and nothing pausing the operation), checks the verification afterwards.
//
// A member is never defragmented while the gate is closed: not while another member is
// unhealthy, and not while the member before it has not been verified.
func NextDefragStep(step Step, reachable bool, pre, checks Gate, t Timing, now time.Time) Action {
	switch step.Name {
	case StepDefragmenting:
		return step.do(Defrag, "defragmented", reachable, notReachable, pre, t, now)
	case StepVerifying:
		reason := checks.Reason
		if !reachable {
			reason = notReachable
		}

		return step.settle(reachable && checks.Open, reason, t, now)
	default:
		return unknownStep(step.Name)
	}
}
