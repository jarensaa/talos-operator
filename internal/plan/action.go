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
	"errors"
	"fmt"
	"strconv"
	"time"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// What every kind of step is made of: the timings that bound it, the step as it was persisted,
// the action a planner answers with, and the sequences the kinds share. A step of any kind waits
// for its node and its gate before it does something, drains a node the same way, and ends its
// turn with the same soak. Those are written here once.

// Default timings (docs/scope.md §8.5.1).
const (
	DefaultTryTimeout  = 5 * time.Minute
	DefaultSoak        = 2 * time.Minute
	DefaultStepTimeout = 10 * time.Minute
	// DefaultDrainTimeout bounds the eviction of a node's pods.
	DefaultDrainTimeout = 10 * time.Minute
	// minVerifyWindow is the least time a try must leave for verification besides the soak.
	minVerifyWindow = 2 * time.Minute
)

// Timing bounds the waits of a node step.
type Timing struct {
	// TryTimeout is the window Talos gives a tried config before reverting it.
	TryTimeout time.Duration
	// Soak is how long a node must stay healthy under a config before it is confirmed.
	Soak time.Duration
	// StepTimeout bounds every other wait: for a gate to open, for a node that needs no change
	// to verify, for a node to come back after a try ended or after a reboot.
	StepTimeout time.Duration
	// DrainTimeout bounds the eviction of a node's pods before an upgrade reboots it.
	DrainTimeout time.Duration
}

// TimingOf reads spec.rollout. A duration that is not set takes its default; one set to zero is
// zero.
func TimingOf(ro talosv1alpha1.Rollout) Timing {
	t := Timing{
		TryTimeout: DefaultTryTimeout, Soak: DefaultSoak, StepTimeout: DefaultStepTimeout, DrainTimeout: DefaultDrainTimeout,
	}

	if ro.TryTimeout != nil {
		t.TryTimeout = ro.TryTimeout.Duration
	}

	if ro.Soak != nil {
		t.Soak = ro.Soak.Duration
	}

	if ro.StepTimeout != nil {
		t.StepTimeout = ro.StepTimeout.Duration
	}

	if ro.DrainTimeout != nil {
		t.DrainTimeout = ro.DrainTimeout.Duration
	}

	return t
}

// Validate refuses timings under which a try could never be confirmed.
func (t Timing) Validate() error {
	if t.Soak < 0 || t.StepTimeout <= 0 || t.DrainTimeout <= 0 {
		return errors.New("soak must not be negative, and stepTimeout and drainTimeout must be positive")
	}

	if t.TryTimeout < t.Soak+minVerifyWindow {
		return fmt.Errorf("tryTimeout %s must exceed soak %s by at least %s, or a tried config could never be confirmed",
			t.TryTimeout, t.Soak, minVerifyWindow)
	}

	if t.StepTimeout <= t.Soak {
		return fmt.Errorf("stepTimeout %s must exceed soak %s", t.StepTimeout, t.Soak)
	}

	return nil
}

// Step is the persisted state of the step in flight, of whatever kind of operation. A kind reads
// what its steps record and leaves the rest alone.
type Step struct {
	Name string
	// Since is when this step began.
	Since time.Time
	// HealthySince is when verification first passed without interruption; zero if it does not.
	HealthySince time.Time
	// PreviousHash is the node's config when a config change began; DesiredHash the one being
	// applied. They are the same for a node that already ran the desired config when its turn
	// came: nothing is applied to it, it is only verified.
	PreviousHash, DesiredHash string
	// Deadline is the end of the try window. It is persisted before the try is applied.
	Deadline time.Time
	// BootID is the node's boot ID recorded before a reboot is asked for. A different boot ID
	// afterwards is the one fact that says the reboot happened.
	BootID string
	// Cordoned is true when the operator decided to cordon the node, which it records before it
	// does so. It only ever uncordons a node it cordoned itself.
	Cordoned bool
	// Dead is true when a human confirmed that the machine that is being removed is gone for good.
	Dead bool
}

// tried reports whether the step applies a config, rather than only verifying the one the node
// runs.
func (s Step) tried() bool { return s.PreviousHash != s.DesiredHash }

func (s Step) timedOut(t Timing, now time.Time) bool {
	return !now.Before(s.Since.Add(t.StepTimeout))
}

// ActionKind is what the executor does next.
type ActionKind int

// The actions of every kind of step, and of a hook. They are one enumeration so that no two
// kinds can share a value, whatever each planner answers with.
const (
	// Wait: nothing to do yet.
	Wait ActionKind = iota
	// Advance: persist the next step. No side effect happens in the same reconcile.
	Advance
	// MarkHealthy and MarkUnhealthy start and reset the soak.
	MarkHealthy
	MarkUnhealthy
	// Complete: the turn is done, and its node healthy.
	Complete
	// Fail: halt the operation.
	Fail

	// Try: validate with a dry run, then apply in try mode with a timeout that ends at the
	// persisted deadline or later.
	Try
	// Rearm: persist a fresh try deadline. Only chosen while the node is still on its previous
	// config before the current deadline, when no try can be pending.
	Rearm
	// Confirm: re-apply the node's active config without a reboot, which makes it persistent.
	Confirm

	// Pull an image to the node.
	Pull
	// Install the pulled image to the inactive boot slot.
	Install
	// MarkCordoned: persist that the operator is about to cordon the node.
	MarkCordoned
	// Cordon the node.
	Cordon
	// Evict the pods that are left.
	Evict
	// Reboot the node; a control plane gives up etcd leadership first.
	Reboot
	// Uncordon the node.
	Uncordon

	// Provision: hand the machine in maintenance mode its config.
	Provision
	// Reset: ask the node to leave the cluster and forget it.
	Reset
	// RemoveMember: take the dead machine's member out of etcd through another control plane.
	RemoveMember
	// DeleteNode: delete the machine's Node from Kubernetes.
	DeleteNode

	// Defrag: defragment the member's database.
	Defrag

	// Run: create a hook's Job.
	Run
	// EndHook: the hook's Job succeeded.
	EndHook

	// actionKinds is how many kinds there are.
	actionKinds
)

var actionNames = [actionKinds]string{
	Wait: "Wait", Advance: "Advance", MarkHealthy: "MarkHealthy", MarkUnhealthy: "MarkUnhealthy", Complete: "Complete", Fail: "Fail",
	Try: "Try", Rearm: "Rearm", Confirm: "Confirm",
	Pull: "Pull", Install: "Install", MarkCordoned: "MarkCordoned", Cordon: "Cordon", Evict: "Evict", Reboot: "Reboot", Uncordon: "Uncordon",
	Provision: "Provision", Reset: "Reset", RemoveMember: "RemoveMember", DeleteNode: "DeleteNode",
	Defrag: "Defrag",
	Run:    "Run", EndHook: "EndHook",
}

func (k ActionKind) String() string {
	if k < 0 || k >= actionKinds {
		return "ActionKind(" + strconv.Itoa(int(k)) + ")"
	}

	return actionNames[k]
}

// Action is the planner's answer for the step in flight.
type Action struct {
	Kind ActionKind
	// Step is the step to persist, for Advance.
	Step string
	// Reason is the failure reason, for Fail.
	Reason string
	// Message explains a Wait or a Fail.
	Message string
}

// Failure reasons every kind of step has.
const (
	// FailTimeout: a bounded wait ran out. A timeout is a failure, never a skip.
	FailTimeout = "Timeout"
	// FailDrainTimeout: the node's pods could not all be evicted in time. The node stays cordoned.
	FailDrainTimeout = "DrainTimeout"
	// FailUnknownStep: the step that was persisted is none this operator knows, as after a
	// downgrade of the operator in the middle of an operation.
	FailUnknownStep = "UnknownStep"
)

const (
	// notReachable is what a step waits for while its node does not answer.
	notReachable = "the node is not reachable"
	// notSent is why a step ran out of time although its node answered and its gate was open:
	// what the executor sent did not get there.
	notSent = "what was asked of the node did not go through"
	// nodeUnread is what a drain waits for while the node's Node could not be read.
	nodeUnread = "the node's Kubernetes Node could not be read"
)

func wait(format string, args ...any) Action {
	return Action{Kind: Wait, Message: fmt.Sprintf(format, args...)}
}

func fail(reason, format string, args ...any) Action {
	return Action{Kind: Fail, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

func advance(step string) Action { return Action{Kind: Advance, Step: step} }

func unknownStep(name string) Action { return fail(FailUnknownStep, "unknown step %q", name) }

// do is a step that does one thing to its node: it waits for the node to be there and for the
// gate to open, does it, and fails when it is not done within the step's timeout. undone says
// what was not done, and away what is waited for while the node is not there.
//
// The gate is what closes it, never the lack of a reason: a closed gate that says nothing still
// keeps the action back.
func (s Step) do(kind ActionKind, undone string, there bool, away string, pre Gate, t Timing, now time.Time) Action {
	waits := ""

	switch {
	case !there:
		waits = away
	case !pre.Open:
		waits = cmp.Or(pre.Reason, "a gate is closed")
	}

	switch {
	case s.timedOut(t, now):
		return fail(FailTimeout, "not %s within %s: %s", undone, t.StepTimeout, cmp.Or(waits, notSent))
	case !there || !pre.Open:
		return wait("%s", waits)
	default:
		return Action{Kind: kind}
	}
}

// drain cordons a node and evicts its pods, within the drain timeout. It reports false once the
// node is cordoned and nothing is left on it: the caller then says what follows.
//
// known is whether the node's Node was read, cordoned whether it is cordoned, and podsLeft how
// many pods still have to leave it, negative when they were not counted. A node is not cordoned
// while pre is closed. A node the operator did not cordon is either not cordoned yet, or cordoned
// by someone else, who is then also the one to uncordon it: the decision to cordon is recorded
// before the cordon (MarkCordoned).
func (s Step) drain(known, cordoned bool, podsLeft int, pre Gate, t Timing, now time.Time) (Action, bool) {
	waits := func() string {
		switch {
		case !known:
			return nodeUnread
		case !cordoned:
			return cmp.Or(pre.Reason, "the node is not cordoned yet")
		case podsLeft < 0:
			return "the node's pods could not be listed"
		default:
			return pods(podsLeft) + " still on the node"
		}
	}

	switch {
	case !now.Before(s.Since.Add(t.DrainTimeout)):
		return fail(FailDrainTimeout, "not drained within %s: %s", t.DrainTimeout, waits()), true
	case !known:
		return wait(nodeUnread), true
	case !cordoned && !pre.Open:
		return wait("%s", pre.Reason), true
	case !cordoned && !s.Cordoned:
		return Action{Kind: MarkCordoned}, true
	case !cordoned:
		return Action{Kind: Cordon}, true
	case podsLeft < 0:
		return wait("the node's pods could not be listed"), true
	case podsLeft > 0:
		return Action{Kind: Evict, Message: waits()}, true
	default:
		return Action{}, false
	}
}

func pods(n int) string {
	if n == 1 {
		return "1 pod"
	}

	return strconv.Itoa(n) + " pods"
}

// settle is the soak at the end of a turn: healthy for t.Soak without interruption, within the
// step's timeout. reason says what is not healthy.
func (s Step) settle(healthy bool, reason string, t Timing, now time.Time) Action {
	switch {
	case healthy && !s.HealthySince.IsZero() && now.Sub(s.HealthySince) >= t.Soak:
		return Action{Kind: Complete}
	case s.timedOut(t, now):
		return fail(FailTimeout, "not verified within %s: %s", t.StepTimeout, reason)
	case !healthy && !s.HealthySince.IsZero():
		return Action{Kind: MarkUnhealthy, Message: reason}
	case !healthy:
		return wait("%s", reason)
	case s.HealthySince.IsZero():
		return Action{Kind: MarkHealthy}
	default:
		return s.soaking(t)
	}
}

// soaking is the wait of a healthy node. Its message does not change while the soak runs.
func (s Step) soaking(t Timing) Action {
	return wait("healthy since %s; soaking for %s", s.HealthySince.UTC().Format(time.RFC3339), t.Soak)
}
