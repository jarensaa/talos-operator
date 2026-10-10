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
	"errors"
	"fmt"
	"time"
)

// Steps of a config change on one node. The step is persisted before its side effect, and a
// resumed operator decides what to do from the step and what it observes on the node, never
// from memory (docs/scope.md §8.1, §8.5.1).
//
// Every change is applied in try mode, whatever its class: a config that breaks the node, or
// only the operator's view of it, is reverted by Talos without anyone having to reach the node.
const (
	// StepTrying applies a config in try mode: Talos reverts it by itself at the deadline unless
	// it is confirmed.
	StepTrying = "Trying"
	// StepVerifying checks the node from outside; it must stay healthy for the soak.
	StepVerifying = "Verifying"
	// StepConfirming makes a tried and verified config persistent.
	StepConfirming = "Confirming"
	// StepRollingBack waits for Talos to revert a try that was not confirmed.
	StepRollingBack = "RollingBack"
)

// Failure reasons of a node step.
const (
	// FailConfigChanged: the node's config is neither the one the step started from nor the one
	// it applies, so something else changed it.
	FailConfigChanged = "ConfigChangedUnderneath"
	// FailTimeout: a bounded wait ran out. A timeout is a failure, never a skip.
	FailTimeout = "Timeout"
	// FailRolledBack: the try was not confirmed and the node is back on its previous config.
	FailRolledBack = "RolledBack"
	// FailNodeUnreachableAfterRollback: the try was not confirmed and the node did not come back on
	// its previous config. This needs a human: Talos only logs a revert that fails.
	FailNodeUnreachableAfterRollback = "NodeUnreachableAfterRollback"
	// FailRejected: Talos refused the config. The executor reports it; no observation leads to it.
	FailRejected = "Rejected"
)

const (
	// ConfirmMargin is how long before the try deadline the operator stops trying to confirm. The
	// deadline is persisted before the try is applied, so Talos's own timer always ends later;
	// the margin covers clock differences between operator replicas.
	ConfirmMargin = 30 * time.Second
	// RollbackGrace is how long after the deadline the operator waits before it expects the node
	// back on its previous config.
	RollbackGrace = 30 * time.Second
)

// Default timings (docs/scope.md §8.5.1).
const (
	DefaultTryTimeout  = 5 * time.Minute
	DefaultSoak        = 2 * time.Minute
	DefaultStepTimeout = 10 * time.Minute
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
	// to verify, for a node to come back after a try ended.
	StepTimeout time.Duration
}

// WithDefaults fills unset timings.
func (t Timing) WithDefaults() Timing {
	if t.TryTimeout == 0 {
		t.TryTimeout = DefaultTryTimeout
	}

	if t.Soak == 0 {
		t.Soak = DefaultSoak
	}

	if t.StepTimeout == 0 {
		t.StepTimeout = DefaultStepTimeout
	}

	return t
}

// Validate refuses timings under which a try could never be confirmed.
func (t Timing) Validate() error {
	if t.Soak < 0 || t.StepTimeout <= 0 {
		return errors.New("soak must not be negative and stepTimeout must be positive")
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

// Step is the persisted state of the node step in flight.
type Step struct {
	Name string
	// Tried is false only for a node that already ran the desired config when its turn came:
	// nothing is applied to it, it is only verified.
	Tried bool
	// PreviousHash is the node's config when the step began; DesiredHash the one being applied.
	PreviousHash, DesiredHash string
	// Since is when this step began.
	Since time.Time
	// Deadline is the end of the try window. It is persisted before the try is applied.
	Deadline time.Time
	// HealthySince is when verification first passed without interruption; zero if it does not.
	HealthySince time.Time
}

// Node is what was just observed on the step's node, through a different node where possible.
type Node struct {
	Reachable bool
	// ActiveHash and PersistentHash are hashes of the node's normalized configs. PersistentHash is
	// empty when the node has no persistent config resource, as after booting from disk.
	ActiveHash, PersistentHash string
}

// ActionKind is what the executor does next.
type ActionKind int

const (
	// Wait: nothing to do yet.
	Wait ActionKind = iota
	// Advance: persist the next step. No side effect happens in the same reconcile.
	Advance
	// MarkHealthy and MarkUnhealthy start and reset the soak.
	MarkHealthy
	MarkUnhealthy
	// Try: validate with a dry run, then apply in try mode with a timeout that ends at the
	// persisted deadline or later.
	Try
	// Rearm: persist a fresh try deadline. Only chosen while the node is still on its previous
	// config before the current deadline, when no try can be pending.
	Rearm
	// Confirm: re-apply the node's active config without a reboot, which makes it persistent.
	Confirm
	// Complete: the node runs the desired config, persistently, and is healthy.
	Complete
	// Fail: halt the operation.
	Fail
)

func (k ActionKind) String() string {
	return [...]string{"Wait", "Advance", "MarkHealthy", "MarkUnhealthy", "Try", "Rearm", "Confirm", "Complete", "Fail"}[k]
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

func wait(format string, args ...any) Action {
	return Action{Kind: Wait, Message: fmt.Sprintf(format, args...)}
}

func fail(reason, format string, args ...any) Action {
	return Action{Kind: Fail, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

func advance(step string) Action { return Action{Kind: Advance, Step: step} }

// NextStep decides what to do for the node step in flight.
//
// pre is the gate for touching the node at all: the cluster is healthy and nothing pauses the
// rollout. It only matters before the config is applied; a step that already changed the node
// always runs to its end. checks is the verification of the node from outside.
//
// The try protocol's safety rests on three rules this function never breaks:
//   - a try is only sent while the node's active config is still the previous one. Talos takes
//     whatever is active as the rollback target, so trying twice would disarm the rollback;
//   - a try is only confirmed while the persisted deadline, minus a margin, is still ahead;
//   - a try that was not confirmed is never reported as anything but a failure.
func NextStep(step Step, node Node, pre, checks Gate, t Timing, now time.Time) Action {
	switch step.Name {
	case StepTrying:
		return nextTrying(step, node, pre, t, now)
	case StepVerifying:
		if step.Tried {
			return nextVerifyingTry(step, node, checks, t, now)
		}

		return nextVerifying(step, node, checks, t, now)
	case StepConfirming:
		return nextConfirming(step, node, now)
	case StepRollingBack:
		return nextRollingBack(step, node, t, now)
	default:
		return fail(FailConfigChanged, "unknown step %q", step.Name)
	}
}

func timedOut(step Step, t Timing, now time.Time) bool {
	return !now.Before(step.Since.Add(t.StepTimeout))
}

func nextTrying(step Step, node Node, pre Gate, t Timing, now time.Time) Action {
	switch {
	case !now.Before(step.Deadline):
		return advance(StepRollingBack)
	case !node.Reachable:
		return wait("the node is not reachable")
	case node.ActiveHash == step.DesiredHash:
		return advance(StepVerifying)
	case node.ActiveHash != step.PreviousHash:
		return fail(FailConfigChanged, "the node's config changed while the operation was in flight")
	case timedOut(step, t, now):
		return fail(FailTimeout, "not applied within %s: %s", t.StepTimeout, pre.Reason)
	case step.Deadline.Sub(now) < t.Soak+ConfirmMargin+minVerifyWindow/2:
		// Too little of the persisted window is left to verify, soak and confirm. The node is
		// still on its previous config before the deadline, so no try is pending and a fresh
		// deadline is safe. Past the deadline that can no longer be told from a reverted try,
		// which is why the first case never re-arms.
		return Action{Kind: Rearm}
	case !pre.Open:
		return wait("%s", pre.Reason)
	default:
		return Action{Kind: Try}
	}
}

// nextVerifying verifies a node nothing was applied to. Its config must be settled (no staged
// config waiting for a reboot) and it must pass the same checks, for the same soak, as a node that
// was changed: the rollout only moves past a node it has seen healthy.
//
// A node booted from disk has no persistent config resource, so a try someone else left pending
// cannot be told from a settled config here. It reverts within the soak and is then caught as a
// config that changed underneath.
func nextVerifying(step Step, node Node, checks Gate, t Timing, now time.Time) Action {
	settled := node.PersistentHash == "" || node.PersistentHash == step.DesiredHash
	healthy := node.Reachable && settled && checks.Open

	switch {
	case node.Reachable && node.ActiveHash != step.DesiredHash:
		return fail(FailConfigChanged, "the node's config changed while the operation was in flight")
	case healthy && !step.HealthySince.IsZero() && now.Sub(step.HealthySince) >= t.Soak:
		return Action{Kind: Complete}
	case timedOut(step, t, now):
		return fail(FailTimeout, "not verified within %s: %s", t.StepTimeout, unverified(node, settled, checks))
	case !healthy && !step.HealthySince.IsZero():
		return Action{Kind: MarkUnhealthy, Message: unverified(node, settled, checks)}
	case !healthy:
		return wait("%s", unverified(node, settled, checks))
	case step.HealthySince.IsZero():
		return Action{Kind: MarkHealthy}
	default:
		return soaking(step, t)
	}
}

func unverified(node Node, settled bool, checks Gate) string {
	switch {
	case !node.Reachable:
		return "the node is not reachable"
	case !settled:
		return "the node has a staged config waiting for a reboot"
	default:
		return checks.Reason
	}
}

// soaking is the wait of a healthy node. Its message does not change while the soak runs.
func soaking(step Step, t Timing) Action {
	return wait("healthy since %s; soaking for %s", step.HealthySince.UTC().Format(time.RFC3339), t.Soak)
}

func nextVerifyingTry(step Step, node Node, checks Gate, t Timing, now time.Time) Action {
	healthy := node.Reachable && checks.Open

	switch {
	case !now.Before(step.Deadline.Add(-ConfirmMargin)):
		return advance(StepRollingBack)
	case node.Reachable && node.ActiveHash != step.DesiredHash:
		// Talos reverted it already, or something else changed the node. Either way it is not
		// the config under test any more.
		return advance(StepRollingBack)
	case !healthy && !step.HealthySince.IsZero():
		return Action{Kind: MarkUnhealthy, Message: unverifiedTry(node, checks)}
	case !healthy:
		return wait("%s", unverifiedTry(node, checks))
	case step.HealthySince.IsZero():
		return Action{Kind: MarkHealthy}
	case now.Sub(step.HealthySince) >= t.Soak:
		return advance(StepConfirming)
	default:
		return soaking(step, t)
	}
}

func unverifiedTry(node Node, checks Gate) string {
	if !node.Reachable {
		return "the node is not reachable"
	}

	return checks.Reason
}

func nextConfirming(step Step, node Node, now time.Time) Action {
	confirmed := node.Reachable && node.ActiveHash == step.DesiredHash && node.PersistentHash == step.DesiredHash

	switch {
	case confirmed:
		return Action{Kind: Complete}
	case !now.Before(step.Deadline.Add(-ConfirmMargin)):
		// A confirm that failed is sent again, but never this close to the end of the window. One
		// that did land without an answer is recognised while rolling back.
		return advance(StepRollingBack)
	case !node.Reachable:
		return wait("the node is not reachable")
	case node.ActiveHash != step.DesiredHash:
		return advance(StepRollingBack)
	default:
		return Action{Kind: Confirm}
	}
}

func nextRollingBack(step Step, node Node, t Timing, now time.Time) Action {
	settled := step.Deadline.Add(RollbackGrace)

	switch {
	case node.Reachable && node.ActiveHash == step.DesiredHash && node.PersistentHash == step.DesiredHash:
		// Only this operator confirms, and only after the soak: a confirm whose answer was lost.
		return Action{Kind: Complete}
	case now.Before(settled):
		return wait("waiting for Talos to revert the unconfirmed config")
	case node.Reachable && node.ActiveHash == step.PreviousHash:
		return fail(FailRolledBack, "the config was not confirmed, and the node is back on its previous config: "+
			"Talos reverted it, or it was never applied")
	case !now.Before(settled.Add(t.StepTimeout)):
		if !node.Reachable {
			return fail(FailNodeUnreachableAfterRollback, "the config was not confirmed and the node has not been reachable for %s after the "+
				"try window ended; check the node's console", t.StepTimeout)
		}

		return fail(FailNodeUnreachableAfterRollback,
			"the config was not confirmed and the node did not return to its previous config within %s "+
				"after the try window ended", t.StepTimeout)
	default:
		return wait("waiting for the node to come back on its previous config")
	}
}

// FirstStep returns the step a node's turn begins with: a try when its config must change, and
// verification alone when it already runs the desired config.
func FirstStep(tried bool) string {
	if tried {
		return StepTrying
	}

	return StepVerifying
}

// Untouched reports whether nothing was sent to the step's node yet, so the step can be dropped
// without leaving anything behind: it still waits to try, and the node is reachable and on its
// previous config before the deadline. Past the deadline the same observation may be a try
// that Talos reverted, which is a failure and never dropped.
func Untouched(step Step, node Node, now time.Time) bool {
	return step.Name == StepTrying && node.Reachable && node.ActiveHash == step.PreviousHash && now.Before(step.Deadline)
}
