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
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// The rollout policy a user declares (docs/scope.md §6.4): when nodes may be changed, what else
// must hold, and which Jobs run around an operation. This file is the part that decides; reading
// the objects and running the Jobs is the controller's.

// Policy is what the user's rollout policy says in this pass. The zero value allows everything.
type Policy struct {
	// Paused is spec.paused.
	Paused bool
	// Invalid says why the timings of spec.rollout cannot be used: under them a tried config
	// could never be confirmed. Empty while they can.
	Invalid string
	// OutsideWindow says why no maintenance window is open: none is, or they cannot be understood.
	// Empty while one is open, or none is declared.
	OutsideWindow string
	// WindowsInvalid is set when OutsideWindow is about windows that cannot be understood.
	WindowsInvalid bool
	// GateClosed names the user's gate that is closed, with what it said. Empty while all are open.
	GateClosed string
}

const paused = "spec.paused is set"

// Gates is the conjunction of the user's gates. It is asked wherever the cluster's own health is.
func (p Policy) Gates() Gate {
	if p.GateClosed != "" {
		return Gate{Reason: p.GateClosed}
	}

	return Gate{Open: true}
}

// Hold is open while a new node may begin: nothing is paused and a maintenance window is open. A
// node that is being changed is finished whatever it says.
func (p Policy) Hold() Gate {
	switch {
	case p.Paused:
		return Gate{Reason: paused}
	case p.OutsideWindow != "":
		return Gate{Reason: p.OutsideWindow}
	}

	return Gate{Open: true}
}

// block is why no operation starts now, whatever is due: it is paused, spec.rollout cannot be
// understood, or no maintenance window is open.
func (p Policy) block() block {
	switch {
	case p.Paused:
		return block{BlockPaused, paused}
	case p.WindowsInvalid:
		return block{BlockInvalid, p.OutsideWindow}
	case p.Invalid != "":
		return block{BlockInvalid, p.Invalid}
	case p.OutsideWindow != "":
		return block{BlockOutsideWindow, p.OutsideWindow}
	}

	return block{}
}

// Window is a recurring maintenance window.
type Window struct {
	// Schedule is a five-field cron expression for when the window opens.
	Schedule string
	Duration time.Duration
	// TimeZone is an IANA name; UTC if empty.
	TimeZone string
}

// maxWindow bounds how long a window may be, so that looking for the one that is open now never
// has to look further back.
const maxWindow = 7 * 24 * time.Hour

// Windows says whether a maintenance window is open at now, and when to look again: when the
// window that is open closes, or when the next one opens. With no windows any time is allowed,
// and there is nothing to look for again.
//
// A window that cannot be understood is an error, and the gate is then closed: a typo in a
// schedule must not turn "Saturday night" into "always".
func Windows(windows []Window, now time.Time) (Gate, time.Time, error) {
	if len(windows) == 0 {
		return Gate{Open: true}, time.Time{}, nil
	}

	var closes, opens time.Time

	for i, w := range windows {
		schedule, loc, err := parseWindow(w)
		if err != nil {
			err = fmt.Errorf("maintenanceWindows[%d]: %w", i, err)
			return Gate{Reason: err.Error()}, time.Time{}, err
		}

		local := now.In(loc)
		// The window that is open now, if one is, began after now-duration: the first opening
		// after that instant is it, or lies in the future.
		start := schedule.Next(local.Add(-w.Duration))
		if start.IsZero() {
			err := fmt.Errorf("maintenanceWindows[%d]: schedule %q never comes", i, w.Schedule)
			return Gate{Reason: err.Error()}, time.Time{}, err
		}

		if !start.After(local) {
			if end := start.Add(w.Duration); end.After(closes) {
				closes = end
			}

			continue
		}

		if opens.IsZero() || start.Before(opens) {
			opens = start
		}
	}

	if !closes.IsZero() {
		return Gate{Open: true, Note: "the maintenance window closes at " + closes.UTC().Format(time.RFC3339)}, closes, nil
	}

	return Gate{Reason: "outside the maintenance windows; the next opens at " + opens.UTC().Format(time.RFC3339)}, opens, nil
}

func parseWindow(w Window) (cron.Schedule, *time.Location, error) {
	if w.Duration <= 0 || w.Duration > maxWindow {
		return nil, nil, fmt.Errorf("duration %s must be positive and at most %s", w.Duration, maxWindow)
	}

	if strings.Contains(w.Schedule, "TZ=") {
		return nil, nil, fmt.Errorf("schedule %q: set the time zone in timeZone", w.Schedule)
	}

	schedule, err := cron.ParseStandard(w.Schedule)
	if err != nil {
		return nil, nil, fmt.Errorf("schedule %q: %w", w.Schedule, err)
	}

	loc := time.UTC
	if w.TimeZone != "" {
		if loc, err = time.LoadLocation(w.TimeZone); err != nil {
			return nil, nil, fmt.Errorf("timeZone %q: %w", w.TimeZone, err)
		}
	}

	return schedule, loc, nil
}

// FailHook: a hook's Job failed, or did not finish in time.
const FailHook = "HookFailed"

// HookJob is what was observed of a hook's Job.
type HookJob struct {
	// Exists is false until the Job was created.
	Exists bool
	// Succeeded and Failed are the Job's own verdict; Message is what it says about a failure.
	Succeeded, Failed bool
	Message           string
}

// NextHook decides what to do for the hook that is running. since is when it was started.
func NextHook(job HookJob, since time.Time, timeout time.Duration, now time.Time) Action {
	switch {
	case job.Succeeded:
		return Action{Kind: EndHook}
	case job.Failed:
		return fail(FailHook, "the hook's Job failed: %s", job.Message)
	case !now.Before(since.Add(timeout)):
		return fail(FailHook, "the hook's Job did not succeed within %s", timeout)
	case !job.Exists:
		return Action{Kind: Run}
	default:
		return wait("waiting for the hook's Job to finish")
	}
}

// Hook stages (docs/scope.md §6.4).
const (
	HookPreRollout  = talosv1alpha1.HookPreRollout
	HookPostRollout = talosv1alpha1.HookPostRollout
	HookPreNode     = talosv1alpha1.HookPreNode
	HookPostNode    = talosv1alpha1.HookPostNode
)

// Hooks is how many hooks each stage has.
type Hooks struct {
	PreRollout, PostRollout, PreNode, PostNode int
}

func (h Hooks) count(stage string) int {
	switch stage {
	case HookPreRollout:
		return h.PreRollout
	case HookPostRollout:
		return h.PostRollout
	case HookPreNode:
		return h.PreNode
	case HookPostNode:
		return h.PostNode
	}

	return 0
}

// Progress is where an operation stands between its turns.
type Progress struct {
	// Announced: the preRollout hooks ran, or there were none.
	Announced bool
	// Ending: the operation is over and only its postRollout hooks remain.
	Ending bool
	// Hook is the stage of the hook that is running, HookIndex its position and HookTurn the turn
	// of a node hook. HookDone is set when its Job succeeded.
	Hook      string
	HookIndex int
	HookTurn  string
	HookDone  bool
	// Current is the turn in flight, Next the first turn of the plan that is neither completed
	// nor skipped. Either may be empty.
	Current, Next string
	// Prepared is the turn whose preNode hooks ran; Finishing the turn that is verified and whose
	// postNode hooks are due.
	Prepared, Finishing string
}

// TurnKind is what an operation does next between its turns.
type TurnKind int

const (
	// TurnWait: nothing may begin; Turn.Reason says why.
	TurnWait TurnKind = iota
	// TurnHook: carry on with the hook that is running.
	TurnHook
	// TurnStartHook: persist Turn.Stage, Turn.Index and Turn.Turn as the hook that runs next.
	TurnStartHook
	// TurnAnnounce: the preRollout hooks are through; nodes may begin.
	TurnAnnounce
	// TurnPrepared: the preNode hooks of Turn.Turn are through.
	TurnPrepared
	// TurnBegin: persist the first step of Turn.Turn.
	TurnBegin
	// TurnStep: carry on with the step in flight.
	TurnStep
	// TurnComplete: Turn.Turn is done, postNode hooks included.
	TurnComplete
	// TurnEnd: every turn is done; the operation succeeded and ends.
	TurnEnd
	// TurnFinish: the operation is over, postRollout hooks included: release the lock.
	TurnFinish
)

// Turn is the planner's answer for an operation between its turns.
type Turn struct {
	Kind TurnKind
	// Stage and Index name a hook, Turn the plan entry concerned.
	Stage  string
	Index  int
	Turn   string
	Reason string
}

// NextTurn decides what an operation does next: which hook, which node, or that it is over. hold
// is Policy.Hold: while it is closed no node begins and no preRollout hook starts.
//
// What this function never does:
//   - begin a node, or its preNode hooks, while hold is closed;
//   - begin a node whose preNode hooks did not run;
//   - count a node as completed before its postNode hooks ran;
//   - release the lock of an operation that announced itself before its postRollout hooks ran;
//   - leave a node that is being changed, or a hook that is running, because hold closed.
func NextTurn(p Progress, hooks Hooks, hold Gate) Turn {
	switch {
	case p.Hook != "" && !p.HookDone:
		return Turn{Kind: TurnHook}
	case p.Hook != "" && p.HookIndex+1 < hooks.count(p.Hook):
		return Turn{Kind: TurnStartHook, Stage: p.Hook, Index: p.HookIndex + 1, Turn: p.HookTurn}
	case p.Hook != "":
		return afterHooks(p)
	case p.Ending && p.Announced && hooks.PostRollout > 0:
		return Turn{Kind: TurnStartHook, Stage: HookPostRollout}
	case p.Ending:
		return Turn{Kind: TurnFinish}
	case p.Current != "":
		return Turn{Kind: TurnStep}
	case p.Finishing != "" && hooks.PostNode > 0:
		return Turn{Kind: TurnStartHook, Stage: HookPostNode, Turn: p.Finishing}
	case p.Finishing != "":
		return Turn{Kind: TurnComplete, Turn: p.Finishing}
	case !p.Announced && !hold.Open:
		return Turn{Kind: TurnWait, Reason: hold.Reason}
	case !p.Announced && hooks.PreRollout > 0:
		return Turn{Kind: TurnStartHook, Stage: HookPreRollout}
	case !p.Announced:
		return Turn{Kind: TurnAnnounce}
	case p.Next == "":
		return Turn{Kind: TurnEnd}
	case !hold.Open:
		return Turn{Kind: TurnWait, Reason: hold.Reason}
	case p.Prepared != p.Next && hooks.PreNode > 0:
		return Turn{Kind: TurnStartHook, Stage: HookPreNode, Turn: p.Next}
	default:
		return Turn{Kind: TurnBegin, Turn: p.Next}
	}
}

// afterHooks is what follows the last hook of a stage.
func afterHooks(p Progress) Turn {
	switch p.Hook {
	case HookPreRollout:
		return Turn{Kind: TurnAnnounce}
	case HookPreNode:
		return Turn{Kind: TurnPrepared, Turn: p.HookTurn}
	case HookPostNode:
		return Turn{Kind: TurnComplete, Turn: p.HookTurn}
	default:
		return Turn{Kind: TurnFinish}
	}
}
