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
	"slices"
	"strings"
	"testing"
	"time"
)

// Words and times the policy specs keep coming back to.
const (
	wordPaused      = "paused"
	wordOutside     = "outside"
	wordAborted     = "aborted"
	nightlySchedule = "0 2 * * *"
	nightlyCloses   = "2026-03-04T05:00:00Z"
	beginW1         = "begin w1"
	verifiedW1      = "verified w1"
	lastHook        = "postRollout[0] "
)

func at(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}

	return parsed
}

func TestWindows(t *testing.T) {
	nightly := Window{Schedule: nightlySchedule, Duration: 3 * time.Hour}
	oslo := Window{Schedule: nightlySchedule, Duration: 3 * time.Hour, TimeZone: "Europe/Oslo"}
	saturday := Window{Schedule: "30 22 * * 6", Duration: 4 * time.Hour}

	tests := []struct {
		name    string
		windows []Window
		now     string
		open    bool
		// again is when to look again: the closing of an open window, the opening of the next.
		again string
	}{
		{"no windows: any time", nil, "2026-03-04T12:00:00Z", true, ""},
		{"before it opens", []Window{nightly}, "2026-03-04T01:59:59Z", false, "2026-03-04T02:00:00Z"},
		{"as it opens", []Window{nightly}, "2026-03-04T02:00:00Z", true, nightlyCloses},
		{"inside", []Window{nightly}, "2026-03-04T04:59:59Z", true, nightlyCloses},
		{"as it closes", []Window{nightly}, nightlyCloses, false, "2026-03-05T02:00:00Z"},
		// 02:00 in Oslo is 01:00 UTC in winter and 00:00 UTC in summer.
		{"a time zone, winter", []Window{oslo}, "2026-01-15T01:30:00Z", true, "2026-01-15T04:00:00Z"},
		{"a time zone, summer", []Window{oslo}, "2026-07-15T00:30:00Z", true, "2026-07-15T03:00:00Z"},
		{"a time zone, summer, an hour late", []Window{oslo}, "2026-07-15T03:30:00Z", false, "2026-07-16T00:00:00Z"},
		// 2026-03-07 is a Saturday; the window runs into Sunday.
		{"across midnight", []Window{saturday}, "2026-03-08T01:00:00Z", true, "2026-03-08T02:30:00Z"},
		{"the wrong day", []Window{saturday}, "2026-03-04T23:00:00Z", false, "2026-03-07T22:30:00Z"},
		{"any of several", []Window{saturday, nightly}, "2026-03-04T03:00:00Z", true, nightlyCloses},
		{"the next of several", []Window{saturday, nightly}, "2026-03-07T12:00:00Z", false, "2026-03-07T22:30:00Z"},
		{"the longest of those open", []Window{nightly, {Schedule: "0 3 * * *", Duration: 5 * time.Hour}},
			"2026-03-04T03:30:00Z", true, "2026-03-04T08:00:00Z"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate, again, err := Windows(tt.windows, at(t, tt.now))
			if err != nil {
				t.Fatal(err)
			}

			if gate.Open != tt.open {
				t.Fatalf("open: got %v (%s), want %v", gate.Open, gate.Reason, tt.open)
			}

			if !tt.open && !strings.Contains(gate.Reason, tt.again) {
				t.Fatalf("a closed window must say when the next opens (%s): %q", tt.again, gate.Reason)
			}

			want := time.Time{}
			if tt.again != "" {
				want = at(t, tt.again)
			}

			if !again.Equal(want) {
				t.Fatalf("look again: got %s, want %s", again.UTC().Format(time.RFC3339), tt.again)
			}
		})
	}
}

// A window nobody can read must not turn into "always": it closes, and says why.
func TestWindowsThatCannotBeUnderstoodAreClosed(t *testing.T) {
	good := Window{Schedule: "* * * * *", Duration: time.Hour}

	for name, w := range map[string]Window{
		"not a schedule":       {Schedule: "every night", Duration: time.Hour},
		"six fields":           {Schedule: "0 0 2 * * *", Duration: time.Hour},
		"a day that is none":   {Schedule: "0 2 * * 9", Duration: time.Hour},
		"a date never reached": {Schedule: "0 0 31 2 *", Duration: time.Hour},
		"no such time zone":    {Schedule: nightlySchedule, Duration: time.Hour, TimeZone: "Europe/Atlantis"},
		"a zone in the cron":   {Schedule: "CRON_TZ=Europe/Oslo 0 2 * * *", Duration: time.Hour},
		"no duration":          {Schedule: nightlySchedule},
		"a negative duration":  {Schedule: nightlySchedule, Duration: -time.Hour},
		"longer than a week":   {Schedule: nightlySchedule, Duration: 8 * 24 * time.Hour},
	} {
		// Even next to a window that is open right now.
		gate, _, err := Windows([]Window{good, w}, at(t, "2026-03-04T03:00:00Z"))
		if err == nil || gate.Open || gate.Reason == "" {
			t.Errorf("%s: got gate %+v, err %v", name, gate, err)
		}
	}
}

func TestGateAnd(t *testing.T) {
	open, noted := Gate{Open: true}, Gate{Open: true, Note: "left out node-a"}
	closed, other := Gate{Reason: "etcd"}, Gate{Reason: "ceph"}

	for _, tt := range []struct {
		a, b, want Gate
	}{
		{open, open, open},
		{open, closed, closed},
		{closed, open, closed},
		// The first that is closed speaks: the cluster's own health comes before a user's gate.
		{closed, other, closed},
		{noted, open, noted},
		{noted, Gate{Open: true, Note: "window"}, Gate{Open: true, Note: "left out node-a; window"}},
		{noted, other, other},
	} {
		if got := tt.a.And(tt.b); got != tt.want {
			t.Errorf("%+v and %+v: got %+v, want %+v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestPolicy(t *testing.T) {
	var none Policy
	if !none.Gates().Open || !none.Hold(false).Open {
		t.Fatal("a cluster that declares no policy is held back by none")
	}

	if none.Hold(true).Open {
		t.Fatal("paused holds every new node")
	}

	p := Policy{OutsideWindow: "outside the maintenance windows", GateClosed: "gate ceph: HEALTH_WARN"}
	if p.Hold(false).Open || p.Hold(false).Reason != p.OutsideWindow {
		t.Fatalf("outside a window no node begins: %+v", p.Hold(false))
	}

	if p.Gates().Open || p.Gates().Reason != p.GateClosed {
		t.Fatalf("a closed gate is closed: %+v", p.Gates())
	}

	// Every kind of operation is held up by the policy, and says which part of it.
	plans := map[string]func(Policy) (string, string){
		"config rollout": func(p Policy) (string, string) {
			r := PlanRollout([]Machine{changed("w1", false, "LiveSafe")}, Settings{Policy: p}, healthyPass())
			return r.BlockReason, r.BlockMessage
		},
		"Talos upgrade": func(p Policy) (string, string) {
			u := PlanUpgrade([]UpgradeMachine{upgradable("w1", false)}, UpgradeSettings{Policy: p}, healthyPass())
			return u.BlockReason, u.BlockMessage
		},
		"Kubernetes upgrade": func(p Policy) (string, string) {
			u := PlanKubernetes([]KubernetesMachine{kubeWorker("w1", "1.36.2", "1.37.1")}, KubernetesSettings{Policy: p}, healthyPass())
			return u.BlockReason, u.BlockMessage
		},
	}

	for name, plan := range plans {
		for _, tt := range []struct {
			policy Policy
			reason string
		}{
			{Policy{}, ""},
			{Policy{OutsideWindow: wordOutside}, BlockOutsideWindow},
			{Policy{OutsideWindow: "maintenanceWindows[0]: bad", WindowsInvalid: true}, BlockInvalid},
			{Policy{GateClosed: "gate ceph: closed"}, BlockGateClosed},
			// When it is, before whether it could.
			{Policy{OutsideWindow: wordOutside, GateClosed: "gate ceph: closed"}, BlockOutsideWindow},
		} {
			reason, message := plan(tt.policy)
			if reason != tt.reason {
				t.Errorf("%s under %+v: got %q (%s), want %q", name, tt.policy, reason, message, tt.reason)
			}

			if want := tt.policy.OutsideWindow + tt.policy.GateClosed; reason != "" && !strings.Contains(want, message) {
				t.Errorf("%s under %+v: the message must be the policy's own: %q", name, tt.policy, message)
			}
		}
	}
}

func TestNextHook(t *testing.T) {
	const timeout = 5 * time.Minute

	tests := []struct {
		name    string
		job     HookJob
		elapsed time.Duration
		want    ActionKind
	}{
		{"not created yet", HookJob{}, 0, Run},
		{"running", HookJob{Exists: true}, time.Minute, Wait},
		{"succeeded", HookJob{Exists: true, Succeeded: true}, time.Minute, EndHook},
		{"failed", HookJob{Exists: true, Failed: true, Message: "BackoffLimitExceeded"}, time.Minute, Fail},
		{"too slow", HookJob{Exists: true}, timeout, Fail},
		// A Job that could not be created for the whole of its time is a failure too, not a wait.
		{"never created", HookJob{}, timeout, Fail},
		// What the Job says counts, however late it is read.
		{"succeeded, read late", HookJob{Exists: true, Succeeded: true}, 2 * timeout, EndHook},
	}

	for _, tt := range tests {
		got := NextHook(tt.job, t0, timeout, t0.Add(tt.elapsed))
		if got.Kind != tt.want {
			t.Errorf("%s: got %s (%s), want %s", tt.name, got.Kind, got.Message, tt.want)
		}

		if got.Kind == Fail && got.Reason != FailHook {
			t.Errorf("%s: reason %q", tt.name, got.Reason)
		}
	}
}

// operation plays an operation through NextTurn the way the controller does, and records what
// happened. It is the reference for what the controller has to persist for each answer.
type operation struct {
	p         Progress
	hooks     Hooks
	plan      []string
	done      []string
	events    []string
	finished  bool
	outcome   string
	stepsLeft int
}

func (o *operation) next() string {
	for _, turn := range o.plan {
		if !slices.Contains(o.done, turn) {
			return turn
		}
	}

	return ""
}

// advance takes one answer of the planner and applies it. It reports whether anything happened.
func (o *operation) advance(hold Gate) bool {
	o.p.Next = o.next()
	turn := NextTurn(o.p, o.hooks, hold)

	switch turn.Kind {
	case TurnWait:
		return false
	case TurnHook:
		// The Job runs and succeeds.
		o.events = append(o.events, fmt.Sprintf("%s[%d] %s", o.p.Hook, o.p.HookIndex, o.p.HookTurn))
		o.p.HookDone = true
	case TurnStartHook:
		o.p.Hook, o.p.HookIndex, o.p.HookTurn, o.p.HookDone = turn.Stage, turn.Index, turn.Turn, false
	case TurnAnnounce:
		o.p.Announced, o.p.Hook = true, ""
	case TurnPrepared:
		o.p.Prepared, o.p.Hook = turn.Turn, ""
	case TurnBegin:
		o.events = append(o.events, "begin "+turn.Turn)
		o.p.Current, o.stepsLeft = turn.Turn, 2
	case TurnStep:
		if o.stepsLeft--; o.stepsLeft == 0 {
			o.events = append(o.events, "verified "+o.p.Current)
			o.p.Finishing, o.p.Current = o.p.Current, ""
		}
	case TurnComplete:
		o.done = append(o.done, turn.Turn)
		o.p.Finishing, o.p.Prepared, o.p.Hook = "", "", ""
	case TurnEnd:
		o.p.Ending, o.outcome = true, "Succeeded"
	case TurnFinish:
		o.finished = true
	}

	return true
}

func (o *operation) run(t *testing.T, hold Gate) {
	t.Helper()

	for i := 0; !o.finished; i++ {
		if !o.advance(hold) {
			return
		}

		if i > 1000 {
			t.Fatal("the operation does not end")
		}
	}
}

func TestAnOperationRunsItsHooksInOrder(t *testing.T) {
	op := &operation{hooks: Hooks{PreRollout: 1, PostRollout: 1, PreNode: 2, PostNode: 1}, plan: []string{"w1", kubeCP1}}
	op.run(t, Gate{Open: true})

	want := []string{
		"preRollout[0] ",
		"preNode[0] w1", "preNode[1] w1", beginW1, verifiedW1, "postNode[0] w1",
		"preNode[0] cp1", "preNode[1] cp1", "begin cp1", "verified cp1", "postNode[0] cp1",
		lastHook,
	}
	if !slices.Equal(op.events, want) || !op.finished || op.outcome != "Succeeded" {
		t.Fatalf("got %q (finished %v, %s)\nwant %q", op.events, op.finished, op.outcome, want)
	}

	// Without hooks an operation is its turns and nothing else.
	plain := &operation{plan: []string{"w1", kubeCP1}}
	plain.run(t, Gate{Open: true})

	if want := []string{beginW1, verifiedW1, "begin cp1", "verified cp1"}; !slices.Equal(plain.events, want) || !plain.finished {
		t.Fatalf("got %q (finished %v)", plain.events, plain.finished)
	}
}

// A window that closes, or a pause, lets the node in flight finish with its hooks and holds the
// next one. Nothing starts while it is closed, and everything goes on when it opens.
func TestAHoldLetsTheNodeInFlightFinish(t *testing.T) {
	hooks := Hooks{PreRollout: 1, PostRollout: 1, PreNode: 1, PostNode: 1}
	closed := Gate{Reason: "outside the maintenance windows"}

	fresh := &operation{hooks: hooks, plan: []string{"w1", kubeCP1}}
	fresh.run(t, closed)

	if len(fresh.events) != 0 {
		t.Fatalf("nothing starts while the hold is closed, not even a preRollout hook: %q", fresh.events)
	}

	op := &operation{hooks: hooks, plan: []string{"w1", kubeCP1}}
	for !slices.Contains(op.events, beginW1) {
		op.advance(Gate{Open: true})
	}

	op.run(t, closed)

	if want := []string{"preRollout[0] ", "preNode[0] w1", beginW1, verifiedW1, "postNode[0] w1"}; !slices.Equal(op.events, want) {
		t.Fatalf("the node in flight is finished, hooks included, and no other begins:\n got %q\nwant %q", op.events, want)
	}

	if op.finished || op.p.Ending {
		t.Fatal("an operation that waits for a window is not over")
	}

	op.run(t, Gate{Open: true})

	if !op.finished || op.events[len(op.events)-1] != "postRollout[0] " {
		t.Fatalf("the operation goes on when the window opens: %q", op.events)
	}

	// The last node's end is the operation's end, window or not: nothing new begins by it.
	last := &operation{hooks: hooks, plan: []string{"w1"}}
	for !slices.Contains(last.events, beginW1) {
		last.advance(Gate{Open: true})
	}

	last.run(t, closed)

	if !last.finished {
		t.Fatalf("an operation with nothing left to begin ends, postRollout hooks included: %q", last.events)
	}
}

// An operation that ends early runs its postRollout hooks if, and only if, it ran its preRollout
// hooks: what the first set up, the second takes down.
func TestAnOperationThatEndsEarlyStillCleansUp(t *testing.T) {
	hooks := Hooks{PreRollout: 1, PostRollout: 2}

	announced := &operation{hooks: hooks, plan: []string{"w1"}, p: Progress{Announced: true, Ending: true}}
	announced.run(t, Gate{Reason: wordPaused})

	if want := []string{lastHook, "postRollout[1] "}; !slices.Equal(announced.events, want) || !announced.finished {
		t.Fatalf("got %q (finished %v)", announced.events, announced.finished)
	}

	unannounced := &operation{hooks: hooks, plan: []string{"w1"}, p: Progress{Ending: true}}
	unannounced.run(t, Gate{Open: true})

	if len(unannounced.events) != 0 || !unannounced.finished {
		t.Fatalf("got %q (finished %v)", unannounced.events, unannounced.finished)
	}
}

// Every situation an operation can be in between its turns, against the rules NextTurn promises.
func TestNextTurnInvariants(t *testing.T) {
	var (
		stages  = []string{"", HookPreRollout, HookPreNode, HookPostNode, HookPostRollout}
		turns   = []string{"", "a", "b"}
		counts  = []int{0, 1, 2}
		checked int
	)

	for _, hold := range []Gate{{Open: true}, {Reason: "held"}} {
		for _, pre := range counts {
			for _, post := range counts {
				hooks := Hooks{PreRollout: pre, PostRollout: post, PreNode: pre, PostNode: post}

				for flags := range 8 {
					for _, stage := range stages {
						for index := range 2 {
							for _, current := range turns {
								for _, next := range []string{"", "a"} {
									for _, prepared := range turns {
										for _, finishing := range turns {
											p := Progress{
												Announced: flags&1 != 0, Ending: flags&2 != 0, HookDone: flags&4 != 0,
												Hook: stage, HookIndex: index, HookTurn: "a",
												Current: current, Next: next, Prepared: prepared, Finishing: finishing,
											}
											if broken := turnRule(p, hooks, hold, NextTurn(p, hooks, hold)); broken != "" {
												t.Fatalf("%s\nprogress %+v\nhooks %+v, hold %+v\nanswer %+v", broken, p, hooks, hold, NextTurn(p, hooks, hold))
											}

											checked++
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	if checked < 50000 {
		t.Fatalf("only %d situations were checked", checked)
	}
}

func turnRule(p Progress, hooks Hooks, hold Gate, turn Turn) string {
	if broken := beginRule(p, hooks, hold, turn); broken != "" {
		return broken
	}

	return endRule(p, hooks, turn)
}

// beginRule holds an answer against the rules for what an operation may begin.
func beginRule(p Progress, hooks Hooks, hold Gate, turn Turn) string {
	starts := func(stage string) bool { return turn.Kind == TurnStartHook && turn.Stage == stage && turn.Index == 0 }
	busy := p.Hook != "" || p.Current != "" || p.Finishing != ""

	switch {
	case !hold.Open && (turn.Kind == TurnBegin || starts(HookPreNode) || starts(HookPreRollout)):
		return "something began while the hold was closed"
	case turn.Kind == TurnBegin && hooks.PreNode > 0 && p.Prepared != turn.Turn:
		return "a node began without its preNode hooks"
	case turn.Kind == TurnBegin && (!p.Announced || p.Ending || busy):
		return "a node began while the operation was not ready for one"
	case turn.Kind == TurnStartHook && turn.Index >= hooks.count(turn.Stage):
		return "a hook that does not exist was started"
	}

	return ""
}

// endRule holds an answer against the rules for what an operation may leave behind.
func endRule(p Progress, hooks Hooks, turn Turn) string {
	busy := p.Hook != "" || p.Current != "" || p.Finishing != ""

	switch {
	case turn.Kind == TurnComplete && hooks.PostNode > 0 && (p.Hook != HookPostNode || !p.HookDone || p.HookIndex+1 < hooks.PostNode):
		return "a node was completed before its postNode hooks ran"
	case turn.Kind == TurnFinish && p.Announced && hooks.PostRollout > 0 &&
		(p.Hook != HookPostRollout || !p.HookDone || p.HookIndex+1 < hooks.PostRollout):
		return "the lock was released before the postRollout hooks ran"
	case turn.Kind == TurnFinish && !p.Ending && p.Hook != HookPostRollout:
		return "the lock was released by an operation that was not over"
	case p.Hook != "" && !p.HookDone && turn.Kind != TurnHook:
		return "a running hook was left"
	case p.Hook == "" && !p.Ending && p.Current != "" && turn.Kind != TurnStep:
		return "a node that is being changed was left"
	case turn.Kind == TurnEnd && (p.Next != "" || busy):
		return "the operation ended with something left to do"
	}

	return ""
}
