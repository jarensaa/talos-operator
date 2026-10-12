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
	"slices"
	"strings"
	"testing"
	"time"
)

func TestPlanDefragment(t *testing.T) {
	const mib = 1 << 20

	member := func(name string, size, inUse int64, leader bool) DefragMember {
		return DefragMember{Name: name, Node: name, Known: true, Size: size * mib, InUse: inUse * mib, Leader: leader}
	}

	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	settings := DefragSettings{
		FreePercent: DefaultDefragmentFreePercent, MinimumSize: DefaultDefragmentMinimumSize, Interval: DefaultDefragmentInterval, Now: now,
	}
	fragmented := []DefragMember{member("a", 400, 100, true), member("b", 400, 110, false), member("c", 390, 100, false)}

	got := PlanDefragment(fragmented, settings, passOf("a", "b", "c"))
	if !slices.Equal(got.Pending, []string{"b", "c", "a"}) || !got.MayStart() || len(got.Fragmented) != 3 {
		t.Fatalf("every member, followers first and the leader last: %+v", got)
	}

	for name, tt := range map[string]struct {
		members []DefragMember
		due     bool
	}{
		"half of it is in use":   {[]DefragMember{member("a", 400, 201, true)}, false},
		"exactly half is free":   {[]DefragMember{member("a", 400, 200, true)}, true},
		"too small to bother":    {[]DefragMember{member("a", 99, 1, true)}, false},
		"just large enough":      {[]DefragMember{member("a", 100, 1, true)}, true},
		"sizes not read":         {[]DefragMember{{Name: "a", Leader: true}}, false},
		"numbers that cannot be": {[]DefragMember{member("a", 400, 500, true)}, false},
		"one of three is fragmented": {
			[]DefragMember{member("a", 400, 390, true), member("b", 400, 100, false), member("c", 400, 390, false)}, true,
		},
		"none of three is":               {[]DefragMember{member("a", 400, 390, true), member("b", 400, 380, false)}, false},
		"an empty database of no member": {nil, false},
	} {
		got := PlanDefragment(tt.members, settings, passOf("a", "b", "c"))
		if (len(got.Pending) > 0) != tt.due {
			t.Errorf("%s: got %+v", name, got)
		}

		// What fragments one member fragments them all: all of them are defragmented.
		if tt.due && len(got.Pending) != len(tt.members) {
			t.Errorf("%s: got %+v", name, got)
		}
	}

	blocked := func(change func(*DefragSettings), p Pass, members []DefragMember) Defragment {
		s := settings
		if change != nil {
			change(&s)
		}

		return PlanDefragment(members, s, p)
	}

	sick := passOf("a", "b", "c")
	sick.Cluster.Members[1].Status.Ready = false

	unread := slices.Clone(fragmented)
	unread[2] = DefragMember{Name: "c", Node: "c"}

	for name, tt := range map[string]struct {
		got    Defragment
		reason string
	}{
		"paused": {blocked(func(s *DefragSettings) { s.Paused = true }, passOf("a", "b", "c"), fragmented), BlockPaused},
		"outside its window": {
			blocked(func(s *DefragSettings) { s.Policy.OutsideWindow = "opens at 02:00" }, passOf("a", "b", "c"), fragmented),
			BlockOutsideWindow,
		},
		"a member unhealthy": {blocked(nil, sick, fragmented), BlockGateClosed},
		"a user's gate": {
			blocked(func(s *DefragSettings) { s.Policy.GateClosed = "storage is rebuilding" }, passOf("a", "b", "c"), fragmented),
			BlockGateClosed,
		},
		"a member not read": {blocked(nil, passOf("a", "b", "c"), unread), BlockNotCurrent},
		"defragmented an hour ago": {
			blocked(func(s *DefragSettings) { s.Last = now.Add(-time.Hour) }, passOf("a", "b", "c"), fragmented), BlockTooSoon,
		},
	} {
		if tt.got.MayStart() || tt.got.BlockReason != tt.reason {
			t.Errorf("%s: got %+v", name, tt.got)
		}
	}

	aDayLater := blocked(func(s *DefragSettings) { s.Last = now.Add(-DefaultDefragmentInterval) }, passOf("a", "b", "c"), fragmented)
	if !aDayLater.MayStart() {
		t.Errorf("a day later it is due again: %+v", aDayLater)
	}
}

func TestNextDefragStep(t *testing.T) {
	long := timing.StepTimeout

	for _, tt := range []struct {
		name         string
		step         string
		reachable    bool
		pre, checks  Gate
		after        time.Duration
		healthySince time.Time
		want         ActionKind
		detail       string
	}{
		{name: "healthy cluster", step: StepDefragmenting, reachable: true, pre: open, want: Defrag},
		{name: "another member is unhealthy", step: StepDefragmenting, reachable: true, pre: closed, want: Wait, detail: "learner"},
		{name: "the member does not answer", step: StepDefragmenting, pre: open, want: Wait, detail: "not reachable"},
		{name: "the gate never opened", step: StepDefragmenting, reachable: true, pre: closed, after: long, want: Fail, detail: FailTimeout},
		{name: "healthy afterwards", step: StepVerifying, reachable: true, checks: open, want: MarkHealthy},
		{name: "soaked", step: StepVerifying, reachable: true, checks: open, healthySince: t0, after: timing.Soak, want: Complete},
		{name: "not back yet", step: StepVerifying, checks: open, want: Wait, detail: "not reachable"},
		{name: "unhealthy afterwards", step: StepVerifying, reachable: true, checks: closed, want: Wait, detail: "learner"},
		{name: "never healthy again", step: StepVerifying, reachable: true, checks: closed, after: long, want: Fail, detail: FailTimeout},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := NextDefragStep(DefragStep{Name: tt.step, Since: t0, HealthySince: tt.healthySince}, tt.reachable, tt.pre, tt.checks,
				timing, t0.Add(tt.after))
			if got.Kind != tt.want || !strings.Contains(got.Step+" "+got.Reason+" "+got.Message, tt.detail) {
				t.Fatalf("got %s %+v", got.Kind, got)
			}
		})
	}

	// Whatever is observed, a member is only defragmented while the gate is open and it answers.
	for _, step := range []string{StepDefragmenting, StepVerifying} {
		for _, reachable := range []bool{false, true} {
			for _, pre := range []Gate{open, closed} {
				got := NextDefragStep(DefragStep{Name: step, Since: t0}, reachable, pre, open, timing, t0)
				if (got.Kind == Defrag) != (step == StepDefragmenting && reachable && pre.Open) {
					t.Fatalf("%s reachable=%t gate=%t: got %s", step, reachable, pre.Open, got.Kind)
				}
			}
		}
	}
}
