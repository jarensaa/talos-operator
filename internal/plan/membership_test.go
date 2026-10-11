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

var membershipTiming = Timing{}.WithDefaults()

// passOf is a healthy pass over a cluster of the given control planes and one worker.
func passOf(controlPlanes ...string) Pass {
	p := healthyPass()
	c := healthyCluster()
	c.Members, c.EtcdMembers = nil, nil

	for i, name := range controlPlanes {
		m := healthyCluster().Members[0]
		m.Hostname = name
		c.Members = append(c.Members, m)
		c.EtcdMembers = append(c.EtcdMembers, EtcdMember{ID: uint64(i + 1), Hostname: name})
	}

	c.Members = append(c.Members, healthyCluster().Members[3])
	p.Cluster = &c

	return p
}

func TestRemovalGateKeepsThreeVoters(t *testing.T) {
	for _, tt := range []struct {
		name          string
		controlPlanes []string
		node          string
		dead          bool
		learner       string
		want          string
	}{
		{name: "a worker", controlPlanes: []string{"a", "b", "c"}, node: "worker-1"},
		{name: "one of four", controlPlanes: []string{"a", "b", "c", "d"}, node: "a"},
		{name: "one of five", controlPlanes: []string{"a", "b", "c", "d", "e"}, node: "a"},
		{name: "one of three", controlPlanes: []string{"a", "b", "c"}, node: "a", want: "would leave etcd with 2 voting members"},
		{name: "one of two", controlPlanes: []string{"a", "b"}, node: "a", want: "would leave etcd with 1 voting members"},
		{name: "the only one", controlPlanes: []string{"a"}, node: "a", want: "the only voting member"},
		// A member that is dead holds no vote that could be lost.
		{name: "a dead one of three", controlPlanes: []string{"a", "b", "c"}, node: "a", dead: true},
		{name: "a dead one of two", controlPlanes: []string{"a", "b"}, node: "a", dead: true},
		{name: "the only one, said to be dead", controlPlanes: []string{"a"}, node: "a", dead: true, want: "the only voting member"},
		// A learner has no vote, and a membership in transition is nobody's moment to leave.
		{name: "one of four, one of them a learner", controlPlanes: []string{"a", "b", "c", "d"}, node: "a", learner: "d",
			want: "d is still a learner"},
		{name: "a node nobody knows", controlPlanes: []string{"a", "b", "c"}, node: "x"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := passOf(tt.controlPlanes...)

			for i, m := range p.Cluster.EtcdMembers {
				p.Cluster.EtcdMembers[i].Learner = m.Hostname == tt.learner
			}

			gate := RemovalGate(p, tt.node, tt.dead)
			if gate.Open != (tt.want == "") || !strings.Contains(gate.Reason, tt.want) {
				t.Fatalf("got open=%t %q", gate.Open, gate.Reason)
			}
		})
	}

	// It is TouchGate first: another node that ails closes it, whoever is to leave.
	p := passOf("a", "b", "c", "d")
	p.Cluster.Members[1].Status.Ready = false

	if gate := RemovalGate(p, "a", false); gate.Open {
		t.Fatal("a control plane must not be taken out while another one is unhealthy")
	}

	// The node's own health is not asked about: an unhealthy node is one to take out.
	if gate := RemovalGate(p, "b", false); !gate.Open {
		t.Fatalf("an unhealthy node must be removable while the others are healthy: %s", gate.Reason)
	}
}

func TestPlanRemoval(t *testing.T) {
	machines := []RemoveMachine{
		{Name: "cp-d", Node: "d", Reachable: true, ControlPlane: true},
		{Name: "w-2", Node: "worker-2", Reachable: true, HostsOperator: true},
		{Name: "w-1", Node: "worker-1", Reachable: true},
	}

	got := PlanRemoval(machines, RemovalSettings{}, passOf("a", "b", "c", "d"))
	if !slices.Equal(got.Pending, []string{"w-1", "w-2", "cp-d"}) || !got.MayStart() {
		t.Fatalf("workers go first, the operator's own node last among them, control planes after: %+v", got)
	}

	// The first one decides whether it starts.
	got = PlanRemoval(machines[:1], RemovalSettings{}, passOf("a", "b", "d"))
	if got.MayStart() || got.BlockReason != BlockQuorumAtRisk {
		t.Fatalf("got %+v", got)
	}

	got = PlanRemoval(machines[:1], RemovalSettings{}, passOf("d"))
	if got.MayStart() || got.BlockReason != BlockLastControlPlane {
		t.Fatalf("got %+v", got)
	}

	got = PlanRemoval(machines, RemovalSettings{Paused: true}, passOf("a", "b", "c", "d"))
	if got.MayStart() || got.BlockReason != BlockPaused {
		t.Fatalf("got %+v", got)
	}

	sick := passOf("a", "b", "c", "d")
	sick.Cluster.Members[0].Status.Ready = false

	got = PlanRemoval(machines, RemovalSettings{}, sick)
	if got.MayStart() || got.BlockReason != BlockGateClosed {
		t.Fatalf("got %+v", got)
	}

	got = PlanRemoval(machines, RemovalSettings{Policy: Policy{GateClosed: "storage is rebuilding"}}, passOf("a", "b", "c", "d"))
	if got.MayStart() || got.BlockReason != BlockGateClosed || !strings.Contains(got.BlockMessage, "storage") {
		t.Fatalf("got %+v", got)
	}

	// What a human left out stays out, and what is left goes on.
	got = PlanRemoval(machines, RemovalSettings{Held: []string{"w-1"}}, passOf("a", "b", "c", "d"))
	if !slices.Equal(got.Pending, []string{"w-2", "cp-d"}) || !slices.Equal(got.Held, []string{"w-1"}) {
		t.Fatalf("got %+v", got)
	}

	// A machine that does not answer is not judged by a pass that could not read it: its removal
	// begins, and its own turn is where it waits, halts, and asks a human.
	silent := []RemoveMachine{{Name: "cp-d", Node: "d", ControlPlane: true}}
	incomplete := passOf("a", "b", "c", "d")
	incomplete.Incomplete = "d: not reachable"

	if got := PlanRemoval(silent, RemovalSettings{}, incomplete); !got.MayStart() {
		t.Fatalf("got %+v", got)
	}

	if got := PlanRemoval(silent, RemovalSettings{Paused: true}, incomplete); got.MayStart() {
		t.Fatalf("a pause holds it like any other: %+v", got)
	}

	if got := PlanRemoval(nil, RemovalSettings{}, passOf("a")); got.MayStart() || len(got.Pending) != 0 {
		t.Fatalf("got %+v", got)
	}
}

func TestPlanJoin(t *testing.T) {
	ready := func(name string, controlPlane bool) JoinMachine {
		return JoinMachine{Name: name, ControlPlane: controlPlane, Provisionable: true, Config: "config-of-" + name, Hostname: name}
	}

	machines := []JoinMachine{ready("cp-d", true), ready("w-3", false), ready("w-2", false)}
	names := []string{"a", "b", "c", "worker-1"}

	got := PlanJoin(machines, JoinSettings{Hostnames: names}, passOf("a", "b", "c"))
	if !slices.Equal(got.Pending, []string{"w-2", "w-3", "cp-d"}) || !got.MayStart() || got.Configs["cp-d"] != "config-of-cp-d" {
		t.Fatalf("workers first, then control planes: %+v", got)
	}

	// A machine that was not found fit to be added waits, and does not hold the others up.
	waiting := JoinMachine{Name: "w-4", NotProvisionable: "the machine at the address is another one"}
	held := JoinMachine{Name: "w-5", Provisionable: true, Hold: true}

	got = PlanJoin(append(machines, waiting, held, JoinMachine{Name: "w-6"}), JoinSettings{Hostnames: names}, passOf("a", "b", "c"))
	if len(got.Pending) != 3 || !got.MayStart() ||
		!slices.Equal(got.Waiting, []string{"w-4: the machine at the address is another one", "w-6: not found in maintenance mode yet"}) {
		t.Fatalf("got %+v", got)
	}

	for name, tt := range map[string]struct {
		settings JoinSettings
		pass     Pass
		machines []JoinMachine
		reason   string
	}{
		"paused":                {JoinSettings{Paused: true, Hostnames: names}, passOf("a", "b", "c"), machines, BlockPaused},
		"outside its window":    {JoinSettings{Policy: Policy{OutsideWindow: "opens at 02:00"}}, passOf("a", "b", "c"), machines, BlockOutsideWindow},
		"a rollout is due":      {JoinSettings{NotAtRest: "node-a does not run its rendered config"}, passOf("a", "b", "c"), machines, BlockNotAtRest},
		"a user's gate":         {JoinSettings{Policy: Policy{GateClosed: "storage is rebuilding"}}, passOf("a", "b", "c"), machines, BlockGateClosed},
		"a name that is taken":  {JoinSettings{Hostnames: append(names, "w-2")}, passOf("a", "b", "c"), machines, BlockHostnameInUse},
		"the cluster not read":  {JoinSettings{Hostnames: names}, Pass{}, machines, BlockGateClosed},
		"a learner in the list": {JoinSettings{Hostnames: names}, withLearner(passOf("a", "b", "c")), machines, BlockGateClosed},
	} {
		got := PlanJoin(tt.machines, tt.settings, tt.pass)
		if got.MayStart() || got.BlockReason != tt.reason {
			t.Errorf("%s: got %+v", name, got)
		}
	}

	// A machine that names itself cannot collide by its config.
	unnamed := ready("w-2", false)
	unnamed.Hostname = ""

	if got := PlanJoin([]JoinMachine{unnamed}, JoinSettings{Hostnames: append(names, "w-2")}, passOf("a", "b", "c")); !got.MayStart() {
		t.Fatalf("got %+v", got)
	}

	// What a human left out with this config stays out; with another config it is to be added again.
	got = PlanJoin(machines, JoinSettings{Hostnames: names, Held: map[string]string{"w-2": "config-of-w-2", "w-3": "an older config"}},
		passOf("a", "b", "c"))
	if !slices.Equal(got.Pending, []string{"w-3", "cp-d"}) || !slices.Equal(got.Held, []string{"w-2"}) {
		t.Fatalf("got %+v", got)
	}
}

func withLearner(p Pass) Pass {
	p.Cluster.EtcdMembers[0].Learner = true

	return p
}

var began = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

func TestNextJoinStep(t *testing.T) {
	candidate := JoinNode{Probed: true, InMaintenance: true, IdentityMatches: true}
	member := JoinNode{Member: true, Running: true, KubernetesKnown: true, Registered: true, NodeReady: true}
	controlPlane := func(n JoinNode, voter bool) JoinNode {
		n.ControlPlane, n.EtcdKnown, n.EtcdVoter = true, true, voter

		return n
	}
	with := func(n JoinNode, change func(*JoinNode)) JoinNode {
		change(&n)

		return n
	}

	long := membershipTiming.StepTimeout
	soaked := began.Add(membershipTiming.Soak)

	for _, tt := range []struct {
		name         string
		step         string
		node         JoinNode
		pre, checks  Gate
		after        time.Duration
		healthySince time.Time
		want         ActionKind
		detail       string
	}{
		{name: "a machine that waits, is the one, and fits", step: StepProvisioning, node: candidate, pre: open, want: Provision},
		{name: "the gate is closed", step: StepProvisioning, node: candidate, pre: closed, want: Wait, detail: "learner"},
		{name: "nothing answers", step: StepProvisioning, node: JoinNode{}, pre: open, want: Wait, detail: "nothing answers"},
		{name: "something else answers", step: StepProvisioning, node: JoinNode{Probed: true}, pre: open, want: Wait, detail: "not waiting"},
		// The config carries every secret of the cluster.
		{name: "another machine answers", step: StepProvisioning, node: with(candidate, func(n *JoinNode) { n.IdentityMatches = false }),
			pre: open, want: Fail, detail: FailIdentity},
		{name: "another machine answers, and the gate is closed", step: StepProvisioning,
			node: with(candidate, func(n *JoinNode) { n.IdentityMatches = false }), pre: closed, want: Fail, detail: FailIdentity},
		{name: "the config does not fit", step: StepProvisioning,
			node: with(candidate, func(n *JoinNode) { n.FitProblems = []string{"the install disk selector matches 2 disks"} }),
			pre:  open, want: Fail, detail: FailDoesNotFit},
		{name: "it never showed up", step: StepProvisioning, node: JoinNode{}, pre: open, after: long, want: Fail, detail: FailTimeout},
		{name: "the gate never opened", step: StepProvisioning, node: candidate, pre: closed, after: long, want: Fail, detail: FailTimeout},
		// It has its config already: it is never handed one again.
		{name: "it already is a member", step: StepProvisioning, node: member, pre: open, want: Advance, detail: StepJoining},
		{name: "it already is a member, late", step: StepProvisioning, node: member, pre: closed, after: 10 * long, want: Advance,
			detail: StepJoining},

		{name: "installing", step: StepJoining, node: JoinNode{}, want: Wait, detail: "has not come up"},
		{name: "booting", step: StepJoining, node: with(member, func(n *JoinNode) { n.Running = false }), want: Wait, detail: "booting"},
		{name: "no Node yet", step: StepJoining, node: with(member, func(n *JoinNode) { n.Registered = false }), want: Wait, detail: "no Node"},
		{name: "Node not ready", step: StepJoining, node: with(member, func(n *JoinNode) { n.NodeReady = false }), want: Wait, detail: "Ready"},
		{name: "Kubernetes not read", step: StepJoining, node: with(member, func(n *JoinNode) { n.KubernetesKnown = false }), want: Wait,
			detail: "could not be read"},
		{name: "a worker that joined", step: StepJoining, node: member, want: Advance, detail: StepVerifying},
		{name: "a control plane that is a learner", step: StepJoining, node: controlPlane(member, false), want: Wait, detail: "voting member"},
		{name: "a control plane, etcd not read", step: StepJoining,
			node: with(controlPlane(member, true), func(n *JoinNode) { n.EtcdKnown = false }), want: Wait, detail: "etcd could not be read"},
		{name: "a control plane that joined", step: StepJoining, node: controlPlane(member, true), want: Advance, detail: StepVerifying},
		{name: "still installing after one step timeout", step: StepJoining, node: JoinNode{}, after: long, want: Wait},
		{name: "never joined", step: StepJoining, node: JoinNode{}, after: JoinTimeoutFactor * long, want: Fail, detail: FailNotJoined},

		{name: "healthy for the first time", step: StepVerifying, node: member, checks: open, want: MarkHealthy},
		{name: "soaking", step: StepVerifying, node: member, checks: open, healthySince: began, after: time.Second, want: Wait, detail: "soaking"},
		{name: "soaked", step: StepVerifying, node: member, checks: open, healthySince: began, after: membershipTiming.Soak, want: Complete},
		{name: "the cluster is not healthy", step: StepVerifying, node: member, checks: closed, want: Wait, detail: "learner"},
		{name: "it stopped being healthy", step: StepVerifying, node: member, checks: closed, healthySince: began, after: time.Second,
			want: MarkUnhealthy},
		{name: "it left again", step: StepVerifying, node: with(member, func(n *JoinNode) { n.NodeReady = false }), checks: open,
			healthySince: soaked, after: time.Minute, want: MarkUnhealthy},
		{name: "never verified", step: StepVerifying, node: member, checks: closed, after: long, want: Fail, detail: FailTimeout},
	} {
		t.Run(tt.name, func(t *testing.T) {
			step := JoinStep{Name: tt.step, Since: began, HealthySince: tt.healthySince}

			got := NextJoinStep(step, tt.node, tt.pre, tt.checks, membershipTiming, began.Add(tt.after))
			if got.Kind != tt.want || !strings.Contains(got.Step+" "+got.Reason+" "+got.Message, tt.detail) {
				t.Fatalf("got %s %+v", got.Kind, got)
			}
		})
	}
}

// Whatever is observed, a config is only handed to a machine that just said it waits, is the one,
// and fits; and a machine that already is a member is never handed one.
func TestJoinNeverProvisionsWhatItShouldNot(t *testing.T) {
	bools := []bool{false, true}

	for _, probed := range bools {
		for _, maintenance := range bools {
			for _, identity := range bools {
				for _, fits := range bools {
					for _, member := range bools {
						for _, pre := range []Gate{open, closed} {
							node := JoinNode{Probed: probed, InMaintenance: maintenance, IdentityMatches: identity, Member: member}
							if !fits {
								node.FitProblems = []string{"no such disk"}
							}

							got := NextJoinStep(JoinStep{Name: StepProvisioning, Since: began}, node, pre, Gate{}, membershipTiming, began)
							allowed := probed && maintenance && identity && fits && !member && pre.Open

							if (got.Kind == Provision) != allowed {
								t.Fatalf("%+v with the gate open=%t: got %s", node, pre.Open, got.Kind)
							}
						}
					}
				}
			}
		}
	}

	for _, step := range []string{StepJoining, StepVerifying} {
		node := JoinNode{Probed: true, InMaintenance: true, IdentityMatches: true}
		if got := NextJoinStep(JoinStep{Name: step, Since: began}, node, open, open, membershipTiming, began); got.Kind == Provision {
			t.Fatalf("%s: a config is handed over in the first step and in no other", step)
		}
	}
}

func TestNextRemovalStep(t *testing.T) {
	live := RemovalNode{Reachable: true, KubernetesKnown: true, Registered: true, PodsLeft: -1}
	with := func(n RemovalNode, change func(*RemovalNode)) RemovalNode {
		change(&n)

		return n
	}
	cordoned := func(pods int) RemovalNode {
		return with(live, func(n *RemovalNode) { n.Unschedulable, n.PodsLeft = true, pods })
	}
	gone := RemovalNode{KubernetesKnown: true, Registered: true, Unschedulable: true}
	wiped := with(gone, func(n *RemovalNode) { n.Wiped = true })
	controlPlane := func(n RemovalNode, member bool) RemovalNode {
		n.ControlPlane, n.EtcdKnown, n.EtcdMember = true, true, member

		return n
	}

	long := membershipTiming.StepTimeout

	for _, tt := range []struct {
		name         string
		step         RemovalStep
		node         RemovalNode
		pre, checks  Gate
		after        time.Duration
		healthySince time.Time
		want         ActionKind
		detail       string
	}{
		{name: "not cordoned yet", step: RemovalStep{Name: StepDraining}, node: live, pre: open, want: MarkCordoned},
		{name: "about to cordon", step: RemovalStep{Name: StepDraining, Cordoned: true}, node: live, pre: open, want: Cordon},
		{name: "the gate is closed", step: RemovalStep{Name: StepDraining}, node: live, pre: closed, want: Wait, detail: "learner"},
		{name: "pods left", step: RemovalStep{Name: StepDraining, Cordoned: true}, node: cordoned(3), pre: open, want: Evict, detail: "3 pods"},
		// A node that is being drained is finished, whatever the gate says by now.
		{name: "pods left, gate closed", step: RemovalStep{Name: StepDraining, Cordoned: true}, node: cordoned(1), pre: closed, want: Evict},
		{name: "drained", step: RemovalStep{Name: StepDraining, Cordoned: true}, node: cordoned(0), pre: open, want: Advance,
			detail: StepResetting},
		{name: "pods not listed", step: RemovalStep{Name: StepDraining, Cordoned: true}, node: cordoned(-1), pre: open, want: Wait},
		{name: "never drained", step: RemovalStep{Name: StepDraining, Cordoned: true}, node: cordoned(2), pre: open,
			after: membershipTiming.DrainTimeout, want: Fail, detail: FailDrainTimeout},
		{name: "no Node to drain", step: RemovalStep{Name: StepDraining}, node: with(live, func(n *RemovalNode) { n.Registered = false }),
			pre: open, want: Advance, detail: StepResetting},
		// What cannot be reset afterwards is not drained first.
		{name: "the machine does not answer", step: RemovalStep{Name: StepDraining}, node: with(live, func(n *RemovalNode) { n.Reachable = false }),
			pre: open, want: Wait, detail: "not reachable"},
		{name: "the machine never answered", step: RemovalStep{Name: StepDraining},
			node: with(live, func(n *RemovalNode) { n.Reachable = false }), pre: open, after: long, want: Fail, detail: FailNodeGone},
		{name: "it left before its turn", step: RemovalStep{Name: StepDraining}, node: wiped, pre: closed, want: Advance, detail: StepLeaving},

		{name: "ready to be reset", step: RemovalStep{Name: StepResetting, Cordoned: true}, node: cordoned(0), pre: open, want: Reset},
		{name: "the gate closed before the reset", step: RemovalStep{Name: StepResetting, Cordoned: true}, node: cordoned(0), pre: closed,
			want: Wait, detail: "learner"},
		{name: "the gate never opened again", step: RemovalStep{Name: StepResetting, Cordoned: true}, node: cordoned(0), pre: closed,
			after: long, want: Fail, detail: FailTimeout},
		{name: "it went quiet before the reset", step: RemovalStep{Name: StepResetting, Cordoned: true}, node: gone, pre: open, want: Wait},
		{name: "it stayed quiet", step: RemovalStep{Name: StepResetting, Cordoned: true}, node: gone, pre: open, after: long, want: Fail,
			detail: FailNodeGone},
		// It was reset: it is never reset again.
		{name: "it is back in maintenance mode", step: RemovalStep{Name: StepResetting, Cordoned: true}, node: wiped, pre: open, want: Advance,
			detail: StepLeaving},

		{name: "a worker on its way out", step: RemovalStep{Name: StepLeaving}, node: gone, want: Wait, detail: "maintenance mode"},
		{name: "a worker that forgot the cluster", step: RemovalStep{Name: StepLeaving}, node: wiped, want: Advance, detail: StepDeleting},
		{name: "a control plane still in etcd", step: RemovalStep{Name: StepLeaving}, node: controlPlane(wiped, true), want: Wait, detail: "etcd"},
		{name: "a control plane that is gone", step: RemovalStep{Name: StepLeaving}, node: controlPlane(wiped, false), want: Advance,
			detail: StepDeleting},
		{name: "etcd not read", step: RemovalStep{Name: StepLeaving}, node: with(controlPlane(wiped, false), func(n *RemovalNode) { n.EtcdKnown = false }),
			want: Wait, detail: "could not be read"},
		// Whether it still holds the cluster's secrets cannot be told.
		{name: "it never came back", step: RemovalStep{Name: StepLeaving}, node: gone, after: long, want: Fail, detail: FailNotWiped},
		{name: "its member never left", step: RemovalStep{Name: StepLeaving}, node: controlPlane(wiped, true), after: long, want: Fail,
			detail: FailTimeout},

		{name: "a dead worker", step: RemovalStep{Name: StepLeaving, Dead: true}, node: gone, pre: open, want: Advance, detail: StepDeleting},
		{name: "a dead control plane", step: RemovalStep{Name: StepLeaving, Dead: true}, node: controlPlane(gone, true), pre: open,
			want: RemoveMember},
		{name: "a dead control plane, gate closed", step: RemovalStep{Name: StepLeaving, Dead: true}, node: controlPlane(gone, true),
			pre: closed, want: Wait, detail: "learner"},
		{name: "a dead control plane, out of etcd", step: RemovalStep{Name: StepLeaving, Dead: true}, node: controlPlane(gone, false),
			pre: open, want: Advance, detail: StepDeleting},
		{name: "a dead control plane, never out of etcd", step: RemovalStep{Name: StepLeaving, Dead: true}, node: controlPlane(gone, true),
			pre: closed, after: long, want: Fail, detail: FailTimeout},

		{name: "its Node is still there", step: RemovalStep{Name: StepDeleting}, node: wiped, want: DeleteNode},
		{name: "its Node is gone", step: RemovalStep{Name: StepDeleting}, node: with(wiped, func(n *RemovalNode) { n.Registered = false }),
			want: Advance, detail: StepVerifying},
		{name: "Kubernetes not read", step: RemovalStep{Name: StepDeleting}, node: with(wiped, func(n *RemovalNode) { n.KubernetesKnown = false }),
			want: Wait},

		{name: "the rest is healthy", step: RemovalStep{Name: StepVerifying}, checks: open, want: MarkHealthy},
		{name: "the rest soaked", step: RemovalStep{Name: StepVerifying}, checks: open, healthySince: began, after: membershipTiming.Soak,
			want: Complete},
		{name: "the rest is not healthy", step: RemovalStep{Name: StepVerifying}, checks: closed, want: Wait, detail: "learner"},
		{name: "the rest never was", step: RemovalStep{Name: StepVerifying}, checks: closed, after: long, want: Fail, detail: FailTimeout},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.step.Since, tt.step.HealthySince = began, tt.healthySince

			got := NextRemovalStep(tt.step, tt.node, tt.pre, tt.checks, membershipTiming, began.Add(tt.after))
			if got.Kind != tt.want || !strings.Contains(got.Step+" "+got.Reason+" "+got.Message, tt.detail) {
				t.Fatalf("got %s %+v", got.Kind, got)
			}
		})
	}
}

// Whatever is observed: nothing is taken from the cluster while the gate is closed, a node is
// reset only while it answers as a member, an etcd member is removed from the outside only for a
// machine a human called dead, and a Node is deleted only once its machine is gone or dead.
func TestRemovalNeverDoesWhatItShouldNot(t *testing.T) {
	bools := []bool{false, true}

	for _, step := range []string{StepDraining, StepResetting, StepLeaving, StepDeleting, StepVerifying} {
		for _, reachable := range bools {
			for _, wiped := range bools {
				for _, dead := range bools {
					for _, member := range bools {
						for _, cordoned := range bools {
							for _, pre := range []Gate{open, closed} {
								node := RemovalNode{
									Reachable: reachable, Wiped: wiped, ControlPlane: true, EtcdKnown: true, EtcdMember: member,
									KubernetesKnown: true, Registered: true, Unschedulable: cordoned,
								}
								got := NextRemovalStep(RemovalStep{Name: step, Since: began, Dead: dead, Cordoned: true}, node, pre, open,
									membershipTiming, began)
								where := func() string { return step + " " + got.Kind.String() }

								switch got.Kind { //nolint:exhaustive // the actions that take something
								case Cordon, Reset, RemoveMember:
									if !pre.Open {
										t.Fatalf("%s with the gate closed: %+v", where(), node)
									}
								}

								if got.Kind == Reset && (!reachable || wiped || step != StepResetting) {
									t.Fatalf("%s: %+v", where(), node)
								}

								if got.Kind == RemoveMember && (!dead || !member || step != StepLeaving) {
									t.Fatalf("%s: dead=%t %+v", where(), dead, node)
								}

								if got.Kind == DeleteNode && step != StepDeleting {
									t.Fatalf("%s: %+v", where(), node)
								}

								if step == StepLeaving && got.Kind == Advance && !dead && (!wiped || member) {
									t.Fatalf("%s: a machine that was not seen to forget the cluster is not done: %+v", where(), node)
								}
							}
						}
					}
				}
			}
		}
	}
}
