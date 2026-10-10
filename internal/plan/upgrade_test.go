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

const (
	bootBefore = "boot-1"
	bootAfter  = "boot-2"
)

func upgradeStep(name string) UpgradeStep { return UpgradeStep{Name: name, Since: t0} }

// before is a node that still runs the old system, after one that came back on the new one, and
// fellBack one that came back on the old one.
func nodeBefore() UpgradeNode {
	return UpgradeNode{Reachable: true, BootID: bootBefore, Running: true, InstallKnown: true, KubernetesKnown: true, NodeReady: true}
}

func nodeAfter() UpgradeNode {
	n := nodeBefore()
	n.BootID, n.Installed = bootAfter, true

	return n
}

func nodeFellBack() UpgradeNode {
	n := nodeBefore()
	n.BootID = bootAfter

	return n
}

func with(n UpgradeNode, mutate func(*UpgradeNode)) UpgradeNode {
	mutate(&n)

	return n
}

// The whole of an upgrade on one node, step by step, as the executor would drive it.
func TestUpgradeHappyPath(t *testing.T) {
	type turn struct {
		name string
		step UpgradeStep
		node UpgradeNode
		pre  Gate
		chk  Gate
		at   time.Duration
		want ActionKind
		next string
	}

	cordonedStep := func(name string) UpgradeStep {
		s := upgradeStep(name)
		s.Cordoned, s.BootID = true, bootBefore

		return s
	}
	cordoned := func(n UpgradeNode, pods int) UpgradeNode {
		n.Unschedulable, n.PodsLeft = true, pods

		return n
	}
	soaked := cordonedStep(StepVerifying)
	soaked.HealthySince = t0

	turns := []turn{
		{"the gate is asked before anything", upgradeStep(StepPulling), nodeBefore(), closed, closed, 0, Wait, ""},
		{"pull", upgradeStep(StepPulling), nodeBefore(), open, closed, 0, Pull, ""},
		{"install", upgradeStep(StepInstalling), nodeBefore(), closed, closed, 0, Install, ""},
		{"decide to cordon", upgradeStep(StepDraining), nodeBefore(), open, closed, 0, MarkCordoned, ""},
		{"cordon", cordonedStep(StepDraining), nodeBefore(), open, closed, 0, Cordon, ""},
		{"evict", cordonedStep(StepDraining), cordoned(nodeBefore(), 3), open, closed, 0, Evict, ""},
		{"evicting goes on when the gate closes", cordonedStep(StepDraining), cordoned(nodeBefore(), 1), closed, closed, 0, Evict, ""},
		{"drained", cordonedStep(StepDraining), cordoned(nodeBefore(), 0), open, closed, 0, Advance, StepRebooting},
		{"reboot", cordonedStep(StepRebooting), cordoned(nodeBefore(), 0), open, closed, 0, Reboot, ""},
		{"down", cordonedStep(StepBooting), UpgradeNode{PodsLeft: -1}, open, closed, time.Minute, Wait, ""},
		{"back, still booting", cordonedStep(StepBooting), with(cordoned(nodeAfter(), 0), func(n *UpgradeNode) { n.Running = false }),
			open, closed, time.Minute, Wait, ""},
		{"back on the new system", cordonedStep(StepBooting), cordoned(nodeAfter(), 0), open, closed, time.Minute, Advance, StepUncordoning},
		{"not Ready to Kubernetes yet", cordonedStep(StepUncordoning), with(cordoned(nodeAfter(), 0), func(n *UpgradeNode) { n.NodeReady = false }),
			open, closed, 0, Wait, ""},
		{"uncordon", cordonedStep(StepUncordoning), cordoned(nodeAfter(), 0), open, closed, 0, Uncordon, ""},
		{"uncordoned", cordonedStep(StepUncordoning), nodeAfter(), open, closed, 0, Advance, StepVerifying},
		{"healthy", cordonedStep(StepVerifying), nodeAfter(), open, open, 0, MarkHealthy, ""},
		{"soaking", soaked, nodeAfter(), open, open, timing.Soak - time.Second, Wait, ""},
		{"done", soaked, nodeAfter(), open, open, timing.Soak, Complete, ""},
	}

	for _, tt := range turns {
		t.Run(tt.name, func(t *testing.T) {
			got := NextUpgradeStep(tt.step, tt.node, tt.pre, tt.chk, timing, t0.Add(tt.at))
			if got.Kind != tt.want || got.Step != tt.next {
				t.Fatalf("got %s (next %q, %s %s), want %s (next %q)", got.Kind, got.Step, got.Reason, got.Message, tt.want, tt.next)
			}
		})
	}
}

func TestUpgradeFailures(t *testing.T) {
	late := timing.StepTimeout
	cordonedStep := func(name string) UpgradeStep {
		s := upgradeStep(name)
		s.Cordoned, s.BootID = true, bootBefore

		return s
	}

	tests := []struct {
		name   string
		step   UpgradeStep
		node   UpgradeNode
		pre    Gate
		at     time.Duration
		reason string
		says   string
	}{
		{"a gate that never opens", upgradeStep(StepPulling), nodeBefore(), closed, late, FailTimeout, closed.Reason},
		{"a node that never answers", upgradeStep(StepInstalling), UpgradeNode{}, open, late, FailTimeout, "not reachable"},
		{"pods that never leave", cordonedStep(StepDraining), with(nodeBefore(), func(n *UpgradeNode) { n.Unschedulable, n.PodsLeft = true, 2 }),
			open, timing.DrainTimeout, FailDrainTimeout, "2 pods"},
		{"a gate closed before the cordon, for the whole drain timeout", upgradeStep(StepDraining), nodeBefore(), closed, timing.DrainTimeout,
			FailDrainTimeout, closed.Reason},
		{"a gate closed before the reboot", cordonedStep(StepRebooting), nodeBefore(), closed, late, FailTimeout, closed.Reason},
		{"a node that never comes back", cordonedStep(StepBooting), UpgradeNode{}, open, late, FailTimeout, "console"},
		{"a node that comes back and never gets ready", cordonedStep(StepBooting),
			with(nodeAfter(), func(n *UpgradeNode) { n.Running = false }), open, late, FailTimeout, "did not become ready"},
		{"a node that comes back on the old system", cordonedStep(StepBooting), nodeFellBack(), open, time.Minute, FailFellBack, "falls back"},
		{"a node Kubernetes never calls Ready", cordonedStep(StepUncordoning),
			with(nodeAfter(), func(n *UpgradeNode) { n.Unschedulable, n.NodeReady = true, false }), open, late, FailTimeout, "stays cordoned"},
		{"a node that is not verified", cordonedStep(StepVerifying), nodeAfter(), open, late, FailTimeout, closed.Reason},
		{"a node that lost the new system while settling", cordonedStep(StepVerifying), nodeFellBack(), open, 0, FailFellBack, "no longer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NextUpgradeStep(tt.step, tt.node, tt.pre, closed, timing, t0.Add(tt.at))
			if got.Kind != Fail || got.Reason != tt.reason || !strings.Contains(strings.ToLower(got.Message), strings.ToLower(tt.says)) {
				t.Fatalf("got %s %s %q, want Fail %s mentioning %q", got.Kind, got.Reason, got.Message, tt.reason, tt.says)
			}
		})
	}
}

// Resuming, and retrying after a failure, starts at the first step whatever the node has been
// through. It must come out right from what the node says.
func TestUpgradeResumesFromWhatTheNodeSays(t *testing.T) {
	for _, name := range []string{StepPulling, StepInstalling} {
		if got := NextUpgradeStep(upgradeStep(name), nodeAfter(), closed, closed, timing, t0); got.Kind != Advance || got.Step != StepUncordoning {
			t.Fatalf("%s on a node that already runs the target must go straight to uncordoning, got %s %s", name, got.Kind, got.Step)
		}
	}

	rebooting := upgradeStep(StepRebooting)
	rebooting.BootID = bootBefore

	if got := NextUpgradeStep(rebooting, nodeAfter(), open, closed, timing, t0); got.Kind != Advance || got.Step != StepBooting {
		t.Fatalf("a node that already rebooted must not be rebooted again, got %s", got.Kind)
	}

	// The reboot went out, but the step after it was never persisted, and the node is down.
	if got := NextUpgradeStep(rebooting, UpgradeNode{PodsLeft: -1}, open, closed, timing, t0.Add(time.Minute)); got.Kind != Wait {
		t.Fatalf("a node that is down is waited for, got %s", got.Kind)
	}
}

func TestOnlyAnOperatorsOwnCordonIsUndone(t *testing.T) {
	// Cordoned by someone else before the upgrade: drained without a mark, and left cordoned.
	foreign := with(nodeBefore(), func(n *UpgradeNode) { n.Unschedulable, n.PodsLeft = true, 0 })

	if got := NextUpgradeStep(upgradeStep(StepDraining), foreign, open, closed, timing, t0); got.Kind != Advance || got.Step != StepRebooting {
		t.Fatalf("a node someone else cordoned is drained as it is, got %s", got.Kind)
	}

	after := with(nodeAfter(), func(n *UpgradeNode) { n.Unschedulable = true })
	if got := NextUpgradeStep(upgradeStep(StepUncordoning), after, open, closed, timing, t0); got.Kind != Advance || got.Step != StepVerifying {
		t.Fatalf("a cordon the operator did not set must not be undone, got %s", got.Kind)
	}
}

// upgradeRule names the rule an action breaks in the given situation, or returns "".
func upgradeRule(step UpgradeStep, node UpgradeNode, pre, checks Gate, now time.Time, action Action) string {
	cordoned := node.KubernetesKnown && node.Unschedulable
	rebooted := node.Reachable && node.BootID != "" && node.BootID != step.BootID

	switch action.Kind { //nolint:exhaustive // only the actions that change something carry rules
	case MarkCordoned, Cordon:
		if !pre.Open || step.Name != StepDraining {
			return "a node must not be cordoned while the gate is closed"
		}
	case Evict:
		if !cordoned {
			return "pods must not be evicted from a node that is not cordoned"
		}
	case Reboot:
		switch {
		case !pre.Open:
			return "a node must not be rebooted while the gate is closed"
		case !node.Reachable || node.BootID != step.BootID:
			return "a reboot must only be asked of a node that still has the recorded boot ID"
		case step.Name != StepRebooting:
			return "a reboot belongs to the rebooting step only"
		}
	case Uncordon:
		if !step.Cordoned || !node.NodeReady {
			return "only a node the operator cordoned is uncordoned, and only once Kubernetes calls it Ready"
		}
	case Complete:
		if !node.Installed || !node.InstallKnown || !node.Running || !checks.Open || now.Sub(step.HealthySince) < timing.Soak {
			return "an upgrade is complete only on the installed system, after a full soak of passing checks"
		}
	case Advance:
		switch action.Step {
		case StepRebooting:
			if !cordoned || node.PodsLeft != 0 {
				return "a node is rebooted only once it is cordoned and drained"
			}
		case StepUncordoning:
			if !node.Installed || !node.InstallKnown || !node.Running {
				return "a node moves on from its reboot only on the installed system"
			}
		case StepBooting:
			if !rebooted {
				return "a node is waited for only once it rebooted or was asked to"
			}
		}
	}

	return ""
}

// The rules of an upgrade, checked over every combination of step, observation, gate and time.
func TestUpgradeInvariants(t *testing.T) {
	var nodes []UpgradeNode

	for _, reachable := range []bool{true, false} {
		for _, boot := range []string{"", bootBefore, bootAfter} {
			for _, running := range []bool{true, false} {
				for _, install := range []int{0, 1, 2} { // unknown, old, new
					for _, kube := range []int{0, 1, 2, 3} { // unknown, schedulable, cordoned, cordoned and not Ready
						for _, pods := range []int{-1, 0, 2} {
							nodes = append(nodes, UpgradeNode{
								Reachable: reachable, BootID: boot, Running: running,
								InstallKnown: install > 0, Installed: install == 2,
								KubernetesKnown: kube > 0, Unschedulable: kube >= 2, NodeReady: kube != 3 && kube != 0,
								PodsLeft: pods,
							})
						}
					}
				}
			}
		}
	}

	offsets := []time.Duration{0, time.Second, timing.Soak, timing.StepTimeout - time.Second, timing.StepTimeout, timing.StepTimeout + time.Hour}
	steps := []string{StepPulling, StepInstalling, StepDraining, StepRebooting, StepBooting, StepUncordoning, StepVerifying}
	checked := 0

	for _, name := range steps {
		for _, ours := range []bool{true, false} {
			for _, node := range nodes {
				for _, offset := range offsets {
					for _, healthyFor := range []time.Duration{-1, 0, timing.Soak} {
						step := UpgradeStep{Name: name, Since: t0, BootID: bootBefore, Cordoned: ours}
						now := t0.Add(offset)

						if healthyFor >= 0 {
							step.HealthySince = now.Add(-healthyFor)
						}

						for _, pre := range []Gate{open, closed} {
							for _, checks := range []Gate{open, closed} {
								checked++

								action := NextUpgradeStep(step, node, pre, checks, timing, now)
								if rule := upgradeRule(step, node, pre, checks, now, action); rule != "" {
									t.Fatalf("%s (ours=%t) %+v +%s pre=%t checks=%t: %s: %s", name, ours, node, offset, pre.Open, checks.Open, action.Kind, rule)
								}
							}
						}
					}
				}
			}
		}
	}

	if checked < 100000 {
		t.Fatalf("only %d situations were checked", checked)
	}
}

func upgradable(name string, controlPlane bool) UpgradeMachine {
	return UpgradeMachine{
		Name: name, ControlPlane: controlPlane, Current: true,
		Installed: "v1.14.1 aaa", Target: "v1.14.2 aaa", Image: "factory.example.com/metal-installer/aaa:v1.14.2",
	}
}

func upToDate(name string, controlPlane bool) UpgradeMachine {
	m := upgradable(name, controlPlane)
	m.Installed = m.Target

	return m
}

func TestPlanUpgradeOrder(t *testing.T) {
	leader := upgradable("cp-b", true)
	leader.EtcdLeader = true

	home := upgradable("cp-a", true)
	home.HostsOperator = true

	workerHome := upgradable("w-a", false)
	workerHome.HostsOperator = true

	got := PlanUpgrade([]UpgradeMachine{
		home, leader, upgradable("cp-c", true), workerHome, upgradable("w-b", false), upToDate("w-c", false),
	}, UpgradeSettings{}, healthyPass())

	// Workers before control planes; within each, the operator's own node last, the etcd leader
	// just before it.
	if want := []string{"w-b", "w-a", "cp-c", "cp-b", "cp-a"}; !slices.Equal(got.Pending, want) || !got.MayStart() {
		t.Fatalf("order: got %v (blocked %q: %s), want %v", got.Pending, got.BlockReason, got.BlockMessage, want)
	}

	if got.Images["cp-a"] != home.Image || len(got.Images) != 5 || got.Change == "" {
		t.Fatalf("every pending machine carries its installer image: %+v", got)
	}

	// An unhealthy node goes first, as in a config rollout: nothing else may be touched meanwhile.
	sick := PlanUpgrade([]UpgradeMachine{upgradable("w-1", false).onNode(worker), upgradable("cp-1", true).onNode(nodeB)},
		UpgradeSettings{}, ailing(nodeB))
	if want := []string{"cp-1", "w-1"}; !slices.Equal(sick.Pending, want) || !sick.MayStart() {
		t.Fatalf("the unhealthy node goes first: got %v, blocked %q (%s)", sick.Pending, sick.BlockReason, sick.BlockMessage)
	}
}

func (m UpgradeMachine) onNode(node string) UpgradeMachine {
	m.Node = node

	return m
}

func TestPlanUpgradeNothingToDo(t *testing.T) {
	got := PlanUpgrade([]UpgradeMachine{upToDate(cp1, true), upToDate(w1, false)}, UpgradeSettings{Paused: true}, closedPass)
	if got.MayStart() || len(got.Pending) != 0 || got.BlockReason != "" {
		t.Fatalf("machines on their target need no upgrade and report no block: %+v", got)
	}
}

func TestPlanUpgradeBlocks(t *testing.T) {
	stale := upgradable(w2, false)
	stale.Current = false

	jump := upgradable(cp1, true)
	jump.Problem = "v1.12.4 cannot be upgraded to v1.14.2 directly"

	change := []UpgradeMachine{upgradable(w1, false), upToDate(cp1, true)}

	tests := []struct {
		name     string
		machines []UpgradeMachine
		settings UpgradeSettings
		pass     Pass
		reason   string
		message  string
	}{
		{"a jump Talos does not allow", []UpgradeMachine{upgradable(w1, false), jump}, UpgradeSettings{}, openPass, BlockIncompatible, "cp-1: v1.12.4"},
		{"incompatible outranks paused", []UpgradeMachine{jump}, UpgradeSettings{Paused: true}, openPass, BlockIncompatible, cp1},
		{"paused", change, UpgradeSettings{Paused: true}, openPass, BlockPaused, "spec.paused"},
		{"stale status", append(slices.Clone(change), stale), UpgradeSettings{}, openPass, BlockNotCurrent, w2},
		{"an aborted upgrade is not started again", change,
			UpgradeSettings{Aborted: map[string]string{w1: upgradable(w1, false).Image}}, openPass, BlockAborted, "aborted"},
		{"a closed gate", change, UpgradeSettings{}, closedPass, BlockGateClosed, closedPass.HealthMessage},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PlanUpgrade(tt.machines, tt.settings, tt.pass)
			if got.MayStart() || got.BlockReason != tt.reason || !strings.Contains(got.BlockMessage, tt.message) {
				t.Fatalf("got reason=%q message=%q, want %q containing %q", got.BlockReason, got.BlockMessage, tt.reason, tt.message)
			}
		})
	}
}

func TestPlanUpgradeHoldsSkipsAndAborts(t *testing.T) {
	held := upgradable(w1, false)
	held.Hold = true

	skipped := upgradable(w2, false)
	settings := UpgradeSettings{Skipped: map[string]string{w2: skipped.Image}}

	got := PlanUpgrade([]UpgradeMachine{held, skipped, upgradable("w-3", false)}, settings, openPass)
	if !slices.Equal(got.Held, []string{w1, w2}) || !slices.Equal(got.Pending, []string{"w-3"}) || !got.MayStart() {
		t.Fatalf("held and skipped machines are left out and named: %+v", got)
	}

	// Another installer than the one that was skipped is a new upgrade.
	skipped.Image = "factory.example.com/metal-installer/aaa:v1.14.3"
	if got := PlanUpgrade([]UpgradeMachine{skipped}, settings, openPass); !got.MayStart() {
		t.Fatalf("a skip covers one installer only: %+v", got)
	}

	aborted := UpgradeSettings{Aborted: map[string]string{w1: upgradable(w1, false).Image}}

	// The abort is spent once the spec asks for something else, or for nothing.
	other := upgradable(w1, false)
	other.Image = skipped.Image

	if got := PlanUpgrade([]UpgradeMachine{other}, aborted, openPass); got.AbortStands || !got.MayStart() {
		t.Fatalf("another installer than the aborted one starts: %+v", got)
	}

	if got := PlanUpgrade([]UpgradeMachine{upToDate(w1, false)}, aborted, openPass); got.AbortStands {
		t.Fatalf("an abort with nothing left to upgrade is spent: %+v", got)
	}

	// While a machine cannot be judged, the abort is kept.
	unknown := upgradable(w1, false)
	unknown.Current = false

	if got := PlanUpgrade([]UpgradeMachine{unknown}, aborted, openPass); !got.AbortStands {
		t.Fatalf("an abort stands while a machine's state is unknown: %+v", got)
	}
}

// A control plane is not taken down while etcd has exactly two members: either one is quorum.
func TestRebootGateGuardsEtcdQuorum(t *testing.T) {
	two := healthyPass()
	two.Cluster.Members = slices.Delete(two.Cluster.Members, 2, 3)
	two.Cluster.EtcdMembers = two.Cluster.EtcdMembers[:2]

	if !TouchGate(two, nodeA).Open {
		t.Fatal("the fixture must be a healthy cluster of two control planes")
	}

	if gate := RebootGate(two, nodeA); gate.Open || !strings.Contains(gate.Reason, "quorum") {
		t.Fatalf("a control plane of two must not be rebooted: %+v", gate)
	}

	if gate := RebootGate(two, worker); !gate.Open {
		t.Fatalf("a worker does not hold quorum: %s", gate.Reason)
	}

	if gate := RebootGate(healthyPass(), nodeA); !gate.Open {
		t.Fatalf("one of three may go: %s", gate.Reason)
	}

	one := healthyPass()
	one.Cluster.Members = one.Cluster.Members[:1]
	one.Cluster.EtcdMembers = one.Cluster.EtcdMembers[:1]

	if gate := RebootGate(one, nodeA); !gate.Open {
		t.Fatalf("a single control plane has no quorum to lose but its own: %s", gate.Reason)
	}

	// Whatever closes the gate for touching a node closes it for rebooting it.
	sick := ailing(nodeB)
	if RebootGate(sick, nodeA).Open {
		t.Fatal("a node is not rebooted while another is unhealthy")
	}
}

// A node in the middle of its own upgrade is cordoned, which Talos's cluster-wide checks fail on.
// That must not keep the upgrade from rebooting it, and must keep everything else from moving.
func TestACordonedNodeIsTheOnlyOneThatMayBeTouched(t *testing.T) {
	p := healthyPass()
	p.Cluster.Members[0].Node.Unschedulable = true
	p.Healthy, p.HealthMessage = false, "some nodes are not schedulable"

	if gate := RebootGate(p, nodeA); !gate.Open || gate.Note == "" {
		t.Fatalf("the cordoned node itself may go on: %+v", gate)
	}

	for _, other := range []string{nodeB, worker} {
		if gate := TouchGate(p, other); gate.Open || !strings.Contains(gate.Reason, nodeA+" is cordoned") {
			t.Fatalf("%s must not be touched while %s is cordoned: %+v", other, nodeA, gate)
		}
	}

	if VerifyGate(p).Open {
		t.Fatal("a cluster with a cordoned node is not verified")
	}
}
