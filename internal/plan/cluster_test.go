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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

func ptr[T any](v T) *T { return &v }

const (
	etcdUnhealthy = "etcd is unhealthy"
	// noSpace is an error an etcd member reports about itself.
	noSpace = "alarm: NOSPACE"
)

// healthyCluster is a three-control-plane cluster that satisfies every precondition, plus one
// worker.
func healthyCluster() Cluster {
	member := func(name string, controlPlane bool) Member {
		return Member{
			Hostname:       name,
			ControlPlane:   controlPlane,
			Version:        "v1.14.0",
			Status:         &MemberStatus{Stage: StageRunning, Ready: true},
			Schematic:      ptr("376567988ad3"),
			ConfigSettled:  ptr(true),
			EtcdStatusRead: controlPlane,
			Node:           &KubernetesNode{Registered: true, Ready: true},
		}
	}

	return Cluster{
		CredentialsAdmin: true,
		Members: []Member{
			member("node-a", true), member("node-b", true), member("node-c", true), member("worker-1", false),
		},
		EtcdMembers: []EtcdMember{
			{ID: 1, Hostname: "node-a"}, {ID: 2, Hostname: "node-b"}, {ID: 3, Hostname: "node-c"},
		},
	}
}

func statuses(ps []talosv1alpha1.Precondition) map[string]metav1.ConditionStatus {
	out := map[string]metav1.ConditionStatus{}
	for _, p := range ps {
		out[p.Name] = p.Status
	}

	return out
}

func TestPreconditionsAllHoldOnAHealthyCluster(t *testing.T) {
	ps := Preconditions(healthyCluster())

	if len(ps) != 7 {
		t.Fatalf("expected seven preconditions, got %d", len(ps))
	}

	for name, s := range statuses(ps) {
		if s != metav1.ConditionTrue {
			t.Errorf("%s: %s", name, s)
		}
	}

	if s, _ := PreconditionsMet(ps); s != metav1.ConditionTrue {
		t.Fatalf("aggregate: %s", s)
	}
}

// Each case breaks exactly one fact; exactly that precondition must flip, and the aggregate with it.
func TestEachPreconditionFailsAlone(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Cluster)
		want   string
		status metav1.ConditionStatus
	}{
		{"credentials without admin", func(c *Cluster) { c.CredentialsAdmin = false },
			PreconditionCredentialsAdmin, metav1.ConditionFalse},
		{"node in maintenance", func(c *Cluster) { c.Members[1].Status = &MemberStatus{Stage: "maintenance"} },
			PreconditionAllMembersRunning, metav1.ConditionFalse},
		{"node not ready", func(c *Cluster) { c.Members[1].Status.Ready = false },
			PreconditionAllMembersRunning, metav1.ConditionFalse},
		{"machine status unreadable", func(c *Cluster) { c.Members[0].Status, c.Members[0].ReadErr = nil, "unavailable" },
			PreconditionAllMembersRunning, metav1.ConditionUnknown},
		{"etcd learner", func(c *Cluster) { c.EtcdMembers[2].Learner = true },
			PreconditionEtcdHealthy, metav1.ConditionFalse},
		{"etcd errors", func(c *Cluster) { c.Members[0].EtcdErrors = []string{noSpace} },
			PreconditionEtcdHealthy, metav1.ConditionFalse},
		// A control plane that was not asked about its etcd member has not said it is fine.
		{"etcd status unreadable", func(c *Cluster) { c.Members[0].EtcdStatusRead = false },
			PreconditionEtcdHealthy, metav1.ConditionUnknown},
		{"stale etcd member", func(c *Cluster) {
			c.EtcdMembers = append(c.EtcdMembers, EtcdMember{ID: 4, Hostname: "old-cp"})
		}, PreconditionEtcdMatchesControlPlanes, metav1.ConditionFalse},
		{"pending staged config", func(c *Cluster) { c.Members[2].ConfigSettled = ptr(false) },
			PreconditionNoPendingConfig, metav1.ConditionFalse},
		{"config unreadable", func(c *Cluster) { c.Members[2].ConfigSettled = nil },
			PreconditionNoPendingConfig, metav1.ConditionUnknown},
		{"unsupported talos", func(c *Cluster) { c.Members[0].VersionProblem = "v1.12.4 is older than v1.13" },
			PreconditionTalosVersionSupported, metav1.ConditionFalse},
		{"version unreadable", func(c *Cluster) { c.Members[3].Version = "" },
			PreconditionTalosVersionSupported, metav1.ConditionUnknown},
		{"no schematic", func(c *Cluster) { c.Members[1].Schematic = ptr("") },
			PreconditionSchematicDetected, metav1.ConditionFalse},
		{"schematic unreadable", func(c *Cluster) { c.Members[1].Schematic = nil },
			PreconditionSchematicDetected, metav1.ConditionUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := healthyCluster()
			tt.mutate(&c)

			ps := Preconditions(c)

			for name, s := range statuses(ps) {
				want := metav1.ConditionTrue
				if name == tt.want {
					want = tt.status
				}

				if s != want {
					t.Errorf("%s: got %s, want %s", name, s, want)
				}
			}

			if s, msg := PreconditionsMet(ps); s != tt.status || !strings.Contains(msg, tt.want) {
				t.Fatalf("aggregate: got %s (%s), want %s naming %s", s, msg, tt.status, tt.want)
			}
		})
	}
}

// A member that was added but never joined has an ID and no name.
func TestAnEtcdMemberThatNeverStartedIsUnhealthy(t *testing.T) {
	c := healthyCluster()
	c.EtcdMembers = append(c.EtcdMembers, EtcdMember{ID: 0xbeef})

	ps := Preconditions(c)
	if got := statuses(ps)[PreconditionEtcdHealthy]; got != metav1.ConditionFalse {
		t.Fatalf("EtcdHealthy: %s", got)
	}

	for _, p := range ps {
		if p.Name == PreconditionEtcdHealthy && !strings.Contains(p.Message, "beef") {
			t.Fatalf("the message must name the member by its ID: %q", p.Message)
		}
	}
}

func TestWorkersAreNotAskedAboutEtcd(t *testing.T) {
	c := healthyCluster()
	if c.Members[3].ControlPlane || c.Members[3].EtcdStatusRead {
		t.Fatal("the fixture's last member must be a worker without an etcd status")
	}

	if got := statuses(Preconditions(c))[PreconditionEtcdHealthy]; got != metav1.ConditionTrue {
		t.Fatalf("a worker has no etcd member to report on: %s", got)
	}
}

func TestUnreadableEtcdListIsUnknownNotFalse(t *testing.T) {
	c := healthyCluster()
	c.EtcdMembers = nil

	got := statuses(Preconditions(c))
	if got[PreconditionEtcdHealthy] != metav1.ConditionUnknown || got[PreconditionEtcdMatchesControlPlanes] != metav1.ConditionUnknown {
		t.Fatalf("got %v", got)
	}
}

func TestAFailureOutranksAnUnknown(t *testing.T) {
	c := healthyCluster()
	c.Members[0].ConfigSettled = nil
	c.Members[1].Status.Ready = false

	if s, msg := PreconditionsMet(Preconditions(c)); s != metav1.ConditionFalse ||
		!strings.Contains(msg, PreconditionNoPendingConfig) || !strings.Contains(msg, PreconditionAllMembersRunning) {
		t.Fatalf("expected False naming both, got %s (%s)", s, msg)
	}

	if s, _ := PreconditionsMet(nil); s != metav1.ConditionTrue {
		t.Fatal("nothing to meet is met; the gates are what refuse a cluster that was not read")
	}
}

// healthyPass is a pass that read everything and found it fine.
func healthyPass() Pass {
	c := healthyCluster()

	return Pass{Reached: true, Matched: true, Cluster: &c, HealthChecked: true, Healthy: true}
}

// The members of healthyCluster, as the gate tests name them.
const (
	nodeA   = "node-a"
	nodeB   = "node-b"
	worker  = "worker-1"
	nobody  = ""
	unknown = "no-such-node"
)

func TestGatesAreOpenOnlyWhenEverythingIsKnownToBeFine(t *testing.T) {
	for _, node := range []string{nobody, nodeA, worker, unknown} {
		if gate := TouchGate(healthyPass(), node); !gate.Open || gate.Note != "" {
			t.Fatalf("a healthy cluster must open the gate for %q and leave nothing unasked: %+v", node, gate)
		}
	}

	if gate := VerifyGate(healthyPass()); !gate.Open {
		t.Fatalf("a healthy cluster must verify: %s", gate.Reason)
	}

	member := func(mutate func(*Member)) func(*Pass) {
		return func(p *Pass) { mutate(&p.Cluster.Members[1]) }
	}

	// Every case breaks something that is not the node the gate is asked for.
	tests := []struct {
		name    string
		breakIt func(*Pass)
		reason  string
	}{
		{"the cluster was not reached", func(p *Pass) { p.Reached = false }, "not reached"},
		{"a node could not be read", func(p *Pass) { p.Incomplete = "version of 10.5.0.3: timeout" }, "10.5.0.3"},
		{"the Kubernetes API could not be read", func(p *Pass) { p.KubernetesUnread = "list nodes: timeout" }, "Kubernetes API"},
		{"members could not be matched", func(p *Pass) { p.Matched = false }, "matched"},
		{"a live member has no TalosMachine", func(p *Pass) { p.Unmatched = []string{"stray"} }, "stray"},
		{"the members were not read", func(p *Pass) { p.Cluster = nil }, "not read"},
		{"a member is not running", member(func(m *Member) { m.Status.Ready = false }), PreconditionAllMembersRunning},
		{"a member's status is unknown", member(func(m *Member) { m.Status = nil }), string(metav1.ConditionUnknown)},
		{"Kubernetes reports a member not Ready", member(func(m *Member) { m.Node.Ready = false }), CheckAllNodesReady},
		{"Kubernetes has no Node for a member", member(func(m *Member) { m.Node.Registered = false }), "no Node for " + nodeB},
		{"the Nodes were not read", member(func(m *Member) { m.Node = nil }), string(metav1.ConditionUnknown)},
		{etcdUnhealthy, member(func(m *Member) { m.EtcdErrors = []string{noSpace} }), PreconditionEtcdHealthy},
		{"a member was not asked about etcd", member(func(m *Member) { m.EtcdStatusRead = false }), string(metav1.ConditionUnknown)},
		{"etcd lost a member", func(p *Pass) { p.Cluster.EtcdMembers = p.Cluster.EtcdMembers[:2] }, PreconditionEtcdMatchesControlPlanes},
		{"the etcd member list was not read", func(p *Pass) { p.Cluster.EtcdMembers = nil }, string(metav1.ConditionUnknown)},
		{"the health check did not run", func(p *Pass) { p.HealthChecked, p.HealthMessage = false, "deadline exceeded" }, "deadline exceeded"},
		{"the health check fails", func(p *Pass) { p.Healthy, p.HealthMessage = false, etcdUnhealthy }, etcdUnhealthy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := healthyPass()
			tt.breakIt(&p)

			for name, gate := range map[string]Gate{"touch": TouchGate(p, nodeA), "touch a worker": TouchGate(p, worker), "verify": VerifyGate(p)} {
				if gate.Open || !strings.Contains(gate.Reason, tt.reason) {
					t.Errorf("%s: expected a closed gate mentioning %q, got open=%t reason=%q", name, tt.reason, gate.Open, gate.Reason)
				}
			}
		})
	}
}

// What a rollout does not depend on does not hold it up: adoption cares where a node was
// installed from and with which role the operator talks to Talos, a config change does not.
func TestGatesOnlyLookAtWhatARolloutDependsOn(t *testing.T) {
	p := healthyPass()
	p.Cluster.CredentialsAdmin = false
	p.Cluster.Members[0].Schematic = ptr("")
	p.Cluster.Members[1].VersionProblem = "v1.12.4 is older than v1.13"

	if s, _ := PreconditionsMet(Preconditions(*p.Cluster)); s != metav1.ConditionFalse {
		t.Fatal("the fixture must fail adoption's preconditions")
	}

	if gate := TouchGate(p, nodeA); !gate.Open {
		t.Fatalf("what only adoption asks for must not close the gate: %s", gate.Reason)
	}

	if gate := VerifyGate(p); !gate.Open {
		t.Fatalf("what only adoption asks for must not fail verification: %s", gate.Reason)
	}
}

// A node under verification runs a tried config: active, not persistent. That must not count
// against it, or no try could ever be verified. Before a node is touched it always counts, the
// node itself included: a try on top of a pending one would make the pending config the one
// Talos falls back to.
func TestAPendingConfigKeepsEveryNodeUntouchedButCanBeVerified(t *testing.T) {
	for _, pending := range []string{nodeA, nodeB} {
		p := healthyPass()

		for i := range p.Cluster.Members {
			if p.Cluster.Members[i].Hostname == pending {
				p.Cluster.Members[i].ConfigSettled = ptr(false)
			}
		}

		// Even an unhealthy node is not tried on top of a pending config.
		p.Cluster.Members[0].Status.Ready = false

		if gate := TouchGate(p, nodeA); gate.Open || !strings.Contains(gate.Reason, PreconditionNoPendingConfig) {
			t.Fatalf("a config pending on %s must keep %s untouched: %+v", pending, nodeA, gate)
		}
	}

	p := healthyPass()
	p.Cluster.Members[0].ConfigSettled = ptr(false)

	if gate := VerifyGate(p); !gate.Open {
		t.Fatalf("a tried config must be verifiable: %s", gate.Reason)
	}
}

// The reason the gate leaves the node out: a config that broke a node is taken back by a
// rollout, which must therefore be able to touch a node that is unhealthy.
func TestAnUnhealthyNodeMayBeTouchedWhileItIsTheOnlyOne(t *testing.T) {
	ailments := map[string]func(*Member){
		"it does not call itself ready":        func(m *Member) { m.Status.Ready = false },
		"it is not running":                    func(m *Member) { m.Status.Stage = "booting" },
		"its etcd member reports errors":       func(m *Member) { m.EtcdErrors = []string{noSpace} },
		"Kubernetes reports it not Ready":      func(m *Member) { m.Node.Ready = false },
		"Kubernetes has no Node for it":        func(m *Member) { m.Node.Registered = false },
		"everything about it is wrong at once": func(m *Member) { m.Status.Ready, m.Node.Ready, m.EtcdErrors = false, false, []string{"x"} },
	}

	for name, ail := range ailments {
		t.Run(name, func(t *testing.T) {
			p := healthyPass()
			ail(&p.Cluster.Members[0])
			// Talos judges the cluster as a whole, so its checks fail with the node.
			p.Healthy, p.HealthMessage = false, nodeA+" is not ready"

			gate := TouchGate(p, nodeA)
			if !gate.Open || !strings.Contains(gate.Note, nodeA) {
				t.Fatalf("the unhealthy node itself must be touchable, and the gate must say what it did not ask: %+v", gate)
			}

			// Whether Talos's checks ran at all does not matter then either.
			p.HealthChecked = false
			if !TouchGate(p, nodeA).Open {
				t.Fatal("Talos's checks are not asked about a cluster whose only fault is the node itself")
			}

			for _, other := range []string{nodeB, worker, nobody, unknown} {
				if TouchGate(p, other).Open {
					t.Fatalf("%q must not be touched while %s is unhealthy", other, nodeA)
				}
			}

			if VerifyGate(p).Open {
				t.Fatal("verification leaves nobody out")
			}

			// A second unhealthy node closes the gate for the first as well.
			p.Cluster.Members[3].Status.Ready = false
			if gate := TouchGate(p, nodeA); gate.Open || !strings.Contains(gate.Reason, worker) {
				t.Fatalf("two unhealthy nodes are one too many: %+v", gate)
			}
		})
	}
}

// Not knowing whether the node is healthy is no licence to skip Talos's checks: they are only
// set aside for a node that is known to be the fault.
func TestWhatIsNotKnownAboutTheNodeExcusesNothing(t *testing.T) {
	unknowns := map[string]func(*Member){
		"its status was not read":      func(m *Member) { m.Status = nil },
		"its etcd status was not read": func(m *Member) { m.EtcdStatusRead = false },
		"its Node was not read":        func(m *Member) { m.Node = nil },
	}

	for name, forget := range unknowns {
		t.Run(name, func(t *testing.T) {
			p := healthyPass()
			forget(&p.Cluster.Members[0])

			if gate := TouchGate(p, nodeA); !gate.Open || gate.Note != "" {
				t.Fatalf("with Talos's checks passing the node's own facts are not needed: %+v", gate)
			}

			p.Healthy, p.HealthMessage = false, etcdUnhealthy
			if TouchGate(p, nodeA).Open {
				t.Fatal("failing checks must close the gate for a node that is not known to be their cause")
			}
		})
	}
}

// Two nodes that answer to one name: the name identifies neither, so nobody is left out.
func TestASharedHostnameLeavesNobodyOut(t *testing.T) {
	p := healthyPass()
	p.Cluster.Members[3].Hostname = nodeA
	p.Cluster.Members[0].Status.Ready = false

	// Talos's checks are left passing, so that only the member's own fact can close the gate.
	if gate := TouchGate(p, nodeA); gate.Open || !strings.Contains(gate.Reason, PreconditionAllMembersRunning) {
		t.Fatalf("a name two members share must not excuse either: %+v", gate)
	}

	p.Healthy = false
	if gate := TouchGate(p, nodeA); gate.Open {
		t.Fatalf("nor set Talos's checks aside: %+v", gate)
	}
}

// ailment is one thing that can be wrong with, or unknown about, a member.
type ailment struct {
	name  string
	apply func(*Member)
	// known is true when the member is known to be unhealthy, false when it is merely not known
	// to be healthy.
	known bool
	// config is true when it concerns the member's config, which is asked of every node.
	config bool
	// controlPlane is true when only a control plane can have it.
	controlPlane bool
}

var ailments = []ailment{
	{name: "healthy", apply: func(*Member) {}},
	{name: "not ready", apply: func(m *Member) { m.Status = &MemberStatus{Stage: StageRunning} }, known: true},
	{name: "status unread", apply: func(m *Member) { m.Status = nil }},
	{name: "etcd errors", apply: func(m *Member) { m.EtcdErrors = []string{"alarm"} }, known: true, controlPlane: true},
	{name: "etcd unread", apply: func(m *Member) { m.EtcdStatusRead = false }, controlPlane: true},
	{name: "Node not Ready", apply: func(m *Member) { m.Node = &KubernetesNode{Registered: true} }, known: true},
	{name: "no Node", apply: func(m *Member) { m.Node = &KubernetesNode{} }, known: true},
	{name: "Node unread", apply: func(m *Member) { m.Node = nil }},
	// A cordoned node is someone's work in progress. Talos's cluster-wide checks fail on it, so it
	// counts like a node that is known to be the fault.
	{name: "cordoned", apply: func(m *Member) { m.Node = &KubernetesNode{Registered: true, Ready: true, Unschedulable: true} }, known: true},
	{name: "config pending", apply: func(m *Member) { m.ConfigSettled = ptr(false) }, config: true},
	{name: "config unread", apply: func(m *Member) { m.ConfigSettled = nil }, config: true},
}

func (a ailment) healthy() bool { return a.name == "healthy" }

// The rules of the gates, checked over every pair of members in every pair of states, every
// verdict of Talos's checks and every node the gate can be asked for, against a statement of the
// rules that shares no code with the gates.
//
//nolint:gocyclo // four nested loops over the situation, and the rules spelled out one by one
func TestGateInvariants(t *testing.T) {
	type talosChecks struct {
		name            string
		checked, passes bool
	}

	verdicts := []talosChecks{{"pass", true, true}, {"fail", true, false}, {"did not run", false, false}}
	names := []string{nodeA, nodeB, "node-c", worker}
	checked := 0

	for i := range names {
		for j := i + 1; j < len(names); j++ {
			for _, first := range ailments {
				for _, second := range ailments {
					state := map[string]ailment{names[i]: first, names[j]: second}

					c := healthyCluster()
					if (first.controlPlane && !c.Members[i].ControlPlane) || (second.controlPlane && !c.Members[j].ControlPlane) {
						continue
					}

					first.apply(&c.Members[i])
					second.apply(&c.Members[j])

					for _, verdict := range verdicts {
						p := Pass{Reached: true, Matched: true, Cluster: &c, HealthChecked: verdict.checked, Healthy: verdict.passes}
						where := fmt.Sprintf("%s %s, %s %s, checks %s", names[i], first.name, names[j], second.name, verdict.name)

						everyoneHealthy := first.healthy() && second.healthy()
						// Verification asks about everyone, and about everything but pending configs.
						if verify := VerifyGate(p); verify.Open != (verdict.passes && healthyOrConfig(first) && healthyOrConfig(second)) {
							t.Errorf("verify: %s: open=%t", where, verify.Open)
						}

						for _, node := range append(slices.Clone(names), nobody, unknown) {
							checked++

							gate := TouchGate(p, node)
							own, isMember := state[node]

							if !isMember {
								own = ailments[0]
							}

							// Everything the gate may never overlook, whoever it is asked for.
							var others []ailment

							for name, a := range state {
								if name != node {
									others = append(others, a)
								}
							}

							othersHealthy := !slices.ContainsFunc(others, func(a ailment) bool { return !a.healthy() })
							configsSettled := !first.config && !second.config

							switch {
							case gate.Open && !othersHealthy:
								t.Errorf("%s: open for %q although another member is not known to be healthy", where, node)
							case gate.Open && !configsSettled:
								t.Errorf("%s: open for %q although a config is pending or unread", where, node)
							case gate.Open && !verdict.passes && !own.known:
								t.Errorf("%s: open for %q although Talos's checks do not pass and the node is not known to be why", where, node)
							case gate.Open && (gate.Note != "") != (own.known):
								t.Errorf("%s: for %q the note must say exactly when Talos's checks were set aside: %q", where, node, gate.Note)
							}

							// And what it must allow, or a rollout could not repair a node.
							switch {
							case everyoneHealthy && verdict.passes && !gate.Open:
								t.Errorf("%s: closed for %q on a healthy cluster: %s", where, node, gate.Reason)
							case own.known && othersHealthy && !gate.Open:
								t.Errorf("%s: closed for %q, the only unhealthy member: %s", where, node, gate.Reason)
							}
						}
					}
				}
			}
		}
	}

	if checked < 4000 {
		t.Fatalf("only %d situations were checked", checked)
	}
}

// healthyOrConfig reports whether a concerns nothing but the member's config.
func healthyOrConfig(a ailment) bool { return a.healthy() || a.config }
