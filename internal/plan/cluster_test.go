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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

func ptr[T any](v T) *T { return &v }

const etcdUnhealthy = "etcd is unhealthy"

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
		{"etcd errors", func(c *Cluster) { c.Members[0].EtcdErrors = []string{"alarm: NOSPACE"} },
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
		t.Fatal("nothing to meet is met; the cluster gate is what refuses an unevaluated cluster")
	}
}

// healthyPass is a pass that read everything and found it fine.
func healthyPass() Pass {
	return Pass{
		Reached:       true,
		Matched:       true,
		Preconditions: Preconditions(healthyCluster()),
		HealthChecked: true,
		Healthy:       true,
	}
}

func TestClusterGateIsOpenOnlyWhenEverythingIsKnownToBeFine(t *testing.T) {
	if gate := ClusterGate(healthyPass(), StartPreconditions); !gate.Open {
		t.Fatalf("a healthy cluster must open the gate: %s", gate.Reason)
	}

	set := func(name string, status metav1.ConditionStatus) func(*Pass) {
		return func(p *Pass) {
			for i := range p.Preconditions {
				if p.Preconditions[i].Name == name {
					p.Preconditions[i].Status, p.Preconditions[i].Message = status, "broken"
				}
			}
		}
	}

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
		{"preconditions were not evaluated", func(p *Pass) { p.Preconditions = nil }, "not evaluated"},
		{"a required precondition is missing", func(p *Pass) { p.Preconditions = p.Preconditions[:1] }, "was not evaluated"},
		{"a member is not running", set(PreconditionAllMembersRunning, metav1.ConditionFalse), PreconditionAllMembersRunning},
		{etcdUnhealthy, set(PreconditionEtcdHealthy, metav1.ConditionFalse), PreconditionEtcdHealthy},
		{"etcd lost a member", set(PreconditionEtcdMatchesControlPlanes, metav1.ConditionFalse), PreconditionEtcdMatchesControlPlanes},
		{"a node has a config pending", set(PreconditionNoPendingConfig, metav1.ConditionFalse), PreconditionNoPendingConfig},
		{"an unknown precondition is not a passing one", set(PreconditionEtcdHealthy, metav1.ConditionUnknown), "Unknown"},
		{"the health check did not run", func(p *Pass) { p.HealthChecked, p.HealthMessage = false, "deadline exceeded" }, "deadline exceeded"},
		{"the health check fails", func(p *Pass) { p.Healthy, p.HealthMessage = false, etcdUnhealthy }, etcdUnhealthy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := healthyPass()
			tt.breakIt(&p)

			gate := ClusterGate(p, StartPreconditions)
			if gate.Open || !strings.Contains(gate.Reason, tt.reason) {
				t.Fatalf("expected a closed gate mentioning %q, got open=%t reason=%q", tt.reason, gate.Open, gate.Reason)
			}
		})
	}
}

// What a rollout does not depend on does not hold it up: adoption cares where a node was
// installed from and with which role the operator talks to Talos, a config change does not.
func TestClusterGateOnlyLooksAtWhatItRequires(t *testing.T) {
	p := healthyPass()

	for i := range p.Preconditions {
		if name := p.Preconditions[i].Name; name == PreconditionSchematicDetected || name == PreconditionTalosVersionSupported {
			p.Preconditions[i].Status = metav1.ConditionFalse
		}
	}

	if gate := ClusterGate(p, StartPreconditions); !gate.Open {
		t.Fatalf("a precondition that is not required must not close the gate: %s", gate.Reason)
	}
}

// A node under verification runs a tried config: active, not persistent. That must not count
// against it, or no try could ever be verified.
func TestVerificationDoesNotCountTheTryItVerifiesAsPendingConfig(t *testing.T) {
	p := healthyPass()

	for i := range p.Preconditions {
		if p.Preconditions[i].Name == PreconditionNoPendingConfig {
			p.Preconditions[i].Status = metav1.ConditionFalse
		}
	}

	if ClusterGate(p, StartPreconditions).Open {
		t.Fatal("a pending config must keep a rollout from touching a node")
	}

	if gate := ClusterGate(p, VerifyPreconditions); !gate.Open {
		t.Fatalf("a tried config must be verifiable: %s", gate.Reason)
	}
}
