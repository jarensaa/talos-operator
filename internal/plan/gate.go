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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// The gates: what must be known to hold before something is done to a node, and while a node
// that was changed is verified (docs/scope.md §6.3, §8.1). A gate is only open when everything
// it asks about is known to be so; a fact that was not read closes it.

// Gate is a condition that must hold before a node is touched.
type Gate struct {
	Open bool
	// Reason says what is closed.
	Reason string
	// Note says what an open gate did not ask about, if anything.
	Note string
	// block is the reason a plan gives for not starting when this gate is what closes it, where
	// that is something more particular than a closed gate: a quorum at risk, a name in use.
	block string
}

// And is the gate that is open when both are. A closed gate keeps its own reason.
func (g Gate) And(other Gate) Gate {
	switch {
	case !g.Open:
		return g
	case !other.Open:
		return other
	}

	notes := make([]string, 0, 2)
	for _, n := range []string{g.Note, other.Note} {
		if n != "" {
			notes = append(notes, n)
		}
	}

	return Gate{Open: true, Note: strings.Join(notes, "; ")}
}

func closedGate(format string, args ...any) Gate { return Gate{Reason: fmt.Sprintf(format, args...)} }

// TouchGate is the gate before something is sent to a node (docs/scope.md §6.3). It is open only
// when all of this is known to be so; anything that could not be determined closes it:
//
//   - every member was read and every live member has a TalosMachine;
//   - etcd has exactly the control planes as started, voting members, and no node has a config
//     pending, the node itself included: Talos falls back to whatever is active when a try
//     arrives, so a try on top of a pending one would disarm it;
//   - every other member is healthy by its own facts: it runs and calls itself ready, Kubernetes
//     reports its Node Ready, and its etcd member reports no errors;
//   - Talos's own health checks pass.
//
// The health of the node itself is left out of it. A node may be the one thing that is wrong:
// touching it takes nothing from the cluster that the cluster still has, and a rollout is how a
// config that broke a node is taken back. When the node is known to be unhealthy, or is cordoned
// as it is in the middle of its own upgrade, Talos's checks are not asked either: they judge the
// cluster as a whole, cannot leave a node out, and cannot pass. The other members then stand on
// their own facts alone, and the gate says so in its Note.
//
// An unhealthy node is therefore only ever touched while it is the only one, which is invariant 2
// of §8.1 for every node, not just for control planes.
func TouchGate(p Pass, node string) Gate {
	if gate := observed(p); !gate.Open {
		return gate
	}

	c := *p.Cluster

	// A name that does not identify exactly one member leaves nobody out.
	target, known := c.member(node)
	if !known {
		node = ""
	}

	gate := ownFacts(c, node, true)
	if !gate.Open {
		return gate
	}

	if known && (target.ails() || target.cordoned()) {
		return Gate{Open: true, Note: node + " is unhealthy or cordoned itself, so Talos's cluster-wide health checks were not asked: " +
			"every other member is healthy by its own report, by Kubernetes and by etcd"}
	}

	return talosHealth(p)
}

// RebootGate is the gate before a node is taken down on purpose: cordoned, drained, rebooted
// (docs/scope.md §7.3). It is TouchGate, and for a control plane also the guard Talos's own
// upgrade path dropped with LifecycleService (§3.2): etcd must not have exactly two members. With
// two, losing either loses quorum, and nothing brings it back while the node is down. With one
// there is no quorum to protect beyond the node itself, and with three or more the others hold it,
// which TouchGate has already established: every other member is a started voter that reports no
// errors.
func RebootGate(p Pass, node string) Gate {
	gate := TouchGate(p, node)
	if !gate.Open {
		return gate
	}

	if target, known := p.Cluster.member(node); known && target.ControlPlane && len(p.Cluster.EtcdMembers) == 2 {
		return closedGate("etcd has exactly two members, so taking %s down would lose quorum", node)
	}

	return gate
}

// VerifyGate is the cluster-wide part of verifying a node whose config was changed
// (docs/scope.md §8.5.1): the whole cluster, that node included, is healthy. Nobody is left out
// and Talos's own health checks must pass. A pending config is not asked about: the node under
// verification runs a tried config, which is exactly a config that is active and not persistent.
func VerifyGate(p Pass) Gate {
	if gate := observed(p); !gate.Open {
		return gate
	}

	if gate := ownFacts(*p.Cluster, "", false); !gate.Open {
		return gate
	}

	return talosHealth(p)
}

// observed is open when the pass saw the whole cluster.
func observed(p Pass) Gate {
	switch {
	case !p.Reached:
		return closedGate("the cluster was not reached")
	case p.Incomplete != "":
		return closedGate("the cluster could not be observed completely: %s", p.Incomplete)
	case p.KubernetesUnread != "":
		return closedGate("the Kubernetes API could not be read: %s", p.KubernetesUnread)
	case !p.Matched:
		return closedGate("live members could not all be matched against TalosMachines")
	case len(p.Unmatched) > 0:
		return closedGate("live members without a TalosMachine: %s", strings.Join(p.Unmatched, ", "))
	case p.Cluster == nil:
		return closedGate("the cluster's members were not read")
	}

	return Gate{Open: true}
}

// ownFacts is open when every member is known to be healthy by its own report, by Kubernetes and
// by etcd, and etcd has exactly the control planes as members. except names a member that is not
// asked about; pending is whether a config that waits on a node closes the gate too.
func ownFacts(c Cluster, except string, pending bool) Gate {
	checks := []talosv1alpha1.Precondition{
		checkAllMembersRunning(c, except),
		checkAllNodesReady(c, except),
		checkEtcdHealthy(c, except),
		checkEtcdMatchesControlPlanes(c),
	}

	if pending {
		checks = append(checks, checkNoPendingConfig(c))
	}

	return holds(checks...)
}

// holds is open when every check is known to hold.
func holds(checks ...talosv1alpha1.Precondition) Gate {
	for _, c := range checks {
		if c.Status != metav1.ConditionTrue {
			return closedGate("%s is %s: %s", c.Name, c.Status, c.Message)
		}
	}

	return Gate{Open: true}
}

func talosHealth(p Pass) Gate {
	switch {
	case !p.HealthChecked:
		return closedGate("Talos's health checks did not run: %s", p.HealthMessage)
	case !p.Healthy:
		return closedGate("Talos's health checks fail: %s", p.HealthMessage)
	}

	return Gate{Open: true}
}
