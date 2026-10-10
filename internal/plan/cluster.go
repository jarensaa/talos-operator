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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// Names of the preconditions a cluster is judged by: before it is adopted (docs/scope.md §7.1
// stage 1), and, for those about its health, before and while a rollout changes a node (§6.3).
const (
	PreconditionCredentialsAdmin         = "CredentialsAdmin"
	PreconditionAllMembersRunning        = "AllMembersRunning"
	PreconditionEtcdHealthy              = "EtcdHealthy"
	PreconditionEtcdMatchesControlPlanes = "EtcdMatchesControlPlanes"
	PreconditionNoPendingConfig          = "NoPendingConfig"
	PreconditionTalosVersionSupported    = "TalosVersionSupported"
	PreconditionSchematicDetected        = "SchematicDetected"
)

// CheckAllNodesReady is a check of the rollout gates only (docs/scope.md §6.3): Kubernetes
// reports every member's Node Ready. Adoption does not ask for it.
const CheckAllNodesReady = "AllNodesReady"

// StageRunning is the Talos machine stage of a booted, configured node.
const StageRunning = "running"

// Member is what one pass over the cluster learned about a node, as far as the gates care. Every
// fact can be unknown, and unknown is never read as fine.
type Member struct {
	Hostname     string
	ControlPlane bool
	// Status is nil when the node's machine status was not read; ReadErr then says why.
	Status  *MemberStatus
	ReadErr string
	// Version is the node's Talos version, empty when not read. VersionProblem is set when this
	// operator does not support it.
	Version        string
	VersionProblem string
	// Schematic is the Image Factory schematic the node was installed from: nil when not read,
	// empty when the node has none.
	Schematic *string
	// ConfigSettled is false while a config waits on the node, staged or tried: its persistent
	// config differs from its active one. Nil when the config was not read.
	ConfigSettled *bool
	// EtcdErrors are the errors the node's etcd member reports. They only mean something when
	// EtcdStatusRead is set. Control planes only.
	EtcdStatusRead bool
	EtcdErrors     []string
	// Node is the member's Node as the Kubernetes API reports it; nil when the Nodes were not read.
	Node *KubernetesNode
}

// KubernetesNode is what the Kubernetes API says about a member, which is the control plane's
// view of it and not the member's own.
type KubernetesNode struct {
	// Registered is false when Kubernetes has no Node for the member.
	Registered bool
	// Ready is true when the Node is Ready and has a network.
	Ready bool
}

// ails reports whether the member is known to be unhealthy itself. A fact that was not read is
// not known: it closes gates, and it never excuses anything.
func (m Member) ails() bool {
	switch {
	case m.Status != nil && (m.Status.Stage != StageRunning || !m.Status.Ready):
		return true
	case m.ControlPlane && m.EtcdStatusRead && len(m.EtcdErrors) > 0:
		return true
	case m.Node != nil && (!m.Node.Registered || !m.Node.Ready):
		return true
	default:
		return false
	}
}

// MemberStatus is a node's position in its boot sequence.
type MemberStatus struct {
	Stage string
	Ready bool
}

// EtcdMember is one entry of the etcd member list.
type EtcdMember struct {
	ID       uint64
	Hostname string
	Learner  bool
}

// Cluster is what one pass learned about the cluster.
type Cluster struct {
	// CredentialsAdmin is whether the operator's Talos identity has the os:admin role.
	CredentialsAdmin bool
	Members          []Member
	// EtcdMembers is nil when the member list was not read.
	EtcdMembers []EtcdMember
}

// member returns the member with the given hostname. It answers only when exactly one member
// carries the name: a name two nodes share identifies neither.
func (c Cluster) member(hostname string) (Member, bool) {
	var (
		found Member
		n     int
	)

	for _, m := range c.Members {
		if hostname != "" && m.Hostname == hostname {
			found = m
			n++
		}
	}

	return found, n == 1
}

// Preconditions judges the cluster. Unknown means that a fact it needs was not read.
func Preconditions(c Cluster) []talosv1alpha1.Precondition {
	return []talosv1alpha1.Precondition{
		checkCredentialsAdmin(c),
		checkAllMembersRunning(c, ""),
		checkEtcdHealthy(c, ""),
		checkEtcdMatchesControlPlanes(c),
		checkNoPendingConfig(c),
		checkTalosVersionSupported(c),
		checkSchematicDetected(c),
	}
}

func precondition(name string, failures, unknown []string) talosv1alpha1.Precondition {
	switch {
	case len(failures) > 0:
		return talosv1alpha1.Precondition{Name: name, Status: metav1.ConditionFalse, Message: strings.Join(failures, "; ")}
	case len(unknown) > 0:
		return talosv1alpha1.Precondition{Name: name, Status: metav1.ConditionUnknown, Message: strings.Join(unknown, "; ")}
	default:
		return talosv1alpha1.Precondition{Name: name, Status: metav1.ConditionTrue}
	}
}

func checkCredentialsAdmin(c Cluster) talosv1alpha1.Precondition {
	if !c.CredentialsAdmin {
		return precondition(PreconditionCredentialsAdmin, []string{"credentials lack the os:admin role"}, nil)
	}

	return precondition(PreconditionCredentialsAdmin, nil, nil)
}

// leftOut reports whether m is the member a check was told not to ask about. An empty name
// leaves nobody out, a member without a hostname included.
func leftOut(m Member, except string) bool { return except != "" && m.Hostname == except }

// checkAllMembersRunning judges every member's own report of itself. except names a member
// whose health is not asked about, or is empty; the same goes for the checks below that take it.
func checkAllMembersRunning(c Cluster, except string) talosv1alpha1.Precondition {
	var failures, unknown []string

	if len(c.Members) == 0 {
		unknown = append(unknown, "no members observed")
	}

	for _, m := range c.Members {
		switch {
		case leftOut(m, except):
		case m.Status == nil:
			unknown = append(unknown, m.Hostname+": "+m.ReadErr)
		case m.Status.Stage != StageRunning || !m.Status.Ready:
			failures = append(failures, fmt.Sprintf("%s is %s (ready=%t)", m.Hostname, m.Status.Stage, m.Status.Ready))
		}
	}

	return precondition(PreconditionAllMembersRunning, failures, unknown)
}

// checkAllNodesReady judges every member by what the Kubernetes API says about its Node. A member
// can call itself ready while the control plane has stopped hearing from it.
func checkAllNodesReady(c Cluster, except string) talosv1alpha1.Precondition {
	var failures, unknown []string

	for _, m := range c.Members {
		switch {
		case leftOut(m, except):
		case m.Node == nil:
			unknown = append(unknown, m.Hostname+": Kubernetes Node not readable")
		case !m.Node.Registered:
			failures = append(failures, "Kubernetes has no Node for "+m.Hostname)
		case !m.Node.Ready:
			failures = append(failures, "Kubernetes reports "+m.Hostname+" not Ready")
		}
	}

	return precondition(CheckAllNodesReady, failures, unknown)
}

// checkEtcdHealthy judges the member list as a whole, whoever is left out: a member that never
// started or is still a learner is a membership in transition. Only what a control plane reports
// about its own etcd member is that node's own health.
func checkEtcdHealthy(c Cluster, except string) talosv1alpha1.Precondition {
	if c.EtcdMembers == nil {
		return precondition(PreconditionEtcdHealthy, nil, []string{"etcd member list not readable"})
	}

	var failures, unknown []string

	for _, m := range c.EtcdMembers {
		switch {
		case m.Hostname == "":
			failures = append(failures, fmt.Sprintf("member %x has not started", m.ID))
		case m.Learner:
			failures = append(failures, m.Hostname+" is still a learner")
		}
	}

	for _, m := range c.Members {
		switch {
		case !m.ControlPlane, leftOut(m, except):
		case !m.EtcdStatusRead:
			// A member that was not asked has not said it is fine.
			unknown = append(unknown, m.Hostname+": etcd status not readable")
		case len(m.EtcdErrors) > 0:
			failures = append(failures, m.Hostname+": "+strings.Join(m.EtcdErrors, ", "))
		}
	}

	return precondition(PreconditionEtcdHealthy, failures, unknown)
}

func checkEtcdMatchesControlPlanes(c Cluster) talosv1alpha1.Precondition {
	if c.EtcdMembers == nil {
		return precondition(PreconditionEtcdMatchesControlPlanes, nil, []string{"etcd member list not readable"})
	}

	var controlPlanes, etcd []string

	for _, m := range c.Members {
		if m.ControlPlane {
			controlPlanes = append(controlPlanes, m.Hostname)
		}
	}

	for _, m := range c.EtcdMembers {
		etcd = append(etcd, m.Hostname)
	}

	slices.Sort(controlPlanes)
	slices.Sort(etcd)

	if !slices.Equal(controlPlanes, etcd) {
		return precondition(PreconditionEtcdMatchesControlPlanes, []string{
			fmt.Sprintf("control planes %v but etcd members %v", controlPlanes, etcd),
		}, nil)
	}

	return precondition(PreconditionEtcdMatchesControlPlanes, nil, nil)
}

func checkNoPendingConfig(c Cluster) talosv1alpha1.Precondition {
	var failures, unknown []string

	for _, m := range c.Members {
		switch {
		case m.ConfigSettled == nil:
			unknown = append(unknown, m.Hostname+": machine config not readable")
		case !*m.ConfigSettled:
			failures = append(failures, m.Hostname+" has a staged or TRY config pending (persistent differs from active)")
		}
	}

	return precondition(PreconditionNoPendingConfig, failures, unknown)
}

func checkTalosVersionSupported(c Cluster) talosv1alpha1.Precondition {
	var failures, unknown []string

	for _, m := range c.Members {
		switch {
		case m.Version == "":
			unknown = append(unknown, m.Hostname+": version not readable")
		case m.VersionProblem != "":
			failures = append(failures, m.Hostname+": "+m.VersionProblem)
		}
	}

	return precondition(PreconditionTalosVersionSupported, failures, unknown)
}

func checkSchematicDetected(c Cluster) talosv1alpha1.Precondition {
	var failures, unknown []string

	for _, m := range c.Members {
		switch {
		case m.Schematic == nil:
			unknown = append(unknown, m.Hostname+": schematic not readable")
		case *m.Schematic == "":
			failures = append(failures, m.Hostname+" has no Image Factory schematic")
		}
	}

	return precondition(PreconditionSchematicDetected, failures, unknown)
}

// PreconditionsMet aggregates preconditions into one status and a message naming what is not met.
func PreconditionsMet(ps []talosv1alpha1.Precondition) (metav1.ConditionStatus, string) {
	status := metav1.ConditionTrue

	var notMet []string

	for _, p := range ps {
		switch p.Status {
		case metav1.ConditionTrue:
		case metav1.ConditionFalse:
			status = metav1.ConditionFalse
			notMet = append(notMet, p.Name)
		default:
			if status == metav1.ConditionTrue {
				status = metav1.ConditionUnknown
			}

			notMet = append(notMet, p.Name)
		}
	}

	if len(notMet) == 0 {
		return status, "all adoption preconditions hold"
	}

	return status, "not met: " + strings.Join(notMet, ", ")
}

// Pass is how one pass over the cluster went, as far as the rollout gates care.
type Pass struct {
	// Reached is false when the Talos API did not answer at all.
	Reached bool
	// Incomplete says what could not be read from Talos, or is empty.
	Incomplete string
	// KubernetesUnread says what could not be read from the Kubernetes API, or is empty.
	KubernetesUnread string
	// Matched is false when it could not be determined which live members have a TalosMachine.
	// Unmatched then means nothing; otherwise it lists the hostnames of those that have none.
	Matched   bool
	Unmatched []string
	// Cluster is what the pass read from the members; nil when the cluster was not reached.
	Cluster *Cluster
	// HealthChecked is false when Talos's own health checks did not run, and HealthMessage says
	// why. Otherwise Healthy is their verdict and HealthMessage what failed.
	HealthChecked bool
	Healthy       bool
	HealthMessage string
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
// config that broke a node is taken back. When the node is known to be unhealthy, Talos's checks
// are not asked either: they judge the cluster as a whole, cannot leave a node out, and cannot
// pass. The other members then stand on their own facts alone, and the gate says so in its Note.
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

	gate := holds(
		checkAllMembersRunning(c, node),
		checkAllNodesReady(c, node),
		checkEtcdHealthy(c, node),
		checkEtcdMatchesControlPlanes(c),
		checkNoPendingConfig(c),
	)
	if !gate.Open {
		return gate
	}

	if known && target.ails() {
		return Gate{Open: true, Note: node + " is unhealthy itself, so Talos's cluster-wide health checks were not asked: " +
			"every other member is healthy by its own report, by Kubernetes and by etcd"}
	}

	return talosHealth(p)
}

// VerifyGate is the cluster-wide part of verifying a node whose config was changed
// (docs/scope.md §8.5.1): the whole cluster, that node included, is healthy. Nobody is left out
// and Talos's own health checks must pass. A pending config is not asked about: the node under
// verification runs a tried config, which is exactly a config that is active and not persistent.
func VerifyGate(p Pass) Gate {
	if gate := observed(p); !gate.Open {
		return gate
	}

	c := *p.Cluster

	gate := holds(
		checkAllMembersRunning(c, ""),
		checkAllNodesReady(c, ""),
		checkEtcdHealthy(c, ""),
		checkEtcdMatchesControlPlanes(c),
	)
	if !gate.Open {
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
