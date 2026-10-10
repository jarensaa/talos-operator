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

// Preconditions judges the cluster. Unknown means that a fact it needs was not read.
func Preconditions(c Cluster) []talosv1alpha1.Precondition {
	return []talosv1alpha1.Precondition{
		checkCredentialsAdmin(c),
		checkAllMembersRunning(c),
		checkEtcdHealthy(c),
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

func checkAllMembersRunning(c Cluster) talosv1alpha1.Precondition {
	var failures, unknown []string

	if len(c.Members) == 0 {
		unknown = append(unknown, "no members observed")
	}

	for _, m := range c.Members {
		switch {
		case m.Status == nil:
			unknown = append(unknown, m.Hostname+": "+m.ReadErr)
		case m.Status.Stage != StageRunning || !m.Status.Ready:
			failures = append(failures, fmt.Sprintf("%s is %s (ready=%t)", m.Hostname, m.Status.Stage, m.Status.Ready))
		}
	}

	return precondition(PreconditionAllMembersRunning, failures, unknown)
}

func checkEtcdHealthy(c Cluster) talosv1alpha1.Precondition {
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
		case !m.ControlPlane:
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

// Preconditions a cluster must meet before a rollout touches a node, and while a changed node is
// verified. The second list leaves out NoPendingConfig: the node under verification runs a tried
// config, which is exactly a config that is active and not persistent.
var (
	StartPreconditions = []string{
		PreconditionAllMembersRunning, PreconditionEtcdHealthy, PreconditionEtcdMatchesControlPlanes, PreconditionNoPendingConfig,
	}
	VerifyPreconditions = []string{
		PreconditionAllMembersRunning, PreconditionEtcdHealthy, PreconditionEtcdMatchesControlPlanes,
	}
)

// Pass is how one pass over the cluster went, as far as the cluster gate cares.
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
	// Preconditions are this pass's; nil when they were not evaluated.
	Preconditions []talosv1alpha1.Precondition
	// HealthChecked is false when Talos's own health checks did not run, and HealthMessage says
	// why. Otherwise Healthy is their verdict and HealthMessage what failed.
	HealthChecked bool
	Healthy       bool
	HealthMessage string
}

// ClusterGate is the cluster-wide gate of a rollout (docs/scope.md §6.3): every member was
// read, every live member has a TalosMachine, the required preconditions hold, and Talos's own
// health checks pass. It is open only when all of that is known to be so: anything that could
// not be determined closes it.
func ClusterGate(p Pass, required []string) Gate {
	closed := func(format string, args ...any) Gate { return Gate{Reason: fmt.Sprintf(format, args...)} }

	switch {
	case !p.Reached:
		return closed("the cluster was not reached")
	case p.Incomplete != "":
		return closed("the cluster could not be observed completely: %s", p.Incomplete)
	case p.KubernetesUnread != "":
		return closed("the Kubernetes API could not be read: %s", p.KubernetesUnread)
	case !p.Matched:
		return closed("live members could not all be matched against TalosMachines")
	case len(p.Unmatched) > 0:
		return closed("live members without a TalosMachine: %s", strings.Join(p.Unmatched, ", "))
	case p.Preconditions == nil:
		return closed("the cluster's preconditions were not evaluated")
	}

	for _, name := range required {
		i := slices.IndexFunc(p.Preconditions, func(pc talosv1alpha1.Precondition) bool { return pc.Name == name })
		if i < 0 {
			return closed("%s was not evaluated", name)
		}

		if pc := p.Preconditions[i]; pc.Status != metav1.ConditionTrue {
			return closed("%s is %s: %s", name, pc.Status, pc.Message)
		}
	}

	switch {
	case !p.HealthChecked:
		return closed("Talos's health checks did not run: %s", p.HealthMessage)
	case !p.Healthy:
		return closed("Talos's health checks fail: %s", p.HealthMessage)
	}

	return Gate{Open: true}
}
