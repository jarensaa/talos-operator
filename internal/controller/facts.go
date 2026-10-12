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

package controller

import (
	"net/netip"
	"slices"

	corev1 "k8s.io/api/core/v1"

	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// nodeFacts is what was read from one member. A nil pointer means the read failed; readErr says why.
type nodeFacts struct {
	hostname    string
	address     netip.Addr
	machineType string
	version     string
	// systemUUID is empty when it could not be read.
	systemUUID string
	// activeConfig is the raw live config. It holds every cluster secret: memory only, never
	// logged or written anywhere but the backup Secret.
	activeConfig []byte
	status       *talos.MachineStatus
	schematicID  *string
	// bootID changes with every boot; empty when it was not read.
	bootID string
	// secureBoot is whether the node's firmware enforces Secure Boot; nil when it was not read.
	secureBoot *bool
	// persistentConfig is the raw persistent config; nil when the node has none, as after
	// booting from disk. The same care applies as for activeConfig.
	persistentConfig []byte
	// configSettled is false while a staged config waits for a reboot.
	configSettled *bool
	// etcdErrors are the errors the node's etcd member reports; they mean nothing unless
	// etcdStatusRead is set. Control planes only.
	etcdErrors     []string
	etcdStatusRead bool
	// etcdLeader is true when the node's etcd member leads; like etcdErrors it needs etcdStatusRead.
	etcdLeader bool
	// etcdSize is the size of the member's database file and etcdInUse what of it holds data, in
	// bytes; they too need etcdStatusRead.
	etcdSize, etcdInUse int64
	// kubeNode is what the Kubernetes API says about the member's Node; nil when the Nodes were
	// not read.
	kubeNode *plan.KubernetesNode
	// read is true when every read of the node succeeded; readErr is the first failure otherwise.
	read    bool
	readErr string
}

// clusterFacts is what one pass read from the cluster.
type clusterFacts struct {
	credentialsAdmin bool
	nodes            []nodeFacts
	// etcdMembers is nil when the member list could not be read.
	etcdMembers []talos.EtcdMember
}

// judged is the cluster as the gates in internal/plan see it: the facts without the configs,
// addresses and identities, which no gate decides on.
func (f clusterFacts) judged() plan.Cluster {
	c := plan.Cluster{CredentialsAdmin: f.credentialsAdmin, Members: make([]plan.Member, 0, len(f.nodes))}

	for _, n := range f.nodes {
		m := plan.Member{
			Hostname:       n.hostname,
			ControlPlane:   n.machineType == machineTypeControlPlane,
			ReadErr:        n.readErr,
			Version:        n.version,
			Schematic:      n.schematicID,
			ConfigSettled:  n.configSettled,
			EtcdStatusRead: n.etcdStatusRead,
			EtcdErrors:     n.etcdErrors,
			Node:           n.kubeNode,
		}

		if n.status != nil {
			m.Status = &plan.MemberStatus{Stage: n.status.Stage, Ready: n.status.Ready}
		}

		if n.version != "" {
			if ok, why := talos.SupportsTalos(n.version); !ok {
				m.VersionProblem = why
			}
		}

		c.Members = append(c.Members, m)
	}

	if f.etcdMembers != nil {
		c.EtcdMembers = make([]plan.EtcdMember, 0, len(f.etcdMembers))
		for _, m := range f.etcdMembers {
			c.EtcdMembers = append(c.EtcdMembers, plan.EtcdMember{ID: m.ID, Hostname: m.Hostname, Learner: m.IsLearner})
		}
	}

	return c
}

// observeKubeNodes records, for every member, what the Kubernetes API says about its Node. A
// member's Node is the one carrying the member's address, or else the one named like it.
func (f *clusterFacts) observeKubeNodes(nodes []corev1.Node) {
	for i := range f.nodes {
		n := &f.nodes[i]
		n.kubeNode = &plan.KubernetesNode{}

		at := slices.IndexFunc(nodes, func(node corev1.Node) bool { return hasAddress(node, n.address) })
		if at < 0 {
			at = slices.IndexFunc(nodes, func(node corev1.Node) bool { return node.Name == n.hostname })
		}

		if at >= 0 {
			n.kubeNode.Registered = true
			n.kubeNode.Unschedulable = nodes[at].Spec.Unschedulable
			n.kubeNode.Ready = nodeConditionTrue(&nodes[at], corev1.NodeReady) &&
				!nodeConditionTrue(&nodes[at], corev1.NodeNetworkUnavailable)
		}
	}
}

func hasAddress(node corev1.Node, addr netip.Addr) bool {
	return slices.ContainsFunc(node.Status.Addresses, func(a corev1.NodeAddress) bool {
		parsed, err := netip.ParseAddr(a.Address)

		return err == nil && addr.IsValid() && parsed == addr
	})
}

// passOf is this pass over the cluster as the rollout's gates see it.
func passOf(obs observation) plan.Pass {
	p := plan.Pass{Reached: obs.facts != nil, Matched: obs.unmatched != nil, Unmatched: obs.unmatched}

	if obs.facts != nil {
		c := obs.facts.judged()
		p.Cluster = &c
	}

	if obs.err != nil {
		p.Incomplete = obs.err.Error()
	}

	if obs.kubeErr != nil {
		p.KubernetesUnread = obs.kubeErr.Error()
	}

	if obs.health == nil {
		p.HealthMessage = obs.healthErrorMsg

		return p
	}

	p.HealthChecked, p.Healthy = true, obs.health.Healthy
	if !p.Healthy {
		p.HealthMessage = lastMessage(obs.health.Messages)
	}

	return p
}
