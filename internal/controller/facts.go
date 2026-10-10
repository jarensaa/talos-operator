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

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
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
	// persistentConfig is the raw persistent config; nil when the node has none, as after
	// booting from disk. The same care applies as for activeConfig.
	persistentConfig []byte
	// configSettled is false while a staged config waits for a reboot.
	configSettled *bool
	// etcdErrors are the errors the node's etcd member reports; they mean nothing unless
	// etcdStatusRead is set. Control planes only.
	etcdErrors     []string
	etcdStatusRead bool
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

// passOf is this pass over the cluster as the rollout's cluster gate sees it.
func passOf(st *talosv1alpha1.TalosClusterStatus, obs observation) plan.Pass {
	p := plan.Pass{Reached: obs.facts != nil, Matched: obs.unmatched != nil, Unmatched: obs.unmatched}

	if obs.err != nil {
		p.Incomplete = obs.err.Error()
	}

	if obs.kubeErr != nil {
		p.KubernetesUnread = obs.kubeErr.Error()
	}

	// applyObservation evaluated them from this pass's facts; without facts the gate is closed
	// before it looks at them.
	if st.Adoption != nil {
		p.Preconditions = st.Adoption.Preconditions
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
