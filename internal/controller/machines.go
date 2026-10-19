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
	"context"
	"errors"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/classify"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// The cluster's machines as one pass sees them. Every kind of operation asks the same things of
// a machine: which pool it is of, which live node carries its identity, what that node is called,
// what its spec renders and how that differs from what the node runs. They are answered here,
// once, from this pass's observation and never from a machine's status: the status only says
// whether the machine's own controller has caught up (see planMachine).

// fleet is every TalosMachine of a cluster with its pool and what this pass read of its node.
type fleet struct {
	tc     *talosv1alpha1.TalosCluster
	reader client.Reader
	// facts is what the pass read from the cluster; nil if it was not reached.
	facts *clusterFacts
	pools map[string]*talosv1alpha1.TalosMachinePool
	// all are the machines that are declared. members are those that count as nodes of the
	// cluster for rollouts, upgrades and the proof: the ones that joined and have not left. A
	// machine that is declared and has not joined, or has left, is the business of adding and
	// removing alone (docs/scope.md §7.6).
	all, members []talosv1alpha1.TalosMachine
	// views is what was worked out of a machine's config in this pass, by its name.
	views map[string]machineView
}

func newFleet(tc *talosv1alpha1.TalosCluster, reader client.Reader, facts *clusterFacts,
	pools map[string]*talosv1alpha1.TalosMachinePool, all []talosv1alpha1.TalosMachine,
) *fleet {
	f := &fleet{tc: tc, reader: reader, facts: facts, pools: pools, all: all, views: map[string]machineView{}}

	f.members = make([]talosv1alpha1.TalosMachine, 0, len(all))

	for i := range all {
		if all[i].IsMember() {
			f.members = append(f.members, all[i])
		}
	}

	return f
}

func machineNamed(machines []talosv1alpha1.TalosMachine, name string) *talosv1alpha1.TalosMachine {
	for i := range machines {
		if machines[i].Name == name {
			return &machines[i]
		}
	}

	return nil
}

// declared is the machine of that name; nil if there is none.
func (f *fleet) declared(name string) *talosv1alpha1.TalosMachine { return machineNamed(f.all, name) }

// member is the member of that name; nil if there is none, or it is no member.
func (f *fleet) member(name string) *talosv1alpha1.TalosMachine { return machineNamed(f.members, name) }

// boundMachine is a TalosMachine with its pool and the live node that carries its hardware identity.
type boundMachine struct {
	tm   *talosv1alpha1.TalosMachine
	pool *talosv1alpha1.TalosMachinePool
	// node is the live node carrying the machine's hardware identity; nil if none answered.
	node *nodeFacts
	// install is what the spec asks the machine to be installed from; installProblem says why
	// that cannot be told, and install is then empty.
	install        adopt.Install
	installProblem string
}

// of joins a TalosMachine with its pool and its live node. A nil machine gives an empty one: a
// machine that no longer exists has nothing to it.
func (f *fleet) of(tm *talosv1alpha1.TalosMachine) boundMachine {
	if tm == nil {
		return boundMachine{}
	}

	m := boundMachine{tm: tm, pool: f.pools[tm.Spec.PoolRef.Name], node: nodeWithIdentity(f.facts, tm.Spec.Identity.SystemUUID)}
	if m.pool == nil {
		m.installProblem = "TalosMachinePool " + tm.Spec.PoolRef.Name + " not found"

		return m
	}

	install, err := adopt.InstallTarget(&f.tc.Spec, &m.pool.Spec)
	if err != nil {
		m.installProblem = err.Error()

		return m
	}

	m.install = install

	return m
}

// nodeWithIdentity returns the observed node with the given SMBIOS UUID. A node whose identity
// could not be read matches nothing: the operator only acts on hardware it has just identified
// (docs/scope.md §8.1, invariant 8).
func nodeWithIdentity(facts *clusterFacts, uuid string) *nodeFacts {
	if facts == nil || uuid == "" {
		return nil
	}

	for i := range facts.nodes {
		if strings.EqualFold(facts.nodes[i].systemUUID, uuid) {
			return &facts.nodes[i]
		}
	}

	return nil
}

func (m boundMachine) controlPlane() bool { return controlPlane(m.pool) }

// controlPlane reports whether pool is one of control planes. A pool that is not there is not.
func controlPlane(pool *talosv1alpha1.TalosMachinePool) bool {
	return pool != nil && pool.Spec.Role == talosv1alpha1.MachineRoleControlPlane
}

// held reports whether the machine is kept out of rollouts and upgrades: it is on hold, or on
// its way out.
func (m boundMachine) held() bool {
	return m.tm.Spec.Hold || m.tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved
}

// hostname is the hostname of the machine's live node; empty if none answered.
func (m boundMachine) hostname() string {
	if m.node == nil {
		return ""
	}

	return m.node.hostname
}

// knownAs is the hostname the machine's node is known under in this pass: what it answered with,
// or what it was last known as.
func (m boundMachine) knownAs() string {
	if m.node != nil {
		return m.node.hostname
	}

	if m.tm != nil && m.tm.Status.Identity != nil {
		return m.tm.Status.Identity.Hostname
	}

	return ""
}

// kubernetesNode is the name of the machine's Node, which outlives the node being unreachable:
// the machine's own controller recorded it.
func (m boundMachine) kubernetesNode() string {
	if m.tm != nil && m.tm.Status.Identity != nil && m.tm.Status.Identity.KubernetesNodeName != "" {
		return m.tm.Status.Identity.KubernetesNodeName
	}

	return m.knownAs()
}

// running reports whether the machine's node said, in this pass, that it finished booting and is
// ready.
func (m boundMachine) running() bool {
	return m.node != nil && m.node.status != nil && m.node.status.Stage == talos.StageRunning && m.node.status.Ready
}

// system is what the node runs, as "<version> <schematic>", and whether that was read.
func (m boundMachine) system() (string, bool) {
	if m.node == nil || m.node.version == "" || m.node.schematicID == nil {
		return "", false
	}

	return m.node.version + " " + *m.node.schematicID, true
}

// installed reports whether the node's version and schematic were read, and whether they are the
// ones the spec asks for. A node whose firmware was not asked about Secure Boot is not known
// either: what it would boot decides whether it may be upgraded at all.
func (m boundMachine) installed() (known, yes bool) {
	if m.node == nil || m.node.version == "" || m.node.schematicID == nil || m.node.secureBoot == nil || m.install.Image == "" {
		return false, false
	}

	return true, m.node.version == m.install.Version && *m.node.schematicID == m.install.SchematicID
}

// machineView is a machine with what this pass worked out of its config.
type machineView struct {
	boundMachine
	// live is the node's normalized active config; nil if it could not be read.
	live *render.Config
	// rendered is what the spec renders now, and desired what a config rollout is to bring the
	// node to: the same, but for the images of the Kubernetes components, which stay as the
	// node has them (withLiveKubernetes). Both are nil if the spec does not render, and problem
	// says why.
	rendered, desired *render.Config
	problem           string
	// withhold is set when a config source is a Secret: what Talos says about the config may
	// quote it, so it is not repeated.
	withhold bool
	change   classify.Change
}

// view is a machine with its config, worked out once in a pass. A nil machine has an empty view:
// no node, nothing rendered.
func (f *fleet) view(ctx context.Context, tm *talosv1alpha1.TalosMachine) (machineView, error) {
	if tm == nil {
		return machineView{}, nil
	}

	if v, done := f.views[tm.Name]; done {
		return v, nil
	}

	v, err := f.render(ctx, tm)
	if err != nil {
		return v, err
	}

	f.views[tm.Name] = v

	return v, nil
}

func (f *fleet) render(ctx context.Context, tm *talosv1alpha1.TalosMachine) (machineView, error) {
	v := machineView{boundMachine: f.of(tm)}
	if v.pool == nil {
		v.problem = v.installProblem

		return v, nil
	}

	if v.node != nil {
		if live, err := normalizedLive(*v.node); err == nil {
			v.live = live
		}
	}

	desired, resolved, err := renderMachine(ctx, f.reader, f.tc, v.pool, tm)
	if err != nil {
		var problem *configProblem
		if !errors.As(err, &problem) {
			return v, err
		}

		v.problem = problem.msg

		return v, nil
	}

	v.rendered, v.desired = desired, desired
	v.withhold = len(resolved.fromSecrets) > 0

	if v.live == nil {
		return v, nil
	}

	if v.desired, _, err = withLiveKubernetes(desired, v.node.activeConfig); err != nil {
		v.rendered, v.desired, v.problem = nil, nil, "keep the node's Kubernetes images: "+err.Error()

		return v, nil //nolint:nilerr // a problem of this machine's config, which the planner is told as such
	}

	_, installed := v.installed()

	if v.change, err = changeFrom(v.live, v.desired, installed); err != nil {
		v.rendered, v.desired, v.problem = nil, nil, "classify the change: "+err.Error()
	}

	return v, nil
}

// changeFrom classifies the change from the config a node runs to the one a rollout would bring
// it to. installed says that the node runs what its config's installer installs: naming that
// installer is then a change like any other, and not one that waits for an upgrade.
func changeFrom(live, desired *render.Config, installed bool) (classify.Change, error) {
	var opts []classify.Option
	if installed {
		opts = append(opts, classify.Installed())
	}

	return classify.Classify(live, desired, opts...)
}

// normalizedLive is the node's active config, normalized.
func normalizedLive(n nodeFacts) (*render.Config, error) {
	if len(n.activeConfig) == 0 {
		return nil, errors.New("not read")
	}

	return normalized(n.activeConfig)
}

// normalized is a raw machine config, normalized.
func normalized(raw []byte) (*render.Config, error) {
	parsed, err := talos.ParseMachineConfig(raw)
	if err != nil {
		return nil, err
	}

	return render.Normalize(parsed)
}

// configHash is the normalized hash of a raw machine config.
func configHash(raw []byte) (string, error) {
	cfg, err := normalized(raw)
	if err != nil {
		return "", err
	}

	return cfg.Hash, nil
}
