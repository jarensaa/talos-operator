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

// Package exporter turns what the operator has written into the status of its resources into
// metrics: an operation in flight or halted, drift, nodes that cannot be read, etcd membership.
//
// It reads the resources when it is scraped and keeps nothing itself. Every replica therefore
// exports the same, the leader or not, a resource that is deleted takes its series with it, and
// nothing here can disagree with the status a person reads. It asks no node anything.
package exporter

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// readTimeout bounds the read behind one scrape. The reads are of a cache, so it only matters
// before the cache has filled.
const readTimeout = 5 * time.Second

// Labels every series of a cluster, and of a machine, carries.
var (
	clusterLabels = []string{"namespace", "taloscluster"}
	machineLabels = []string{"namespace", "taloscluster", "machine"}
)

func desc(name, help string, base []string, more ...string) *prometheus.Desc {
	return prometheus.NewDesc("talos_operator_"+name, help, append(append([]string{}, base...), more...), nil)
}

var (
	infoDesc = desc("info", "The operator that is running. Always 1; no series means no operator is scraped.",
		nil, "version", "revision")

	clusterPhaseDesc = desc("cluster_phase", "Where a TalosCluster is in its life. 1 for the phase it is in.",
		clusterLabels, "phase")
	clusterConditionDesc = desc("cluster_condition",
		"A condition of a TalosCluster, with the status and reason it has now. Always 1.",
		clusterLabels, "condition", "status", "reason")
	clusterObservedDesc = desc("cluster_observed_timestamp_seconds",
		"When the operator last finished observing the cluster.", clusterLabels)
	clusterVersionDesc = desc("cluster_version_info",
		"The version of Talos or Kubernetes a cluster's spec asks for, and the lowest one its nodes run. Always 1.",
		clusterLabels, "component", "desired", "running")
	clusterPlanDesc = desc("cluster_version_plan_steps",
		"Upgrades between the versions the cluster runs and the ones its spec asks for.", clusterLabels)
	clusterEtcdDesc      = desc("cluster_etcd_members", "Members of etcd, as last observed.", clusterLabels, "role")
	clusterOperationDesc = desc("cluster_operation_in_progress",
		"Whether an operation holds the cluster: a config rollout or an upgrade, halted or not.", clusterLabels)
	clusterHaltedDesc = desc("cluster_operation_halted",
		"Whether the cluster's operation has failed and waits for a person.", clusterLabels)
	clusterApprovalDesc = desc("cluster_approval_pending",
		"Whether a change waits for a person's approval.", clusterLabels)

	operationInfoDesc = desc("operation_info", "The operation that holds the cluster. Always 1.",
		clusterLabels, "type", "target")
	operationStartDesc = desc("operation_start_timestamp_seconds", "When the operation began.", clusterLabels, "type")
	operationTurnsDesc = desc("operation_turns",
		"Turns of the operation: all that are planned, and those completed or skipped so far.",
		clusterLabels, "type", "state")
	operationFailureDesc = desc("operation_failure_info",
		"The failure an operation is halted at. Always 1, and there only while it is halted.",
		clusterLabels, "type", "machine", "step", "reason", "hook")
	operationFailureTimeDesc = desc("operation_failure_timestamp_seconds",
		"When the failure an operation is halted at was recorded.", clusterLabels, "type")

	machineInfoDesc = desc("machine_info", "A TalosMachine and what its node runs. Always 1.",
		machineLabels, "pool", "role", "node", "talos_version", "kubelet_version", "schematic")
	machinePhaseDesc = desc("machine_phase", "Where a TalosMachine is in its life. 1 for the phase it is in.",
		machineLabels, "phase")
	machineConditionDesc = desc("machine_condition",
		"A condition of a TalosMachine, with the status and reason it has now. Always 1.",
		machineLabels, "condition", "status", "reason")
	machineReadyDesc = desc("machine_ready",
		"Whether the node reported itself ready when it was last read. 0 too while it cannot be read.", machineLabels)
	machineHoldDesc   = desc("machine_hold", "Whether the machine is on hold, and so left out of rollouts.", machineLabels)
	machineChangeDesc = desc("machine_config_change_info",
		"The class of the change between the config a node runs and the one rendered for it. There only while they differ.",
		machineLabels, "class")
	machineEtcdDesc = desc("machine_etcd_member",
		"Whether a control plane is a member of etcd, as a voter or a learner. 0 for both while it is neither.",
		machineLabels, "role")
)

// Roles of an etcd member.
const (
	voter   = "voter"
	learner = "learner"
)

// Collector exports the state of every TalosCluster and TalosMachine a reader has.
type Collector struct {
	reader     client.Reader
	readErrors prometheus.Counter
}

var _ prometheus.Collector = (*Collector)(nil)

// New returns a collector that reads from reader at every scrape: the manager's cache.
func New(reader client.Reader) *Collector {
	return &Collector{
		reader: reader,
		readErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "talos_operator_state_read_errors_total",
			Help: "Scrapes at which the operator could not read its resources, and so exported nothing about them.",
		}),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	c.readErrors.Describe(ch)

	for _, d := range []*prometheus.Desc{
		infoDesc,
		clusterPhaseDesc, clusterConditionDesc, clusterObservedDesc, clusterVersionDesc, clusterPlanDesc, clusterEtcdDesc,
		clusterOperationDesc, clusterHaltedDesc, clusterApprovalDesc,
		operationInfoDesc, operationStartDesc, operationTurnsDesc, operationFailureDesc, operationFailureTimeDesc,
		machineInfoDesc, machinePhaseDesc, machineConditionDesc, machineReadyDesc, machineHoldDesc, machineChangeDesc,
		machineEtcdDesc,
	} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	version, revision := build()
	ch <- gauge(infoDesc, 1, version, revision)

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	// A scrape that cannot read exports nothing about the resources, and says so: half of a
	// state would look like a state.
	metrics, err := c.state(ctx)
	if err != nil {
		c.readErrors.Inc()

		metrics = nil
	}

	c.readErrors.Collect(ch)

	for _, m := range metrics {
		ch <- m
	}
}

func (c *Collector) state(ctx context.Context) ([]prometheus.Metric, error) {
	clusters := &talosv1alpha1.TalosClusterList{}
	if err := c.reader.List(ctx, clusters); err != nil {
		return nil, err
	}

	pools := &talosv1alpha1.TalosMachinePoolList{}
	if err := c.reader.List(ctx, pools); err != nil {
		return nil, err
	}

	machines := &talosv1alpha1.TalosMachineList{}
	if err := c.reader.List(ctx, machines); err != nil {
		return nil, err
	}

	var out []prometheus.Metric

	for i := range clusters.Items {
		out = append(out, clusterMetrics(&clusters.Items[i])...)
	}

	byName := map[types.NamespacedName]*talosv1alpha1.TalosMachinePool{}
	for i := range pools.Items {
		byName[client.ObjectKeyFromObject(&pools.Items[i])] = &pools.Items[i]
	}

	for i := range machines.Items {
		tm := &machines.Items[i]

		// A machine whose pool is not there belongs to no cluster that could be named.
		pool := byName[types.NamespacedName{Namespace: tm.Namespace, Name: tm.Spec.PoolRef.Name}]
		if pool == nil {
			continue
		}

		out = append(out, machineMetrics(tm, pool)...)
	}

	return out, nil
}

func clusterMetrics(tc *talosv1alpha1.TalosCluster) []prometheus.Metric {
	st := &tc.Status
	base := []string{tc.Namespace, tc.Name}
	with := func(more ...string) []string { return append(append([]string{}, base...), more...) }

	out := []prometheus.Metric{
		gauge(clusterOperationDesc, truth(st.Operation != nil), base...),
		gauge(clusterHaltedDesc, truth(st.Operation != nil && st.Operation.Failure != nil), base...),
		gauge(clusterApprovalDesc, truth(st.Approval != nil), base...),
	}

	if st.Phase != "" {
		out = append(out, gauge(clusterPhaseDesc, 1, with(string(st.Phase))...))
	}

	out = append(out, conditions(clusterConditionDesc, base, st.Conditions)...)

	desired := talosv1alpha1.Versions{}
	if tc.Spec.Versions != nil {
		desired = *tc.Spec.Versions
	}

	running := talosv1alpha1.VersionsStatus{}
	if st.Versions != nil {
		running = *st.Versions
	}

	out = append(out,
		gauge(clusterVersionDesc, 1, with("talos", desired.Talos, running.Talos)...),
		gauge(clusterVersionDesc, 1, with("kubernetes", desired.Kubernetes, running.Kubernetes)...),
		gauge(clusterPlanDesc, float64(len(running.Plan)), base...),
	)

	if obs := st.Observed; obs != nil {
		learners := 0

		for _, member := range obs.EtcdMembers {
			if member.IsLearner {
				learners++
			}
		}

		out = append(out,
			gauge(clusterEtcdDesc, float64(len(obs.EtcdMembers)-learners), with(voter)...),
			gauge(clusterEtcdDesc, float64(learners), with(learner)...),
		)

		if obs.ObservedAt != nil {
			out = append(out, gauge(clusterObservedDesc, seconds(*obs.ObservedAt), base...))
		}
	}

	if op := st.Operation; op != nil {
		kind := string(op.Type)

		out = append(out,
			gauge(operationInfoDesc, 1, with(kind, op.Target)...),
			gauge(operationStartDesc, seconds(op.StartedAt), with(kind)...),
			gauge(operationTurnsDesc, float64(len(op.Plan)), with(kind, "planned")...),
			gauge(operationTurnsDesc, float64(len(op.Completed)), with(kind, "completed")...),
			gauge(operationTurnsDesc, float64(len(op.Skipped)), with(kind, "skipped")...),
		)

		if failure := op.Failure; failure != nil {
			out = append(out, gauge(operationFailureDesc, 1, with(kind, failure.Machine, failure.Step, failure.Reason, failure.Hook)...))

			if failure.At != nil {
				out = append(out, gauge(operationFailureTimeDesc, seconds(*failure.At), with(kind)...))
			}
		}
	}

	return out
}

func machineMetrics(tm *talosv1alpha1.TalosMachine, pool *talosv1alpha1.TalosMachinePool) []prometheus.Metric {
	st := &tm.Status
	base := []string{tm.Namespace, pool.Spec.ClusterRef.Name, tm.Name}
	with := func(more ...string) []string { return append(append([]string{}, base...), more...) }

	var node, talosVersion, schematic, kubelet string

	if st.Identity != nil {
		node = st.Identity.KubernetesNodeName
	}

	ready := false

	if st.Talos != nil {
		talosVersion, schematic, ready = st.Talos.Version, st.Talos.SchematicID, st.Talos.Ready
	}

	if st.Kubelet != nil {
		kubelet = st.Kubelet.Version
	}

	out := []prometheus.Metric{
		gauge(machineInfoDesc, 1, with(pool.Name, string(pool.Spec.Role), node, talosVersion, kubelet, schematic)...),
		gauge(machineReadyDesc, truth(ready), base...),
		gauge(machineHoldDesc, truth(tm.Spec.Hold), base...),
	}

	if st.Phase != "" {
		out = append(out, gauge(machinePhaseDesc, 1, with(string(st.Phase))...))
	}

	out = append(out, conditions(machineConditionDesc, base, st.Conditions)...)

	if st.Config != nil && st.Config.ChangeClass != "" {
		out = append(out, gauge(machineChangeDesc, 1, with(st.Config.ChangeClass)...))
	}

	// A machine that has not joined, or has left, is no control plane of the cluster yet or any
	// more: it has no place in etcd to be missing from.
	if pool.Spec.Role == talosv1alpha1.MachineRoleControlPlane && st.JoinedAt != nil && st.RemovedAt == nil {
		member := st.Etcd != nil && st.Etcd.MemberID != ""
		isLearner := member && st.Etcd.IsLearner

		out = append(out,
			gauge(machineEtcdDesc, truth(member && !isLearner), with(voter)...),
			gauge(machineEtcdDesc, truth(isLearner), with(learner)...),
		)
	}

	return out
}

// conditions is one series for each condition, labelled with what it says now. The message is
// left out: it is prose, it changes, and it can quote what a node said.
func conditions(d *prometheus.Desc, base []string, list []metav1.Condition) []prometheus.Metric {
	out := make([]prometheus.Metric, 0, len(list))

	for _, cond := range list {
		labels := append(append([]string{}, base...), cond.Type, string(cond.Status), cond.Reason)
		out = append(out, gauge(d, 1, labels...))
	}

	return out
}

func gauge(d *prometheus.Desc, value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(d, prometheus.GaugeValue, value, labels...)
}

func truth(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

func seconds(t metav1.Time) float64 { return float64(t.Unix()) }

// build is the version and the commit the binary was built from, as far as the build recorded
// them.
func build() (version, revision string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", ""
	}

	version = info.Main.Version

	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			revision = setting.Value
		}
	}

	if len(revision) > 12 {
		revision = revision[:12]
	}

	return version, revision
}
