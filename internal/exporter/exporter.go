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
//
// One thing is not in status: when the operator last finished observing a cluster. A cluster at
// rest is not written to, so status says only when what was observed last changed. The replica
// that observes, the leader, knows it, and exports it (Observations); every other replica exports
// what status says, which is never later.
package exporter

import (
	"context"
	"runtime/debug"
	"slices"
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

// labels are the labels every series of one resource carries: their names where a metric is
// described, their values where a series is made.
type labels []string

// with returns the labels followed by more, in a slice of its own.
func (l labels) with(more ...string) []string { return slices.Concat(l, more) }

// Labels every series of a cluster, and of a machine, carries.
var (
	clusterLabels = labels{"namespace", "taloscluster"}
	machineLabels = labels{"namespace", "taloscluster", "machine"}
)

// descs is every metric about the resources, in the order they are declared. Describe sends it,
// so a metric cannot be exported without being described.
var descs []*prometheus.Desc

func desc(name, help string, base labels, more ...string) *prometheus.Desc {
	d := prometheus.NewDesc("talos_operator_"+name, help, base.with(more...), nil)
	descs = append(descs, d)

	return d
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
	clusterCertificateExpiryDesc = desc("cluster_certificate_expiry_timestamp_seconds",
		"When a certificate the cluster depends on stops being valid: the operator's own client certificate, or one of "+
			"the cluster's certificate authorities.", clusterLabels, "certificate")
	clusterCertificateIssuedDesc = desc("cluster_certificate_issued_timestamp_seconds",
		"When the validity of a certificate the cluster depends on began.", clusterLabels, "certificate")
	clusterDefragmentedDesc = desc("cluster_etcd_defragmented_timestamp_seconds",
		"When the operator last began to defragment etcd. There only once it has.", clusterLabels)

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
	machineEtcdLeaderDesc = desc("machine_etcd_leader",
		"Whether the control plane's etcd member led etcd when it was last read. There only while it can be read.", machineLabels)
	machineEtcdSizeDesc = desc("machine_etcd_db_size_bytes",
		"The size of the database file of the control plane's etcd member. It follows the file once that has moved by a "+
			"twentieth of its size. There only while it can be read.", machineLabels)
	machineEtcdInUseDesc = desc("machine_etcd_db_size_in_use_bytes",
		"How much of the database file of the control plane's etcd member holds data: the rest is what a defragmentation "+
			"gives back. As coarse as the size.", machineLabels)

	// What Talos itself says about a node. Each is there only while the node can be read.
	machineServiceDesc = desc("machine_service_healthy",
		"Whether a Talos service of the node runs and passes its health check.", machineLabels, "service")
	machineDiagnosticDesc = desc("machine_diagnostic_info",
		"A warning Talos raises about the node's own configuration. Always 1, and there only while Talos raises it.",
		machineLabels, "id")
	machineTimeDesc = desc("machine_time_synced", "Whether the node's clock is synchronised.", machineLabels)
	machineLinkDesc = desc("machine_link_up",
		"Whether a physical network link of the node is up; bond names the bond it is a member of, if it is one.",
		machineLabels, "link", "bond")
	machineLinkSpeedDesc = desc("machine_link_speed_mbps",
		"The speed of a physical network link of the node. There only for a link that reports one.", machineLabels, "link")
	machineVolumeDesc = desc("machine_volume_ready",
		"Whether one of Talos's volumes on the node is ready, with the phase Talos reports for it.",
		machineLabels, "volume", "phase")
)

// volumeReady is the phase of a volume that is in order.
const volumeReady = "ready"

// Roles of an etcd member.
const (
	voter   = "voter"
	learner = "learner"
)

// Observations says when this replica last finished observing a cluster, if it did.
type Observations interface {
	LastObserved(cluster types.NamespacedName) (time.Time, bool)
}

// Collector exports the state of every TalosCluster and TalosMachine a reader has.
type Collector struct {
	reader       client.Reader
	observations Observations
	readErrors   prometheus.Counter
}

var _ prometheus.Collector = (*Collector)(nil)

// New returns a collector that reads from reader at every scrape: the manager's cache. It asks
// observations when a cluster was last observed; nil means this replica never observes one.
func New(reader client.Reader, observations Observations) *Collector {
	return &Collector{
		reader:       reader,
		observations: observations,
		readErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "talos_operator_state_read_errors_total",
			Help: "Scrapes at which the operator could not read its resources, and so exported nothing about them.",
		}),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	c.readErrors.Describe(ch)

	for _, d := range descs {
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
		out = append(out, c.clusterMetrics(&clusters.Items[i])...)
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

func (c *Collector) clusterMetrics(tc *talosv1alpha1.TalosCluster) []prometheus.Metric {
	st := &tc.Status
	base := labels{tc.Namespace, tc.Name}

	out := []prometheus.Metric{
		gauge(clusterOperationDesc, truth(st.Operation != nil), base...),
		gauge(clusterHaltedDesc, truth(st.Operation != nil && st.Operation.Failure != nil), base...),
		gauge(clusterApprovalDesc, truth(st.Approval != nil), base...),
	}

	if st.Phase != "" {
		out = append(out, gauge(clusterPhaseDesc, 1, base.with(string(st.Phase))...))
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
		gauge(clusterVersionDesc, 1, base.with("talos", desired.Talos, running.Talos)...),
		gauge(clusterVersionDesc, 1, base.with("kubernetes", desired.Kubernetes, running.Kubernetes)...),
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
			gauge(clusterEtcdDesc, float64(len(obs.EtcdMembers)-learners), base.with(voter)...),
			gauge(clusterEtcdDesc, float64(learners), base.with(learner)...),
		)
	}

	if at, ok := c.lastObserved(tc); ok {
		out = append(out, gauge(clusterObservedDesc, float64(at.Unix()), base...))
	}

	for _, cert := range st.Certificates {
		out = append(out,
			gauge(clusterCertificateExpiryDesc, seconds(cert.ExpiresAt), base.with(cert.Name)...),
			gauge(clusterCertificateIssuedDesc, seconds(cert.IssuedAt), base.with(cert.Name)...),
		)
	}

	if done := st.Maintenance; done != nil && done.EtcdDefragmentedAt != nil {
		out = append(out, gauge(clusterDefragmentedDesc, seconds(*done.EtcdDefragmentedAt), base...))
	}

	if op := st.Operation; op != nil {
		kind := string(op.Type)

		out = append(out,
			gauge(operationInfoDesc, 1, base.with(kind, op.Target)...),
			gauge(operationStartDesc, seconds(op.StartedAt), base.with(kind)...),
			gauge(operationTurnsDesc, float64(len(op.Plan)), base.with(kind, "planned")...),
			gauge(operationTurnsDesc, float64(len(op.Completed)), base.with(kind, "completed")...),
			gauge(operationTurnsDesc, float64(len(op.Skipped)), base.with(kind, "skipped")...),
		)

		if failure := op.Failure; failure != nil {
			out = append(out, gauge(operationFailureDesc, 1, base.with(kind, failure.Machine, failure.Step, failure.Reason, failure.Hook)...))

			if failure.At != nil {
				out = append(out, gauge(operationFailureTimeDesc, seconds(*failure.At), base.with(kind)...))
			}
		}
	}

	return out
}

func machineMetrics(tm *talosv1alpha1.TalosMachine, pool *talosv1alpha1.TalosMachinePool) []prometheus.Metric {
	st := &tm.Status
	base := labels{tm.Namespace, pool.Spec.ClusterRef.Name, tm.Name}

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
		gauge(machineInfoDesc, 1, base.with(pool.Name, string(pool.Spec.Role), node, talosVersion, kubelet, schematic)...),
		gauge(machineReadyDesc, truth(ready), base...),
		gauge(machineHoldDesc, truth(tm.Spec.Hold), base...),
	}

	if st.Phase != "" {
		out = append(out, gauge(machinePhaseDesc, 1, base.with(string(st.Phase))...))
	}

	out = append(out, conditions(machineConditionDesc, base, st.Conditions)...)

	if st.Config != nil && st.Config.ChangeClass != "" {
		out = append(out, gauge(machineChangeDesc, 1, base.with(st.Config.ChangeClass)...))
	}

	// A machine that has not joined, or has left, is no control plane of the cluster yet or any
	// more: it has no place in etcd to be missing from.
	if pool.Spec.Role == talosv1alpha1.MachineRoleControlPlane && tm.IsMember() {
		member := st.Etcd != nil && st.Etcd.MemberID != ""
		isLearner := member && st.Etcd.IsLearner

		out = append(out,
			gauge(machineEtcdDesc, truth(member && !isLearner), base.with(voter)...),
			gauge(machineEtcdDesc, truth(isLearner), base.with(learner)...),
		)

		// A size of zero is a member that was not read: an etcd database is never empty.
		if member && st.Etcd.DBSize > 0 {
			out = append(out,
				gauge(machineEtcdLeaderDesc, truth(st.Etcd.Leader), base...),
				gauge(machineEtcdSizeDesc, float64(st.Etcd.DBSize), base...),
				gauge(machineEtcdInUseDesc, float64(st.Etcd.DBSizeInUse), base...),
			)
		}
	}

	return append(out, healthMetrics(st.Health, base)...)
}

// healthMetrics is what Talos said about the node when it was last read. Nothing of it is
// exported for a node that was not read: what is not known is not said to be fine, or broken.
func healthMetrics(health *talosv1alpha1.NodeHealth, base labels) []prometheus.Metric {
	if health == nil {
		return nil
	}

	var out []prometheus.Metric

	for _, service := range health.Services {
		out = append(out, gauge(machineServiceDesc, truth(service.Running && service.Healthy), base.with(service.Name)...))
	}

	for _, diagnostic := range health.Diagnostics {
		out = append(out, gauge(machineDiagnosticDesc, 1, base.with(diagnostic.ID)...))
	}

	if health.TimeSynced != nil {
		out = append(out, gauge(machineTimeDesc, truth(*health.TimeSynced), base...))
	}

	for _, link := range health.Links {
		out = append(out, gauge(machineLinkDesc, truth(link.Up), base.with(link.Name, link.Bond)...))

		if link.SpeedMbps > 0 {
			out = append(out, gauge(machineLinkSpeedDesc, float64(link.SpeedMbps), base.with(link.Name)...))
		}
	}

	for _, volume := range health.Volumes {
		out = append(out, gauge(machineVolumeDesc, truth(volume.Phase == volumeReady), base.with(volume.Name, volume.Phase)...))
	}

	return out
}

// conditions is one series for each condition, labelled with what it says now. The message is
// left out: it is prose, it changes, and it can quote what a node said.
func conditions(d *prometheus.Desc, base labels, list []metav1.Condition) []prometheus.Metric {
	out := make([]prometheus.Metric, 0, len(list))

	for _, cond := range list {
		out = append(out, gauge(d, 1, base.with(cond.Type, string(cond.Status), cond.Reason)...))
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

// lastObserved is when the cluster was last observed: when this replica last finished observing
// it, and never before status last recorded an observation. A replica that does not observe
// exports what status says, so the alert has an age to judge whatever replica is scraped, and
// one that grows while nobody observes.
func (c *Collector) lastObserved(tc *talosv1alpha1.TalosCluster) (time.Time, bool) {
	var at time.Time

	if obs := tc.Status.Observed; obs != nil && obs.ObservedAt != nil {
		at = obs.ObservedAt.Time
	}

	if c.observations != nil {
		if mine, ok := c.observations.LastObserved(client.ObjectKeyFromObject(tc)); ok && mine.After(at) {
			at = mine
		}
	}

	return at, !at.IsZero()
}

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
