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

package exporter

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

const (
	namespace = "talos-operator"
	cluster   = "my-cluster"
)

// epoch is when everything in a fixture happened: the moment a test of the alert rules starts.
var epoch = metav1.NewTime(time.Unix(0, 0))

func condition(kind string, status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: kind, Status: status, Reason: reason, Message: "a message that is not exported", LastTransitionTime: epoch}
}

// fixture is a cluster of three control planes and a worker, managed, in sync and healthy.
type fixture struct {
	cluster  *talosv1alpha1.TalosCluster
	pools    []*talosv1alpha1.TalosMachinePool
	machines []*talosv1alpha1.TalosMachine
}

func healthy() *fixture {
	f := &fixture{
		cluster: &talosv1alpha1.TalosCluster{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: cluster},
			Spec: talosv1alpha1.TalosClusterSpec{
				Versions: &talosv1alpha1.Versions{Talos: "v1.14.2", Kubernetes: "1.37.1"},
			},
			Status: talosv1alpha1.TalosClusterStatus{
				Phase: talosv1alpha1.ClusterPhaseManaged,
				Conditions: []metav1.Condition{
					condition(talosv1alpha1.ConditionCredentialsValid, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded),
					condition(talosv1alpha1.ConditionTalosAPIReachable, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded),
					condition(talosv1alpha1.ConditionHealthy, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded),
					condition(talosv1alpha1.ConditionConfigInSync, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded),
					condition(talosv1alpha1.ConditionVersionsReachable, metav1.ConditionTrue, talosv1alpha1.ReasonUpToDate),
				},
				Observed: &talosv1alpha1.ObservedState{
					EtcdMembers: []talosv1alpha1.EtcdMember{{ID: "a1", Hostname: "node-a"}, {ID: "b2", Hostname: "node-b"}, {ID: "c3", Hostname: "node-c"}},
					ObservedAt:  &epoch,
				},
				Versions: &talosv1alpha1.VersionsStatus{Talos: "v1.14.2", Kubernetes: "1.37.1"},
			},
		},
	}

	for name, role := range map[string]talosv1alpha1.MachineRole{
		"control-planes": talosv1alpha1.MachineRoleControlPlane, "workers": talosv1alpha1.MachineRoleWorker,
	} {
		f.pools = append(f.pools, &talosv1alpha1.TalosMachinePool{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec:       talosv1alpha1.TalosMachinePoolSpec{ClusterRef: corev1.LocalObjectReference{Name: cluster}, Role: role},
		})
	}

	for i, name := range []string{"node-a", "node-b", "node-c", "node-d"} {
		tm := &talosv1alpha1.TalosMachine{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec:       talosv1alpha1.TalosMachineSpec{PoolRef: corev1.LocalObjectReference{Name: "control-planes"}},
			Status: talosv1alpha1.TalosMachineStatus{
				Phase: "Ready",
				Conditions: []metav1.Condition{
					condition(talosv1alpha1.ConditionNodeObserved, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded),
					condition(talosv1alpha1.ConditionConfigMatchesLive, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded),
				},
				Identity: &talosv1alpha1.ObservedIdentity{KubernetesNodeName: name},
				Talos:    &talosv1alpha1.TalosState{Version: "v1.14.2", SchematicID: strings.Repeat("a", 64), Ready: true},
				Kubelet:  &talosv1alpha1.KubeletState{Version: "1.37.1"},
				Etcd:     &talosv1alpha1.EtcdState{MemberID: fmt.Sprintf("%c%d", 'a'+i, i+1)},
			},
		}

		if name == "node-d" {
			tm.Spec.PoolRef.Name = "workers"
			tm.Status.Etcd = nil
		}

		f.machines = append(f.machines, tm)
	}

	return f
}

func (f *fixture) machine(name string) *talosv1alpha1.TalosMachine {
	for _, tm := range f.machines {
		if tm.Name == name {
			return tm
		}
	}

	panic("no machine " + name)
}

// set replaces the condition of the same type.
func set(list *[]metav1.Condition, c metav1.Condition) {
	for i := range *list {
		if (*list)[i].Type == c.Type {
			(*list)[i] = c

			return
		}
	}

	*list = append(*list, c)
}

func (f *fixture) objects() []client.Object {
	out := []client.Object{f.cluster}

	for _, pool := range f.pools {
		out = append(out, pool)
	}

	for _, tm := range f.machines {
		out = append(out, tm)
	}

	return out
}

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := talosv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	return s
}

// sample is one series as the endpoint serves it.
type sample struct {
	name   string
	labels map[string]string
	value  float64
}

// String is the series in the notation of a query, with every label that has a value.
func (s sample) String() string {
	names := make([]string, 0, len(s.labels))
	for name, value := range s.labels {
		if value != "" {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	pairs := make([]string, 0, len(names))
	for _, name := range names {
		pairs = append(pairs, fmt.Sprintf("%s=%q", name, s.labels[name]))
	}

	return s.name + "{" + strings.Join(pairs, ",") + "}"
}

// scrape is everything the collector exports, gathered the way the metrics endpoint does it: a
// collector that exports the same series twice, or one it did not describe, fails here.
func scrape(t *testing.T, c *Collector) []sample {
	t.Helper()

	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(c); err != nil {
		t.Fatal(err)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}

	var out []sample

	for _, family := range families {
		for _, m := range family.GetMetric() {
			s := sample{name: family.GetName(), labels: map[string]string{}}

			for _, pair := range m.GetLabel() {
				s.labels[pair.GetName()] = pair.GetValue()
			}

			switch family.GetType() {
			case dto.MetricType_COUNTER:
				s.value = m.GetCounter().GetValue()
			default:
				s.value = m.GetGauge().GetValue()
			}

			out = append(out, s)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })

	return out
}

// of names a series of the fixture's cluster: its name and its labels in pairs, besides the two
// every series of a cluster carries.
func of(name string, pairs ...string) string {
	s := sample{name: name, labels: map[string]string{"namespace": namespace, "taloscluster": cluster}}

	for i := 0; i+1 < len(pairs); i += 2 {
		s.labels[pairs[i]] = pairs[i+1]
	}

	return s.String()
}

func collect(t *testing.T, objects ...client.Object) map[string]float64 {
	t.Helper()

	reader := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(objects...).Build()

	out := map[string]float64{}
	for _, s := range scrape(t, New(reader)) {
		// The build is the test binary's, and not something to compare.
		if s.name == "talos_operator_info" {
			s.labels = nil
		}

		out[s.String()] = s.value
	}

	return out
}

func TestCollectorExportsWhatStatusSays(t *testing.T) {
	f := healthy()
	f.machine("node-b").Spec.Hold = true
	f.machine("node-c").Status.Etcd.IsLearner = true
	f.machine("node-d").Status.Config = &talosv1alpha1.ConfigState{ChangeClass: "LiveSafe"}
	f.cluster.Status.Observed.EtcdMembers[2].IsLearner = true
	f.cluster.Status.Versions.Plan = []talosv1alpha1.VersionStep{{Kind: "Kubernetes", From: "1.36.5", To: "1.37.1"}}
	f.cluster.Status.Versions.Kubernetes = "1.36.5"

	got := collect(t, f.objects()...)

	schematic := strings.Repeat("a", 64)

	for series, want := range map[string]float64{
		"talos_operator_info{}":                    1,
		"talos_operator_state_read_errors_total{}": 0,

		of("talos_operator_cluster_phase", "phase", "Managed"):                                                         1,
		of("talos_operator_cluster_operation_in_progress"):                                                             0,
		of("talos_operator_cluster_operation_halted"):                                                                  0,
		of("talos_operator_cluster_approval_pending"):                                                                  0,
		of("talos_operator_cluster_observed_timestamp_seconds"):                                                        0,
		of("talos_operator_cluster_version_plan_steps"):                                                                1,
		of("talos_operator_cluster_etcd_members", "role", "voter"):                                                     2,
		of("talos_operator_cluster_etcd_members", "role", "learner"):                                                   1,
		of("talos_operator_cluster_version_info", "component", "talos", "desired", "v1.14.2", "running", "v1.14.2"):    1,
		of("talos_operator_cluster_version_info", "component", "kubernetes", "desired", "1.37.1", "running", "1.36.5"): 1,
		of("talos_operator_cluster_condition", "condition", "Healthy", "status", "True", "reason", "Succeeded"):        1,

		of("talos_operator_machine_info", "machine", "node-a", "pool", "control-planes", "role", "ControlPlane", "node", "node-a",
			"talos_version", "v1.14.2", "kubelet_version", "1.37.1", "schematic", schematic): 1,
		of("talos_operator_machine_info", "machine", "node-d", "pool", "workers", "role", "Worker", "node", "node-d",
			"talos_version", "v1.14.2", "kubelet_version", "1.37.1", "schematic", schematic): 1,
		of("talos_operator_machine_phase", "machine", "node-a", "phase", "Ready"):                                                         1,
		of("talos_operator_machine_ready", "machine", "node-a"):                                                                           1,
		of("talos_operator_machine_hold", "machine", "node-a"):                                                                            0,
		of("talos_operator_machine_hold", "machine", "node-b"):                                                                            1,
		of("talos_operator_machine_etcd_member", "machine", "node-a", "role", "voter"):                                                    1,
		of("talos_operator_machine_etcd_member", "machine", "node-a", "role", "learner"):                                                  0,
		of("talos_operator_machine_etcd_member", "machine", "node-c", "role", "voter"):                                                    0,
		of("talos_operator_machine_etcd_member", "machine", "node-c", "role", "learner"):                                                  1,
		of("talos_operator_machine_config_change_info", "machine", "node-d", "class", "LiveSafe"):                                         1,
		of("talos_operator_machine_condition", "machine", "node-a", "condition", "NodeObserved", "status", "True", "reason", "Succeeded"): 1,
	} {
		if value, ok := got[series]; !ok || value != want {
			t.Errorf("%s: got %v (there: %t), want %v", series, value, ok, want)
		}
	}

	for series := range got {
		// A worker is no member of etcd, and nothing says so either way. A message is prose.
		if strings.HasPrefix(series, `talos_operator_machine_etcd_member{machine="node-d"`) || strings.Contains(series, "message") {
			t.Errorf("exported: %s", series)
		}

		// Nothing is in flight, so nothing is said about an operation.
		if strings.HasPrefix(series, "talos_operator_operation_") {
			t.Errorf("exported without an operation: %s", series)
		}
	}
}

func TestCollectorExportsTheOperation(t *testing.T) {
	f := healthy()
	f.cluster.Status.Approval = &talosv1alpha1.PendingApproval{Hash: "abc"}
	f.cluster.Status.Operation = &talosv1alpha1.Operation{
		ID: "op-1", Type: talosv1alpha1.OperationTalosUpgrade, Target: "v1.14.2", StartedAt: metav1.NewTime(time.Unix(120, 0)),
		Plan: []string{"node-d", "node-a", "node-b", "node-c"}, Completed: []string{"node-d"},
	}

	const kind = "TalosUpgrade"

	running := map[string]float64{
		of("talos_operator_cluster_operation_in_progress"):                       1,
		of("talos_operator_cluster_operation_halted"):                            0,
		of("talos_operator_cluster_approval_pending"):                            1,
		of("talos_operator_operation_info", "type", kind, "target", "v1.14.2"):   1,
		of("talos_operator_operation_start_timestamp_seconds", "type", kind):     120,
		of("talos_operator_operation_turns", "type", kind, "state", "planned"):   4,
		of("talos_operator_operation_turns", "type", kind, "state", "completed"): 1,
		of("talos_operator_operation_turns", "type", kind, "state", "skipped"):   0,
	}

	got := collect(t, f.objects()...)

	for series, want := range running {
		if value, ok := got[series]; !ok || value != want {
			t.Errorf("%s: got %v (there: %t), want %v", series, value, ok, want)
		}
	}

	for series := range got {
		if strings.Contains(series, "operation_failure") {
			t.Errorf("exported for an operation that has not failed: %s", series)
		}
	}

	at := metav1.NewTime(time.Unix(300, 0))
	f.cluster.Status.Operation.Failure = &talosv1alpha1.OperationFailure{
		Machine: "node-a", Step: "Booting", Reason: "NodeUnreachable", Message: "not exported", At: &at,
	}

	got = collect(t, f.objects()...)

	for series, want := range map[string]float64{
		of("talos_operator_cluster_operation_halted"): 1,
		of("talos_operator_operation_failure_info", "type", kind, "machine", "node-a", "step", "Booting", "reason", "NodeUnreachable"): 1,
		of("talos_operator_operation_failure_timestamp_seconds", "type", kind):                                                         300,
	} {
		if value, ok := got[series]; !ok || value != want {
			t.Errorf("%s: got %v (there: %t), want %v", series, value, ok, want)
		}
	}
}

// A machine whose pool is gone belongs to no cluster that could be named, and a resource that is
// deleted takes its series with it: nothing is remembered between two scrapes.
func TestCollectorKeepsNothing(t *testing.T) {
	f := healthy()
	f.machine("node-d").Spec.PoolRef.Name = "gone"

	reader := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(f.objects()...).Build()
	c := New(reader)

	count := func(substr string) int {
		n := 0

		for _, s := range scrape(t, c) {
			if strings.Contains(s.String(), substr) {
				n++
			}
		}

		return n
	}

	if count(`machine="node-d"`) != 0 || count(`machine="node-a"`) == 0 {
		t.Errorf("node-d has %d series and node-a %d", count(`machine="node-d"`), count(`machine="node-a"`))
	}

	if err := reader.Delete(context.Background(), f.machine("node-a")); err != nil {
		t.Fatal(err)
	}

	if err := reader.Delete(context.Background(), f.cluster); err != nil {
		t.Fatal(err)
	}

	if count(`machine="node-a"`) != 0 || count("talos_operator_cluster_") != 0 || count(`machine="node-b"`) == 0 {
		t.Errorf("after deleting: node-a has %d series, the cluster %d, node-b %d",
			count(`machine="node-a"`), count("talos_operator_cluster_"), count(`machine="node-b"`))
	}
}

// failing is a reader that cannot read one kind.
type failing struct {
	client.Reader
	kind string
}

func (f failing) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if fmt.Sprintf("%T", list) == f.kind {
		return errors.New("the cache is not there")
	}

	return f.Reader.List(ctx, list, opts...)
}

// Half of a state would look like a state: a scrape that cannot read everything exports nothing
// about the resources, and counts.
func TestCollectorSaysWhenItCannotRead(t *testing.T) {
	f := healthy()
	reader := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(f.objects()...).Build()

	for _, kind := range []string{"*v1alpha1.TalosClusterList", "*v1alpha1.TalosMachinePoolList", "*v1alpha1.TalosMachineList"} {
		c := New(failing{Reader: reader, kind: kind})

		for scrapes := 1; scrapes <= 2; scrapes++ {
			var names []string

			for _, s := range scrape(t, c) {
				names = append(names, s.name)

				if s.name == "talos_operator_state_read_errors_total" && s.value != float64(scrapes) {
					t.Errorf("%s: %v errors after %d scrapes", kind, s.value, scrapes)
				}
			}

			if !slices.Equal(names, []string{"talos_operator_info", "talos_operator_state_read_errors_total"}) {
				t.Errorf("%s: exported %v", kind, names)
			}
		}
	}
}
