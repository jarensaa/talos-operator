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
	"net/netip"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
	"github.com/jarensaa/talos-operator/internal/target"
)

// What the gates in internal/plan judge is derived from what a pass read. Nothing that was not
// read may arrive there looking like an answer.
func TestFactsReachTheGatesWithUnknownsIntact(t *testing.T) {
	settled := true
	schematicID := schematic

	facts := clusterFacts{
		credentialsAdmin: true,
		nodes: []nodeFacts{
			{
				hostname: "cp-read", machineType: machineTypeControlPlane, version: "v1.14.2",
				status: &talos.MachineStatus{Stage: stageRunning, Ready: true}, schematicID: &schematicID,
				configSettled: &settled, etcdStatusRead: true, etcdErrors: []string{"alarm: NOSPACE"}, read: true,
			},
			{hostname: "cp-dark", machineType: machineTypeControlPlane, readErr: "timeout"},
			{hostname: "worker-old", machineType: machineTypeWorker, version: "v1.12.4"},
		},
		etcdMembers: []talos.EtcdMember{{ID: 1, Hostname: "cp-read", IsLearner: true}},
	}

	got := facts.judged()

	read, dark, old := got.Members[0], got.Members[1], got.Members[2]

	if !got.CredentialsAdmin || len(got.Members) != 3 || len(got.EtcdMembers) != 1 || !got.EtcdMembers[0].Learner {
		t.Fatalf("cluster: %+v", got)
	}

	if !read.ControlPlane || read.Status == nil || !read.Status.Ready || read.VersionProblem != "" || !read.EtcdStatusRead ||
		len(read.EtcdErrors) != 1 || read.ConfigSettled == nil || read.Schematic == nil {
		t.Fatalf("a node read in full: %+v", read)
	}

	if dark.Status != nil || dark.Version != "" || dark.Schematic != nil || dark.ConfigSettled != nil || dark.EtcdStatusRead ||
		dark.ReadErr != "timeout" {
		t.Fatalf("a node that did not answer must be unknown in every respect: %+v", dark)
	}

	if old.ControlPlane || old.VersionProblem == "" {
		t.Fatalf("an unsupported Talos version must arrive with the reason: %+v", old)
	}

	// An unread member list is not an empty one.
	facts.etcdMembers = nil
	if facts.judged().EtcdMembers != nil {
		t.Fatal("an etcd member list that was not read must stay nil")
	}

	ps := plan.Preconditions(got)
	if s, _ := plan.PreconditionsMet(ps); s != metav1.ConditionFalse {
		t.Fatalf("this cluster meets nothing much: %s", s)
	}
}

func TestPassOfReportsWhatThePassCouldNotDetermine(t *testing.T) {
	st := &talosv1alpha1.TalosClusterStatus{}

	blind := passOf(st, observation{healthErrorMsg: "deadline exceeded"})
	if blind.Reached || blind.Matched || blind.HealthChecked || blind.Preconditions != nil || blind.HealthMessage != "deadline exceeded" {
		t.Fatalf("a pass that reached nothing: %+v", blind)
	}

	if plan.ClusterGate(blind, plan.StartPreconditions).Open {
		t.Fatal("a pass that reached nothing must not open the gate")
	}

	st.Adoption = &talosv1alpha1.AdoptionStatus{Preconditions: []talosv1alpha1.Precondition{{Name: plan.PreconditionEtcdHealthy}}}

	full := passOf(st, observation{
		facts:     &clusterFacts{},
		unmatched: []string{},
		health:    &talos.HealthResult{Healthy: true, Messages: []string{"all checks passed"}},
	})
	if !full.Reached || !full.Matched || !full.HealthChecked || !full.Healthy || full.HealthMessage != "" || len(full.Preconditions) != 1 {
		t.Fatalf("a pass that read everything: %+v", full)
	}

	troubled := passOf(st, observation{
		facts:     &clusterFacts{},
		unmatched: []string{"stray"},
		err:       errors.New("cp-2 (10.5.0.3): timeout"),
		kubeErr:   errors.New("list nodes: timeout"),
		health:    &talos.HealthResult{Messages: []string{"waiting", etcdUnhealthy}},
	})
	if troubled.Incomplete == "" || troubled.KubernetesUnread == "" || len(troubled.Unmatched) != 1 || troubled.Healthy ||
		troubled.HealthMessage != etcdUnhealthy {
		t.Fatalf("a pass over a cluster in trouble: %+v", troubled)
	}
}

func TestStepNodeConcludesNothingFromAPartialView(t *testing.T) {
	raw := []byte("version: v1alpha1\nmachine:\n  type: worker\ncluster: {}\n")

	hash, err := configHash(raw)
	if err != nil {
		t.Fatal(err)
	}

	live, err := normalizedLive(nodeFacts{activeConfig: raw})
	if err != nil {
		t.Fatal(err)
	}

	full := machineView{node: &nodeFacts{activeConfig: raw, persistentConfig: raw}, live: live}
	if got := stepNode(full); !got.Reachable || got.ActiveHash != hash || got.PersistentHash != hash {
		t.Fatalf("a node read in full: %+v", got)
	}

	fromDisk := machineView{node: &nodeFacts{activeConfig: raw}, live: live}
	if got := stepNode(fromDisk); !got.Reachable || got.PersistentHash != "" {
		t.Fatalf("a node booted from disk has no persistent config: %+v", got)
	}

	for name, v := range map[string]machineView{
		"no node with the machine's identity": {live: live},
		"active config not read":              {node: &nodeFacts{}},
		"persistent config unreadable":        {node: &nodeFacts{activeConfig: raw, persistentConfig: []byte("not: [a config")}, live: live},
	} {
		if got := stepNode(v); got != (plan.Node{}) {
			t.Errorf("%s: expected an unreachable node, got %+v", name, got)
		}
	}
}

func TestNodeWithIdentityMatchesOnlyHardwareItCouldIdentify(t *testing.T) {
	facts := &clusterFacts{nodes: []nodeFacts{
		{hostname: "unread"},
		{hostname: "a", systemUUID: "AAAAAAAA-BBBB-4CCC-8DDD-000000000001"},
	}}

	if n := nodeWithIdentity(facts, "aaaaaaaa-bbbb-4ccc-8ddd-000000000001"); n == nil || n.hostname != "a" {
		t.Fatalf("UUIDs match whatever their case: %+v", n)
	}

	for name, n := range map[string]*nodeFacts{
		"another UUID":   nodeWithIdentity(facts, "aaaaaaaa-bbbb-4ccc-8ddd-000000000002"),
		"an empty UUID":  nodeWithIdentity(facts, ""),
		"no observation": nodeWithIdentity(nil, "aaaaaaaa-bbbb-4ccc-8ddd-000000000001"),
	} {
		if n != nil {
			t.Errorf("%s must match no node, least of all one whose identity was not read: %+v", name, n)
		}
	}
}

func TestEveryFailureGetsItsOwnToken(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	seen := map[string]bool{}

	for attempt := range int32(3) {
		for _, at := range []time.Time{now, now.Add(time.Nanosecond)} {
			op := &talosv1alpha1.Operation{
				ID: "op-1", Current: &talosv1alpha1.OperationStep{Machine: "w-1", Step: plan.StepTrying, Attempt: attempt},
			}
			failOperation(op, plan.FailRolledBack, "reverted", at)

			f := op.Failure
			if f.Token == "" || seen[f.Token] {
				t.Fatalf("token %q is empty or was used before", f.Token)
			}

			seen[f.Token] = true

			for _, annotation := range []string{talosv1alpha1.AnnotationRetry, talosv1alpha1.AnnotationSkipMachine, talosv1alpha1.AnnotationAbort} {
				if !strings.Contains(f.Remediation, annotation+"="+f.Token) {
					t.Fatalf("the remediation must spell out %s with the token: %q", annotation, f.Remediation)
				}
			}

			if f.Machine != "w-1" || f.Step != plan.StepTrying || f.Reason != plan.FailRolledBack || !f.At.Time.Equal(at) {
				t.Fatalf("failure: %+v", f)
			}
		}
	}
}

func TestTheNodeBeingChangedIsNeverAnEndpoint(t *testing.T) {
	const ns = "talos-operator"

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	if err := talosv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	first, second := netip.MustParseAddr("10.0.0.11"), netip.MustParseAddr("10.0.0.12")

	tc := &talosv1alpha1.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "happy", Namespace: ns, UID: types.UID("happy-uid")},
		Spec: talosv1alpha1.TalosClusterSpec{Credentials: talosv1alpha1.Credentials{
			Source:    talosv1alpha1.CredentialsSourceSecret,
			SecretRef: &talosv1alpha1.SecretKeyReference{Name: "talosconfig"},
		}},
	}
	tm := &talosv1alpha1.TalosMachine{
		ObjectMeta: metav1.ObjectMeta{Name: cp1Machine, Namespace: ns},
		Spec:       talosv1alpha1.TalosMachineSpec{Address: first.String()},
	}

	build := func(endpoints ...string) (client.Client, *target.Resolver, *fake.Factory) {
		c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(tm, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "talosconfig", Namespace: ns},
			Data:       map[string][]byte{creds.DefaultTalosconfigKey: talostest.AdminTalosconfig(t, endpoints...)},
		}).Build()
		f := &fake.Factory{Scenario: fake.NewScenario()}

		return c, &target.Resolver{Client: c, Factory: f}, f
	}

	ctx := context.Background()
	c, resolver, factory := build(first.String(), second.String())

	if opts := operationEndpoints(ctx, c, tc); opts.Exclude.IsValid() {
		t.Fatalf("nothing is excluded while no node is being changed: %+v", opts)
	}

	tc.Status.Operation = &talosv1alpha1.Operation{Current: &talosv1alpha1.OperationStep{Machine: cp1Machine}}

	opts := operationEndpoints(ctx, c, tc)
	if opts.Exclude != first {
		t.Fatalf("the node being changed must be excluded: %+v", opts)
	}

	if _, used, err := resolveOutside(ctx, resolver, tc, opts); err != nil || used.Exclude != first {
		t.Fatalf("resolve: %v, %+v", err, used)
	}

	if got := factory.Targets()[0].Endpoints; len(got) != 1 || got[0].Addr != second {
		t.Fatalf("the node must be reached through the other control plane: %+v", got)
	}

	// A halted operation keeps its step, and its node stays out: it may well be the broken one.
	tc.Status.Operation.Failure = &talosv1alpha1.OperationFailure{Machine: cp1Machine}
	if opts := operationEndpoints(ctx, c, tc); opts.Exclude != first {
		t.Fatalf("the node of a halted step stays excluded: %+v", opts)
	}

	// With a single control plane there is no other way in.
	_, resolver, factory = build(first.String())

	_, used, err := resolveOutside(ctx, resolver, tc, opts)
	if err != nil || used.Exclude.IsValid() {
		t.Fatalf("a single control plane must be reached through itself: %v, %+v", err, used)
	}

	if got := factory.Targets()[0].Endpoints; len(got) != 1 || got[0].Addr != first {
		t.Fatalf("endpoints: %+v", got)
	}

	// A step for a machine that is gone excludes nothing rather than failing the observation.
	tc.Status.Operation.Current.Machine = "gone"
	if opts := operationEndpoints(ctx, c, tc); opts.Exclude.IsValid() {
		t.Fatalf("an unknown machine excludes nothing: %+v", opts)
	}
}
