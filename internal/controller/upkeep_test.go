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
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
	"github.com/jarensaa/talos-operator/internal/target"
)

const mebibyte = int64(1) << 20

// Status follows the size of an etcd database at a distance, or it would be written in every pass.
func TestFollowedSize(t *testing.T) {
	const total = 100 * mebibyte

	for _, tt := range []struct {
		name               string
		reported, now, out int64
	}{
		{"nothing reported yet", 0, 40 * mebibyte, 40 * mebibyte},
		{"not read", 40 * mebibyte, 0, 0},
		{"a change that every write makes", 40 * mebibyte, 41 * mebibyte, 40 * mebibyte},
		{"just short of a twentieth", 40 * mebibyte, 45*mebibyte - 1, 40 * mebibyte},
		{"a twentieth", 40 * mebibyte, 45 * mebibyte, 45 * mebibyte},
		{"what a defragmentation gives back", 90 * mebibyte, 40 * mebibyte, 40 * mebibyte},
	} {
		if got := followedSize(tt.reported, tt.now, total); got != tt.out {
			t.Errorf("%s: got %d, want %d", tt.name, got, tt.out)
		}
	}
}

func TestClipCutsOnARuneBoundary(t *testing.T) {
	if got := clip("häää", 4); got != "hä" {
		t.Fatalf("got %q", got)
	}

	if got := clip("short", 512); got != "short" {
		t.Fatalf("got %q", got)
	}
}

var _ = Describe("Upkeep", func() {
	var (
		ns                string
		scenario          *fake.Scenario
		factory           *fake.Factory
		reconciler        *TalosClusterReconciler
		machineReconciler *TalosMachineReconciler
		tc                *talosv1alpha1.TalosCluster
		clock             time.Time
		machines          []member
	)

	get := func(m member) *talosv1alpha1.TalosMachine {
		GinkgoHelper()

		tm := &talosv1alpha1.TalosMachine{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: m.name}, tm)).To(Succeed())

		return tm
	}

	round := func() {
		GinkgoHelper()

		for _, m := range machines {
			lease := &coordinationv1.Lease{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nodeLeaseNamespace, Name: m.name}, lease)).To(Succeed())

			if !scenario.Down(m.addr) {
				lease.Spec.RenewTime = &metav1.MicroTime{Time: clock}
				Expect(k8sClient.Update(ctx, lease)).To(Succeed())
			}

			_, err := machineReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: m.name}})
			Expect(err).NotTo(HaveOccurred())
		}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
	}

	defragmented := func() []string {
		var out []string

		for _, call := range scenario.CallsFor(fake.MethodEtcdDefragment) {
			for _, m := range machines {
				if m.addr == call.Node {
					out = append(out, m.name)
				}
			}
		}

		return out
	}

	settle := func(stop func() bool) {
		GinkgoHelper()

		// A defragmentation that succeeds moves its step on, so a pass that leaves the cluster as
		// it was did nothing, or tried again what failed before.
		for range 40 {
			version := tc.ResourceVersion

			round()

			if stop != nil && stop() {
				return
			}

			if tc.ResourceVersion == version {
				return
			}
		}

		Fail("the controllers never settled")
	}

	maintained := func() *metav1.Condition {
		cond := condition(tc, talosv1alpha1.ConditionEtcdMaintained)
		if cond == nil {
			return &metav1.Condition{}
		}

		return cond
	}

	runUntil := func(what string, done func() bool) {
		GinkgoHelper()

		for range 120 {
			if done() {
				return
			}

			clock = clock.Add(10 * time.Second)
			settle(done)
		}

		Fail(fmt.Sprintf("never happened: %s; operation: %+v; EtcdMaintained: %+v; RolloutBlocked: %+v", what, tc.Status.Operation,
			maintained(), condition(tc, talosv1alpha1.ConditionRolloutBlocked)))
	}

	updateCluster := func(mutate func(*talosv1alpha1.TalosCluster)) {
		GinkgoHelper()

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
		mutate(tc)
		Expect(k8sClient.Update(ctx, tc)).To(Succeed())
	}

	annotate := func(annotation, value string) {
		GinkgoHelper()

		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			if tc.Annotations == nil {
				tc.Annotations = map[string]string{}
			}

			tc.Annotations[annotation] = value
		})
	}

	// sizes sets the size of a member's database file and what of it holds data, in MiB.
	sizes := func(m member, size, inUse int64) {
		scenario.UpdateNode(m.addr, func(n *fake.Node) { n.EtcdDBSize, n.EtcdDBInUse = size*mebibyte, inUse*mebibyte })
	}

	idle := func() bool { return tc.Status.Operation == nil }

	// rested is the cluster after a defragmentation: the pass after the one that ended it says so.
	rested := func() bool { return idle() && maintained().Status == metav1.ConditionTrue }

	halted := func() bool { return tc.Status.Operation != nil && tc.Status.Operation.Failure != nil }

	// start brings up a cluster of the given machines, adopted, with its config managed and,
	// unless told otherwise, its upkeep enabled.
	start := func(maintenance talosv1alpha1.CapabilityState, members ...member) {
		GinkgoHelper()

		machines = members

		fakes := make([]fake.Node, 0, len(members))

		for _, m := range members {
			machineType := fake.TypeWorker
			if m.controlPlane() {
				machineType = fake.TypeControlPlane
			}

			n := fakeNode(m.addr, machineType, m.name, m.uuid, m.etcd)
			n.Services = []talos.ServiceStatus{{ID: "apid", Running: true, Healthy: true}, {ID: "kubelet", Running: true, Healthy: true}}
			n.Links = []talos.Link{{Name: "eth0", Up: true, SpeedMbps: 1000}}
			n.Volumes = []talos.Volume{{Name: "EPHEMERAL", Phase: talos.VolumeReady}, {Name: "STATE", Phase: talos.VolumeReady}}
			n.Logs = map[string]string{
				"kubelet": "the kubelet of " + m.name + "\n", "machined": "machined\n", "apid": "apid\n", "cri": "cri\n",
				"etcd": "etcd of " + m.name + "\n", "dmesg": "the kernel of " + m.name + "\n",
			}

			if m.controlPlane() {
				n.EtcdDBSize, n.EtcdDBInUse = 200*mebibyte, 150*mebibyte
			}

			fakes = append(fakes, n)
		}

		scenario = fake.NewScenario(fakes...)
		factory = &fake.Factory{Scenario: scenario}

		for _, m := range members {
			kube := kubeNode(m.name)
			kube.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			Expect(k8sClient.Create(ctx, kube)).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, kube)).To(Succeed())

			Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{Name: m.name, Namespace: nodeLeaseNamespace},
				Spec:       coordinationv1.LeaseSpec{RenewTime: &metav1.MicroTime{Time: clock}},
			}))).To(Succeed())
		}

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: credentialsSecret, Namespace: ns},
			Data:       map[string][]byte{creds.DefaultTalosconfigKey: talostest.AdminTalosconfig(GinkgoT(), cpAddr.String())},
		})).To(Succeed())

		tc = &talosv1alpha1.TalosCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns},
			Spec: talosv1alpha1.TalosClusterSpec{
				Credentials: secretCredentials(),
				Rollout: talosv1alpha1.Rollout{
					Soak: &metav1.Duration{Duration: 20 * time.Second}, StepTimeout: &metav1.Duration{Duration: 5 * time.Minute},
				},
			},
		}
		reproduceLiveConfigs(tc)
		Expect(k8sClient.Create(ctx, tc)).To(Succeed())
		createPools(ns)

		evictor, err := NewPodEvictor(cfg)
		Expect(err).NotTo(HaveOccurred())

		reconciler = &TalosClusterReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), APIReader: k8sClient, Evictor: evictor,
			Resolver:         &target.Resolver{Client: k8sClient, Factory: factory},
			Recorder:         events.NewFakeRecorder(4096),
			Now:              func() time.Time { return clock },
			KubernetesChecks: KubernetesCheckerFunc(func(context.Context, string, string) ([]string, error) { return nil, nil }),
		}
		machineReconciler = &TalosMachineReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Resolver: reconciler.Resolver}

		for _, m := range members {
			Expect(k8sClient.Create(ctx, machine(ns, m.name, m.role, m.addr, m.uuid))).To(Succeed())
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
		Expect(err).NotTo(HaveOccurred())

		settle(nil)
		Expect(condition(tc, talosv1alpha1.ConditionConfigProven).Status).To(Equal(metav1.ConditionTrue), "the cluster must adopt cleanly")

		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Management.Config = talosv1alpha1.ConfigManagementManage
			tc.Spec.Management.Maintenance = maintenance
		})
		settle(nil)

		Expect(condition(tc, talosv1alpha1.ConditionConfigHandover).Status).To(Equal(metav1.ConditionTrue))
		Expect(maintained().Status).To(Equal(metav1.ConditionTrue), maintained().Message)
		Expect(defragmented()).To(BeEmpty(), "an etcd that is not fragmented is left alone")
	}

	BeforeEach(func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "upkeep-"}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name

		clock = time.Now().Truncate(time.Second)

		DeferCleanup(func() {
			Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
			Expect(k8sClient.DeleteAllOf(ctx, &coordinationv1.Lease{}, client.InNamespace(nodeLeaseNamespace))).To(Succeed())
		})
	})

	It("reports what Talos says about a node, and nothing of it once the node does not answer", func() {
		start(talosv1alpha1.CapabilityDisabled, cpA, wOne)

		health := get(wOne).Status.Health
		Expect(health).NotTo(BeNil())
		Expect(health.Services).To(ConsistOf(
			talosv1alpha1.ServiceHealth{Name: "apid", Running: true, Healthy: true},
			talosv1alpha1.ServiceHealth{Name: "kubelet", Running: true, Healthy: true},
		))
		Expect(health.TimeSynced).To(HaveValue(BeTrue()))
		Expect(health.Diagnostics).To(BeEmpty())
		Expect(health.Links).To(Equal([]talosv1alpha1.LinkHealth{{Name: "eth0", Up: true, SpeedMbps: 1000}}))
		Expect(health.Volumes).To(HaveLen(2))

		etcd := get(cpA).Status.Etcd
		Expect(etcd.Leader).To(BeTrue())
		Expect(etcd.DBSize).To(Equal(200 * mebibyte))
		Expect(etcd.DBSizeInUse).To(Equal(150 * mebibyte))
		Expect(get(wOne).Status.Etcd).To(BeNil())

		scenario.UpdateNode(wOne.addr, func(n *fake.Node) {
			// A service without a health check is as healthy as running makes it.
			n.Services = []talos.ServiceStatus{
				{ID: "apid", Running: true, Unknown: true}, {ID: "kubelet", Running: true}, {ID: "udevd", Unknown: true},
			}
			n.Diagnostics = []talos.Diagnostic{{ID: "address-overlap", Message: strings.Repeat("ä", 400)}}
			n.TimeUnsynced = true
			n.Links = []talos.Link{{Name: "eth0", Up: true, SpeedMbps: 10000, Bond: "bond0"}, {Name: "eth1", Bond: "bond0"}}
			n.Volumes = []talos.Volume{{Name: "EPHEMERAL", Phase: "failed", Error: "no disk matches"}, {Name: "STATE", Phase: talos.VolumeReady}}
		})
		// Every write to the cluster changes how much of the file is in use. Status does not follow that.
		sizes(cpA, 200, 151)
		version := get(cpA).ResourceVersion
		round()

		health = get(wOne).Status.Health
		Expect(health.Services).To(ConsistOf(
			talosv1alpha1.ServiceHealth{Name: "apid", Running: true, Healthy: true},
			talosv1alpha1.ServiceHealth{Name: "kubelet", Running: true},
			talosv1alpha1.ServiceHealth{Name: "udevd"},
		))
		Expect(health.Diagnostics).To(HaveLen(1))
		Expect(health.Diagnostics[0].ID).To(Equal("address-overlap"))
		Expect(len(health.Diagnostics[0].Message)).To(BeNumerically("<=", 512), "what Talos says is cut to what status holds")
		Expect(health.TimeSynced).To(HaveValue(BeFalse()))
		Expect(health.Links).To(Equal([]talosv1alpha1.LinkHealth{
			{Name: "eth0", Up: true, SpeedMbps: 10000, Bond: "bond0"}, {Name: "eth1", Bond: "bond0"},
		}))
		Expect(health.Volumes[0]).To(Equal(talosv1alpha1.VolumeHealth{Name: "EPHEMERAL", Phase: "failed", Error: "no disk matches"}))
		Expect(get(cpA).ResourceVersion).To(Equal(version), "a database that moved by a megabyte must not write status")

		// A node that does not answer is not said to be anything: not healthy, and not leading.
		scenario.Kill(wOne.addr)
		scenario.FailNext(fake.MethodEtcdStatus, cpA.addr, errors.New("etcd status: context deadline exceeded"))
		round()

		Expect(get(wOne).Status.Health).To(BeNil())

		etcd = get(cpA).Status.Etcd
		Expect(etcd.MemberID).NotTo(BeEmpty(), "which member it is does not change while nobody looks")
		Expect(etcd.Leader).To(BeFalse())
		Expect(etcd.DBSize).To(BeZero())
		Expect(get(cpA).Status.Health).NotTo(BeNil(), "what was read of the node is still reported")
	})

	It("says when the certificates the cluster depends on end", func() {
		start(talosv1alpha1.CapabilityDisabled, cpA, wOne)

		names := make([]string, 0, len(tc.Status.Certificates))

		for _, cert := range tc.Status.Certificates {
			names = append(names, cert.Name)

			Expect(cert.ExpiresAt.After(cert.IssuedAt.Time)).To(BeTrue(), cert.Name)
			Expect(cert.ExpiresAt.After(clock)).To(BeTrue(), cert.Name)
		}

		Expect(names).To(Equal([]string{"etcd", "kubernetes", "kubernetes-aggregator", "operator", "os"}))

		// Credentials that can no longer be read keep the date they had: it is the one that matters.
		before := tc.Status.Certificates
		Expect(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecret, Namespace: ns}})).To(Succeed())
		round()

		Expect(condition(tc, talosv1alpha1.ConditionCredentialsValid).Status).To(Equal(metav1.ConditionFalse))
		Expect(tc.Status.Certificates).To(Equal(before))
	})

	It("defragments every member when one is fragmented, the leader last, and not again within the interval", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		// More than half of one member's file is free space.
		sizes(cpB, 400, 100)
		runUntil("etcd is defragmented", func() bool { return rested() && len(defragmented()) == 3 })

		// The first control plane leads etcd, and gives the lead away before its turn.
		Expect(defragmented()).To(Equal([]string{cpB.name, cpC.name, cpA.name}))
		Expect(scenario.CallsFor(fake.MethodEtcdForfeitLeadership)).To(HaveLen(1))
		Expect(scenario.CallsFor(fake.MethodEtcdForfeitLeadership)[0].Node).To(Equal(cpA.addr))

		record := tc.Status.History[0]
		Expect(record.Type).To(Equal(talosv1alpha1.OperationEtcdDefragment))
		Expect(record.Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(record.Completed).To(Equal([]string{cpB.name, cpC.name, cpA.name}))

		Expect(scenario.Node(cpB.addr).EtcdDBSize).To(Equal(100 * mebibyte))
		Expect(tc.Status.Maintenance.EtcdDefragmentedAt).NotTo(BeNil())
		Expect(maintained().Status).To(Equal(metav1.ConditionTrue), maintained().Message)
		Expect(factory.KeeperTargets()).NotTo(BeEmpty())

		// It is fragmented again an hour later. That waits for the interval.
		clock = clock.Add(time.Hour)
		sizes(cpC, 400, 100)
		settle(nil)

		Expect(idle()).To(BeTrue())
		Expect(maintained().Status).To(Equal(metav1.ConditionFalse))
		Expect(maintained().Reason).To(Equal(talosv1alpha1.ReasonTooSoon), maintained().Message)
		Expect(maintained().Message).To(ContainSubstring(cpC.name))
		Expect(defragmented()).To(HaveLen(3))

		clock = clock.Add(24 * time.Hour)
		runUntil("etcd is defragmented again", func() bool { return rested() && len(defragmented()) == 6 })
		Expect(maintained().Status).To(Equal(metav1.ConditionTrue), maintained().Message)
	})

	It("leaves a fragmented etcd alone while maintenance is not enabled", func() {
		start(talosv1alpha1.CapabilityDisabled, cpA, wOne)

		sizes(cpA, 400, 100)
		clock = clock.Add(time.Minute)
		settle(nil)

		Expect(idle()).To(BeTrue())
		Expect(maintained().Status).To(Equal(metav1.ConditionFalse))
		Expect(maintained().Reason).To(Equal(talosv1alpha1.ReasonMaintenanceDisabled), maintained().Message)
		Expect(defragmented()).To(BeEmpty())
		Expect(factory.KeeperTargets()).To(BeEmpty(), "no connection that may defragment is even made")
	})

	It("does not defragment a small database, however empty it is", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, wOne)

		sizes(cpA, 60, 5)
		clock = clock.Add(time.Minute)
		settle(nil)

		Expect(maintained().Status).To(Equal(metav1.ConditionTrue), maintained().Message)
		Expect(defragmented()).To(BeEmpty())

		// Unless the spec says that is big enough.
		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Maintenance.Etcd.MinimumSize = new(resource.MustParse("50Mi"))
		})
		runUntil("etcd is defragmented", func() bool { return rested() && len(defragmented()) == 1 })
	})

	It("starts no defragmentation while the cluster is not healthy, and no member before the last one is verified", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{"a node is not ready"}})
		sizes(cpB, 400, 100)
		clock = clock.Add(time.Minute)
		settle(nil)

		Expect(idle()).To(BeTrue())
		Expect(maintained().Reason).To(Equal(plan.BlockGateClosed), maintained().Message)
		Expect(defragmented()).To(BeEmpty())

		scenario.SetHealth(talos.HealthResult{Healthy: true})
		runUntil("the first member is defragmented", func() bool { return len(defragmented()) == 1 })

		// The cluster stops being healthy while the first member is verified: the second waits.
		scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{"etcd is not healthy"}})

		for range 6 {
			clock = clock.Add(30 * time.Second)
			settle(nil)
		}

		Expect(defragmented()).To(Equal([]string{cpB.name}))
		Expect(tc.Status.Operation.Completed).To(BeEmpty())

		scenario.SetHealth(talos.HealthResult{Healthy: true})
		runUntil("etcd is defragmented", func() bool { return rested() && len(defragmented()) == 3 })
	})

	It("does not begin the next member while paused", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		sizes(cpB, 400, 100)
		runUntil("the first member is defragmented", func() bool { return len(defragmented()) == 1 })

		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })

		for range 12 {
			clock = clock.Add(30 * time.Second)
			settle(nil)
		}

		// The member that was defragmented is verified; nobody else is touched.
		Expect(tc.Status.Operation.Completed).To(Equal([]string{cpB.name}))
		Expect(defragmented()).To(Equal([]string{cpB.name}))
		Expect(condition(tc, talosv1alpha1.ConditionRolloutBlocked).Reason).To(Equal(talosv1alpha1.ReasonPaused))

		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
		runUntil("etcd is defragmented", func() bool { return rested() && len(defragmented()) == 3 })
	})

	It("halts at a member that cannot be defragmented, keeps what the node said, and goes on when told to leave it out", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		scenario.FailNext(fake.MethodEtcdDefragment, cpB.addr, errors.New("defragment: rpc error: code = Unavailable"))
		sizes(cpB, 400, 100)
		runUntil("the operation halts", halted)

		failure := tc.Status.Operation.Failure
		Expect(failure.Machine).To(Equal(cpB.name))
		Expect(failure.Step).To(Equal(plan.StepDefragmenting))
		Expect(failure.Reason).To(Equal(plan.FailTimeout))
		Expect(maintained().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed), maintained().Message)
		Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))

		// The node answered, so what it said when the operation halted is kept.
		Expect(failure.Bundle).To(Equal(clusterName + "-support-bundle"))

		bundle := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: failure.Bundle}, bundle)).To(Succeed())
		Expect(metav1.IsControlledBy(bundle, tc)).To(BeTrue())
		Expect(string(bundle.Data["services.txt"])).To(ContainSubstring("kubelet running=true healthy=true"))
		Expect(unzipped(bundle.Data["kubelet.log.gz"])).To(Equal("the kubelet of " + cpB.name + "\n"))
		Expect(unzipped(bundle.Data["etcd.log.gz"])).To(Equal("etcd of " + cpB.name + "\n"))
		Expect(unzipped(bundle.Data["dmesg.log.gz"])).To(Equal("the kernel of " + cpB.name + "\n"))
		Expect(string(bundle.Data["failure.txt"])).To(And(
			ContainSubstring("machine: "+cpB.name), ContainSubstring("reason: "+plan.FailTimeout), ContainSubstring("not read: none"),
		))

		// It is collected once: the logs are not read again while the operation waits.
		logs := len(scenario.CallsFor(fake.MethodLogs))
		clock = clock.Add(time.Minute)
		settle(nil)
		Expect(scenario.CallsFor(fake.MethodLogs)).To(HaveLen(logs))

		annotate(talosv1alpha1.AnnotationSkipMachine, failure.Token)
		runUntil("the others are defragmented", idle)

		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(tc.Status.History[0].Skipped).To(Equal([]string{cpB.name}))
		Expect(tc.Status.History[0].Completed).To(Equal([]string{cpC.name, cpA.name}))
	})

	It("says so when nothing could be collected from a node that does not answer", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		sizes(cpB, 400, 100)
		runUntil("the first member's turn begins", func() bool {
			return tc.Status.Operation != nil && tc.Status.Operation.Current != nil
		})

		scenario.Kill(cpB.addr)
		runUntil("the operation halts", halted)

		// For a while the node may come back and still say something.
		Expect(tc.Status.Operation.Failure.Bundle).To(BeEmpty())

		clock = clock.Add(6 * time.Minute)
		settle(nil)

		Expect(tc.Status.Operation.Failure.Bundle).To(HavePrefix("not collected: "))
		Expect(defragmented()).To(BeEmpty())

		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName + "-support-bundle"}, &corev1.Secret{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "%v", err)

		annotate(talosv1alpha1.AnnotationAbort, tc.Status.Operation.Failure.Token)
		runUntil("the operation ends", idle)
		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
	})

	It("does not write its bundle over a Secret that is not its own", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		theirs := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: clusterName + "-support-bundle"},
			Data:       map[string][]byte{"theirs": []byte("something else")},
		}
		Expect(k8sClient.Create(ctx, theirs)).To(Succeed())

		scenario.FailNext(fake.MethodEtcdDefragment, cpB.addr, errors.New("defragment: rpc error: code = Unavailable"))
		sizes(cpB, 400, 100)
		runUntil("the operation halts", halted)

		Expect(tc.Status.Operation.Failure.Bundle).To(And(HavePrefix("not collected: "), ContainSubstring("is not this cluster's")))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(theirs), theirs)).To(Succeed())
		Expect(theirs.Data).To(Equal(map[string][]byte{"theirs": []byte("something else")}))
	})

	It("ends a defragmentation when maintenance is disabled, and touches no further member", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		sizes(cpB, 400, 100)
		runUntil("the first member is defragmented", func() bool { return len(defragmented()) == 1 })

		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Management.Maintenance = talosv1alpha1.CapabilityDisabled
		})
		runUntil("the operation ends", idle)

		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
		Expect(defragmented()).To(Equal([]string{cpB.name}))
	})
})

// unzipped is a log of a support bundle as it was read from the node.
func unzipped(data []byte) string {
	GinkgoHelper()

	r, err := gzip.NewReader(bytes.NewReader(data))
	Expect(err).NotTo(HaveOccurred())

	raw, err := io.ReadAll(r)
	Expect(err).NotTo(HaveOccurred())

	return string(raw)
}
