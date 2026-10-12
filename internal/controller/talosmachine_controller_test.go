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
	"fmt"
	"net/netip"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
	"github.com/jarensaa/talos-operator/internal/target"
)

var _ = Describe("TalosMachine binding", func() {
	var (
		ns         string
		scenario   *fake.Scenario
		reconciler *TalosMachineReconciler
	)

	reconcileMachine := func(tm *talosv1alpha1.TalosMachine) *talosv1alpha1.TalosMachine {
		GinkgoHelper()

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tm)})
		Expect(err).NotTo(HaveOccurred())

		got := &talosv1alpha1.TalosMachine{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), got)).To(Succeed())

		return got
	}

	matched := func(tm *talosv1alpha1.TalosMachine) *metav1.Condition {
		return meta.FindStatusCondition(tm.Status.Conditions, talosv1alpha1.ConditionMatched)
	}

	BeforeEach(func() {
		ns = specCluster("bind-", nil).Namespace

		Expect(k8sClient.Create(ctx, kubeNode(cpNodename))).To(Succeed())

		scenario = observedCluster()
		reconciler = &TalosMachineReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Resolver: &target.Resolver{Client: k8sClient, Factory: &fake.Factory{Scenario: scenario}},
		}
	})

	It("binds by systemUUID and reports the node's state", func() {
		tm := machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID)
		Expect(k8sClient.Create(ctx, tm)).To(Succeed())

		got := reconcileMachine(tm)

		Expect(matched(got).Status).To(Equal(metav1.ConditionTrue))
		Expect(got.Status.Phase).To(Equal(talosv1alpha1.MachinePhaseReady))
		Expect(got.Status.Identity).To(Equal(&talosv1alpha1.ObservedIdentity{
			SystemUUID: cpUUID, Hostname: cpNodename, KubernetesNodeName: cpNodename,
		}))
		Expect(got.Status.Talos).To(Equal(&talosv1alpha1.TalosState{
			Version: talosVersion, SchematicID: schematic, BootID: cpNodename + "-boot-1", Stage: talos.StageRunning, Ready: true,
		}))
		Expect(got.Status.Kubelet).To(Equal(&talosv1alpha1.KubeletState{Version: kubernetesVersion}))
		Expect(got.Status.Etcd).To(Equal(&talosv1alpha1.EtcdState{MemberID: "a1", Leader: true}))
		Expect(got.Status.Config.LiveHash).To(Equal(normalizedHash(scenario.Node(cpAddr).Config.Active)))

		scenario.AssertNoMutations(GinkgoT())
	})

	Context("a node that cannot be read in full", func() {
		var tm *talosv1alpha1.TalosMachine

		observed := func(tm *talosv1alpha1.TalosMachine) *metav1.Condition {
			GinkgoHelper()

			c := meta.FindStatusCondition(tm.Status.Conditions, talosv1alpha1.ConditionNodeObserved)
			Expect(c).NotTo(BeNil())

			return c
		}

		callsTo := func(addr netip.Addr) int {
			n := 0

			for _, c := range scenario.Calls() {
				if c.Node == addr {
					n++
				}
			}

			return n
		}

		BeforeEach(func() {
			tm = machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID)
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())

			got := reconcileMachine(tm)
			Expect(got.Status.Phase).To(Equal(talosv1alpha1.MachinePhaseReady))
			Expect(observed(got).Status).To(Equal(metav1.ConditionTrue))
		})

		It("is not Ready on the strength of what it said last time", func() {
			scenario.FailNext(fake.MethodMachineStatus, cpAddr, status.Error(codes.Internal, "resource unavailable"))

			got := reconcileMachine(tm)

			Expect(got.Status.Phase).To(Equal(talosv1alpha1.MachinePhasePending))
			Expect(got.Status.Talos.Ready).To(BeFalse())
			Expect(got.Status.Talos.Stage).To(BeEmpty())
			Expect(observed(got).Status).To(Equal(metav1.ConditionFalse))
			Expect(observed(got).Message).To(Equal("not read in the last pass: machine status"))

			// What the node did answer is current, and it is still the node it was.
			Expect(matched(got).Status).To(Equal(metav1.ConditionTrue))
			Expect(got.Status.Talos.BootID).To(Equal(cpNodename + "-boot-1"))
			Expect(got.Status.Talos.Version).To(Equal(talosVersion))

			scenario.FailNext(fake.MethodMachineStatus, cpAddr, nil)

			got = reconcileMachine(tm)
			Expect(got.Status.Phase).To(Equal(talosv1alpha1.MachinePhaseReady))
			Expect(observed(got).Status).To(Equal(metav1.ConditionTrue))
		})

		It("does not show the boot ID of a boot it may have left", func() {
			scenario.FailNext(fake.MethodBootID, cpAddr, status.Error(codes.Internal, "resource unavailable"))

			got := reconcileMachine(tm)

			Expect(got.Status.Talos.BootID).To(BeEmpty())
			Expect(observed(got).Message).To(ContainSubstring("boot ID"))
			// The version of a node rarely changes, and is more use stale than missing.
			Expect(got.Status.Talos.Version).To(Equal(talosVersion))
			Expect(got.Status.Phase).To(Equal(talosv1alpha1.MachinePhaseReady), "the node itself said it is ready")
		})

		It("forgets the live config hash it can no longer vouch for", func() {
			Expect(reconcileMachine(tm).Status.Config.LiveHash).NotTo(BeEmpty())

			scenario.FailNext(fake.MethodMachineConfig, cpAddr, status.Error(codes.Internal, "resource unavailable"))

			got := reconcileMachine(tm)

			Expect(got.Status.Config.LiveHash).To(BeEmpty())
			Expect(got.Status.Config.DesiredHash).To(BeEmpty(), "nothing was rendered before the bundle exists")
			Expect(meta.FindStatusCondition(got.Status.Conditions, talosv1alpha1.ConditionConfigMatchesLive).Status).
				To(Equal(metav1.ConditionUnknown))
			Expect(observed(got).Message).To(ContainSubstring("machine config"))
		})

		It("stops asking a node that does not answer, and names everything it did not learn", func() {
			scenario.FailNext(fake.MethodVersion, cpAddr, status.Error(codes.Unavailable, "connection refused"))

			before := callsTo(cpAddr)
			got := reconcileMachine(tm)

			// System information, machine type and members answered; the version did not.
			Expect(callsTo(cpAddr)-before).To(Equal(4), "nothing is asked of a node after it failed to answer")
			Expect(observed(got).Status).To(Equal(metav1.ConditionFalse))
			Expect(observed(got).Message).To(Equal(
				"not read in the last pass: version, boot ID, schematic, machine status, nodename, machine config, etcd status, " +
					"services, diagnostics, time status, links, volumes"))
			Expect(got.Status.Phase).To(Equal(talosv1alpha1.MachinePhasePending))
			Expect(got.Status.Talos).To(Equal(&talosv1alpha1.TalosState{Version: talosVersion, SchematicID: schematic}))
			Expect(got.Status.Config.LiveHash).To(BeEmpty())
			// Which member of etcd it is stays; that it led, and how it was, does not.
			Expect(got.Status.Etcd).To(Equal(&talosv1alpha1.EtcdState{MemberID: "a1"}))
			Expect(got.Status.Health).To(BeNil())
		})

		It("keeps nothing that only described a moment once the node cannot be identified", func() {
			scenario.FailNext(fake.MethodSystemInformation, cpAddr, status.Error(codes.Unavailable, "connection refused"))

			got := reconcileMachine(tm)

			Expect(matched(got).Status).To(Equal(metav1.ConditionUnknown))
			Expect(observed(got).Status).To(Equal(metav1.ConditionFalse))
			Expect(got.Status.Phase).To(Equal(talosv1alpha1.MachinePhasePending))
			Expect(got.Status.Talos).To(Equal(&talosv1alpha1.TalosState{Version: talosVersion, SchematicID: schematic}))
			Expect(got.Status.Config.LiveHash).To(BeEmpty())
			// Who it was stays on record.
			Expect(got.Status.Identity.Hostname).To(Equal(cpNodename))
		})
	})

	It("matches systemUUIDs case-insensitively", func() {
		tm := machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, "AAAAAAAA-BBBB-4CCC-8DDD-000000000001")
		Expect(k8sClient.Create(ctx, tm)).To(Succeed())

		Expect(matched(reconcileMachine(tm)).Status).To(Equal(metav1.ConditionTrue))
	})

	It("refuses a node whose hardware does not match, and trusts nothing else about it", func() {
		tm := machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, workerUUID)
		Expect(k8sClient.Create(ctx, tm)).To(Succeed())

		got := reconcileMachine(tm)

		Expect(matched(got).Status).To(Equal(metav1.ConditionFalse))
		Expect(matched(got).Reason).To(Equal(talosv1alpha1.ReasonIdentityMismatch))
		Expect(got.Status.Talos).To(BeNil())
		Expect(scenario.CallsFor(fake.MethodVersion)).To(BeEmpty())
	})

	It("refuses a node whose role differs from the spec", func() {
		tm := machine(ns, "worker-1", talosv1alpha1.MachineRoleControlPlane, workerAddr, workerUUID)
		Expect(k8sClient.Create(ctx, tm)).To(Succeed())

		got := reconcileMachine(tm)

		Expect(matched(got).Status).To(Equal(metav1.ConditionFalse))
		Expect(matched(got).Reason).To(Equal(talosv1alpha1.ReasonRoleMismatch))
	})

	It("flags two machines claiming the same hardware as a conflict on both", func() {
		a := machine(ns, "cp-a", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID)
		b := machine(ns, "cp-b", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID)
		Expect(k8sClient.Create(ctx, a)).To(Succeed())
		Expect(k8sClient.Create(ctx, b)).To(Succeed())

		for _, tm := range []*talosv1alpha1.TalosMachine{a, b} {
			got := reconcileMachine(tm)
			Expect(matched(got).Status).To(Equal(metav1.ConditionFalse))
			Expect(matched(got).Reason).To(Equal(talosv1alpha1.ReasonConflict))
		}
	})

	Context("config comparison", func() {
		const (
			labelPatch     = "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  example.com/layer: %s\n"
			sensitivePatch = "sensitive-patch"
		)

		configMatches := func(tm *talosv1alpha1.TalosMachine) *metav1.Condition {
			return meta.FindStatusCondition(tm.Status.Conditions, talosv1alpha1.ConditionConfigMatchesLive)
		}

		BeforeEach(func() {
			data, err := adopt.EncodeBundle(talostest.Bundle(GinkgoT()))
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-talos-secrets", Namespace: ns},
				Data:       map[string][]byte{talosv1alpha1.DefaultSecretsKey: data},
			})).To(Succeed())

			tc := &talosv1alpha1.TalosCluster{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName}, tc)).To(Succeed())
			reproduceLiveConfigs(tc)
			Expect(k8sClient.Update(ctx, tc)).To(Succeed())
		})

		It("reports a machine whose spec reproduces the live config", func() {
			tm := machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())

			got := reconcileMachine(tm)

			cond := configMatches(got)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue), cond.Message)
			Expect(got.Status.Config.DesiredHash).To(Equal(got.Status.Config.LiveHash))
			Expect(got.Status.Config.Diff).To(BeEmpty())

			scenario.AssertNoMutations(GinkgoT())
		})

		It("names the differing document and turns green once the config is fixed", func() {
			tm := machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
			tm.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: fmt.Sprintf(labelPatch, "edge")}}
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())

			got := reconcileMachine(tm)

			cond := configMatches(got)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonMismatch))
			Expect(cond.Message).To(ContainSubstring("v1alpha1/KubeNodeConfig (changed)"))
			// From live (a/) to desired (b/): what applying the spec would change.
			Expect(got.Status.Config.Diff).To(ContainSubstring("+labels:\n+    example.com/layer: edge\n"), got.Status.Config.Diff)
			Expect(got.Status.Config.DesiredHash).NotTo(Equal(got.Status.Config.LiveHash))

			got.Spec.Config = nil
			Expect(k8sClient.Update(ctx, got)).To(Succeed())

			got = reconcileMachine(got)
			Expect(configMatches(got).Status).To(Equal(metav1.ConditionTrue))
			Expect(got.Status.Config.Diff).To(BeEmpty())

			scenario.AssertNoMutations(GinkgoT())
		})

		It("resolves pool config from a shared TalosMachineConfig and reports a missing one", func() {
			shared := &talosv1alpha1.TalosMachineConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "labels", Namespace: ns},
				Spec:       talosv1alpha1.TalosMachineConfigSpec{Documents: fmt.Sprintf(labelPatch, "shared")},
			}
			Expect(k8sClient.Create(ctx, shared)).To(Succeed())

			pool := &talosv1alpha1.TalosMachinePool{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: workerPool}, pool)).To(Succeed())
			pool.Spec.Config = []talosv1alpha1.ConfigSource{{ConfigRef: &talosv1alpha1.ConfigReference{Name: "labels"}}}
			Expect(k8sClient.Update(ctx, pool)).To(Succeed())

			worker := machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
			cp := machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID)
			Expect(k8sClient.Create(ctx, worker)).To(Succeed())
			Expect(k8sClient.Create(ctx, cp)).To(Succeed())

			Expect(configMatches(reconcileMachine(worker)).Message).To(ContainSubstring("v1alpha1/KubeNodeConfig"))
			Expect(configMatches(reconcileMachine(cp)).Status).To(Equal(metav1.ConditionTrue),
				"the worker pool's config does not reach control planes")

			// Editing the shared config re-reconciles exactly the machines that use it.
			Expect(machinesForObject(k8sClient)(ctx, shared)).To(ConsistOf(
				reconcile.Request{NamespacedName: client.ObjectKeyFromObject(worker)}))

			Expect(k8sClient.Delete(ctx, shared)).To(Succeed())

			cond := configMatches(reconcileMachine(worker))
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonConfigUnresolved))
			Expect(cond.Message).To(Equal("TalosMachineConfig labels not found"))
		})

		It("applies cluster, pool and machine layers in that order", func() {
			tc := &talosv1alpha1.TalosCluster{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: clusterName}, tc)).To(Succeed())
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: fmt.Sprintf(labelPatch, "cluster")}}
			Expect(k8sClient.Update(ctx, tc)).To(Succeed())

			pool := &talosv1alpha1.TalosMachinePool{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: workerPool}, pool)).To(Succeed())
			pool.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: fmt.Sprintf(labelPatch, "pool")}}
			Expect(k8sClient.Update(ctx, pool)).To(Succeed())

			tm := machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
			tm.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: fmt.Sprintf(labelPatch, "machine")}}
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())

			// The same key at every layer: the machine's value is the one rendered.
			got := reconcileMachine(tm)
			Expect(got.Status.Config.Diff).To(ContainSubstring("example.com/layer: machine"))
			Expect(got.Status.Config.Diff).NotTo(ContainSubstring("example.com/layer: pool"))
			Expect(got.Status.Config.Diff).NotTo(ContainSubstring("example.com/layer: cluster"))
		})

		It("withholds the diff of documents set by config from a Secret", func() {
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: sensitivePatch, Namespace: ns},
				Data:       map[string][]byte{"patch.yaml": []byte(fmt.Sprintf(labelPatch, "hunter2"))},
			})).To(Succeed())

			tm := machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
			tm.Spec.Config = []talosv1alpha1.ConfigSource{
				{SecretKeyRef: &talosv1alpha1.SecretKeyReference{Name: sensitivePatch, Key: "patch.yaml"}},
			}
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())

			got := reconcileMachine(tm)

			cond := configMatches(got)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Message).To(ContainSubstring("v1alpha1/KubeNodeConfig"))
			Expect(got.Status.Config.Diff).To(ContainSubstring("withheld"))
			Expect(cond.Message + got.Status.Config.Diff).NotTo(ContainSubstring("hunter2"))

			Expect(machinesForObject(k8sClient)(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: sensitivePatch, Namespace: ns},
			})).To(ConsistOf(reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tm)}))
		})

		It("waits for the secrets bundle", func() {
			Expect(k8sClient.Delete(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-talos-secrets", Namespace: ns},
			})).To(Succeed())

			tm := machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())

			cond := configMatches(reconcileMachine(tm))
			Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
			Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonSecretNotFound))
		})

		It("does not compare configs of an unmatched machine", func() {
			tm := machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, cpUUID)
			Expect(k8sClient.Create(ctx, tm)).To(Succeed())

			Expect(configMatches(reconcileMachine(tm)).Status).To(Equal(metav1.ConditionUnknown))
			Expect(scenario.CallsFor(fake.MethodMachineConfig)).To(BeEmpty())
		})
	})

	It("reports a missing pool, and a pool of a missing cluster", func() {
		tm := machine(ns, "orphan", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
		tm.Spec.PoolRef.Name = "missing"
		Expect(k8sClient.Create(ctx, tm)).To(Succeed())

		got := reconcileMachine(tm)
		Expect(matched(got).Status).To(Equal(metav1.ConditionFalse))
		Expect(matched(got).Reason).To(Equal(talosv1alpha1.ReasonNotFound))
		Expect(matched(got).Message).To(ContainSubstring("TalosMachinePool missing"))

		Expect(k8sClient.Create(ctx, &talosv1alpha1.TalosMachinePool{
			ObjectMeta: metav1.ObjectMeta{Name: "missing", Namespace: ns},
			Spec: talosv1alpha1.TalosMachinePoolSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "absent"},
				Role:       talosv1alpha1.MachineRoleWorker,
			},
		})).To(Succeed())

		got = reconcileMachine(tm)
		Expect(matched(got).Reason).To(Equal(talosv1alpha1.ReasonNotFound))
		Expect(matched(got).Message).To(ContainSubstring("TalosCluster absent"))
	})
})

// normalizedHash is the hash status.config reports for a raw config.
func normalizedHash(raw []byte) string {
	GinkgoHelper()

	cfg, err := talos.ParseMachineConfig(raw)
	Expect(err).NotTo(HaveOccurred())

	n, err := render.Normalize(cfg)
	Expect(err).NotTo(HaveOccurred())

	return n.Hash
}
