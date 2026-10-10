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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

const bondDocuments = `apiVersion: v1alpha1
kind: BondConfig
name: bond0
links: [eth0, eth1]
bondMode: 802.3ad
---
apiVersion: v1alpha1
kind: DHCPv4Config
name: bond0
`

var _ = Describe("Pools and shared configs", func() {
	var (
		ns          string
		poolRec     *TalosMachinePoolReconciler
		configRec   *TalosMachineConfigReconciler
		sharedNet   *talosv1alpha1.TalosMachineConfig
		workersPool = client.ObjectKey{Name: workerPool}
	)

	reconcileAndGet := func(r reconcile.Reconciler, obj client.Object) error {
		GinkgoHelper()

		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
		Expect(err).NotTo(HaveOccurred())

		return k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	}

	update := func(obj client.Object, change func()) {
		GinkgoHelper()

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		change()
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
	}

	BeforeEach(func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "pools-"}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name
		workersPool.Namespace = ns

		Expect(k8sClient.Create(ctx, &talosv1alpha1.TalosCluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns}})).To(Succeed())
		createPools(ns)

		sharedNet = &talosv1alpha1.TalosMachineConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "bonded-network", Namespace: ns},
			Spec:       talosv1alpha1.TalosMachineConfigSpec{Documents: bondDocuments},
		}
		Expect(k8sClient.Create(ctx, sharedNet)).To(Succeed())

		poolRec = &TalosMachinePoolReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		configRec = &TalosMachineConfigReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	})

	It("counts a pool's members and keeps a pool with members until they leave", func() {
		a := machine(ns, "worker-a", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
		b := machine(ns, "worker-b", talosv1alpha1.MachineRoleWorker, cpAddr, cpUUID)
		Expect(k8sClient.Create(ctx, a)).To(Succeed())
		Expect(k8sClient.Create(ctx, b)).To(Succeed())

		meta.SetStatusCondition(&a.Status.Conditions, metav1.Condition{
			Type: talosv1alpha1.ConditionMatched, Status: metav1.ConditionTrue, Reason: talosv1alpha1.ReasonSucceeded,
		})
		Expect(k8sClient.Status().Update(ctx, a)).To(Succeed())

		pool := &talosv1alpha1.TalosMachinePool{}
		Expect(k8sClient.Get(ctx, workersPool, pool)).To(Succeed())

		// The first pass adds the finalizer, the next one reports.
		Expect(reconcileAndGet(poolRec, pool)).To(Succeed())
		Expect(controllerutil.ContainsFinalizer(pool, talosv1alpha1.FinalizerInUse)).To(BeTrue())
		Expect(reconcileAndGet(poolRec, pool)).To(Succeed())

		Expect(pool.Status.Members).To(Equal(int32(2)))
		Expect(pool.Status.Matched).To(Equal(int32(1)))
		inUse := meta.FindStatusCondition(pool.Status.Conditions, talosv1alpha1.ConditionInUse)
		Expect(inUse.Status).To(Equal(metav1.ConditionTrue))
		Expect(inUse.Message).To(ContainSubstring("worker-a, worker-b"))

		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
		Expect(reconcileAndGet(poolRec, pool)).To(Succeed(), "a pool with members stays")
		Expect(meta.FindStatusCondition(pool.Status.Conditions, talosv1alpha1.ConditionInUse).Message).To(ContainSubstring("deletion waits"))

		Expect(k8sClient.Delete(ctx, a)).To(Succeed())
		Expect(k8sClient.Delete(ctx, b)).To(Succeed())
		Expect(reconcileAndGet(poolRec, pool)).To(Satisfy(apierrors.IsNotFound), "the last member leaving releases the pool")
	})

	It("reports a shared config's documents and everything that uses it", func() {
		Expect(k8sClient.Create(ctx, machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID))).To(Succeed())
		Expect(k8sClient.Create(ctx, machine(ns, "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID))).To(Succeed())

		Expect(reconcileAndGet(configRec, sharedNet)).To(Succeed())
		Expect(reconcileAndGet(configRec, sharedNet)).To(Succeed())

		Expect(meta.IsStatusConditionTrue(sharedNet.Status.Conditions, talosv1alpha1.ConditionValid)).To(BeTrue())
		Expect(sharedNet.Status.DocumentKeys).To(ConsistOf("v1alpha1/BondConfig/bond0", "v1alpha1/DHCPv4Config/bond0"))
		Expect(sharedNet.Status.UsedBy).To(Equal(&talosv1alpha1.ConfigUsage{}))

		ref := []talosv1alpha1.ConfigSource{{ConfigRef: &talosv1alpha1.ConfigReference{Name: sharedNet.Name}}}

		pool := &talosv1alpha1.TalosMachinePool{}
		Expect(k8sClient.Get(ctx, workersPool, pool)).To(Succeed())
		update(pool, func() { pool.Spec.Config = ref })

		Expect(reconcileAndGet(configRec, sharedNet)).To(Succeed())
		Expect(sharedNet.Status.UsedBy).To(Equal(&talosv1alpha1.ConfigUsage{Pools: []string{workerPool}, Machines: 1}))

		// Referenced cluster-wide, it reaches every machine through every pool.
		tc := &talosv1alpha1.TalosCluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns}}
		update(tc, func() { tc.Spec.Config = ref })

		Expect(reconcileAndGet(configRec, sharedNet)).To(Succeed())
		Expect(sharedNet.Status.UsedBy).To(Equal(&talosv1alpha1.ConfigUsage{
			Clusters: []string{clusterName}, Pools: []string{workerPool}, Machines: 2,
		}))

		Expect(configsInNamespace(k8sClient)(ctx, pool)).To(ConsistOf(reconcile.Request{NamespacedName: client.ObjectKeyFromObject(sharedNet)}))
	})

	It("refuses JSON6902 documents", func() {
		invalid := &talosv1alpha1.TalosMachineConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "json-patch", Namespace: ns},
			Spec:       talosv1alpha1.TalosMachineConfigSpec{Documents: "- op: add\n  path: /machine/foo\n  value: bar\n"},
		}
		Expect(k8sClient.Create(ctx, invalid)).To(Succeed())

		Expect(reconcileAndGet(configRec, invalid)).To(Succeed())
		Expect(reconcileAndGet(configRec, invalid)).To(Succeed())

		valid := meta.FindStatusCondition(invalid.Status.Conditions, talosv1alpha1.ConditionValid)
		Expect(valid.Status).To(Equal(metav1.ConditionFalse))
		Expect(valid.Message).To(ContainSubstring("JSON6902"))
		Expect(invalid.Status.DocumentKeys).To(BeEmpty())
	})

	It("keeps a referenced config until nothing uses it", func() {
		tm := machine(ns, "worker-1", talosv1alpha1.MachineRoleWorker, workerAddr, workerUUID)
		tm.Spec.Config = []talosv1alpha1.ConfigSource{{ConfigRef: &talosv1alpha1.ConfigReference{Name: sharedNet.Name}}}
		Expect(k8sClient.Create(ctx, tm)).To(Succeed())

		Expect(reconcileAndGet(configRec, sharedNet)).To(Succeed())
		Expect(k8sClient.Delete(ctx, sharedNet)).To(Succeed())

		Expect(reconcileAndGet(configRec, sharedNet)).To(Succeed(), "a referenced config stays")
		Expect(meta.FindStatusCondition(sharedNet.Status.Conditions, talosv1alpha1.ConditionInUse).Message).To(ContainSubstring("deletion waits"))

		update(tm, func() { tm.Spec.Config = nil })
		Expect(reconcileAndGet(configRec, sharedNet)).To(Satisfy(apierrors.IsNotFound), "dropping the last reference releases it")
	})
})
