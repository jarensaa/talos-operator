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
	"fmt"
	"net/netip"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
	"github.com/jarensaa/talos-operator/internal/target"
)

// member is one machine of a spec's cluster: its TalosMachine, its fake Talos node, and its
// Kubernetes Node with the Lease its kubelet renews.
type member struct {
	// name names the TalosMachine.
	name string
	// node is the hostname Talos reports, which is the name of the Kubernetes Node.
	node string
	addr netip.Addr
	uuid string
	role talosv1alpha1.MachineRole
	// etcd is the ID of the machine's etcd member; zero for a machine that has none.
	etcd uint64
}

func (m member) controlPlane() bool { return m.role == talosv1alpha1.MachineRoleControlPlane }

// fakeNode is the machine as Talos: healthy, and running the config liveConfig generates for its role.
func (m member) fakeNode() fake.Node { return fakeNode(m.addr, m.role, m.node, m.uuid, m.etcd) }

// nothingStranded is what an API server answers that holds nothing a Kubernetes upgrade would strand.
var nothingStranded = KubernetesCheckerFunc(func(context.Context, string, string) ([]string, error) { return nil, nil })

// specCluster is what every spec starts from: a namespace of its own, and in it the talosconfig
// Secret, the TalosCluster that names it, and the cluster's pools. complete, if given, fills in
// the rest of the TalosCluster before it is created. What a spec leaves outside its namespace, the
// Nodes and their Leases, is removed when the spec ends.
func specCluster(prefix string, complete func(*talosv1alpha1.TalosCluster)) *talosv1alpha1.TalosCluster {
	GinkgoHelper()

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: prefix}}
	Expect(k8sClient.Create(ctx, namespace)).To(Succeed())

	DeferCleanup(func() {
		Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
		Expect(k8sClient.DeleteAllOf(ctx, &coordinationv1.Lease{}, client.InNamespace(nodeLeaseNamespace))).To(Succeed())
	})

	Expect(k8sClient.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: credentialsSecret, Namespace: namespace.Name},
		Data:       map[string][]byte{creds.DefaultTalosconfigKey: talostest.AdminTalosconfig(GinkgoT(), cpAddr.String())},
	})).To(Succeed())

	tc := &talosv1alpha1.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace.Name},
		Spec:       talosv1alpha1.TalosClusterSpec{Credentials: secretCredentials()},
	}
	if complete != nil {
		complete(tc)
	}

	Expect(k8sClient.Create(ctx, tc)).To(Succeed())
	createPools(tc.Namespace)

	return tc
}

// getMachine reads a TalosMachine as the API server has it.
func getMachine(ns, name string) *talosv1alpha1.TalosMachine {
	GinkgoHelper()

	tm := &talosv1alpha1.TalosMachine{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, tm)).To(Succeed())

	return tm
}

// harness is a cluster and the operator that manages it, driven by the spec one pass at a time:
// fake Talos nodes, their Kubernetes Nodes and Leases in envtest, both controllers, and a clock
// only the spec moves. A spec gets its own (newHarness) and brings the cluster up with start.
type harness struct {
	ns       string
	scenario *fake.Scenario
	// factory makes the connections of the operator that runs now (see takeOver).
	factory           *fake.Factory
	recorder          *events.FakeRecorder
	reconciler        *TalosClusterReconciler
	machineReconciler *TalosMachineReconciler
	// tc is the TalosCluster as the last round left it.
	tc *talosv1alpha1.TalosCluster
	// clock is the operator's clock; the specs move it.
	clock time.Time
	// declared are the machines that have a TalosMachine: the ones a round reconciles.
	declared []member

	// What the areas the specs cover do differently. An area sets these before it calls start.

	// prefix begins the name of the spec's namespace.
	prefix string
	// heartbeat is what the kubelets do between two passes of the operator. It is renewLeases
	// unless an area plays more of the cluster than that.
	heartbeat func()
	// sent counts what the operator sent to Talos, for settle: a round that sent something has
	// not settled. Without it settle watches the cluster's status alone.
	sent func() int
	// patience is how many steps runEvery takes before it gives up.
	patience int
	// watched are the conditions runEvery prints, next to the operation, when it gives up.
	watched []string
	// dress, if set, changes the fake Talos node of a machine before the cluster is brought up.
	dress func(member, *fake.Node)
	// complete, if set, changes the TalosCluster before it is created.
	complete func(*talosv1alpha1.TalosCluster)
}

// newHarness is the harness of one spec. Nothing exists yet: start brings the cluster up.
func newHarness(prefix string, patience int, watched ...string) *harness {
	h := &harness{prefix: prefix, patience: patience, watched: watched, clock: time.Now().Truncate(time.Second)}
	h.heartbeat = h.renewLeases

	return h
}

// start brings up a cluster of the given machines, and has the operator adopt it and take over
// its config. manage, if given, switches on what else of spec.management the specs need, in the
// update that sets config: Manage.
func (h *harness) start(manage func(*talosv1alpha1.Management), machines ...member) {
	GinkgoHelper()

	fakes := make([]fake.Node, 0, len(machines))

	for _, m := range machines {
		n := m.fakeNode()
		if h.dress != nil {
			h.dress(m, &n)
		}

		fakes = append(fakes, n)
	}

	h.scenario = fake.NewScenario(fakes...)
	h.recorder = events.NewFakeRecorder(4096)

	h.tc = specCluster(h.prefix, func(tc *talosv1alpha1.TalosCluster) {
		// A short soak keeps the specs short; the try window stays at its default.
		tc.Spec.Rollout = talosv1alpha1.Rollout{Soak: &metav1.Duration{Duration: 20 * time.Second}}
		reproduceLiveConfigs(tc)

		if h.complete != nil {
			h.complete(tc)
		}
	})
	h.ns = h.tc.Namespace

	for _, m := range machines {
		h.register(m)
	}

	h.reconciler = h.newReconciler()
	h.machineReconciler = &TalosMachineReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Resolver: h.reconciler.Resolver}

	// The specs bring their own machines, under their own names, so the operator writes none.
	for _, m := range machines {
		h.declare(m)
	}

	// Adopt: the first pass stores the bundle the machines render with.
	_, err := h.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(h.tc)})
	Expect(err).NotTo(HaveOccurred())

	h.settle(nil)
	Expect(condition(h.tc, talosv1alpha1.ConditionConfigProven).Status).To(Equal(metav1.ConditionTrue), "the cluster must adopt cleanly")

	h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
		tc.Spec.Management.Config = talosv1alpha1.ConfigManagementManage

		if manage != nil {
			manage(&tc.Spec.Management)
		}
	})
	h.settle(nil)

	Expect(condition(h.tc, talosv1alpha1.ConditionConfigHandover).Status).To(Equal(metav1.ConditionTrue))

	if h.sent != nil {
		Expect(h.sent()).To(BeZero(), "adopting and taking over must send nothing")
	}
}

// newReconciler is an operator process of its own: it has its own connections to Talos, and
// nothing in memory.
func (h *harness) newReconciler() *TalosClusterReconciler {
	GinkgoHelper()

	evictor, err := NewPodEvictor(cfg)
	Expect(err).NotTo(HaveOccurred())

	h.factory = &fake.Factory{Scenario: h.scenario}

	return &TalosClusterReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Resolver:  &target.Resolver{Client: k8sClient, Factory: h.factory},
		Recorder:  h.recorder,
		APIReader: k8sClient,
		Evictor:   evictor,
		Now:       func() time.Time { return h.clock },
		// The API server of these specs has nothing a Kubernetes upgrade would strand.
		KubernetesChecks: nothingStranded,
	}
}

// takeOver replaces the operator with another replica: a new process, which knows nothing but
// what is in status and on the nodes.
func (h *harness) takeOver() {
	GinkgoHelper()

	h.reconciler = h.newReconciler()
	h.machineReconciler.Resolver = h.reconciler.Resolver
}

// register is a machine's kubelet registering its Node, Ready, with the Lease it renews.
func (h *harness) register(m member) {
	GinkgoHelper()

	kube := kubeNode(m.node)
	kube.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	Expect(k8sClient.Create(ctx, kube)).To(Succeed())
	Expect(k8sClient.Status().Update(ctx, kube)).To(Succeed())

	// A Lease outlives its Node here: the specs' API server has nothing that collects it.
	err := k8sClient.Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: m.node, Namespace: nodeLeaseNamespace},
		Spec:       coordinationv1.LeaseSpec{RenewTime: &metav1.MicroTime{Time: h.clock}},
	})
	Expect(client.IgnoreAlreadyExists(err)).To(Succeed())
}

// declare creates the TalosMachine of a machine. Every round reconciles it from then on.
func (h *harness) declare(m member) {
	GinkgoHelper()

	Expect(k8sClient.Create(ctx, machine(h.ns, m.name, m.role, m.addr, m.uuid))).To(Succeed())

	h.declared = append(h.declared, m)
}

// renewLeases is the kubelets of the machines that run: they renew their Leases.
func (h *harness) renewLeases() {
	GinkgoHelper()

	for _, m := range h.declared {
		lease := &coordinationv1.Lease{}

		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nodeLeaseNamespace, Name: m.node}, lease)
		if apierrors.IsNotFound(err) || h.scenario.Down(m.addr) || h.scenario.Node(m.addr).Maintenance {
			continue
		}

		Expect(err).NotTo(HaveOccurred())

		lease.Spec.RenewTime = &metav1.MicroTime{Time: h.clock}
		Expect(k8sClient.Update(ctx, lease)).To(Succeed())
	}
}

// round lets the kubelets and both controllers run once, machines first.
func (h *harness) round() {
	GinkgoHelper()

	h.heartbeat()

	for _, m := range h.declared {
		_, err := h.machineReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: h.ns, Name: m.name}})
		Expect(err).NotTo(HaveOccurred())
	}

	_, err := h.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(h.tc)})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(h.tc), h.tc)).To(Succeed())
}

// settle runs rounds until one changes nothing: no status, and nothing that sent counts. It stops
// early as soon as stop, if given, holds.
func (h *harness) settle(stop func() bool) {
	GinkgoHelper()

	sent := func() int {
		if h.sent == nil {
			return 0
		}

		return h.sent()
	}

	for range 40 {
		version, before := h.tc.ResourceVersion, sent()

		h.round()

		if stop != nil && stop() {
			return
		}

		if h.tc.ResourceVersion == version && sent() == before {
			return
		}
	}

	Fail("the controllers never settled")
}

// advance moves the clock and lets everything settle.
func (h *harness) advance(d time.Duration) {
	GinkgoHelper()

	h.clock = h.clock.Add(d)
	h.settle(nil)
}

// at sets the clock and lets everything settle.
func (h *harness) at(t time.Time) {
	GinkgoHelper()

	h.clock = t
	h.settle(nil)
}

// runEvery advances time in steps until done holds.
func (h *harness) runEvery(step time.Duration, what string, done func() bool) {
	GinkgoHelper()

	for range h.patience {
		if done() {
			return
		}

		h.clock = h.clock.Add(step)
		h.settle(done)
	}

	var state strings.Builder

	for _, kind := range h.watched {
		fmt.Fprintf(&state, "; %s: %+v", kind, condition(h.tc, kind))
	}

	Fail(fmt.Sprintf("never happened: %s; operation: %+v%s", what, h.tc.Status.Operation, state.String()))
}

// runUntil advances time the way an operation usually sees it pass: a few seconds at a time.
func (h *harness) runUntil(what string, done func() bool) {
	GinkgoHelper()

	h.runEvery(10*time.Second, what, done)
}

// updateCluster changes the TalosCluster as the API server has it.
func (h *harness) updateCluster(mutate func(*talosv1alpha1.TalosCluster)) {
	GinkgoHelper()

	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(h.tc), h.tc)).To(Succeed())
	mutate(h.tc)
	Expect(k8sClient.Update(ctx, h.tc)).To(Succeed())
}

// annotate sets an annotation of the TalosCluster: a human's word to the operator.
func (h *harness) annotate(key, value string) {
	GinkgoHelper()

	h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
		if tc.Annotations == nil {
			tc.Annotations = map[string]string{}
		}

		tc.Annotations[key] = value
	})
}

// machine reads the TalosMachine of that name.
func (h *harness) machine(name string) *talosv1alpha1.TalosMachine {
	GinkgoHelper()

	return getMachine(h.ns, name)
}
