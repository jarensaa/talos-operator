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
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
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

// member is one machine of the specs' cluster: its TalosMachine, its fake Talos node, and the
// name its Kubernetes Node has.
type member struct {
	name string
	addr netip.Addr
	uuid string
	role talosv1alpha1.MachineRole
	etcd uint64
}

func (m member) controlPlane() bool { return m.role == talosv1alpha1.MachineRoleControlPlane }

func memberAt(name string, last int, role talosv1alpha1.MachineRole, etcd uint64) member {
	return member{
		name: name, addr: netip.AddrFrom4([4]byte{10, 5, 0, byte(last)}),
		uuid: fmt.Sprintf("aaaaaaaa-bbbb-4ccc-8ddd-0000000001%02d", last), role: role, etcd: etcd,
	}
}

// The machines the specs use. The first control plane has the address the specs' credentials name.
var (
	cpA   = memberAt("cp-a", 2, talosv1alpha1.MachineRoleControlPlane, 0xa1)
	cpB   = memberAt("cp-b", 3, talosv1alpha1.MachineRoleControlPlane, 0xb2)
	cpC   = memberAt("cp-c", 4, talosv1alpha1.MachineRoleControlPlane, 0xc3)
	cpD   = memberAt("cp-d", 5, talosv1alpha1.MachineRoleControlPlane, 0xd4)
	wOne  = memberAt("w-1", 11, talosv1alpha1.MachineRoleWorker, 0)
	wTwo  = memberAt("w-2", 12, talosv1alpha1.MachineRoleWorker, 0)
	other = "aaaaaaaa-bbbb-4ccc-8ddd-999999999999"
)

var _ = Describe("Adding and removing machines", func() {
	var (
		ns                string
		scenario          *fake.Scenario
		reconciler        *TalosClusterReconciler
		machineReconciler *TalosMachineReconciler
		tc                *talosv1alpha1.TalosCluster
		clock             time.Time
		// declared are the machines that have a TalosMachine.
		declared []member
	)

	fakeOf := func(m member) fake.Node {
		machineType := fake.TypeWorker
		if m.controlPlane() {
			machineType = fake.TypeControlPlane
		}

		return fakeNode(m.addr, machineType, m.name, m.uuid, m.etcd)
	}

	// kubelet is the machine's kubelet registering its Node.
	kubelet := func(m member) {
		GinkgoHelper()

		kube := kubeNode(m.name)
		kube.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
		Expect(k8sClient.Create(ctx, kube)).To(Succeed())
		Expect(k8sClient.Status().Update(ctx, kube)).To(Succeed())

		Expect(k8sClient.Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: m.name, Namespace: nodeLeaseNamespace},
			Spec:       coordinationv1.LeaseSpec{RenewTime: &metav1.MicroTime{Time: clock}},
		})).To(Succeed())
	}

	// declare creates the TalosMachine of a machine.
	declare := func(m member, mutate func(*talosv1alpha1.TalosMachine)) {
		GinkgoHelper()

		tm := machine(ns, m.name, m.role, m.addr, m.uuid)
		if mutate != nil {
			mutate(tm)
		}

		Expect(k8sClient.Create(ctx, tm)).To(Succeed())

		declared = append(declared, m)
	}

	// waiting puts a machine on the network that has no config and waits for one.
	waiting := func(m member, mutate func(*fake.Node)) {
		n := fakeOf(m)
		n.Maintenance, n.Config = true, talos.MachineConfigs{}

		if mutate != nil {
			mutate(&n)
		}

		scenario.Add(n)
	}

	// heartbeat is the kubelets of the machines that run: they renew their Leases.
	heartbeat := func() {
		GinkgoHelper()

		for _, m := range declared {
			lease := &coordinationv1.Lease{}

			err := k8sClient.Get(ctx, client.ObjectKey{Namespace: nodeLeaseNamespace, Name: m.name}, lease)
			if apierrors.IsNotFound(err) || scenario.Down(m.addr) || scenario.Node(m.addr).Maintenance {
				continue
			}

			Expect(err).NotTo(HaveOccurred())

			lease.Spec.RenewTime = &metav1.MicroTime{Time: clock}
			Expect(k8sClient.Update(ctx, lease)).To(Succeed())
		}
	}

	round := func() {
		GinkgoHelper()

		heartbeat()

		for _, m := range declared {
			_, err := machineReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: m.name}})
			Expect(err).NotTo(HaveOccurred())
		}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
	}

	// mutations counts what was sent to Talos that changes something.
	mutations := func() int {
		n := 0

		for _, method := range []string{
			fake.MethodApplyConfiguration, fake.MethodMaintenanceApply, fake.MethodReset, fake.MethodEtcdRemoveMember, fake.MethodReboot,
		} {
			for _, c := range scenario.CallsFor(method) {
				if !c.DryRun {
					n++
				}
			}
		}

		return n
	}

	settle := func(stop func() bool) {
		GinkgoHelper()

		for range 40 {
			version, sent := tc.ResourceVersion, mutations()

			round()

			if stop != nil && stop() {
				return
			}

			if tc.ResourceVersion == version && mutations() == sent {
				return
			}
		}

		Fail("the controllers never settled")
	}

	runEvery := func(step time.Duration, what string, done func() bool) {
		GinkgoHelper()

		for range 120 {
			if done() {
				return
			}

			clock = clock.Add(step)
			settle(done)
		}

		Fail(fmt.Sprintf("never happened: %s; operation: %+v; RolloutBlocked: %+v; MembershipCurrent: %+v", what, tc.Status.Operation,
			condition(tc, talosv1alpha1.ConditionRolloutBlocked), condition(tc, talosv1alpha1.ConditionMembershipCurrent)))
	}

	runUntil := func(what string, done func() bool) {
		GinkgoHelper()

		runEvery(10*time.Second, what, done)
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

	get := func(m member) *talosv1alpha1.TalosMachine {
		GinkgoHelper()

		tm := &talosv1alpha1.TalosMachine{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: m.name}, tm)).To(Succeed())

		return tm
	}

	remove := func(m member) {
		GinkgoHelper()

		tm := get(m)
		tm.Spec.DesiredState = talosv1alpha1.DesiredStateRemoved
		Expect(k8sClient.Update(ctx, tm)).To(Succeed())
	}

	machineCondition := func(m member, kind string) *metav1.Condition {
		GinkgoHelper()

		cond := apimeta.FindStatusCondition(get(m).Status.Conditions, kind)
		if cond == nil {
			return &metav1.Condition{}
		}

		return cond
	}

	sent := func(method string, m member) int {
		n := 0

		for _, c := range scenario.CallsFor(method) {
			if c.Node == m.addr {
				n++
			}
		}

		return n
	}

	hasNode := func(m member) bool {
		GinkgoHelper()

		err := k8sClient.Get(ctx, client.ObjectKey{Name: m.name}, &corev1.Node{})
		if err != nil {
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "%v", err)
		}

		return err == nil
	}

	operation := func() string {
		if tc.Status.Operation == nil {
			return ""
		}

		return string(tc.Status.Operation.Type)
	}

	stepIs := func(step string) func() bool {
		return func() bool {
			op := tc.Status.Operation
			return op != nil && op.Current != nil && op.Current.Step == step
		}
	}

	halted := func(reason string) func() bool {
		return func() bool {
			op := tc.Status.Operation

			return op != nil && op.Failure != nil && op.Failure.Reason == reason
		}
	}

	current := func() *metav1.Condition {
		cond := condition(tc, talosv1alpha1.ConditionMembershipCurrent)
		if cond == nil {
			return &metav1.Condition{}
		}

		return cond
	}

	done := func() bool { return tc.Status.Operation == nil && current().Status == metav1.ConditionTrue }

	// start brings up a cluster of the given machines, adopted, with config and membership managed.
	start := func(machines ...member) {
		GinkgoHelper()

		fakes := make([]fake.Node, 0, len(machines))
		for _, m := range machines {
			fakes = append(fakes, fakeOf(m))
		}

		scenario = fake.NewScenario(fakes...)

		for _, m := range machines {
			kubelet(m)
		}

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: credentialsSecret, Namespace: ns},
			Data:       map[string][]byte{creds.DefaultTalosconfigKey: talostest.AdminTalosconfig(GinkgoT(), cpAddr.String())},
		})).To(Succeed())

		tc = &talosv1alpha1.TalosCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns},
			Spec: talosv1alpha1.TalosClusterSpec{
				Credentials: secretCredentials(),
				Rollout:     talosv1alpha1.Rollout{Soak: &metav1.Duration{Duration: 20 * time.Second}},
			},
		}
		reproduceLiveConfigs(tc)
		Expect(k8sClient.Create(ctx, tc)).To(Succeed())
		createPools(ns)

		evictor, err := NewPodEvictor(cfg)
		Expect(err).NotTo(HaveOccurred())

		reconciler = &TalosClusterReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), APIReader: k8sClient, Evictor: evictor,
			Resolver:         &target.Resolver{Client: k8sClient, Factory: &fake.Factory{Scenario: scenario}},
			Recorder:         events.NewFakeRecorder(4096),
			Now:              func() time.Time { return clock },
			KubernetesChecks: KubernetesCheckerFunc(func(context.Context, string, string) ([]string, error) { return nil, nil }),
		}
		machineReconciler = &TalosMachineReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Resolver: reconciler.Resolver}

		for _, m := range machines {
			declare(m, nil)
		}

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
		Expect(err).NotTo(HaveOccurred())

		settle(nil)
		Expect(condition(tc, talosv1alpha1.ConditionConfigProven).Status).To(Equal(metav1.ConditionTrue), "the cluster must adopt cleanly")

		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Management.Config = talosv1alpha1.ConfigManagementManage
			tc.Spec.Management.Membership = talosv1alpha1.CapabilityEnabled
		})
		settle(nil)

		Expect(condition(tc, talosv1alpha1.ConditionConfigHandover).Status).To(Equal(metav1.ConditionTrue))
		Expect(current().Status).To(Equal(metav1.ConditionTrue), current().Message)
		Expect(mutations()).To(BeZero(), "adopting and taking over must send nothing")
	}

	// boots is the machine coming up after it took its config: as what it was declared to be.
	boots := func(m member) {
		GinkgoHelper()

		Expect(scenario.Boot(m.addr)).To(BeTrue(), m.name+" must be on its way up")
		kubelet(m)
	}

	BeforeEach(func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "membership-"}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name

		clock = time.Now().Truncate(time.Second)
		declared = nil

		DeferCleanup(func() {
			Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
			Expect(k8sClient.DeleteAllOf(ctx, &coordinationv1.Lease{}, client.InNamespace(nodeLeaseNamespace))).To(Succeed())
		})
	})

	It("adds a worker that waits in maintenance mode, and touches nothing else", func() {
		start(cpA, wOne)
		waiting(wTwo, nil)
		declare(wTwo, nil)

		runUntil("the machine is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, wTwo) == 1 })
		Expect(operation()).To(Equal(string(talosv1alpha1.OperationAddMachine)))
		Expect(tc.Status.Operation.Plan).To(Equal([]string{wTwo.name}))
		Expect(get(wTwo).Status.Phase).To(BeElementOf(talosv1alpha1.MachinePhase("Provisioning"), talosv1alpha1.MachinePhase("Joining")))

		// It installs itself: nothing answers, and nothing is sent again.
		clock = clock.Add(5 * time.Minute)
		settle(nil)
		Expect(stepIs(plan.StepJoining)()).To(BeTrue())
		Expect(sent(fake.MethodMaintenanceApply, wTwo)).To(Equal(1))

		boots(wTwo)
		runUntil("the machine is a member", done)

		tm := get(wTwo)
		Expect(tm.Status.JoinedAt).NotTo(BeNil())
		Expect(tm.Status.Phase).To(Equal(talosv1alpha1.MachinePhase("Ready")))
		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(tc.Status.History[0].Completed).To(Equal([]string{wTwo.name}))
		Expect(condition(tc, talosv1alpha1.ConditionConfigInSync).Status).To(Equal(metav1.ConditionTrue))

		// The one thing that was sent anywhere is the machine's config, over the maintenance connection.
		Expect(mutations()).To(Equal(1))
		Expect(scenario.Node(wTwo.addr).Config.Active).NotTo(BeEmpty())
	})

	It("sends nothing to a machine that is not the one that was declared", func() {
		start(cpA, wOne)
		waiting(wTwo, func(n *fake.Node) { n.SystemInformation.UUID = other })
		declare(wTwo, nil)

		clock = clock.Add(time.Minute)
		settle(nil)

		cond := machineCondition(wTwo, talosv1alpha1.ConditionProvisionable)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonIdentityMismatch), cond.Message)
		Expect(get(wTwo).Status.Phase).To(Equal(talosv1alpha1.MachinePhase("AwaitingIdentity")))

		Expect(operation()).To(BeEmpty())
		Expect(current().Reason).To(Equal(talosv1alpha1.ReasonAwaitingMachine), current().Message)
		Expect(mutations()).To(BeZero())
	})

	It("halts without sending anything when the machine turns out to be another one in its turn", func() {
		start(cpA, wOne)
		waiting(wTwo, nil)
		declare(wTwo, nil)

		// It is the right machine when it is looked at, and another one answers when its turn comes.
		runUntil("the turn begins", stepIs(plan.StepProvisioning))
		scenario.UpdateNode(wTwo.addr, func(n *fake.Node) { n.SystemInformation.UUID = other })

		runUntil("the operation halts", halted(plan.FailIdentity))
		Expect(mutations()).To(BeZero())
		Expect(condition(tc, talosv1alpha1.ConditionRolloutBlocked).Message).To(ContainSubstring("sent nothing"))
	})

	It("does not hand over a config that does not fit the machine", func() {
		start(cpA, wOne)
		waiting(wTwo, func(n *fake.Node) {
			n.FitProblems = []string{"the install disk selector matches 2 disks (/dev/sda, /dev/sdb)"}
		})
		declare(wTwo, nil)

		clock = clock.Add(time.Minute)
		settle(nil)

		cond := machineCondition(wTwo, talosv1alpha1.ConditionProvisionable)
		Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonConfigDoesNotFit), cond.Message)
		Expect(cond.Message).To(ContainSubstring("matches 2 disks"))
		Expect(operation()).To(BeEmpty())
		Expect(mutations()).To(BeZero())
	})

	It("asks a machine nothing while membership is not enabled", func() {
		start(cpA, wOne)
		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Management.Membership = talosv1alpha1.CapabilityDisabled })

		waiting(wTwo, nil)
		declare(wTwo, nil)
		remove(wOne)

		clock = clock.Add(time.Minute)
		settle(nil)

		Expect(machineCondition(wTwo, talosv1alpha1.ConditionProvisionable).Reason).To(Equal(talosv1alpha1.ReasonMembershipDisabled))
		Expect(scenario.CallsFor(fake.MethodMaintenanceProbe)).To(BeEmpty(), "a machine in maintenance mode is not even asked who it is")
		Expect(operation()).To(BeEmpty())
		Expect(current().Reason).To(Equal(talosv1alpha1.ReasonMembershipDisabled), current().Message)
		Expect(mutations()).To(BeZero())
	})

	It("does not hold a rollout up for a machine that has not joined, and adds it once the cluster is at rest", func() {
		start(cpA, wOne)
		declare(wTwo, nil)

		// Nothing answers at its address yet. A change to every machine's config rolls out all the same.
		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: labelConfig("one")}}
		})
		runUntil("the rollout is done", func() bool {
			return tc.Status.Operation == nil && len(tc.Status.History) == 1 &&
				condition(tc, talosv1alpha1.ConditionConfigInSync).Status == metav1.ConditionTrue
		})
		Expect(tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationConfigRollout))
		Expect(tc.Status.History[0].Completed).To(ConsistOf(cpA.name, wOne.name))
		Expect(current().Reason).To(Equal(talosv1alpha1.ReasonAwaitingMachine))

		// The machine shows up while another change is on its way: it waits for that.
		waiting(wTwo, nil)
		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: labelConfig("two")}}
		})
		runUntil("the second rollout is under way", func() bool { return operation() == string(talosv1alpha1.OperationConfigRollout) })
		Expect(sent(fake.MethodMaintenanceApply, wTwo)).To(BeZero())

		runUntil("the machine is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, wTwo) == 1 })
		Expect(tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationConfigRollout), "the rollout went first")
		Expect(string(scenario.Node(wTwo.addr).Config.Active)).To(ContainSubstring("rollout: two"), "it joins on what its peers run")

		boots(wTwo)
		runUntil("the machine is a member", done)
	})

	It("adds a control plane, and is not done until it is a voting member of etcd", func() {
		start(cpA, cpB, cpC, wOne)
		waiting(cpD, func(n *fake.Node) { n.EtcdLearner = true })
		declare(cpD, nil)

		runUntil("the machine is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, cpD) == 1 })
		boots(cpD)

		// It is up, its Node is Ready, and it is a learner still.
		runEvery(10*time.Second, "the machine is up", func() bool { return get(cpD).Status.JoinedAt != nil })
		clock = clock.Add(2 * time.Minute)
		settle(nil)
		Expect(stepIs(plan.StepJoining)()).To(BeTrue(), "%+v", tc.Status.Operation.Current)
		Expect(current().Message).To(ContainSubstring("voting member"))

		scenario.UpdateNode(cpD.addr, func(n *fake.Node) { n.EtcdLearner = false })
		runUntil("the control plane is a member", done)

		Expect(tc.Status.Observed.EtcdMembers).To(HaveLen(4))
		Expect(mutations()).To(Equal(1))
	})

	It("halts when a machine took its config and never came up, and does not add again what a human aborted", func() {
		start(cpA, wOne)
		waiting(wTwo, nil)
		declare(wTwo, nil)

		runUntil("the machine is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, wTwo) == 1 })

		clock = clock.Add(plan.JoinTimeoutFactor*plan.DefaultStepTimeout - time.Minute)
		settle(nil)
		Expect(tc.Status.Operation.Failure).To(BeNil(), "a machine has three step timeouts to install itself")

		runEvery(30*time.Second, "the operation halts", halted(plan.FailNotJoined))

		annotate(talosv1alpha1.AnnotationAbort, tc.Status.Operation.Failure.Token)
		runUntil("the operation ends", func() bool { return tc.Status.Operation == nil })
		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))

		// The machine is back in maintenance mode, and is left there.
		scenario.UpdateNode(wTwo.addr, func(n *fake.Node) { n.Maintenance, n.Config = true, talos.MachineConfigs{} })
		Expect(scenario.Boot(wTwo.addr)).To(BeTrue())

		clock = clock.Add(10 * time.Minute)
		settle(nil)
		Expect(operation()).To(BeEmpty())
		Expect(sent(fake.MethodMaintenanceApply, wTwo)).To(Equal(1))
		Expect(tc.Status.Membership.Add).To(HaveKey(wTwo.name))
	})

	It("removes a worker: drained, reset, seen to have forgotten the cluster, and its Node deleted", func() {
		start(cpA, wOne, wTwo)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: ns},
			Spec: corev1.PodSpec{
				NodeName: wTwo.name, TerminationGracePeriodSeconds: new(int64),
				Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/app:1"}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		remove(wTwo)

		runUntil("the node is reset", func() bool { return sent(fake.MethodReset, wTwo) == 1 })
		Expect(operation()).To(Equal(string(talosv1alpha1.OperationRemoveMachine)))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod))).To(BeTrue(), "its pods were evicted first")
		Expect(hasNode(wTwo)).To(BeTrue(), "its Node stays until the machine is seen to be gone")

		// It is wiping itself and rebooting: nothing answers, and nothing is concluded.
		clock = clock.Add(2 * time.Minute)
		settle(nil)
		Expect(stepIs(plan.StepLeaving)()).To(BeTrue())
		Expect(hasNode(wTwo)).To(BeTrue())

		Expect(scenario.Boot(wTwo.addr)).To(BeTrue())
		runUntil("the machine is gone", done)

		Expect(hasNode(wTwo)).To(BeFalse())
		Expect(sent(fake.MethodReset, wTwo)).To(Equal(1), "a machine is reset once")
		Expect(mutations()).To(Equal(1))

		tm := get(wTwo)
		Expect(tm.Status.RemovedAt).NotTo(BeNil())
		Expect(tm.Status.Phase).To(Equal(talosv1alpha1.MachinePhase("Removed")))
		Expect(tc.Status.History[0].Completed).To(Equal([]string{wTwo.name}))

		// What is left is a cluster of two, which goes on as one: a rollout does not wait for it.
		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: labelConfig("after")}}
		})
		runUntil("a rollout to the rest is done", func() bool {
			return tc.Status.Operation == nil && condition(tc, talosv1alpha1.ConditionConfigInSync).Status == metav1.ConditionTrue &&
				tc.Status.History[0].Type == talosv1alpha1.OperationConfigRollout
		})
		Expect(tc.Status.History[0].Completed).To(ConsistOf(cpA.name, wOne.name))
	})

	It("replaces a control plane: the old one stays until the new one is a member", func() {
		start(cpA, cpB, cpC, wOne)

		// One commit: the new machine is declared and the old one is to go.
		waiting(cpD, nil)
		declare(cpD, nil)
		remove(cpC)

		runUntil("the new control plane is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, cpD) == 1 })
		Expect(sent(fake.MethodReset, cpC)).To(BeZero())
		Expect(scenario.Node(cpC.addr).EtcdMemberID).NotTo(BeZero(), "three voting members while the fourth is on its way")

		boots(cpD)
		runUntil("the old control plane is reset", func() bool { return sent(fake.MethodReset, cpC) == 1 })
		Expect(tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationAddMachine), "the new one joined first")
		Expect(get(cpD).Status.JoinedAt).NotTo(BeNil())

		Expect(scenario.Boot(cpC.addr)).To(BeTrue())
		runUntil("the old control plane is gone", done)

		Expect(hasNode(cpC)).To(BeFalse())
		Expect(tc.Status.Observed.EtcdMembers).To(HaveLen(3))

		hostnames := make([]string, 0, 3)
		for _, m := range tc.Status.Observed.EtcdMembers {
			hostnames = append(hostnames, m.Hostname)
		}

		slices.Sort(hostnames)
		Expect(hostnames).To(Equal([]string{cpA.name, cpB.name, cpD.name}))
	})

	It("refuses to take a control plane out of three", func() {
		start(cpA, cpB, cpC, wOne)
		remove(cpC)

		clock = clock.Add(10 * time.Minute)
		settle(nil)

		blocked := condition(tc, talosv1alpha1.ConditionRolloutBlocked)
		Expect(blocked).NotTo(BeNil())
		Expect(blocked.Reason).To(Equal(plan.BlockQuorumAtRisk), blocked.Message)
		Expect(blocked.Message).To(ContainSubstring("add its replacement first"))
		Expect(operation()).To(BeEmpty())
		Expect(mutations()).To(BeZero())

		kube := &corev1.Node{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: cpC.name}, kube)).To(Succeed())
		Expect(kube.Spec.Unschedulable).To(BeFalse(), "a node that may not leave is not cordoned either")
	})

	It("halts when the machine does not answer, and takes a dead worker out once a human says it is dead", func() {
		start(cpA, wOne, wTwo)
		scenario.Kill(wTwo.addr)
		remove(wTwo)

		runEvery(time.Minute, "the removal halts", halted(plan.FailNodeGone))
		Expect(mutations()).To(BeZero())
		Expect(hasNode(wTwo)).To(BeTrue())

		kube := &corev1.Node{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: wTwo.name}, kube)).To(Succeed())
		Expect(kube.Spec.Unschedulable).To(BeFalse(), "nothing was taken from the cluster")
		Expect(tc.Status.Operation.Failure.Remediation).To(ContainSubstring(talosv1alpha1.AnnotationConfirmDead))

		// Until then it was asked whether it had left by itself. From here on it is asked nothing.
		asked := len(scenario.CallsFor(fake.MethodMaintenanceProbe))

		annotate(talosv1alpha1.AnnotationConfirmDead, tc.Status.Operation.Failure.Token)
		runUntil("the machine is taken out", done)

		Expect(hasNode(wTwo)).To(BeFalse())
		Expect(get(wTwo).Status.RemovedAt).NotTo(BeNil())
		Expect(mutations()).To(BeZero(), "nothing on a dead machine is wiped")
		Expect(scenario.CallsFor(fake.MethodMaintenanceProbe)).To(HaveLen(asked), "a dead machine is asked nothing")

		// The cluster's discovery still lists it. It does not count as a member that cannot be read.
		Expect(condition(tc, talosv1alpha1.ConditionTalosAPIReachable).Status).To(Equal(metav1.ConditionTrue))
	})

	It("takes the etcd member of a dead control plane out through another one", func() {
		start(cpA, cpB, cpC, cpD, wOne)
		scenario.Kill(cpD.addr)
		remove(cpD)

		runEvery(time.Minute, "the removal halts", halted(plan.FailNodeGone))
		Expect(scenario.CallsFor(fake.MethodEtcdRemoveMember)).To(BeEmpty(), "not before a human said so")

		annotate(talosv1alpha1.AnnotationConfirmDead, tc.Status.Operation.Failure.Token)
		runUntil("the machine is taken out", done)

		removals := scenario.CallsFor(fake.MethodEtcdRemoveMember)
		Expect(removals).To(HaveLen(1))
		Expect(removals[0].Node).NotTo(Equal(cpD.addr), "through a control plane that answers")
		Expect(scenario.Node(cpD.addr).EtcdMemberID).To(BeZero())
		Expect(sent(fake.MethodReset, cpD)).To(BeZero())
		Expect(hasNode(cpD)).To(BeFalse())
		Expect(tc.Status.Observed.EtcdMembers).To(HaveLen(3))
	})

	It("halts when a machine that was reset is not seen to have forgotten the cluster", func() {
		start(cpA, wOne, wTwo)
		remove(wTwo)

		runUntil("the node is reset", func() bool { return sent(fake.MethodReset, wTwo) == 1 })

		// It never comes back.
		runEvery(time.Minute, "the removal halts", halted(plan.FailNotWiped))
		Expect(hasNode(wTwo)).To(BeTrue(), "its Node is not deleted on a guess")
		Expect(strings.ToLower(tc.Status.Operation.Failure.Message)).To(ContainSubstring("secrets"))

		// It comes back after all, and a retry sees it.
		Expect(scenario.Boot(wTwo.addr)).To(BeTrue())
		annotate(talosv1alpha1.AnnotationRetry, tc.Status.Operation.Failure.Token)
		runUntil("the machine is gone", done)

		Expect(hasNode(wTwo)).To(BeFalse())
		Expect(sent(fake.MethodReset, wTwo)).To(Equal(1), "a machine that forgot the cluster is not reset again")
	})
})
