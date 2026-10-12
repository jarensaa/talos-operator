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
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
)

// memberAt is a machine whose Node has the machine's name.
func memberAt(name string, last int, role talosv1alpha1.MachineRole, etcd uint64) member {
	return member{
		name: name, node: name, addr: netip.AddrFrom4([4]byte{10, 5, 0, byte(last)}),
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
	var h *harness

	// waiting puts a machine on the network that has no config and waits for one.
	waiting := func(m member, mutate func(*fake.Node)) {
		n := m.fakeNode()
		n.Maintenance, n.Config = true, talos.MachineConfigs{}

		if mutate != nil {
			mutate(&n)
		}

		h.scenario.Add(n)
	}

	// mutations counts what was sent to Talos that changes something.
	mutations := func() int {
		n := 0

		for _, method := range []string{
			fake.MethodApplyConfiguration, fake.MethodMaintenanceApply, fake.MethodReset, fake.MethodEtcdRemoveMember, fake.MethodReboot,
		} {
			for _, c := range h.scenario.CallsFor(method) {
				if !c.DryRun {
					n++
				}
			}
		}

		return n
	}

	remove := func(m member) {
		GinkgoHelper()

		tm := h.machine(m.name)
		tm.Spec.DesiredState = talosv1alpha1.DesiredStateRemoved
		Expect(k8sClient.Update(ctx, tm)).To(Succeed())
	}

	machineCondition := func(m member, kind string) *metav1.Condition {
		GinkgoHelper()

		cond := apimeta.FindStatusCondition(h.machine(m.name).Status.Conditions, kind)
		if cond == nil {
			return &metav1.Condition{}
		}

		return cond
	}

	sent := func(method string, m member) int {
		n := 0

		for _, c := range h.scenario.CallsFor(method) {
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
		if h.tc.Status.Operation == nil {
			return ""
		}

		return string(h.tc.Status.Operation.Type)
	}

	stepIs := func(step string) func() bool {
		return func() bool {
			op := h.tc.Status.Operation
			return op != nil && op.Current != nil && op.Current.Step == step
		}
	}

	halted := func(reason string) func() bool {
		return func() bool {
			op := h.tc.Status.Operation

			return op != nil && op.Failure != nil && op.Failure.Reason == reason
		}
	}

	current := func() *metav1.Condition {
		cond := condition(h.tc, talosv1alpha1.ConditionMembershipCurrent)
		if cond == nil {
			return &metav1.Condition{}
		}

		return cond
	}

	done := func() bool { return h.tc.Status.Operation == nil && current().Status == metav1.ConditionTrue }

	// start brings up a cluster of the given machines, adopted, with config and membership managed.
	start := func(machines ...member) {
		GinkgoHelper()

		h.start(func(m *talosv1alpha1.Management) { m.Membership = talosv1alpha1.CapabilityEnabled }, machines...)
		Expect(current().Status).To(Equal(metav1.ConditionTrue), current().Message)
	}

	// boots is the machine coming up after it took its config: as what it was declared to be.
	boots := func(m member) {
		GinkgoHelper()

		Expect(h.scenario.Boot(m.addr)).To(BeTrue(), m.name+" must be on its way up")
		h.register(m)
	}

	BeforeEach(func() {
		h = newHarness("membership-", 120, talosv1alpha1.ConditionRolloutBlocked, talosv1alpha1.ConditionMembershipCurrent)
		h.sent = mutations
	})

	It("adds a worker that waits in maintenance mode, and touches nothing else", func() {
		start(cpA, wOne)
		waiting(wTwo, nil)
		h.declare(wTwo)

		h.runUntil("the machine is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, wTwo) == 1 })
		Expect(operation()).To(Equal(string(talosv1alpha1.OperationAddMachine)))
		Expect(h.tc.Status.Operation.Plan).To(Equal([]string{wTwo.name}))
		Expect(h.machine(wTwo.name).Status.Phase).To(
			BeElementOf(talosv1alpha1.MachinePhase("Provisioning"), talosv1alpha1.MachinePhase("Joining")))

		// It installs itself: nothing answers, and nothing is sent again.
		h.clock = h.clock.Add(5 * time.Minute)
		h.settle(nil)
		Expect(stepIs(plan.StepJoining)()).To(BeTrue())
		Expect(sent(fake.MethodMaintenanceApply, wTwo)).To(Equal(1))

		boots(wTwo)
		h.runUntil("the machine is a member", done)

		tm := h.machine(wTwo.name)
		Expect(tm.Status.JoinedAt).NotTo(BeNil())
		Expect(tm.Status.Phase).To(Equal(talosv1alpha1.MachinePhase("Ready")))
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(h.tc.Status.History[0].Completed).To(Equal([]string{wTwo.name}))
		Expect(condition(h.tc, talosv1alpha1.ConditionConfigInSync).Status).To(Equal(metav1.ConditionTrue))

		// The one thing that was sent anywhere is the machine's config, over the maintenance connection.
		Expect(mutations()).To(Equal(1))
		Expect(h.scenario.Node(wTwo.addr).Config.Active).NotTo(BeEmpty())
	})

	It("sends nothing to a machine that is not the one that was declared", func() {
		start(cpA, wOne)
		waiting(wTwo, func(n *fake.Node) { n.SystemInformation.UUID = other })
		h.declare(wTwo)

		h.clock = h.clock.Add(time.Minute)
		h.settle(nil)

		cond := machineCondition(wTwo, talosv1alpha1.ConditionProvisionable)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonIdentityMismatch), cond.Message)
		Expect(h.machine(wTwo.name).Status.Phase).To(Equal(talosv1alpha1.MachinePhase("AwaitingIdentity")))

		Expect(operation()).To(BeEmpty())
		Expect(current().Reason).To(Equal(talosv1alpha1.ReasonAwaitingMachine), current().Message)
		Expect(mutations()).To(BeZero())
	})

	It("halts without sending anything when the machine turns out to be another one in its turn", func() {
		start(cpA, wOne)
		waiting(wTwo, nil)
		h.declare(wTwo)

		// It is the right machine when it is looked at, and another one answers when its turn comes.
		h.runUntil("the turn begins", stepIs(plan.StepProvisioning))
		h.scenario.UpdateNode(wTwo.addr, func(n *fake.Node) { n.SystemInformation.UUID = other })

		h.runUntil("the operation halts", halted(plan.FailIdentity))
		Expect(mutations()).To(BeZero())
		Expect(condition(h.tc, talosv1alpha1.ConditionRolloutBlocked).Message).To(ContainSubstring("sent nothing"))
	})

	It("does not hand over a config that does not fit the machine", func() {
		start(cpA, wOne)
		waiting(wTwo, func(n *fake.Node) {
			n.FitProblems = []string{"the install disk selector matches 2 disks (/dev/sda, /dev/sdb)"}
		})
		h.declare(wTwo)

		h.clock = h.clock.Add(time.Minute)
		h.settle(nil)

		cond := machineCondition(wTwo, talosv1alpha1.ConditionProvisionable)
		Expect(cond.Reason).To(Equal(talosv1alpha1.ReasonConfigDoesNotFit), cond.Message)
		Expect(cond.Message).To(ContainSubstring("matches 2 disks"))
		Expect(operation()).To(BeEmpty())
		Expect(mutations()).To(BeZero())
	})

	It("asks a machine nothing while membership is not enabled", func() {
		start(cpA, wOne)
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Management.Membership = talosv1alpha1.CapabilityDisabled })

		waiting(wTwo, nil)
		h.declare(wTwo)
		remove(wOne)

		h.clock = h.clock.Add(time.Minute)
		h.settle(nil)

		Expect(machineCondition(wTwo, talosv1alpha1.ConditionProvisionable).Reason).To(Equal(talosv1alpha1.ReasonMembershipDisabled))
		Expect(h.scenario.CallsFor(fake.MethodMaintenanceProbe)).To(BeEmpty(), "a machine in maintenance mode is not even asked who it is")
		Expect(operation()).To(BeEmpty())
		Expect(current().Reason).To(Equal(talosv1alpha1.ReasonMembershipDisabled), current().Message)
		Expect(mutations()).To(BeZero())
	})

	It("does not hold a rollout up for a machine that has not joined, and adds it once the cluster is at rest", func() {
		start(cpA, wOne)
		h.declare(wTwo)

		// Nothing answers at its address yet. A change to every machine's config rolls out all the same.
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: labelConfig("one")}}
		})
		h.runUntil("the rollout is done", func() bool {
			return h.tc.Status.Operation == nil && len(h.tc.Status.History) == 1 &&
				condition(h.tc, talosv1alpha1.ConditionConfigInSync).Status == metav1.ConditionTrue
		})
		Expect(h.tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationConfigRollout))
		Expect(h.tc.Status.History[0].Completed).To(ConsistOf(cpA.name, wOne.name))
		Expect(current().Reason).To(Equal(talosv1alpha1.ReasonAwaitingMachine))

		// The machine shows up while another change is on its way: it waits for that.
		waiting(wTwo, nil)
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: labelConfig("two")}}
		})
		h.runUntil("the second rollout is under way", func() bool { return operation() == string(talosv1alpha1.OperationConfigRollout) })
		Expect(sent(fake.MethodMaintenanceApply, wTwo)).To(BeZero())

		h.runUntil("the machine is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, wTwo) == 1 })
		Expect(h.tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationConfigRollout), "the rollout went first")
		Expect(string(h.scenario.Node(wTwo.addr).Config.Active)).To(ContainSubstring("rollout: two"), "it joins on what its peers run")

		boots(wTwo)
		h.runUntil("the machine is a member", done)
	})

	It("adds a control plane, and is not done until it is a voting member of etcd", func() {
		start(cpA, cpB, cpC, wOne)
		waiting(cpD, func(n *fake.Node) { n.EtcdLearner = true })
		h.declare(cpD)

		h.runUntil("the machine is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, cpD) == 1 })
		boots(cpD)

		// It is up, its Node is Ready, and it is a learner still.
		h.runEvery(10*time.Second, "the machine is up", func() bool { return h.machine(cpD.name).Status.JoinedAt != nil })
		h.clock = h.clock.Add(2 * time.Minute)
		h.settle(nil)
		Expect(stepIs(plan.StepJoining)()).To(BeTrue(), "%+v", h.tc.Status.Operation.Current)
		Expect(current().Message).To(ContainSubstring("voting member"))

		h.scenario.UpdateNode(cpD.addr, func(n *fake.Node) { n.EtcdLearner = false })
		h.runUntil("the control plane is a member", done)

		Expect(h.tc.Status.Observed.EtcdMembers).To(HaveLen(4))
		Expect(mutations()).To(Equal(1))
	})

	It("halts when a machine took its config and never came up, and does not add again what a human aborted", func() {
		start(cpA, wOne)
		waiting(wTwo, nil)
		h.declare(wTwo)

		h.runUntil("the machine is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, wTwo) == 1 })

		h.clock = h.clock.Add(plan.JoinTimeoutFactor*plan.DefaultStepTimeout - time.Minute)
		h.settle(nil)
		Expect(h.tc.Status.Operation.Failure).To(BeNil(), "a machine has three step timeouts to install itself")

		h.runEvery(30*time.Second, "the operation halts", halted(plan.FailNotJoined))

		h.annotate(talosv1alpha1.AnnotationAbort, h.tc.Status.Operation.Failure.Token)
		h.runUntil("the operation ends", func() bool { return h.tc.Status.Operation == nil })
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))

		// The machine is back in maintenance mode, and is left there.
		h.scenario.UpdateNode(wTwo.addr, func(n *fake.Node) { n.Maintenance, n.Config = true, talos.MachineConfigs{} })
		Expect(h.scenario.Boot(wTwo.addr)).To(BeTrue())

		h.clock = h.clock.Add(10 * time.Minute)
		h.settle(nil)
		Expect(operation()).To(BeEmpty())
		Expect(sent(fake.MethodMaintenanceApply, wTwo)).To(Equal(1))
		Expect(h.tc.Status.Holds[talosv1alpha1.OperationAddMachine].Aborted).To(HaveKey(wTwo.name))
	})

	It("removes a worker: drained, reset, seen to have forgotten the cluster, and its Node deleted", func() {
		start(cpA, wOne, wTwo)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: h.ns},
			Spec: corev1.PodSpec{
				NodeName: wTwo.name, TerminationGracePeriodSeconds: new(int64),
				Containers: []corev1.Container{{Name: "main", Image: "registry.example.com/app:1"}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		remove(wTwo)

		h.runUntil("the node is reset", func() bool { return sent(fake.MethodReset, wTwo) == 1 })
		Expect(operation()).To(Equal(string(talosv1alpha1.OperationRemoveMachine)))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod))).To(BeTrue(), "its pods were evicted first")
		Expect(hasNode(wTwo)).To(BeTrue(), "its Node stays until the machine is seen to be gone")

		// It is wiping itself and rebooting: nothing answers, and nothing is concluded.
		h.clock = h.clock.Add(2 * time.Minute)
		h.settle(nil)
		Expect(stepIs(plan.StepLeaving)()).To(BeTrue())
		Expect(hasNode(wTwo)).To(BeTrue())

		Expect(h.scenario.Boot(wTwo.addr)).To(BeTrue())
		h.runUntil("the machine is gone", done)

		Expect(hasNode(wTwo)).To(BeFalse())
		Expect(sent(fake.MethodReset, wTwo)).To(Equal(1), "a machine is reset once")
		Expect(mutations()).To(Equal(1))

		tm := h.machine(wTwo.name)
		Expect(tm.Status.RemovedAt).NotTo(BeNil())
		Expect(tm.Status.Phase).To(Equal(talosv1alpha1.MachinePhase("Removed")))
		Expect(h.tc.Status.History[0].Completed).To(Equal([]string{wTwo.name}))

		// What is left is a cluster of two, which goes on as one: a rollout does not wait for it.
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: labelConfig("after")}}
		})
		h.runUntil("a rollout to the rest is done", func() bool {
			return h.tc.Status.Operation == nil && condition(h.tc, talosv1alpha1.ConditionConfigInSync).Status == metav1.ConditionTrue &&
				h.tc.Status.History[0].Type == talosv1alpha1.OperationConfigRollout
		})
		Expect(h.tc.Status.History[0].Completed).To(ConsistOf(cpA.name, wOne.name))
	})

	It("adds a machine again that was taken out, once it is asked to be present", func() {
		start(cpA, wOne, wTwo)
		remove(wTwo)

		h.runUntil("the node is reset", func() bool { return sent(fake.MethodReset, wTwo) == 1 })
		Expect(h.scenario.Boot(wTwo.addr)).To(BeTrue())
		h.runUntil("the machine is gone", done)

		// It waits in maintenance mode, as what it is, and is left there while it is to stay out.
		h.scenario.UpdateNode(wTwo.addr, func(n *fake.Node) {
			n.MachineType, n.Nodename, n.MachineStatus = fake.TypeWorker, wTwo.name, talos.MachineStatus{Stage: talos.StageRunning, Ready: true}
		})
		h.clock = h.clock.Add(10 * time.Minute)
		h.settle(nil)
		Expect(sent(fake.MethodMaintenanceApply, wTwo)).To(BeZero())
		Expect(machineCondition(wTwo, talosv1alpha1.ConditionProvisionable).Status).To(BeEmpty(), "a machine that is to stay out is not asked")

		tm := h.machine(wTwo.name)
		tm.Spec.DesiredState = talosv1alpha1.DesiredStatePresent
		Expect(k8sClient.Update(ctx, tm)).To(Succeed())

		h.runUntil("the machine is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, wTwo) == 1 })
		boots(wTwo)
		h.runUntil("the machine is a member again", done)

		tm = h.machine(wTwo.name)
		Expect(tm.Status.RemovedAt).To(BeNil())
		Expect(tm.Status.JoinedAt).NotTo(BeNil())
		Expect(tm.Status.Phase).To(Equal(talosv1alpha1.MachinePhase("Ready")))
	})

	It("replaces a control plane: the old one stays until the new one is a member", func() {
		start(cpA, cpB, cpC, wOne)

		// One commit: the new machine is declared and the old one is to go.
		waiting(cpD, nil)
		h.declare(cpD)
		remove(cpC)

		h.runUntil("the new control plane is handed its config", func() bool { return sent(fake.MethodMaintenanceApply, cpD) == 1 })
		Expect(sent(fake.MethodReset, cpC)).To(BeZero())
		Expect(h.scenario.Node(cpC.addr).EtcdMemberID).NotTo(BeZero(), "three voting members while the fourth is on its way")

		boots(cpD)
		h.runUntil("the old control plane is reset", func() bool { return sent(fake.MethodReset, cpC) == 1 })
		Expect(h.tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationAddMachine), "the new one joined first")
		Expect(h.machine(cpD.name).Status.JoinedAt).NotTo(BeNil())

		Expect(h.scenario.Boot(cpC.addr)).To(BeTrue())
		h.runUntil("the old control plane is gone", done)

		Expect(hasNode(cpC)).To(BeFalse())
		Expect(h.tc.Status.Observed.EtcdMembers).To(HaveLen(3))

		hostnames := make([]string, 0, 3)
		for _, m := range h.tc.Status.Observed.EtcdMembers {
			hostnames = append(hostnames, m.Hostname)
		}

		slices.Sort(hostnames)
		Expect(hostnames).To(Equal([]string{cpA.name, cpB.name, cpD.name}))
	})

	It("refuses to take a control plane out of three", func() {
		start(cpA, cpB, cpC, wOne)
		remove(cpC)

		h.clock = h.clock.Add(10 * time.Minute)
		h.settle(nil)

		blocked := condition(h.tc, talosv1alpha1.ConditionRolloutBlocked)
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
		h.scenario.Kill(wTwo.addr)
		remove(wTwo)

		h.runEvery(time.Minute, "the removal halts", halted(plan.FailNodeGone))
		Expect(mutations()).To(BeZero())
		Expect(hasNode(wTwo)).To(BeTrue())

		kube := &corev1.Node{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: wTwo.name}, kube)).To(Succeed())
		Expect(kube.Spec.Unschedulable).To(BeFalse(), "nothing was taken from the cluster")
		Expect(h.tc.Status.Operation.Failure.Remediation).To(ContainSubstring(talosv1alpha1.AnnotationConfirmDead))

		// Until then it was asked whether it had left by itself. From here on it is asked nothing.
		asked := len(h.scenario.CallsFor(fake.MethodMaintenanceProbe))

		h.annotate(talosv1alpha1.AnnotationConfirmDead, h.tc.Status.Operation.Failure.Token)
		h.runUntil("the machine is taken out", done)

		Expect(hasNode(wTwo)).To(BeFalse())
		Expect(h.machine(wTwo.name).Status.RemovedAt).NotTo(BeNil())
		Expect(mutations()).To(BeZero(), "nothing on a dead machine is wiped")
		Expect(h.scenario.CallsFor(fake.MethodMaintenanceProbe)).To(HaveLen(asked), "a dead machine is asked nothing")

		// The cluster's discovery still lists it. It does not count as a member that cannot be read.
		Expect(condition(h.tc, talosv1alpha1.ConditionTalosAPIReachable).Status).To(Equal(metav1.ConditionTrue))
	})

	It("takes the etcd member of a dead control plane out through another one", func() {
		start(cpA, cpB, cpC, cpD, wOne)
		h.scenario.Kill(cpD.addr)
		remove(cpD)

		h.runEvery(time.Minute, "the removal halts", halted(plan.FailNodeGone))
		Expect(h.scenario.CallsFor(fake.MethodEtcdRemoveMember)).To(BeEmpty(), "not before a human said so")

		h.annotate(talosv1alpha1.AnnotationConfirmDead, h.tc.Status.Operation.Failure.Token)
		h.runUntil("the machine is taken out", done)

		removals := h.scenario.CallsFor(fake.MethodEtcdRemoveMember)
		Expect(removals).To(HaveLen(1))
		Expect(removals[0].Node).NotTo(Equal(cpD.addr), "through a control plane that answers")
		Expect(h.scenario.Node(cpD.addr).EtcdMemberID).To(BeZero())
		Expect(sent(fake.MethodReset, cpD)).To(BeZero())
		Expect(hasNode(cpD)).To(BeFalse())
		Expect(h.tc.Status.Observed.EtcdMembers).To(HaveLen(3))
	})

	It("halts when a machine that was reset is not seen to have forgotten the cluster", func() {
		start(cpA, wOne, wTwo)
		remove(wTwo)

		h.runUntil("the node is reset", func() bool { return sent(fake.MethodReset, wTwo) == 1 })

		// It never comes back.
		h.runEvery(time.Minute, "the removal halts", halted(plan.FailNotWiped))
		Expect(hasNode(wTwo)).To(BeTrue(), "its Node is not deleted on a guess")
		Expect(strings.ToLower(h.tc.Status.Operation.Failure.Message)).To(ContainSubstring("secrets"))

		// It comes back after all, and a retry sees it.
		Expect(h.scenario.Boot(wTwo.addr)).To(BeTrue())
		h.annotate(talosv1alpha1.AnnotationRetry, h.tc.Status.Operation.Failure.Token)
		h.runUntil("the machine is gone", done)

		Expect(hasNode(wTwo)).To(BeFalse())
		Expect(sent(fake.MethodReset, wTwo)).To(Equal(1), "a machine that forgot the cluster is not reset again")
	})
})
