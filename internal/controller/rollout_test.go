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
	"net/netip"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	talosmachine "github.com/siderolabs/talos/pkg/machinery/config/machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
)

// The cluster the rollout specs manage: one control plane and two workers.
var (
	worker2Addr = netip.MustParseAddr("10.5.0.4")

	rolloutNodes = []member{
		{cp1Machine, "roll-cp-1", cpAddr, cpUUID, talosv1alpha1.MachineRoleControlPlane, 0xa1},
		{worker1, "roll-worker-1", workerAddr, workerUUID, talosv1alpha1.MachineRoleWorker, 0},
		{worker2, "roll-worker-2", worker2Addr, "aaaaaaaa-bbbb-4ccc-8ddd-000000000003", talosv1alpha1.MachineRoleWorker, 0},
	}
)

// The machines of the specs' cluster, and what the specs say went wrong with it.
const (
	cp1Machine = "cp-1"
	worker1    = "worker-1"
	worker2    = "worker-2"

	etcdUnhealthy = "etcd is unhealthy"
)

// tried and confirmed name the two things the operator sends a node, as applies lists them.
func tried(machine string) string     { return machine + " try" }
func confirmed(machine string) string { return machine + " confirm" }

// Config documents the specs roll out.
const (
	// A resolver the specs use to cut a node off: see cutOff.
	deadResolver = "10.99.99.99"

	resolverConfig = "apiVersion: v1alpha1\nkind: ResolverConfig\nnameservers:\n  - address: " + deadResolver + "\n"
	volumeConfig   = "apiVersion: v1alpha1\nkind: VolumeConfig\nname: EPHEMERAL\nprovisioning:\n  maxSize: 50GiB\n"
	tokenConfig    = "machine:\n  token: aaaaaa.bbbbbbbbbbbbbbbb\n"
	// What someone changes on a node by hand.
	editedLabel = "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  edited: \"true\"\n"
)

func labelConfig(value string) string {
	return "apiVersion: v1alpha1\nkind: KubeNodeConfig\nlabels:\n  rollout: " + value + "\n"
}

// rolloutHarness is the harness of the specs that change a running cluster node by node: config
// rollouts, upgrades of Talos and of Kubernetes, the policy all of them keep to, and what the
// operator knows of versions. Their cluster is rolloutNodes, adopted, with its config managed.
type rolloutHarness struct {
	*harness

	// silent kubelets renew no Lease although their node's Talos API answers.
	silent map[netip.Addr]bool
	// kubelets, if set, is what else happens in the cluster between two passes of the operator:
	// the controllers and kubelets a spec plays.
	kubelets func()
}

// startRollout brings the rollout specs' cluster up for one spec, adopted and taken over.
func startRollout() *rolloutHarness {
	GinkgoHelper()

	h := &rolloutHarness{
		harness: newHarness("rollout-", 80, talosv1alpha1.ConditionConfigInSync),
		silent:  map[netip.Addr]bool{},
	}
	h.heartbeat = h.kubeletsRun
	h.sent = func() int { return len(h.applies()) }

	h.start(nil, rolloutNodes...)
	Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)

	return h
}

func (h *rolloutHarness) node(machine string) member {
	GinkgoHelper()

	for _, n := range rolloutNodes {
		if n.name == machine {
			return n
		}
	}

	Fail("no node " + machine)

	return member{}
}

// kubeletsRun is the kubelets, and Kubernetes watching them: every node that can reach the API
// server renews its Lease, and a node whose kubelet is down (see sicken) is not Ready.
func (h *rolloutHarness) kubeletsRun() {
	GinkgoHelper()

	for _, n := range rolloutNodes {
		live := h.scenario.Node(n.addr)
		// A node that is rebooting has no kubelet either.
		sick := h.scenario.Down(n.addr) || (live.UnhealthyWhen != nil && live.UnhealthyWhen(live.Config.Active))

		// Kubernetes notices a kubelet that stopped: the Node is no longer Ready.
		kube := &corev1.Node{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: n.node}, kube)).To(Succeed())

		ready := corev1.ConditionTrue
		if sick {
			ready = corev1.ConditionFalse
		}

		if kube.Status.Conditions[0].Status != ready {
			kube.Status.Conditions[0].Status = ready
			Expect(k8sClient.Status().Update(ctx, kube)).To(Succeed())
		}

		if sick || h.silent[n.addr] || (live.UnreachableWhen != nil && live.UnreachableWhen(live.Config.Active)) {
			continue
		}

		lease := &coordinationv1.Lease{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nodeLeaseNamespace, Name: n.node}, lease)).To(Succeed())
		lease.Spec.RenewTime = &metav1.MicroTime{Time: h.clock}
		Expect(k8sClient.Update(ctx, lease)).To(Succeed())
	}

	if h.kubelets != nil {
		h.kubelets()
	}
}

// applies lists every config the operator really applied, in order, as "<machine> try" or
// "<machine> confirm".
func (h *rolloutHarness) applies() []string {
	var out []string

	for _, c := range h.scenario.CallsFor(fake.MethodApplyConfiguration) {
		if c.DryRun {
			continue
		}

		name := c.Node.String()

		for _, n := range rolloutNodes {
			if n.addr == c.Node {
				name = n.name
			}
		}

		switch c.Mode {
		case talos.ApplyTry:
			out = append(out, tried(name))
		case talos.ApplyNoReboot:
			out = append(out, confirmed(name))
		default:
			out = append(out, name+" unexpected")
		}
	}

	return out
}

func (h *rolloutHarness) operation() *talosv1alpha1.Operation { return h.tc.Status.Operation }
func (h *rolloutHarness) started() bool                       { return h.operation() != nil }
func (h *rolloutHarness) idle() bool                          { return h.operation() == nil }
func (h *rolloutHarness) failed() bool                        { return h.operation() != nil && h.operation().Failure != nil }

func (h *rolloutHarness) stepIs(machine, step string) func() bool {
	return func() bool {
		cur := h.tc.Status.Operation
		return cur != nil && cur.Current != nil && cur.Current.Machine == machine && cur.Current.Step == step
	}
}

func (h *rolloutHarness) inSync() *metav1.Condition {
	GinkgoHelper()

	c := condition(h.tc, talosv1alpha1.ConditionConfigInSync)
	Expect(c).NotTo(BeNil())

	return c
}

func (h *rolloutHarness) blocked() *metav1.Condition {
	return condition(h.tc, talosv1alpha1.ConditionRolloutBlocked)
}

// setPoolConfig sets the config layer of a role's pool.
func (h *rolloutHarness) setPoolConfig(role talosv1alpha1.MachineRole, documents ...string) {
	GinkgoHelper()

	pool := &talosv1alpha1.TalosMachinePool{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: h.ns, Name: poolFor(role)}, pool)).To(Succeed())

	pool.Spec.Config = nil
	for _, d := range documents {
		pool.Spec.Config = append(pool.Spec.Config, talosv1alpha1.ConfigSource{Inline: d})
	}

	Expect(k8sClient.Update(ctx, pool)).To(Succeed())
}

// cutOff makes a node unreachable for as long as its active config carries the dead resolver:
// its Talos API stops answering and its kubelet stops renewing its Lease.
func (h *rolloutHarness) cutOff(machine string) {
	h.scenario.UpdateNode(h.node(machine).addr, func(n *fake.Node) {
		n.UnreachableWhen = func(active []byte) bool { return bytes.Contains(active, []byte(deadResolver)) }
	})
}

// sicken makes a node unhealthy for as long as its active config contains marker, or for good
// if marker is empty: the node answers, but its kubelet is down. Talos reports the node not
// ready and fails the cluster's health check on it, and Kubernetes reports it not Ready.
func (h *rolloutHarness) sicken(machine, marker string) {
	h.scenario.UpdateNode(h.node(machine).addr, func(n *fake.Node) {
		n.UnhealthyWhen = func(active []byte) bool { return bytes.Contains(active, []byte(marker)) }
	})
}

// expireTry is Talos's own timer: it restores the config a try replaced.
func (h *rolloutHarness) expireTry(machine string) {
	GinkgoHelper()

	Expect(h.scenario.ExpireTry(h.node(machine).addr)).To(BeTrue(), "a try must be pending on "+machine)
}

var _ = Describe("Config rollout", func() {
	var h *rolloutHarness

	BeforeEach(func() { h = startRollout() })

	It("takes over every node's config as the baseline drift is measured from", func() {
		for _, n := range rolloutNodes {
			cfg := h.machine(n.name).Status.Config
			Expect(cfg.AppliedHash).To(Equal(cfg.LiveHash), n.name)
			Expect(cfg.AppliedHash).To(Equal(cfg.DesiredHash), n.name)
		}

		Expect(h.operation()).To(BeNil())
		Expect(h.blocked()).To(BeNil())
	})

	It("rolls a label out one node at a time, each confirmed only after staying healthy", func() {
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.settle(h.started)

		// Planning takes the lock and sends nothing.
		Expect(h.operation()).NotTo(BeNil())
		op := h.operation().DeepCopy()
		Expect(op.Type).To(Equal(talosv1alpha1.OperationConfigRollout))
		Expect(op.Plan).To(Equal([]string{worker1, worker2}))
		Expect(op.Targets).To(HaveLen(2))
		Expect(op.Bases).To(HaveLen(2))
		Expect(h.applies()).To(BeEmpty())
		Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonRollingOut))

		// The step is persisted before the try is sent.
		h.runUntil("worker-1 is being tried", h.stepIs(worker1, plan.StepTrying))
		Expect(h.applies()).To(BeEmpty())

		step := h.operation().Current
		Expect(step.PreviousHash).To(Equal(op.Bases[worker1]))
		Expect(step.DesiredHash).To(Equal(op.Targets[worker1]))
		Expect(step.Deadline.Time).To(BeTemporally("==", step.Since.Add(plan.DefaultTryTimeout)))
		Expect(step.Class).To(Equal("LiveSafe"))

		h.runUntil("worker-1 is verified", h.stepIs(worker1, plan.StepVerifying))
		Expect(h.applies()).To(Equal([]string{tried(worker1)}))
		Expect(h.scenario.TryPending(workerAddr)).To(BeTrue(), "the config is only tried, not persistent")

		h.runUntil("the rollout ends", h.idle)

		Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}),
			"one node at a time, a try never repeated, the control plane never touched")

		h.settle(nil)
		Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
		Expect(h.blocked()).To(BeNil())
		Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseManaged))

		Expect(h.tc.Status.History).To(HaveLen(1))
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(h.tc.Status.History[0].Completed).To(Equal([]string{worker1, worker2}))

		for _, name := range []string{worker1, worker2} {
			live := h.scenario.Node(h.node(name).addr)
			Expect(string(live.Config.Active)).To(ContainSubstring("rollout: one"))
			Expect(live.Config.Persistent).To(Equal(live.Config.Active), "confirmed configs are persistent")
			Expect(h.scenario.TryPending(h.node(name).addr)).To(BeFalse())

			cfg := h.machine(name).Status.Config
			Expect(cfg.AppliedHash).To(Equal(cfg.DesiredHash))
			Expect(cfg.LiveHash).To(Equal(cfg.DesiredHash))
		}

		Expect(drain(h.recorder)).To(ContainElements(
			ContainSubstring("RolloutStarted"), ContainSubstring("MachineConfigured"), ContainSubstring("RolloutSucceeded")))
	})

	It("rolls a cluster-wide change out to workers first, then the control plane", func() {
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: labelConfig("everywhere")}}
		})
		h.settle(h.started)
		Expect(h.operation().Plan).To(Equal([]string{worker1, worker2, cp1Machine}))

		h.runUntil("the rollout ends", h.idle)
		Expect(h.applies()).To(Equal([]string{
			tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2), tried(cp1Machine), confirmed(cp1Machine),
		}))

		h.settle(nil)
		Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
	})

	Context("a change that cuts a node off", func() {
		BeforeEach(func() {
			h.cutOff(worker1)
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, resolverConfig)
			h.runUntil("worker-1 is tried", func() bool { return len(h.applies()) > 0 })

			Expect(h.applies()).To(Equal([]string{tried(worker1)}))
			Expect(h.operation().Current.Class).To(Equal("Connectivity"))

			// The node went dark with the try, so the operator never even sees the config active.
			h.advance(time.Minute)
			Expect(h.operation().Current.Step).To(Equal(plan.StepTrying))
			Expect(h.inSync().Message).To(ContainSubstring("not reachable"))
		})

		// windowPasses lets the try window pass without the node ever answering.
		windowPasses := func() time.Time {
			GinkgoHelper()

			deadline := h.operation().Current.Deadline.Time

			h.at(deadline.Add(-time.Second))
			Expect(h.operation().Current.Step).To(Equal(plan.StepTrying), "it waits for the whole window")

			h.at(deadline)
			Expect(h.operation().Current.Step).To(Equal(plan.StepRollingBack))
			Expect(h.applies()).To(Equal([]string{tried(worker1)}))

			return deadline
		}

		// rollBack lets the window pass, then Talos revert, and waits for the operator to notice.
		rollBack := func() {
			GinkgoHelper()

			deadline := windowPasses()
			h.expireTry(worker1)

			h.at(deadline.Add(plan.RollbackGrace - time.Second))
			Expect(h.failed()).To(BeFalse(), "Talos gets a moment to revert")

			h.at(deadline.Add(plan.RollbackGrace))
			Expect(h.failed()).To(BeTrue())
		}

		It("is reverted by Talos, never confirmed, and halts the rollout before another node is touched", func() {
			rollBack()

			Expect(h.applies()).To(Equal([]string{tried(worker1)}), "no confirm, no second try, no other node")

			f := h.operation().Failure
			Expect(f.Machine).To(Equal(worker1))
			Expect(f.Reason).To(Equal(plan.FailRolledBack))
			Expect(f.Token).NotTo(BeEmpty())
			Expect(f.Remediation).To(ContainSubstring(talosv1alpha1.AnnotationRetry + "=" + f.Token))

			Expect(h.blocked().Status).To(Equal(metav1.ConditionTrue))
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
			Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
			Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))
			Expect(drain(h.recorder)).To(ContainElement(ContainSubstring("RolloutHalted")))

			live := h.scenario.Node(workerAddr)
			Expect(string(live.Config.Active)).NotTo(ContainSubstring(deadResolver), "the node is back on its previous config")
			Expect(h.machine(worker1).Status.Config.AppliedHash).To(Equal(h.operation().Bases[worker1]))

			// It stays halted, whatever time passes.
			h.advance(time.Hour)
			Expect(h.applies()).To(Equal([]string{tried(worker1)}))
			Expect(h.failed()).To(BeTrue())
		})

		It("is not confirmed by an operator that takes over in the middle of the try", func() {
			// A new process: nothing but status.operation tells it a try is in flight.
			h.takeOver()

			rollBack()

			Expect(h.applies()).To(Equal([]string{tried(worker1)}))
			Expect(h.operation().Failure.Reason).To(Equal(plan.FailRolledBack))
		})

		It("escalates when the node does not come back on its previous config", func() {
			deadline := windowPasses()

			// Talos's revert never shows: the node stays dark.
			h.at(deadline.Add(plan.RollbackGrace + plan.DefaultStepTimeout - time.Second))
			Expect(h.failed()).To(BeFalse(), "a node gets stepTimeout to come back")

			h.at(deadline.Add(plan.RollbackGrace + plan.DefaultStepTimeout))
			Expect(h.failed()).To(BeTrue())
			Expect(h.operation().Failure.Reason).To(Equal(plan.FailNodeUnreachableAfterRollback))
			Expect(h.applies()).To(Equal([]string{tried(worker1)}))
		})

		It("ignores an annotation that does not carry the failure's token", func() {
			rollBack()

			for _, a := range []string{talosv1alpha1.AnnotationRetry, talosv1alpha1.AnnotationSkipMachine, talosv1alpha1.AnnotationAbort} {
				h.annotate(a, "true")
			}

			h.advance(time.Minute)
			Expect(h.failed()).To(BeTrue())
			Expect(h.applies()).To(Equal([]string{tried(worker1)}))
		})

		It("runs the step again on retry, from what the node runs now", func() {
			rollBack()

			// Whatever cut the node off is fixed.
			h.scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.UnreachableWhen = nil })
			h.annotate(talosv1alpha1.AnnotationRetry, h.operation().Failure.Token)

			h.settle(func() bool { return !h.failed() })
			Expect(h.operation().Failure).To(BeNil())
			Expect(h.operation().Current.Attempt).To(Equal(int32(2)))
			Expect(h.operation().Current.Step).To(Equal(plan.StepTrying))

			h.runUntil("the rollout ends", h.idle)
			Expect(h.applies()).To(Equal([]string{tried(worker1), tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(drain(h.recorder)).To(ContainElement(ContainSubstring("RolloutResumed")))
		})

		It("leaves the machine out on skip-machine and goes on, and does not try the same config on it again", func() {
			rollBack()
			target := h.operation().Targets[worker1]

			h.annotate(talosv1alpha1.AnnotationSkipMachine, h.operation().Failure.Token)
			h.runUntil("the rollout ends", h.idle)

			Expect(h.applies()).To(Equal([]string{tried(worker1), tried(worker2), confirmed(worker2)}))
			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(h.tc.Status.History[0].Skipped).To(Equal([]string{worker1}))
			Expect(h.tc.Status.Holds[talosv1alpha1.OperationConfigRollout].Skipped).To(HaveKeyWithValue(worker1, target))

			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil(), "a skipped config is not rolled out to the machine again")
			Expect(h.inSync().Status).To(Equal(metav1.ConditionFalse))
			Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonHeld))
			Expect(h.inSync().Message).To(ContainSubstring(worker1))

			// A different config is a different matter.
			h.scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.UnreachableWhen = nil })
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("instead"))
			h.runUntil("the new config is rolled out", func() bool {
				return h.idle() && len(h.tc.Status.History) == 2
			})

			h.settle(nil)
			Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
			Expect(h.tc.Status.Holds).To(BeEmpty(), "a skip is spent once the machine renders another config")
		})

		It("ends the rollout on abort and does not start the same one again", func() {
			rollBack()

			h.annotate(talosv1alpha1.AnnotationAbort, h.operation().Failure.Token)
			h.settle(h.idle)

			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
			Expect(h.tc.Status.Holds[talosv1alpha1.OperationConfigRollout].Aborted).To(HaveLen(2))

			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil())
			Expect(h.applies()).To(Equal([]string{tried(worker1)}))
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonAborted))
			Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonAborted))
			Expect(drain(h.recorder)).To(ContainElement(ContainSubstring("RolloutAborted")))

			// Reverting the spec leaves nothing to roll out, and the abort is spent.
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker)
			h.advance(time.Minute)

			Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
			Expect(h.blocked()).To(BeNil())
			Expect(h.tc.Status.Holds).To(BeEmpty())
			Expect(h.applies()).To(Equal([]string{tried(worker1)}))
		})
	})

	It("does not confirm a node whose kubelet stopped renewing its Lease, though Talos and its Ready condition look fine", func() {
		h.silent[workerAddr] = true

		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("worker-1 is verified", h.stepIs(worker1, plan.StepVerifying))

		h.advance(2 * time.Minute)
		Expect(h.operation().Current.HealthySince).To(BeNil(), "the soak does not start without a heartbeat")
		Expect(h.inSync().Message).To(ContainSubstring("Lease"))

		// It stops hoping while Talos's own timer still runs, so a confirm can never come late.
		deadline := h.operation().Current.Deadline.Time

		h.at(deadline.Add(-plan.ConfirmMargin - time.Second))
		Expect(h.operation().Current.Step).To(Equal(plan.StepVerifying))

		h.at(deadline.Add(-plan.ConfirmMargin))
		Expect(h.operation().Current.Step).To(Equal(plan.StepRollingBack))

		// Even a node that turns healthy now is not confirmed any more.
		h.silent[workerAddr] = false
		h.at(deadline.Add(-time.Second))
		Expect(h.applies()).To(Equal([]string{tried(worker1)}))

		h.expireTry(worker1)
		h.at(deadline.Add(plan.RollbackGrace))
		Expect(h.operation().Failure.Reason).To(Equal(plan.FailRolledBack))
		Expect(h.applies()).To(Equal([]string{tried(worker1)}))
	})

	It("starts the soak over when a check fails in the middle of it", func() {
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("worker-1 soaks", func() bool {
			return h.stepIs(worker1, plan.StepVerifying)() && h.operation().Current.HealthySince != nil
		})

		first := h.operation().Current.HealthySince.Time

		h.scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{etcdUnhealthy}})
		h.advance(10 * time.Second)
		Expect(h.operation().Current.HealthySince).To(BeNil())
		Expect(h.applies()).To(Equal([]string{tried(worker1)}))

		h.scenario.SetHealth(talos.HealthResult{Healthy: true})
		h.runUntil("worker-1 soaks again", func() bool {
			return h.operation() != nil && h.operation().Current != nil && h.operation().Current.HealthySince != nil
		})
		Expect(h.operation().Current.HealthySince.Time).To(BeTemporally(">", first))

		h.runUntil("the rollout ends", h.idle)
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
	})

	It("refuses to change a protected field", func() {
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: tokenConfig}}
		})
		h.advance(time.Minute)

		Expect(h.operation()).To(BeNil())
		Expect(h.blocked().Status).To(Equal(metav1.ConditionTrue))
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonProtectedChange))
		Expect(h.blocked().Message).To(ContainSubstring("machine.token"))
		Expect(h.blocked().Message).NotTo(ContainSubstring("aaaaaa"), "the value of a protected field is never shown")
		Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonProtectedChange))
		Expect(h.machine(cp1Machine).Status.Config.ChangeClass).To(Equal("Protected"))
		Expect(drain(h.recorder)).To(ContainElement(ContainSubstring("RolloutBlocked is True")))

		h.advance(time.Hour)
		Expect(h.applies()).To(BeEmpty())

		// One blocked machine blocks the rollout for every machine.
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.advance(time.Minute)
		Expect(h.operation()).To(BeNil())
		Expect(h.applies()).To(BeEmpty())

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Config = nil })
		h.runUntil("the label is rolled out", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })
		Expect(h.applies()).To(HaveLen(4))
	})

	It("waits for a human to approve a storage change, by the hash of exactly that change", func() {
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, volumeConfig)
		h.advance(time.Minute)

		Expect(h.operation()).To(BeNil())
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonAwaitingApproval))

		approval := h.tc.Status.Approval
		Expect(approval).NotTo(BeNil())
		Expect(approval.Machines).To(HaveLen(2))
		Expect(approval.Machines[0]).To(And(ContainSubstring(worker1), ContainSubstring("Storage"), ContainSubstring("VolumeConfig")))
		Expect(h.blocked().Message).To(ContainSubstring(approval.Hash))
		Expect(h.machine(worker1).Status.Config.Diff).To(ContainSubstring("maxSize"), "the diff to review is on the machine")

		h.annotate(talosv1alpha1.AnnotationApproveDiff, "not-the-hash")
		h.advance(time.Minute)
		Expect(h.operation()).To(BeNil())
		Expect(h.applies()).To(BeEmpty())

		h.annotate(talosv1alpha1.AnnotationApproveDiff, approval.Hash)
		h.runUntil("the approved change is rolled out", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })

		Expect(h.applies()).To(HaveLen(4))
		Expect(h.tc.Status.Approval).To(BeNil())

		// The approval was for that change only.
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, strings.Replace(volumeConfig, "50GiB", "60GiB", 1))
		h.advance(time.Minute)
		Expect(h.operation()).To(BeNil())
		Expect(h.tc.Status.Approval.Hash).NotTo(Equal(approval.Hash))
		Expect(h.applies()).To(HaveLen(4))
	})

	Context("a node changed by hand", func() {
		BeforeEach(func() {
			edited := liveConfig(talosmachine.TypeWorker, editedLabel)
			h.scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Config.Active = edited })
		})

		It("is reported and left alone under driftPolicy Report", func() {
			h.advance(time.Hour)

			Expect(h.inSync().Status).To(Equal(metav1.ConditionFalse))
			Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonDrift))
			Expect(h.inSync().Message).To(ContainSubstring(worker1))
			Expect(h.blocked()).To(BeNil())
			Expect(h.operation()).To(BeNil())
			Expect(h.applies()).To(BeEmpty())
		})

		It("is put back under driftPolicy Enforce", func() {
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Rollout.DriftPolicy = talosv1alpha1.DriftPolicyEnforce })
			h.runUntil("the drift is undone", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })

			Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}))
			Expect(string(h.scenario.Node(workerAddr).Config.Active)).NotTo(ContainSubstring("edited"))

			h.settle(nil)
			Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
		})

		It("becomes the baseline once the spec says the same", func() {
			tm := h.machine(worker1)
			tm.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: editedLabel}}
			Expect(k8sClient.Update(ctx, tm)).To(Succeed())

			h.advance(time.Minute)
			Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
			Expect(h.applies()).To(BeEmpty())

			cfg := h.machine(worker1).Status.Config
			Expect(cfg.AppliedHash).To(Equal(cfg.LiveHash))
		})
	})

	// A config can pass its soak, be confirmed, and break the node later. The rollout that takes
	// it back has to touch a node that is unhealthy, which is the one case the gate allows.
	Context("a config that breaks a node after it was confirmed", func() {
		const bad = "bad"

		rolledOut := []string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}

		BeforeEach(func() {
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig(bad))
			h.runUntil("the config is rolled out", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })
			Expect(h.applies()).To(Equal(rolledOut))
		})

		// breaks is the config doing its damage on the given machines, some time after the rollout.
		breaks := func(machines ...string) {
			GinkgoHelper()

			for _, m := range machines {
				h.sicken(m, "rollout: "+bad)
			}

			h.advance(time.Hour)

			Expect(condition(h.tc, talosv1alpha1.ConditionHealthy).Status).To(Equal(metav1.ConditionFalse))
			Expect(h.operation()).To(BeNil())
			Expect(h.applies()).To(Equal(rolledOut), "an unhealthy node alone is nothing the operator acts on")
		}

		It("is taken back by a rollout, which repairs the unhealthy node before it touches another", func() {
			breaks(worker2)

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker)
			h.settle(h.started)
			Expect(h.operation()).NotTo(BeNil(), h.inSync().Message)
			Expect(h.operation().Plan).To(Equal([]string{worker2, worker1}), "the node to repair goes first")

			// The node is healthy again as soon as the config is gone, and is only then confirmed.
			h.runUntil("worker-2 is verified", h.stepIs(worker2, plan.StepVerifying))
			Expect(h.applies()).To(Equal(append(slices.Clone(rolledOut), tried(worker2))))
			Expect(string(h.scenario.Node(worker2Addr).Config.Active)).NotTo(ContainSubstring(bad))

			h.runUntil("the rollout ends", func() bool { return h.idle() && len(h.tc.Status.History) == 2 })
			Expect(h.applies()).To(Equal(append(slices.Clone(rolledOut),
				tried(worker2), confirmed(worker2), tried(worker1), confirmed(worker1))))
			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))

			h.settle(nil)
			Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
			Expect(condition(h.tc, talosv1alpha1.ConditionHealthy).Status).To(Equal(metav1.ConditionTrue))
			Expect(h.blocked()).To(BeNil())
		})

		It("is not taken back while a second node is unhealthy", func() {
			breaks(worker1, worker2)

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker)
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
			Expect(h.blocked().Message).To(ContainSubstring(h.node(worker2).node), "the gate names the node that is not the one to repair")
			Expect(h.applies()).To(Equal(rolledOut))
		})

		It("keeps every healthy node untouched for as long as the broken one is not repaired", func() {
			breaks(worker2)

			// A change for the control plane only: the unhealthy worker is not part of it.
			h.setPoolConfig(talosv1alpha1.MachineRoleControlPlane, labelConfig("one"))
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
			Expect(h.blocked().Message).To(ContainSubstring(h.node(worker2).node))
			Expect(h.applies()).To(Equal(rolledOut))

			// With the workers' config taken back as well, the repair goes first and the rest follows.
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker)
			h.runUntil("the rollout ends", func() bool { return h.idle() && len(h.tc.Status.History) == 2 })
			Expect(h.applies()[len(rolledOut):]).To(Equal([]string{
				tried(worker2), confirmed(worker2), tried(worker1), confirmed(worker1), tried(cp1Machine), confirmed(cp1Machine),
			}))
		})
	})

	It("does not keep a config that leaves an unhealthy node unhealthy, and halts before another node is touched", func() {
		h.sicken(worker2, "")
		h.advance(time.Minute)

		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("worker-2 is tried", h.stepIs(worker2, plan.StepVerifying))
		Expect(h.operation().Plan).To(Equal([]string{worker2, worker1}))
		Expect(h.applies()).To(Equal([]string{tried(worker2)}))

		// Verification leaves nobody out: the node itself has to be healthy for the whole soak.
		deadline := h.operation().Current.Deadline.Time
		h.at(deadline.Add(-plan.ConfirmMargin))
		Expect(h.operation().Current.Step).To(Equal(plan.StepRollingBack))

		h.expireTry(worker2)
		h.at(deadline.Add(plan.RollbackGrace))

		Expect(h.failed()).To(BeTrue())
		Expect(h.operation().Failure.Machine).To(Equal(worker2))
		Expect(h.operation().Failure.Reason).To(Equal(plan.FailRolledBack))
		Expect(h.applies()).To(Equal([]string{tried(worker2)}), "never confirmed, and worker-1 never touched")

		h.advance(time.Hour)
		Expect(h.applies()).To(Equal([]string{tried(worker2)}))
	})

	It("is not written to while a rollout that is due waits", func() {
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.settle(nil)
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonPaused))

		// What is blocked is said by clearing RolloutBlocked and setting it again in every pass. A
		// condition's transition time has the resolution of a second, so two passes more than a
		// second apart would each write the status if that counted as a transition.
		written, since := h.tc.ResourceVersion, h.blocked().LastTransitionTime

		time.Sleep(1100 * time.Millisecond)
		h.round()

		Expect(h.tc.ResourceVersion).To(Equal(written), "a cluster at rest is not written to")
		Expect(h.blocked().LastTransitionTime).To(Equal(since))
	})

	It("touches no node while paused, and goes on when the pause is lifted", func() {
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonPaused))
		Expect(h.applies()).To(BeEmpty())

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
		h.runUntil("worker-1 is done", func() bool { return h.operation() != nil && len(h.operation().Completed) == 1 })

		// Pausing between two nodes: the lock is kept, the next node is not started, and no
		// amount of waiting turns that into a failure.
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		h.advance(time.Hour)

		Expect(h.operation()).NotTo(BeNil())
		Expect(h.operation().Failure).To(BeNil())
		Expect(h.operation().Current).To(BeNil())
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonPaused))
		Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}))

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
		h.runUntil("the rollout ends", h.idle)
		Expect(h.applies()).To(HaveLen(4))
	})

	It("finishes a node whose config is being tried when paused, rather than leaving it half done", func() {
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("worker-1 is verified", h.stepIs(worker1, plan.StepVerifying))

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		h.runUntil("worker-1 is done", func() bool { return h.operation() != nil && len(h.operation().Completed) == 1 })

		h.advance(time.Hour)
		Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}))
		Expect(h.operation().Current).To(BeNil())
	})

	It("does not start while the cluster is unhealthy, and says what is wrong", func() {
		h.scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{etcdUnhealthy}})
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
		Expect(h.blocked().Message).To(ContainSubstring(etcdUnhealthy))
		Expect(h.applies()).To(BeEmpty())

		h.scenario.SetHealth(talos.HealthResult{Healthy: true})
		h.runUntil("the rollout ends", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })
	})

	It("halts when the cluster does not get healthy for the next node, instead of waiting forever or going on", func() {
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("worker-1 is done", func() bool { return h.operation() != nil && len(h.operation().Completed) == 1 })

		h.scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{etcdUnhealthy}})
		h.runEvery(time.Minute, "the rollout halts", h.failed)

		Expect(h.operation().Failure.Machine).To(Equal(worker2))
		Expect(h.operation().Failure.Reason).To(Equal(plan.FailTimeout))
		Expect(h.operation().Failure.Message).To(ContainSubstring(etcdUnhealthy))
		Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}))
	})

	It("ends a rollout whose spec moved on, and plans the rest afresh", func() {
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("worker-1 is done", func() bool { return h.operation() != nil && len(h.operation().Completed) == 1 })

		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("two"))
		h.runUntil("the first rollout ends", func() bool { return len(h.tc.Status.History) == 1 })

		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSuperseded))
		Expect(h.tc.Status.History[0].Completed).To(Equal([]string{worker1}))

		h.runUntil("the second rollout ends", func() bool { return h.idle() && len(h.tc.Status.History) == 2 })
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))

		for _, name := range []string{worker1, worker2} {
			Expect(string(h.scenario.Node(h.node(name).addr).Config.Active)).To(ContainSubstring("rollout: two"))
		}

		Expect(string(h.scenario.Node(worker2Addr).Config.Active)).NotTo(ContainSubstring("rollout: one"))
		Expect(h.applies()).To(Equal([]string{
			tried(worker1), confirmed(worker1), tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2),
		}), "worker-2 never got the config that was superseded")
	})

	It("leaves a machine on hold out of the rollout", func() {
		tm := h.machine(worker1)
		tm.Spec.Hold = true
		Expect(k8sClient.Update(ctx, tm)).To(Succeed())

		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("the rollout ends", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })

		Expect(h.applies()).To(Equal([]string{tried(worker2), confirmed(worker2)}))

		h.settle(nil)
		Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonHeld))
		Expect(h.inSync().Message).To(ContainSubstring(worker1))
	})

	It("halts at once when Talos refuses the config, having applied nothing", func() {
		h.scenario.FailNext(fake.MethodApplyConfiguration, workerAddr, status.Error(codes.InvalidArgument, "unknown field"))

		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("the rollout halts", h.failed)

		Expect(h.operation().Failure.Reason).To(Equal(plan.FailRejected))
		Expect(h.operation().Failure.Message).To(ContainSubstring("unknown field"))
		Expect(h.applies()).To(BeEmpty(), "the dry run caught it")
	})

	It("keeps trying through an error that may pass", func() {
		h.scenario.FailNext(fake.MethodApplyConfiguration, workerAddr, status.Error(codes.Unavailable, "connection refused"))

		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("worker-1 is being tried", h.stepIs(worker1, plan.StepTrying))
		h.advance(time.Minute)

		Expect(h.failed()).To(BeFalse())
		Expect(h.inSync().Message).To(ContainSubstring("connection refused"))

		h.scenario.FailNext(fake.MethodApplyConfiguration, workerAddr, nil)
		h.runUntil("the rollout ends", h.idle)
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
	})

	It("lets go of a rollout when config management returns to Observe, confirming nothing", func() {
		h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		h.runUntil("worker-1 is verified", h.stepIs(worker1, plan.StepVerifying))

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Management.Config = talosv1alpha1.ConfigManagementObserve
		})
		h.advance(time.Hour)

		Expect(h.operation()).To(BeNil())
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
		Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopted))
		Expect(h.applies()).To(Equal([]string{tried(worker1)}), "observing writes nothing: Talos reverts the try by itself")
		Expect(h.scenario.TryPending(workerAddr)).To(BeTrue())
	})

	It("refuses a rollout whose timings could never confirm a try", func() {
		// The API server refuses these too; this is what an operator would do with a spec that
		// predates the rule.
		Expect(plan.TimingOf(talosv1alpha1.Rollout{
			TryTimeout: &metav1.Duration{Duration: time.Minute}, Soak: &metav1.Duration{Duration: time.Minute},
		}).Validate()).NotTo(Succeed())

		Expect(plan.TimingOf(talosv1alpha1.Rollout{}).Validate()).To(Succeed())
		Expect(plan.TimingOf(talosv1alpha1.Rollout{Soak: &metav1.Duration{}}).Soak).To(BeZero(), "a soak set to zero is zero, not the default")
	})
})
