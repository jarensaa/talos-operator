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
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	talosmachine "github.com/siderolabs/talos/pkg/machinery/config/machine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/catalog"
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

// What a hook's Job does in the specs that play the Job controller.
const (
	hookSucceeds = "succeed"
	hookFails    = "fail"
	// hookJob is the JobSpec of the specs' hooks.
	hookJob = `{"template":{"spec":{"containers":[{"name":"hook","image":"registry.example.com/hook:1"}]}}}`
)

// upstream is what exists upstream in the specs that ask: a newer Talos patch release and its
// installer, a Talos minor this operator does not support, and the newest Kubernetes releases.
func upstream() *catalog.Fake {
	return &catalog.Fake{
		Talos: []string{"v1.13.9", talosVersion, newerTalosVersion, "v1.15.0"},
		Installers: map[string]bool{
			schematic + " " + talosVersion: true, schematic + " " + newerTalosVersion: true,
		},
		Latest: map[string]string{"1.36": "1.36.5", "1.37": "1.37.1"},
		Tags:   map[string]bool{"v" + kubernetesVersion: true, "v1.37.2": true},
	}
}

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
			Expect(h.machine(worker1).Status.Config.SkippedHash).To(Equal(target))

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
			Expect(h.machine(worker1).Status.Config.SkippedHash).To(BeEmpty())
		})

		It("ends the rollout on abort and does not start the same one again", func() {
			rollBack()

			h.annotate(talosv1alpha1.AnnotationAbort, h.operation().Failure.Token)
			h.settle(h.idle)

			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
			Expect(h.tc.Status.AbortedTargets).To(HaveLen(2))

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
			Expect(h.tc.Status.AbortedTargets).To(BeEmpty())
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

	// A Talos upgrade: another version, or another installer, installed to every node in turn
	// (docs/scope.md §7.3).
	Context("a Talos upgrade", func() {
		newerImage := "factory.talos.dev/metal-installer/" + schematic + ":" + newerTalosVersion

		// called lists the machines a Talos call went to, in order.
		called := func(method string) []string {
			var out []string

			for _, c := range h.scenario.CallsFor(method) {
				for _, n := range rolloutNodes {
					if n.addr == c.Node {
						out = append(out, n.name)
					}
				}
			}

			return out
		}

		kubeNodeOf := func(machine string) *corev1.Node {
			GinkgoHelper()

			kube := &corev1.Node{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: h.node(machine).node}, kube)).To(Succeed())

			return kube
		}

		cordoned := func(machine string) bool {
			GinkgoHelper()

			return kubeNodeOf(machine).Spec.Unschedulable
		}

		// addPod puts a running pod on a machine's node.
		addPod := func(machine, name string, mutate func(*corev1.Pod)) *corev1.Pod {
			GinkgoHelper()

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.ns, Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					NodeName:                      h.node(machine).node,
					TerminationGracePeriodSeconds: new(int64),
					Containers:                    []corev1.Container{{Name: "main", Image: "registry.example.com/app:1"}},
				},
			}
			if mutate != nil {
				mutate(pod)
			}

			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

			return pod
		}

		exists := func(pod *corev1.Pod) bool {
			GinkgoHelper()

			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
			if err != nil {
				Expect(err).To(Satisfy(apierrors.IsNotFound))
			}

			return err == nil
		}

		// upgradeTo asks for another Talos version, with upgrades enabled.
		upgradeTo := func(version string) {
			GinkgoHelper()

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
				tc.Spec.Versions.Talos = version
				tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
			})
		}

		done := func(n int) func() bool {
			return func() bool { return h.operation() == nil || len(h.operation().Completed) >= n }
		}

		down := func(machine string) func() bool {
			return func() bool { return h.scenario.Down(h.node(machine).addr) }
		}

		// reboots lets the upgrade take a machine down, and brings it back.
		reboots := func(machine string) {
			GinkgoHelper()

			h.runUntil(machine+" is rebooted", down(machine))
			Expect(h.scenario.Boot(h.node(machine).addr)).To(BeTrue())
		}

		upToDate := func() *metav1.Condition {
			GinkgoHelper()

			c := condition(h.tc, talosv1alpha1.ConditionTalosUpToDate)
			Expect(c).NotTo(BeNil())

			return c
		}

		It("reports every node up to date while nothing is asked for", func() {
			h.settle(nil)

			Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
			Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())
		})

		It("takes every node to the new version, one at a time: installed, drained, rebooted and verified", func() {
			app := addPod(worker1, "app", nil)
			daemon := addPod(worker1, "daemon", func(pod *corev1.Pod) {
				pod.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "DaemonSet", Name: "daemon", UID: "aaaaaaaa-bbbb-4ccc-8ddd-0000000000aa", Controller: new(true),
				}}
			})
			static := addPod(worker1, "static", func(pod *corev1.Pod) {
				pod.Annotations = map[string]string{mirrorPodAnnotation: "file"}
			})
			elsewhere := addPod(worker2, "elsewhere", nil)

			upgradeTo(newerTalosVersion)
			h.settle(h.started)

			By("planning takes the lock and sends nothing")
			Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationTalosUpgrade))
			Expect(h.operation().Plan).To(Equal([]string{worker1, worker2, cp1Machine}), "workers before the control plane")
			Expect(h.operation().Targets).To(HaveKeyWithValue(worker1, newerImage))
			Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUpgrading))

			By("installing the new system while the node keeps doing its work")
			h.runUntil("worker-1 is to be drained", h.stepIs(worker1, plan.StepDraining))
			Expect(called(fake.MethodImagePull)).To(Equal([]string{worker1}))
			Expect(called(fake.MethodInstall)).To(Equal([]string{worker1}))
			Expect(h.scenario.CallsFor(fake.MethodInstall)[0].Image).To(Equal(newerImage))
			Expect(cordoned(worker1)).To(BeFalse(), "nothing is taken from the cluster before the install is done")
			Expect(exists(app)).To(BeTrue())
			Expect(h.scenario.Node(workerAddr).Version.Tag).To(Equal(talosVersion), "an install changes nothing until the reboot")

			By("draining the node, then rebooting it")
			h.runUntil("worker-1 is rebooted", down(worker1))
			Expect(cordoned(worker1)).To(BeTrue())
			Expect(h.operation().Current.Cordoned).To(BeTrue())
			Expect(exists(app)).To(BeFalse(), "its pods were evicted")
			Expect(exists(daemon)).To(BeTrue(), "a DaemonSet's pod would only come straight back")
			Expect(exists(static)).To(BeTrue(), "a static pod is not Kubernetes' to move")
			Expect(exists(elsewhere)).To(BeTrue(), "no other node is drained")
			Expect(called(fake.MethodReboot)).To(Equal([]string{worker1}))
			Expect(called(fake.MethodEtcdForfeitLeadership)).To(BeEmpty(), "a worker leads no etcd")
			Expect(h.operation().Current.Step).To(Equal(plan.StepBooting))
			Expect(h.operation().Current.PreRebootBootID).To(Equal(h.node(worker1).node + "-boot-1"))

			By("waiting for it, and not asking twice")
			h.advance(time.Minute)
			Expect(called(fake.MethodReboot)).To(Equal([]string{worker1}))
			Expect(called(fake.MethodImagePull)).To(Equal([]string{worker1}), "no other node is begun meanwhile")

			By("taking it back once it runs the new system and stayed healthy")
			Expect(h.scenario.Boot(workerAddr)).To(BeTrue())
			h.runUntil("worker-1 is done", done(1))
			Expect(cordoned(worker1)).To(BeFalse())
			Expect(h.scenario.Node(workerAddr).Version.Tag).To(Equal(newerTalosVersion))

			By("going on with the other worker, and the control plane last")
			reboots(worker2)
			h.runUntil("worker-2 is done", done(2))
			Expect(exists(elsewhere)).To(BeFalse())

			reboots(cp1Machine)
			h.runUntil("the upgrade ends", func() bool { return len(h.tc.Status.History) >= 1 })

			Expect(called(fake.MethodReboot)).To(Equal([]string{worker1, worker2, cp1Machine}), "every node once, in order")
			Expect(called(fake.MethodEtcdForfeitLeadership)).To(Equal([]string{cp1Machine}))

			upgraded := h.tc.Status.History[len(h.tc.Status.History)-1]
			Expect(upgraded.Type).To(Equal(talosv1alpha1.OperationTalosUpgrade))
			Expect(upgraded.Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(upgraded.Completed).To(Equal([]string{worker1, worker2, cp1Machine}))
			Expect(drain(h.recorder)).To(ContainElements(ContainSubstring("UpgradeStarted"), ContainSubstring("MachineUpgraded")))

			By("then bringing each node's config in line: it names the installer the node now runs")
			h.runUntil("the config follows", func() bool { return h.idle() && len(h.tc.Status.History) == 2 })

			configured := h.tc.Status.History[0]
			Expect(configured.Type).To(Equal(talosv1alpha1.OperationConfigRollout))
			Expect(configured.Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(configured.Completed).To(Equal([]string{worker1, worker2, cp1Machine}))
			Expect(string(h.scenario.Node(cpAddr).Config.Active)).To(ContainSubstring(newerImage))

			h.settle(nil)
			Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
			Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
			Expect(h.blocked()).To(BeNil())
			Expect(called(fake.MethodReboot)).To(HaveLen(3), "bringing the config in line reboots nothing")
		})

		It("upgrades nothing while upgrades are not enabled, and says what is waiting", func() {
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Versions.Talos = newerTalosVersion })
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Status).To(Equal(metav1.ConditionFalse))
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUpgradesDisabled))
			Expect(upToDate().Message).To(ContainSubstring("spec.management.upgrades"))

			// The config names an installer no node runs: that is not a config rollout's to apply.
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonAwaitingUpgrade))
			Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonAwaitingUpgrade))
			Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())
			Expect(h.applies()).To(BeEmpty())
		})

		It("refuses a downgrade before any node is touched", func() {
			upgradeTo("v1.14.1")
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(upToDate().Message).To(ContainSubstring("downgrades are not supported"))
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(h.blocked().Message).To(ContainSubstring("upgrade"))
			Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())

			// Asking for what the nodes run again clears it.
			upgradeTo(talosVersion)
			h.settle(nil)
			Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
			Expect(h.blocked()).To(BeNil())
		})

		It("refuses an installer that does not exist before a node is asked to pull it, and only when that is certain", func() {
			source := upstream()
			h.reconciler.Catalog = source

			upgradeTo("v1.14.9")
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(upToDate().Message).To(And(ContainSubstring("no installer of Talos v1.14.9"), ContainSubstring(schematic)))
			Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())

			// A source that cannot be reached makes nothing false: the node is where the answer is.
			source.Err = errors.New("no route to host")
			h.reconciler.Catalog = source
			upgradeTo("v1.14.8")
			h.runUntil("a node is asked to pull", func() bool { return len(h.scenario.CallsFor(fake.MethodImagePull)) > 0 })
			Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationTalosUpgrade))
		})

		It("leaves the node the operator runs on for last", func() {
			h.reconciler.NodeName = h.node(worker1).node

			upgradeTo(newerTalosVersion)
			h.settle(h.started)

			Expect(h.operation().Plan).To(Equal([]string{worker2, worker1, cp1Machine}))
		})

		It("is carried on by another replica from what is persisted, without a second reboot", func() {
			upgradeTo(newerTalosVersion)
			h.runUntil("worker-1 is rebooted", down(worker1))

			// The leader is gone with its node, or for any other reason. The other replica knows
			// nothing but what is in status and on the nodes.
			h.takeOver()

			h.advance(time.Minute)
			Expect(h.scenario.Boot(workerAddr)).To(BeTrue())
			h.runUntil("worker-1 is done", done(1))

			Expect(called(fake.MethodReboot)).To(Equal([]string{worker1}))
			Expect(called(fake.MethodInstall)).To(Equal([]string{worker1}))
			Expect(cordoned(worker1)).To(BeFalse())
		})

		It("halts when a node comes back on the system it ran before, leaving it cordoned and every other node alone", func() {
			h.scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.FallsBack = true })

			upgradeTo(newerTalosVersion)
			reboots(worker1)
			h.runUntil("the upgrade halts", h.failed)

			f := h.operation().Failure
			Expect(f.Machine).To(Equal(worker1))
			Expect(f.Reason).To(Equal(plan.FailFellBack))
			Expect(f.Remediation).To(ContainSubstring(talosv1alpha1.AnnotationRetry + "=" + f.Token))
			Expect(cordoned(worker1)).To(BeTrue(), "a node that did not come back as expected is not handed back to the scheduler")
			Expect(h.scenario.Node(workerAddr).Version.Tag).To(Equal(talosVersion))

			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
			Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))
			Expect(drain(h.recorder)).To(ContainElement(ContainSubstring("UpgradeHalted")))

			h.advance(time.Hour)
			Expect(called(fake.MethodReboot)).To(Equal([]string{worker1}), "it stays halted: no second reboot, no other node")
			Expect(called(fake.MethodImagePull)).To(Equal([]string{worker1}))

			By("retrying once the cause is found")
			h.scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.FallsBack = false })
			h.annotate(talosv1alpha1.AnnotationRetry, f.Token)

			reboots(worker1)
			h.runUntil("worker-1 is done", done(1))
			Expect(cordoned(worker1)).To(BeFalse(), "the cordon the operator set is still its own to undo")
			Expect(h.scenario.Node(workerAddr).Version.Tag).To(Equal(newerTalosVersion))
		})

		It("halts on a drain that a disruption budget never allows, with the node cordoned and not rebooted", func() {
			guarded := addPod(worker1, "guarded", nil)
			budget := &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Name: "guarded", Namespace: h.ns},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MaxUnavailable: new(intstr.FromInt32(0)),
					Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "guarded"}},
				},
			}
			Expect(k8sClient.Create(ctx, budget)).To(Succeed())

			upgradeTo(newerTalosVersion)
			h.runUntil("worker-1 is being drained", func() bool { return h.stepIs(worker1, plan.StepDraining)() && cordoned(worker1) })

			h.advance(time.Minute)
			Expect(exists(guarded)).To(BeTrue(), "an eviction the budget refuses does not happen")
			Expect(condition(h.tc, talosv1alpha1.ConditionTalosUpToDate).Message).To(ContainSubstring("PodDisruptionBudget"))

			h.runEvery(time.Minute, "the upgrade halts", h.failed)

			f := h.operation().Failure
			Expect(f.Reason).To(Equal(plan.FailDrainTimeout))
			Expect(f.Message).To(ContainSubstring("1 pod"))
			Expect(cordoned(worker1)).To(BeTrue())
			Expect(called(fake.MethodReboot)).To(BeEmpty(), "a node that is not drained is not rebooted")
			Expect(exists(guarded)).To(BeTrue())

			By("retrying once the budget lets the pod go")
			Expect(k8sClient.Delete(ctx, budget)).To(Succeed())
			h.annotate(talosv1alpha1.AnnotationRetry, f.Token)

			reboots(worker1)
			h.runUntil("worker-1 is done", done(1))
			Expect(exists(guarded)).To(BeFalse())
			Expect(cordoned(worker1)).To(BeFalse())
		})

		It("halts on an installer that cannot be pulled, before anything changed, and does not start again after an abort", func() {
			h.scenario.FailNext(fake.MethodImagePull, workerAddr, status.Error(codes.NotFound, "manifest unknown"))

			upgradeTo(newerTalosVersion)
			h.runUntil("the upgrade halts", h.failed)

			f := h.operation().Failure
			Expect(f.Reason).To(Equal(plan.FailImagePull))
			Expect(f.Message).To(ContainSubstring(newerImage))
			Expect(cordoned(worker1)).To(BeFalse())
			Expect(called(fake.MethodInstall)).To(BeEmpty())
			Expect(called(fake.MethodReboot)).To(BeEmpty())

			h.annotate(talosv1alpha1.AnnotationAbort, f.Token)
			h.scenario.FailNext(fake.MethodImagePull, workerAddr, nil)
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
			Expect(h.tc.Status.Upgrades.Aborted).To(HaveKeyWithValue(worker1, newerImage))
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonAborted))
			Expect(called(fake.MethodImagePull)).To(HaveLen(1), "an aborted upgrade is not started again as it is")

			By("asking for what the nodes run again")
			upgradeTo(talosVersion)
			h.settle(nil)
			Expect(h.tc.Status.Upgrades).To(BeNil(), "the abort is spent")
			Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
		})

		It("leaves out a machine that is skipped, and does not come back for it with the same installer", func() {
			h.scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.InstallExitCode = 1 })

			upgradeTo(newerTalosVersion)
			h.runUntil("the upgrade halts", h.failed)

			f := h.operation().Failure
			Expect(f.Reason).To(Equal(plan.FailInstall))
			Expect(cordoned(worker1)).To(BeFalse(), "a failed install leaves the node as it was")

			h.annotate(talosv1alpha1.AnnotationSkipMachine, f.Token)
			reboots(worker2)
			h.runUntil("worker-2 is done", done(1))
			reboots(cp1Machine)
			h.runUntil("the upgrade ends", func() bool { return h.idle() && len(h.tc.Status.History) >= 1 })

			h.advance(time.Hour)
			Expect(h.tc.Status.Upgrades.Skipped).To(HaveKeyWithValue(worker1, newerImage))
			Expect(upToDate().Status).To(Equal(metav1.ConditionFalse))
			Expect(upToDate().Message).To(ContainSubstring(worker1))
			Expect(called(fake.MethodInstall)).To(Equal([]string{worker1, worker2, cp1Machine}), "the skipped machine is not tried again")
		})

		It("does not undo a cordon that someone else set", func() {
			kube := kubeNodeOf(worker1)
			kube.Spec.Unschedulable = true
			Expect(k8sClient.Update(ctx, kube)).To(Succeed())

			// A cordoned node is someone's work in progress: nothing else is begun, but the node
			// itself may be upgraded.
			upgradeTo(newerTalosVersion)
			reboots(worker1)
			h.runUntil("worker-1 is settling", h.stepIs(worker1, plan.StepVerifying))

			Expect(h.operation().Current.Cordoned).To(BeFalse())
			Expect(cordoned(worker1)).To(BeTrue(), "whoever cordoned it uncordons it")
		})

		It("leaves a node it has not taken anything from alone while paused, and finishes one it has", func() {
			upgradeTo(newerTalosVersion)
			h.runUntil("worker-1 is to be drained", h.stepIs(worker1, plan.StepDraining))

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
			h.advance(time.Hour)

			Expect(h.operation()).NotTo(BeNil())
			Expect(h.operation().Failure).To(BeNil(), "waiting out a pause is not a failure")
			Expect(h.operation().Current).To(BeNil())
			Expect(cordoned(worker1)).To(BeFalse())
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonPaused))

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
			h.runUntil("worker-1 is cordoned", func() bool { return cordoned(worker1) })

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
			reboots(worker1)
			h.runUntil("worker-1 is done", done(1))
			Expect(cordoned(worker1)).To(BeFalse(), "a node that was taken down is brought all the way back")

			h.advance(time.Hour)
			Expect(called(fake.MethodImagePull)).To(Equal([]string{worker1, worker1}), "and no other node is begun")
		})
	})

	Context("the rollout policy", func() {
		// daily is a maintenance window that opens at the given minute every day.
		daily := func(opens time.Time, lasts time.Duration) talosv1alpha1.MaintenanceWindow {
			utc := opens.UTC()

			return talosv1alpha1.MaintenanceWindow{
				Schedule: fmt.Sprintf("%d %d * * *", utc.Minute(), utc.Hour()), Duration: metav1.Duration{Duration: lasts},
			}
		}

		setWindows := func(windows ...talosv1alpha1.MaintenanceWindow) {
			GinkgoHelper()
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Rollout.MaintenanceWindows = windows })
		}

		It("starts nothing outside its maintenance windows, and starts when one opens", func() {
			opens := h.clock.Truncate(time.Minute).Add(time.Hour)
			setWindows(daily(opens, 30*time.Minute))
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.advance(10 * time.Minute)

			Expect(h.operation()).To(BeNil())
			Expect(h.applies()).To(BeEmpty())
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonOutsideWindow))
			Expect(h.blocked().Message).To(ContainSubstring(opens.UTC().Format(time.RFC3339)), "it says when the next window opens")
			Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonOutsideWindow))

			h.at(opens.Add(-time.Second))
			Expect(h.operation()).To(BeNil(), "not a second early")

			h.at(opens)
			h.runUntil("the rollout ends", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })
			Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
			Expect(h.blocked()).To(BeNil())
		})

		It("finishes the node it is at when the window closes, and begins the next when one opens again", func() {
			opened := h.clock.Truncate(time.Minute)
			// It closes half a minute from now: time for one node's try, not for its soak.
			setWindows(daily(opened, h.clock.Sub(opened)+30*time.Second))
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.settle(nil)
			Expect(h.stepIs(worker1, plan.StepVerifying)()).To(BeTrue(), "%+v", h.operation())

			h.advance(10 * time.Second)
			Expect(h.operation().Current.HealthySince).NotTo(BeNil(), h.inSync().Message)
			Expect(h.blocked()).To(BeNil(), "the window is still open")

			// The window closes in the middle of the node's soak.
			h.advance(30 * time.Second)
			Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}), "the node that was being changed is finished")
			Expect(h.started()).To(BeTrue(), "the rollout is not over: it waits")
			Expect(h.operation().Completed).To(Equal([]string{worker1}))
			Expect(h.operation().Current).To(BeNil())
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonOutsideWindow))
			Expect(h.inSync().Message).To(ContainSubstring("outside the maintenance windows"))

			h.advance(time.Hour)
			Expect(h.applies()).To(HaveLen(2), "and no other node is begun")

			h.at(opened.Add(24 * time.Hour))
			h.runUntil("the rollout ends", h.idle)
			Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
			Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		})

		It("starts nothing while a window cannot be understood, rather than at any time", func() {
			setWindows(talosv1alpha1.MaintenanceWindow{Schedule: "61 2 * * *", Duration: metav1.Duration{Duration: time.Hour}})
			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(h.applies()).To(BeEmpty())
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonInvalidRollout))
			Expect(h.blocked().Message).To(ContainSubstring("maintenanceWindows[0]"))

			setWindows(talosv1alpha1.MaintenanceWindow{
				Schedule: "0 2 * * *", Duration: metav1.Duration{Duration: time.Hour}, TimeZone: "Europe/Atlantis",
			})
			h.advance(time.Minute)
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonInvalidRollout))
			Expect(h.blocked().Message).To(ContainSubstring("Europe/Atlantis"))
			Expect(h.applies()).To(BeEmpty())
		})

		Context("with a gate on an object in the cluster", func() {
			const healthy, degraded = "HEALTH_OK", "HEALTH_WARN"

			// storage plays the object a gate asks about: a storage cluster's health.
			storage := func(health string) {
				GinkgoHelper()

				cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "storage", Namespace: h.ns}}
				_, err := controllerutil.CreateOrUpdate(ctx, k8sClient, cm, func() error {
					cm.Data = map[string]string{"health": health}

					return nil
				})
				Expect(err).NotTo(HaveOccurred())
			}

			gate := func(expression string) {
				GinkgoHelper()
				h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
					tc.Spec.Rollout.Gates = []talosv1alpha1.RolloutGate{{
						Name:       "storage-healthy",
						Object:     talosv1alpha1.GateObject{APIVersion: "v1", Kind: "ConfigMap", Namespace: h.ns, Name: "storage"},
						Expression: expression,
					}}
				})
			}

			BeforeEach(func() { gate(`object.data.health == "` + healthy + `"`) })

			It("starts nothing while the gate is not true, and says what it said", func() {
				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.advance(time.Minute)
				Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
				Expect(h.blocked().Message).To(And(ContainSubstring("gate storage-healthy"), ContainSubstring("does not exist")))

				storage(degraded)
				h.advance(time.Minute)
				Expect(h.blocked().Message).To(And(ContainSubstring("gate storage-healthy"), ContainSubstring("is false")))
				Expect(h.operation()).To(BeNil())
				Expect(h.applies()).To(BeEmpty())

				// A field that is not there is not true either.
				gate(`object.status.health == "` + healthy + `"`)
				h.advance(time.Minute)
				Expect(h.blocked().Message).To(ContainSubstring("no such key"))

				gate(`object.data.health ==`)
				h.advance(time.Minute)
				Expect(h.blocked().Message).To(ContainSubstring("does not compile"))
				Expect(h.applies()).To(BeEmpty())

				gate(`object.data.health == "` + healthy + `"`)
				storage(healthy)
				h.runUntil("the rollout ends", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })
				Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
			})

			It("does not confirm a node, or begin the next, until the gate is true again after it", func() {
				storage(healthy)
				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.settle(nil)
				Expect(h.stepIs(worker1, plan.StepVerifying)()).To(BeTrue(), "%+v", h.operation())

				// The change of the node set something off that the gate watches.
				storage(degraded)
				h.advance(time.Minute)
				Expect(h.applies()).To(Equal([]string{tried(worker1)}), "a node is not done while a gate is closed")
				Expect(h.inSync().Message).To(ContainSubstring("gate storage-healthy"))

				storage(healthy)
				h.advance(10 * time.Second)
				Expect(h.applies()).To(Equal([]string{tried(worker1)}), "the soak starts over once the gate is true")

				h.runUntil("the rollout ends", h.idle)
				Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
			})

			It("begins no other node while the gate is closed, and halts when it stays closed", func() {
				storage(healthy)

				// The gate closes the moment the first node is done.
				h.kubelets = func() {
					GinkgoHelper()

					if slices.Contains(h.applies(), confirmed(worker1)) {
						storage(degraded)
					}
				}

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.runUntil("the first node is done", func() bool { return h.started() && len(h.operation().Completed) == 1 })
				h.advance(time.Minute)

				Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}), "nothing is sent to a node behind a closed gate")
				Expect(h.inSync().Message).To(ContainSubstring("gate storage-healthy"))
				Expect(h.failed()).To(BeFalse())

				// Like the cluster's own health: a wait for it is bounded, and ends in a halt.
				h.runUntil("the wait for the gate runs out", h.failed)
				Expect(h.operation().Failure.Reason).To(Equal(plan.FailTimeout))
				Expect(h.operation().Failure.Message).To(ContainSubstring("gate storage-healthy"))
				Expect(h.applies()).To(HaveLen(2))
			})

			It("lets Talos take a config back when the gate stays closed for the whole try", func() {
				storage(healthy)
				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.settle(nil)
				storage(degraded)

				deadline := h.operation().Current.Deadline.Time
				h.at(deadline)
				h.expireTry(worker1)
				h.at(deadline.Add(plan.RollbackGrace))

				Expect(h.failed()).To(BeTrue())
				Expect(h.operation().Failure.Reason).To(Equal(plan.FailRolledBack))
				Expect(h.applies()).To(Equal([]string{tried(worker1)}), "never confirmed, and no other node touched")
			})
		})

		Context("with hooks", func() {
			var (
				// seen lists the hooks' Jobs in the order they were created, as "<stage> <machine>".
				seen []string
				// jobs maps what seen lists to the Job's name.
				jobs map[string]string
				// outcome is what a hook's Job does: succeed, fail, or run on ("").
				outcome func(stage, machine string) string
			)

			hook := func(name string, timeout time.Duration) talosv1alpha1.Hook {
				out := talosv1alpha1.Hook{
					Name: name,
					Job:  runtime.RawExtension{Raw: []byte(hookJob)},
				}
				if timeout > 0 {
					out.Timeout = &metav1.Duration{Duration: timeout}
				}

				return out
			}

			setHooks := func(hooks talosv1alpha1.RolloutHooks) {
				GinkgoHelper()
				h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Rollout.Hooks = &hooks })
			}

			everyStage := func() talosv1alpha1.RolloutHooks {
				return talosv1alpha1.RolloutHooks{
					PreRollout: []talosv1alpha1.Hook{hook("announce", 0)}, PostRollout: []talosv1alpha1.Hook{hook("all-clear", 0)},
					PreNode: []talosv1alpha1.Hook{hook("prepare", 0)}, PostNode: []talosv1alpha1.Hook{hook("release", 0)},
				}
			}

			envOf := func(job *batchv1.Job) map[string]string {
				env := map[string]string{}
				for _, e := range job.Spec.Template.Spec.Containers[0].Env {
					env[e.Name] = e.Value
				}

				return env
			}

			getJob := func(key string) *batchv1.Job {
				GinkgoHelper()

				job := &batchv1.Job{}
				Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: h.ns, Name: jobs[key]}, job)).To(Succeed(), "no Job for "+key)

				return job
			}

			BeforeEach(func() {
				seen, jobs = nil, map[string]string{}
				outcome = func(string, string) string { return hookSucceeds }

				// The Job controller, and the pods it would run: each hook's Job ends as the spec says.
				h.kubelets = func() {
					GinkgoHelper()

					var list batchv1.JobList
					Expect(k8sClient.List(ctx, &list, client.InNamespace(h.ns), client.HasLabels{talosv1alpha1.LabelHook})).To(Succeed())

					for i := range list.Items {
						job := &list.Items[i]
						env := envOf(job)
						key := strings.TrimSpace(env["TALOS_HOOK"] + " " + env["TALOS_MACHINE"])

						if _, known := jobs[key+" "+job.Name]; !known {
							jobs[key+" "+job.Name] = job.Name
							jobs[key] = job.Name
							seen = append(seen, key)
						}

						if len(job.Status.Conditions) > 0 {
							continue
						}

						now := metav1.NewTime(h.clock)

						switch outcome(env["TALOS_HOOK"], env["TALOS_MACHINE"]) {
						case hookSucceeds:
							job.Status.StartTime, job.Status.CompletionTime, job.Status.Succeeded = &now, &now, 1
							job.Status.Conditions = []batchv1.JobCondition{
								{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
								{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
							}
						case hookFails:
							job.Status.StartTime, job.Status.Failed = &now, 1
							job.Status.Conditions = []batchv1.JobCondition{
								{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: now, Reason: "BackoffLimitExceeded"},
								{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: now, Reason: "BackoffLimitExceeded",
									Message: "Job has reached the specified backoff limit"},
							}
						default:
							continue
						}

						Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
					}
				}
			})

			It("runs them around the rollout and around each node, in order, and ends only when the last is through", func() {
				setHooks(everyStage())

				// The first hook of a node runs on: nothing is sent to the node while it does.
				outcome = func(stage, _ string) string {
					if stage == talosv1alpha1.HookPreNode {
						return ""
					}

					return hookSucceeds
				}

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.runUntil("the node's hook is started", func() bool { return len(seen) == 2 })
				h.advance(time.Minute)
				Expect(seen).To(Equal([]string{talosv1alpha1.HookPreRollout, "preNode " + worker1}))
				Expect(h.applies()).To(BeEmpty(), "nothing is sent to a node before its preNode hooks are through")
				Expect(h.inSync().Message).To(ContainSubstring("preNode hook prepare"))

				job := getJob("preNode " + worker1)
				Expect(envOf(job)).To(Equal(map[string]string{
					"TALOS_CLUSTER": h.tc.Name, "TALOS_OPERATION": "ConfigRollout", "TALOS_OPERATION_ID": h.operation().ID,
					"TALOS_HOOK": "preNode", "TALOS_MACHINE": worker1, "TALOS_NODE": h.node(worker1).node,
				}))
				Expect(job.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
				Expect(*job.Spec.BackoffLimit).To(BeZero(), "a hook that fails is not run again behind the operator's back")
				Expect(*job.Spec.ActiveDeadlineSeconds).To(Equal(int64(plan.DefaultStepTimeout.Seconds())))
				Expect(metav1.IsControlledBy(job, h.tc)).To(BeTrue())
				Expect(job.Name).To(HavePrefix(h.tc.Name + "-prepare-"))

				outcome = func(string, string) string { return hookSucceeds }
				h.runUntil("the rollout ends", h.idle)

				Expect(seen).To(Equal([]string{
					talosv1alpha1.HookPreRollout,
					"preNode " + worker1, "postNode " + worker1,
					"preNode " + worker2, "postNode " + worker2,
					talosv1alpha1.HookPostRollout,
				}))
				Expect(h.applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
				Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
				Expect(h.tc.Status.History[0].Completed).To(Equal([]string{worker1, worker2}))
				Expect(envOf(getJob(talosv1alpha1.HookPostRollout))).NotTo(HaveKey("TALOS_MACHINE"))
			})

			It("holds the lock until the postRollout hooks are through", func() {
				setHooks(talosv1alpha1.RolloutHooks{PostRollout: []talosv1alpha1.Hook{hook("all-clear", 0)}})
				outcome = func(string, string) string { return "" }

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.runUntil("the last hook is started", func() bool { return len(seen) == 1 })
				h.advance(time.Minute)

				Expect(h.started()).To(BeTrue(), "the operation is not over while it cleans up")
				Expect(h.operation().Ending.Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
				Expect(h.applies()).To(HaveLen(4))

				// Nothing else starts meanwhile: the lock is the operation's.
				h.setPoolConfig(talosv1alpha1.MachineRoleControlPlane, labelConfig("two"))
				h.advance(time.Minute)
				Expect(h.applies()).To(HaveLen(4))

				outcome = func(string, string) string { return hookSucceeds }
				h.runUntil("both rollouts end", func() bool { return h.idle() && len(h.tc.Status.History) == 2 })
				Expect(h.applies()).To(HaveLen(6))
			})

			It("halts on a hook that fails, with the node untouched, and runs it again on retry", func() {
				setHooks(everyStage())
				outcome = func(stage, machine string) string {
					if stage == talosv1alpha1.HookPreNode && machine == worker1 {
						return hookFails
					}

					return hookSucceeds
				}

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.runUntil("the rollout halts", h.failed)

				f := h.operation().Failure
				Expect(f.Reason).To(Equal(plan.FailHook))
				Expect(f.Hook).To(Equal(talosv1alpha1.HookPreNode))
				Expect(f.Machine).To(Equal(worker1))
				Expect(f.Step).To(Equal("preNode/prepare"))
				Expect(f.Message).To(ContainSubstring("BackoffLimitExceeded"))
				Expect(f.Remediation).To(And(ContainSubstring("kubectl logs -n "+h.ns+" job/"+jobs["preNode "+worker1]),
					ContainSubstring(talosv1alpha1.AnnotationRetry+"="+f.Token), ContainSubstring(talosv1alpha1.AnnotationSkipMachine+"="+f.Token)))
				Expect(h.applies()).To(BeEmpty())
				Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
				Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))

				h.advance(time.Hour)
				Expect(h.applies()).To(BeEmpty(), "it stays halted, whatever time passes")

				first := jobs["preNode "+worker1]
				outcome = func(string, string) string { return hookSucceeds }
				h.annotate(talosv1alpha1.AnnotationRetry, f.Token)
				h.runUntil("the rollout ends", h.idle)

				Expect(jobs["preNode "+worker1]).NotTo(Equal(first), "a retry is a Job of its own")
				Expect(seen).To(Equal([]string{
					talosv1alpha1.HookPreRollout, "preNode " + worker1, "preNode " + worker1, "postNode " + worker1,
					"preNode " + worker2, "postNode " + worker2, talosv1alpha1.HookPostRollout,
				}))
				Expect(h.applies()).To(HaveLen(4))
			})

			It("leaves a node out when its hook failed and a human says so, and still cleans up", func() {
				setHooks(everyStage())
				outcome = func(stage, machine string) string {
					if stage == talosv1alpha1.HookPreNode && machine == worker1 {
						return hookFails
					}

					return hookSucceeds
				}

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.runUntil("the rollout halts", h.failed)
				h.annotate(talosv1alpha1.AnnotationSkipMachine, h.operation().Failure.Token)
				h.runUntil("the rollout ends", h.idle)

				Expect(h.applies()).To(Equal([]string{tried(worker2), confirmed(worker2)}))
				Expect(h.tc.Status.History[0].Skipped).To(Equal([]string{worker1}))
				Expect(seen).To(Equal([]string{
					talosv1alpha1.HookPreRollout, "preNode " + worker1, "preNode " + worker2, "postNode " + worker2, talosv1alpha1.HookPostRollout,
				}))

				h.advance(time.Hour)
				Expect(h.operation()).To(BeNil(), "and the rollout does not come back for it")
				Expect(h.inSync().Reason).To(Equal(talosv1alpha1.ReasonHeld))
			})

			It("runs the postRollout hooks of a rollout a human aborts, and not of one that never announced itself", func() {
				setHooks(everyStage())
				outcome = func(stage, _ string) string {
					if stage == talosv1alpha1.HookPreNode {
						return hookFails
					}

					return hookSucceeds
				}

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.runUntil("the rollout halts", h.failed)
				h.annotate(talosv1alpha1.AnnotationAbort, h.operation().Failure.Token)
				h.runUntil("the rollout ends", h.idle)

				Expect(seen).To(Equal([]string{talosv1alpha1.HookPreRollout, "preNode " + worker1, talosv1alpha1.HookPostRollout}))
				Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
				Expect(h.applies()).To(BeEmpty())
				h.advance(time.Hour)
				Expect(h.operation()).To(BeNil(), "an aborted rollout is not started again")

				// Another change, whose very first hook fails: what was not set up is not taken down.
				seen = nil
				outcome = func(string, string) string { return hookFails }

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("two"))
				h.runUntil("the rollout halts", h.failed)
				Expect(h.operation().Failure.Hook).To(Equal(talosv1alpha1.HookPreRollout))
				Expect(h.operation().Failure.Remediation).NotTo(ContainSubstring(talosv1alpha1.AnnotationSkipMachine))

				h.annotate(talosv1alpha1.AnnotationAbort, h.operation().Failure.Token)
				h.runUntil("the rollout ends", h.idle)
				Expect(seen).To(Equal([]string{talosv1alpha1.HookPreRollout}))
			})

			It("halts on a hook that does not finish in its time", func() {
				setHooks(talosv1alpha1.RolloutHooks{PreNode: []talosv1alpha1.Hook{hook("prepare", 2*time.Minute)}})
				outcome = func(string, string) string { return "" }

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.runUntil("the hook is started", func() bool { return len(seen) == 1 })
				Expect(*getJob("preNode " + worker1).Spec.ActiveDeadlineSeconds).To(Equal(int64(120)))

				since := h.operation().Hook.Since.Time
				h.at(since.Add(2*time.Minute - time.Second))
				Expect(h.failed()).To(BeFalse())

				h.at(since.Add(2 * time.Minute))
				Expect(h.failed()).To(BeTrue())
				Expect(h.operation().Failure.Message).To(ContainSubstring("did not succeed within 2m0s"))
				Expect(h.applies()).To(BeEmpty())
			})

			It("halts on a hook whose Job the API server refuses", func() {
				setHooks(talosv1alpha1.RolloutHooks{PreRollout: []talosv1alpha1.Hook{{
					Name: "broken", Job: runtime.RawExtension{Raw: []byte(`{"template":{"spec":{"containers":[]}}}`)},
				}}})

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.runUntil("the rollout halts", h.failed)
				Expect(h.operation().Failure.Message).To(ContainSubstring("could not be created"))
				Expect(seen).To(BeEmpty())
				Expect(h.applies()).To(BeEmpty())
			})

			It("is carried on by another replica without running a hook twice", func() {
				setHooks(everyStage())
				outcome = func(stage, _ string) string {
					if stage == talosv1alpha1.HookPreNode {
						return ""
					}

					return hookSucceeds
				}

				h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
				h.runUntil("the node's hook is started", func() bool { return len(seen) == 2 })

				// A new process: nothing but status.operation tells it a hook is running.
				h.takeOver()
				h.advance(time.Minute)
				Expect(seen).To(HaveLen(2), "the Job that runs is found, not started again")

				outcome = func(string, string) string { return hookSucceeds }
				h.runUntil("the rollout ends", h.idle)
				Expect(seen).To(HaveLen(6))
			})
		})
	})

	Context("a Kubernetes upgrade", func() {
		const (
			patch     = "1.37.1"
			apiServer = "registry.k8s.io/kube-apiserver:v"
			kubelet   = "ghcr.io/siderolabs/kubelet:v"
		)

		var (
			// stuck lists the turns whose component does not come up on the new image: it keeps
			// running the old one.
			stuck map[string]bool
			// running is what each component really runs, by turn.
			running map[string]string
		)

		imagesOf := func(machine string) map[string]string {
			GinkgoHelper()

			images, err := talos.KubernetesImages(h.scenario.Node(h.node(machine).addr).Config.Active)
			Expect(err).NotTo(HaveOccurred())

			return images
		}

		// pulled lists the images pulled for an upgrade, in order, as "<machine> <image>".
		pulled := func() []string {
			var out []string

			for _, c := range h.scenario.CallsFor(fake.MethodKubernetesImagePull) {
				for _, n := range rolloutNodes {
					if n.addr == c.Node {
						out = append(out, n.name+" "+c.Image)
					}
				}
			}

			return out
		}

		upgradeKubernetes := func(version string) {
			GinkgoHelper()
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
				tc.Spec.Versions.Kubernetes = version
				tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
			})
		}

		upToDate := func() *metav1.Condition {
			GinkgoHelper()

			c := condition(h.tc, talosv1alpha1.ConditionKubernetesUpToDate)
			Expect(c).NotTo(BeNil())

			return c
		}

		turnIs := func(component, machine, step string) func() bool {
			return func() bool {
				cur := h.operation()

				return cur != nil && cur.Current != nil && cur.Current.Component == component && cur.Current.Machine == machine &&
					cur.Current.Step == step
			}
		}

		kubernetesOperations := func() []talosv1alpha1.OperationRecord {
			var out []talosv1alpha1.OperationRecord

			for _, record := range h.tc.Status.History {
				if record.Type == talosv1alpha1.OperationKubernetesUpgrade {
					out = append(out, record)
				}
			}

			return out
		}

		settled := func() bool {
			return h.idle() && upToDate().Status == metav1.ConditionTrue && h.inSync().Status == metav1.ConditionTrue
		}

		BeforeEach(func() {
			stuck, running = map[string]bool{}, map[string]string{}

			// The kubelets, as far as Kubernetes itself goes: each runs the components its node's
			// active config names, mirrors the static pods into the API, and reports its own version.
			h.kubelets = func() {
				GinkgoHelper()

				for _, n := range rolloutNodes {
					for component, image := range imagesOf(n.name) {
						key := plan.TurnKey(component, n.name)
						if !stuck[key] || running[key] == "" {
							running[key] = image
						}

						if !plan.Runs(component) {
							continue
						}

						if component == plan.ComponentKubelet {
							version, _ := plan.VersionOf(running[key])

							kube := &corev1.Node{}
							Expect(k8sClient.Get(ctx, client.ObjectKey{Name: n.node}, kube)).To(Succeed())

							if kube.Status.NodeInfo.KubeletVersion != "v"+version {
								kube.Status.NodeInfo.KubeletVersion = "v" + version
								Expect(k8sClient.Status().Update(ctx, kube)).To(Succeed())
							}

							continue
						}

						pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: component + "-" + n.node, Namespace: kubeSystem}}

						switch err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod); {
						case apierrors.IsNotFound(err):
							pod.Labels = map[string]string{"k8s-app": component}
							pod.Spec = corev1.PodSpec{NodeName: n.node, Containers: []corev1.Container{{Name: component, Image: running[key]}}}
							Expect(k8sClient.Create(ctx, pod)).To(Succeed())

							pod.Status.Phase = corev1.PodRunning
							pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
							Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
						case err != nil:
							Expect(err).NotTo(HaveOccurred())
						case pod.Spec.Containers[0].Image != running[key]:
							pod.Spec.Containers[0].Image = running[key]
							Expect(k8sClient.Update(ctx, pod)).To(Succeed())
						}
					}
				}
			}

			DeferCleanup(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{}, client.InNamespace(kubeSystem), client.GracePeriodSeconds(0))).To(Succeed())
			})

			h.settle(nil)
		})

		It("reports every node up to date while nothing is asked for", func() {
			Expect(upToDate().Status).To(Equal(metav1.ConditionTrue), upToDate().Message)
			Expect(pulled()).To(BeEmpty())
			Expect(h.applies()).To(BeEmpty())
		})

		It("takes every component to the new version, the API server first, each seen running before it is confirmed", func() {
			upgradeKubernetes(patch)
			h.settle(h.started)

			Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationKubernetesUpgrade))
			Expect(h.operation().Target).To(Equal(patch))
			Expect(h.operation().Plan).To(Equal([]string{
				"kube-apiserver/" + cp1Machine, "kube-controller-manager/" + cp1Machine, "kube-scheduler/" + cp1Machine,
				"kube-proxy/" + cp1Machine, "kubelet/" + cp1Machine, "kubelet/" + worker1, "kubelet/" + worker2,
			}))
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUpgrading))

			h.runUntil("the API server is tried", turnIs(plan.ComponentAPIServer, cp1Machine, plan.StepVerifying))
			Expect(pulled()).To(Equal([]string{cp1Machine + " " + apiServer + patch}), "the image is on the node before its config changes")
			Expect(h.applies()).To(Equal([]string{tried(cp1Machine)}))
			Expect(imagesOf(cp1Machine)[plan.ComponentAPIServer]).To(Equal(apiServer + patch))
			Expect(imagesOf(cp1Machine)[plan.ComponentControllerManager]).To(HaveSuffix(":v"+kubernetesVersion), "one component at a time")
			Expect(h.scenario.TryPending(cpAddr)).To(BeTrue(), "tried, not yet kept")
			Expect(h.operation().Current.Deadline.Sub(h.clock)).To(BeNumerically(">", plan.DefaultStepTimeout),
				"an API server's restart leaves a control plane the time to settle that a reboot leaves a node")

			h.runUntil("the controller manager is tried", turnIs(plan.ComponentControllerManager, cp1Machine, plan.StepVerifying))
			Expect(h.operation().Current.Deadline.Sub(h.clock)).To(BeNumerically("<=", plan.DefaultTryTimeout),
				"no other turn gets more than any change")

			h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
			Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))

			Expect(pulled()).To(Equal([]string{
				cp1Machine + " " + apiServer + patch,
				cp1Machine + " registry.k8s.io/kube-controller-manager:v" + patch,
				cp1Machine + " registry.k8s.io/kube-scheduler:v" + patch,
				cp1Machine + " " + kubelet + patch, worker1 + " " + kubelet + patch, worker2 + " " + kubelet + patch,
			}))
			// Five changes of the control plane's config, the kube-proxy image among them, for which
			// nothing is pulled; then one for each worker's kubelet.
			Expect(h.applies()).To(Equal([]string{
				tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine), confirmed(cp1Machine),
				tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine), confirmed(cp1Machine),
				tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2),
			}))

			for _, n := range rolloutNodes {
				for component, image := range imagesOf(n.name) {
					Expect(image).To(HaveSuffix(":v"+patch), "%s on %s", component, n.name)
				}

				Expect(h.scenario.TryPending(n.addr)).To(BeFalse())
			}

			// Nothing is left for a config rollout: every image the version names is the upgrade's.
			h.runUntil("everything settles", settled)
			Expect(upToDate().Message).To(ContainSubstring("all 3 nodes"))
			Expect(h.applies()).To(HaveLen(14))
			Expect(h.tc.Status.History).To(HaveLen(1))
			Expect(drain(h.recorder)).To(ContainElement(ContainSubstring("KubernetesUpgradeStarted")))
		})

		It("changes nothing while upgrades are not enabled, and says what is waiting", func() {
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Versions.Kubernetes = patch })
			h.advance(time.Hour)

			Expect(upToDate().Status).To(Equal(metav1.ConditionFalse))
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonUpgradesDisabled))
			Expect(upToDate().Message).To(ContainSubstring(cp1Machine))
			Expect(h.operation()).To(BeNil())
			Expect(pulled()).To(BeEmpty())

			// And no config rollout applies the images behind the upgrade's back.
			Expect(h.applies()).To(BeEmpty())

			for _, n := range rolloutNodes {
				for component, image := range imagesOf(n.name) {
					Expect(image).To(HaveSuffix(":v"+kubernetesVersion), "%s on %s", component, n.name)
				}
			}

			Expect(h.inSync().Status).To(Equal(metav1.ConditionTrue), h.inSync().Message)
			matches := apimeta.FindStatusCondition(h.machine(worker1).Status.Conditions, talosv1alpha1.ConditionConfigMatchesLive)
			Expect(matches.Status).To(Equal(metav1.ConditionTrue))
			Expect(matches.Message).To(ContainSubstring("apart from the Kubernetes version"), "a machine says what its comparison leaves out")
		})

		// What stands in the way of an upgrade is often something in the config: a flag the new
		// version removed. Taking it out is a config change, and must not wait for the upgrade
		// that waits for it.
		It("rolls out a config change while the upgrade is refused, without touching the images", func() {
			h.scenario.SetRemovedInKubernetes(patch, "kubelet on 10.5.0.3 is started with --container-hints, which the new version removed")
			upgradeKubernetes(patch)
			h.advance(time.Minute)
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(h.applies()).To(BeEmpty())

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.runUntil("the config change is rolled out", func() bool { return h.idle() && len(h.tc.Status.History) == 1 })

			Expect(h.tc.Status.History[0].Type).To(Equal(talosv1alpha1.OperationConfigRollout))
			Expect(h.tc.Status.History[0].Completed).To(Equal([]string{worker1, worker2}))
			Expect(string(h.scenario.Node(workerAddr).Config.Active)).To(ContainSubstring("rollout: one"))
			Expect(imagesOf(worker1)[plan.ComponentKubelet]).To(HaveSuffix(":v"+kubernetesVersion), "the image is not a config rollout's to change")
			Expect(pulled()).To(BeEmpty())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible), "and the upgrade is still refused")

			// With the way cleared, the upgrade goes ahead, and keeps what the rollout applied.
			h.scenario.SetRemovedInKubernetes(patch)
			h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
			Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(imagesOf(worker1)[plan.ComponentKubelet]).To(HaveSuffix(":v" + patch))
			Expect(string(h.scenario.Node(workerAddr).Config.Active)).To(ContainSubstring("rollout: one"))

			h.runUntil("everything settles", settled)
		})

		It("refuses a version the nodes' Talos does not run, before any node is touched, and says which to upgrade first", func() {
			upgradeKubernetes("1.38.0")
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(upToDate().Message).To(And(ContainSubstring("does not support Kubernetes 1.38.0"), ContainSubstring("upgrade Talos first"),
				ContainSubstring(h.node(worker1).node+" ("+talosVersion+")")))
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(pulled()).To(BeEmpty())
			Expect(h.applies()).To(BeEmpty())
		})

		It("refuses a downgrade", func() {
			upgradeKubernetes("1.36.4")
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(upToDate().Message).To(ContainSubstring("not downgraded"))
			Expect(pulled()).To(BeEmpty())
		})

		It("refuses an upgrade that would strand what the new version removes, before any node is touched", func() {
			// A flag a component is started with.
			h.scenario.SetRemovedInKubernetes(patch, "kubelet on 10.5.0.3 is started with --container-hints, which the new version removed")
			upgradeKubernetes(patch)
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(upToDate().Message).To(ContainSubstring("--container-hints"))
			Expect(pulled()).To(BeEmpty())

			// Objects of an API it no longer serves.
			h.scenario.SetRemovedInKubernetes(patch)
			h.reconciler.KubernetesChecks = KubernetesCheckerFunc(func(context.Context, string, string) ([]string, error) {
				return []string{"2 object(s) of workloads.v1alpha2.scheduling.k8s.io exist, an API the new version no longer serves"}, nil
			})
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Message).To(ContainSubstring("workloads.v1alpha2.scheduling.k8s.io"))
			Expect(pulled()).To(BeEmpty())
			Expect(h.applies()).To(BeEmpty())

			// What could not be asked is not fine either.
			h.reconciler.KubernetesChecks = KubernetesCheckerFunc(func(context.Context, string, string) ([]string, error) {
				return nil, errors.New("the server is currently unable to handle the request")
			})
			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonAwaitingStatus))
			Expect(upToDate().Message).To(ContainSubstring("could not be checked"))

			h.reconciler.KubernetesChecks = nil
			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil(), "an operator that cannot ask does not assume")

			h.reconciler.KubernetesChecks = nothingStranded
			h.scenario.FailNext(fake.MethodKubernetesUpgradeChecks, workerAddr, status.Error(codes.Unavailable, "connection refused"))
			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonAwaitingStatus))

			h.scenario.FailNext(fake.MethodKubernetesUpgradeChecks, workerAddr, nil)
			h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
			Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		})

		It("asks the Kubernetes API itself for objects of removed APIs", func() {
			checks := KubernetesCheckerFunc(func(ctx context.Context, from, to string) ([]string, error) {
				return talos.RemovedAPIObjects(ctx, cfg, from, to)
			})

			// The API server of these specs serves none of the APIs that 1.37 removes.
			Expect(checks.RemovedAPIObjects(ctx, "1.36.2", "1.37.1")).To(BeEmpty())
		})

		Context("with a component that does not come up", func() {
			var deadline time.Time

			BeforeEach(func() {
				stuck[plan.TurnKey(plan.ComponentControllerManager, cp1Machine)] = true

				upgradeKubernetes(patch)
				h.runUntil("the controller manager is tried", turnIs(plan.ComponentControllerManager, cp1Machine, plan.StepVerifying))
				h.advance(time.Minute)
				Expect(h.operation().Current.HealthySince).To(BeNil())
				Expect(upToDate().Message).To(ContainSubstring("kube-controller-manager on " + h.node(cp1Machine).node + " does not run"))

				// Talos takes the image back when the try ends.
				deadline = h.operation().Current.Deadline.Time
				h.at(deadline)
				h.expireTry(cp1Machine)
				h.at(deadline.Add(plan.RollbackGrace))
			})

			It("halts, with the component back on its previous image and nothing after it touched", func() {
				Expect(h.failed()).To(BeTrue())

				f := h.operation().Failure
				Expect(f.Reason).To(Equal(plan.FailRolledBack))
				Expect(f.Machine).To(Equal(cp1Machine))
				Expect(f.Remediation).To(And(ContainSubstring(talosv1alpha1.AnnotationRetry+"="+f.Token),
					ContainSubstring(talosv1alpha1.AnnotationAbort+"="+f.Token)))
				Expect(f.Remediation).NotTo(ContainSubstring(talosv1alpha1.AnnotationSkipMachine),
					"a Kubernetes upgrade cannot leave a node out")

				Expect(imagesOf(cp1Machine)[plan.ComponentControllerManager]).To(HaveSuffix(":v" + kubernetesVersion))
				Expect(imagesOf(cp1Machine)[plan.ComponentAPIServer]).To(HaveSuffix(":v"+patch), "what was confirmed stays")
				Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
				Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))

				before := h.applies()
				h.annotate(talosv1alpha1.AnnotationSkipMachine, f.Token)
				h.advance(time.Hour)
				Expect(h.failed()).To(BeTrue(), "and an annotation that asks for it is not followed")
				Expect(h.applies()).To(Equal(before))
				Expect(pulled()).To(HaveLen(2))
			})

			It("runs the turn again on retry", func() {
				stuck = map[string]bool{}
				h.annotate(talosv1alpha1.AnnotationRetry, h.operation().Failure.Token)
				h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })

				Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
				Expect(imagesOf(worker2)[plan.ComponentKubelet]).To(HaveSuffix(":v" + patch))
			})

			It("ends on abort, and does not start again until the spec asks for another version", func() {
				stuck = map[string]bool{}
				h.annotate(talosv1alpha1.AnnotationAbort, h.operation().Failure.Token)
				h.settle(h.idle)

				Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
				Expect(h.tc.Status.Upgrades.Kubernetes).To(Equal(patch))

				sent := len(h.applies())
				h.advance(time.Hour)
				Expect(h.operation()).To(BeNil())
				Expect(h.applies()).To(HaveLen(sent))
				Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonAborted))

				upgradeKubernetes("1.37.2")
				h.runUntil("the upgrade to the other version ends", func() bool { return len(kubernetesOperations()) == 2 })
				Expect(h.tc.Status.Upgrades).To(BeNil())
				Expect(imagesOf(cp1Machine)[plan.ComponentControllerManager]).To(HaveSuffix(":v1.37.2"))
			})
		})

		It("halts on an image that cannot be pulled, with nothing changed", func() {
			h.scenario.FailNext(fake.MethodKubernetesImagePull, cpAddr, status.Error(codes.NotFound, "manifest unknown"))
			upgradeKubernetes(patch)
			h.runUntil("the upgrade halts", h.failed)

			Expect(h.operation().Failure.Reason).To(Equal(plan.FailImagePull))
			Expect(h.operation().Failure.Message).To(ContainSubstring(apiServer + patch))
			Expect(h.applies()).To(BeEmpty())
			Expect(imagesOf(cp1Machine)[plan.ComponentAPIServer]).To(HaveSuffix(":v" + kubernetesVersion))
		})

		It("takes a version that is more than a minor away one minor at a time", func() {
			// The cluster is two minors behind what its spec asks for.
			for _, n := range rolloutNodes {
				h.scenario.UpdateNode(n.addr, func(fn *fake.Node) {
					images, err := talos.KubernetesImages(fn.Config.Active)
					Expect(err).NotTo(HaveOccurred())

					for component, image := range images {
						fn.Config.Active, err = talos.WithKubernetesImage(fn.Config.Active, component, strings.Replace(image, kubernetesVersion, "1.35.4", 1))
						Expect(err).NotTo(HaveOccurred())
					}
				})
			}

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled })
			h.settle(h.started)
			Expect(h.operation().Target).To(Equal("1.36.0"), "the first release of the minor in between")

			h.runUntil("the first hop ends", func() bool { return len(kubernetesOperations()) == 1 })
			for _, n := range rolloutNodes {
				Expect(imagesOf(n.name)[plan.ComponentKubelet]).To(Equal(kubelet + "1.36.0"))
			}

			h.runUntil("the second hop ends", func() bool { return len(kubernetesOperations()) == 2 })
			for _, n := range rolloutNodes {
				for _, image := range imagesOf(n.name) {
					Expect(image).To(HaveSuffix(":v" + kubernetesVersion))
				}
			}

			h.runUntil("everything settles", settled)
			Expect(pulled()).To(HaveLen(12))
		})

		It("goes through the newest release it knows of a minor on the way, and refuses a release that does not exist", func() {
			h.reconciler.Catalog = upstream()

			upgradeKubernetes("1.37.9")
			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(upToDate().Message).To(And(ContainSubstring("these images do not exist"),
				ContainSubstring("registry.k8s.io/kube-apiserver:v1.37.9"), ContainSubstring("ghcr.io/siderolabs/kubelet:v1.37.9")))
			Expect(pulled()).To(BeEmpty())

			// Two minors behind what the spec asks for now.
			for _, n := range rolloutNodes {
				h.scenario.UpdateNode(n.addr, func(fn *fake.Node) {
					images, err := talos.KubernetesImages(fn.Config.Active)
					Expect(err).NotTo(HaveOccurred())

					for component, image := range images {
						fn.Config.Active, err = talos.WithKubernetesImage(fn.Config.Active, component, strings.Replace(image, kubernetesVersion, "1.35.4", 1))
						Expect(err).NotTo(HaveOccurred())
					}
				})
			}

			upgradeKubernetes(kubernetesVersion)
			h.settle(h.started)
			Expect(h.operation().Target).To(Equal("1.36.5"), "not the first release of the minor in between, but the newest")
			Expect(h.tc.Status.Versions.Plan).To(Equal([]talosv1alpha1.VersionStep{
				{Kind: plan.StepKubernetes, From: "1.35.4", To: "1.36.5"}, {Kind: plan.StepKubernetes, From: "1.36.5", To: kubernetesVersion},
			}))

			h.runUntil("both hops end", func() bool { return len(kubernetesOperations()) == 2 })
			Expect(imagesOf(worker1)[plan.ComponentKubelet]).To(HaveSuffix(":v" + kubernetesVersion))
		})

		It("is carried on by another replica from what is persisted, without a second try", func() {
			upgradeKubernetes(patch)
			h.runUntil("the API server is tried", turnIs(plan.ComponentAPIServer, cp1Machine, plan.StepVerifying))

			h.takeOver()

			h.runUntil("the scheduler's turn", turnIs(plan.ComponentScheduler, cp1Machine, plan.StepVerifying))
			Expect(h.applies()).To(Equal([]string{
				tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine), confirmed(cp1Machine), tried(cp1Machine),
			}))
		})

		It("finishes the component it is at when paused, and begins no other", func() {
			upgradeKubernetes(patch)
			h.runUntil("the API server is tried", turnIs(plan.ComponentAPIServer, cp1Machine, plan.StepVerifying))

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
			h.runUntil("the API server is confirmed", func() bool { return len(h.operation().Completed) == 1 })
			h.advance(time.Hour)

			Expect(h.applies()).To(Equal([]string{tried(cp1Machine), confirmed(cp1Machine)}))
			Expect(pulled()).To(HaveLen(1), "not even the next image is pulled")
			Expect(h.started()).To(BeTrue())
			Expect(h.blocked().Reason).To(Equal(talosv1alpha1.ReasonPaused))

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
			h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
		})

		It("pulls nothing while paused, and does not count the pause against the turn", func() {
			// The first pull does not get through, so the turn is still at its beginning when the
			// pause comes.
			h.scenario.FailNext(fake.MethodKubernetesImagePull, cpAddr, status.Error(codes.Unavailable, "connection refused"))
			upgradeKubernetes(patch)
			h.runUntil("the first turn begins", turnIs(plan.ComponentAPIServer, cp1Machine, plan.StepPulling))

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
			h.scenario.FailNext(fake.MethodKubernetesImagePull, cpAddr, nil)
			tries := len(h.scenario.CallsFor(fake.MethodKubernetesImagePull))
			h.advance(time.Hour)

			Expect(h.scenario.CallsFor(fake.MethodKubernetesImagePull)).To(HaveLen(tries), "a paused upgrade asks nothing of a node")
			Expect(h.applies()).To(BeEmpty())
			Expect(h.failed()).To(BeFalse(), "a pause is not a timeout")
			Expect(h.operation().Current).To(BeNil(), "the turn is dropped, and begins again later")

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
			h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
			Expect(kubernetesOperations()[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		})

		It("does not start on a cluster that is not healthy", func() {
			h.sicken(worker2, "")
			upgradeKubernetes(patch)
			h.advance(time.Hour)

			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
			Expect(pulled()).To(BeEmpty())
		})

		It("refuses to leave a control plane out, and leaves a worker on hold behind", func() {
			hold := func(machine string, held bool) {
				GinkgoHelper()

				tm := h.machine(machine)
				tm.Spec.Hold = held
				Expect(k8sClient.Update(ctx, tm)).To(Succeed())
			}

			hold(cp1Machine, true)
			upgradeKubernetes(patch)
			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil())
			Expect(upToDate().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(upToDate().Message).To(ContainSubstring("cannot leave a control plane out"))

			hold(cp1Machine, false)
			hold(worker2, true)
			h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })

			Expect(imagesOf(worker1)[plan.ComponentKubelet]).To(HaveSuffix(":v" + patch))
			Expect(imagesOf(worker2)[plan.ComponentKubelet]).To(HaveSuffix(":v" + kubernetesVersion))
			h.runUntil("the rest settles", func() bool { return h.idle() && upToDate().Reason == talosv1alpha1.ReasonHeld })
			Expect(upToDate().Message).To(ContainSubstring(worker2))
		})

		It("runs the rollout's hooks around the upgrade, and none around its nodes", func() {
			var hooks []string

			inner := h.kubelets
			h.kubelets = func() {
				GinkgoHelper()
				inner()

				var list batchv1.JobList
				Expect(k8sClient.List(ctx, &list, client.InNamespace(h.ns), client.HasLabels{talosv1alpha1.LabelHook})).To(Succeed())

				for i := range list.Items {
					job := &list.Items[i]
					if len(job.Status.Conditions) > 0 {
						continue
					}

					hooks = append(hooks, job.Labels[talosv1alpha1.LabelHook])

					now := metav1.NewTime(h.clock)
					job.Status.StartTime, job.Status.CompletionTime, job.Status.Succeeded = &now, &now, 1
					job.Status.Conditions = []batchv1.JobCondition{
						{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
						{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
					}
					Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
				}
			}

			job := runtime.RawExtension{Raw: []byte(hookJob)}
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
				tc.Spec.Rollout.Hooks = &talosv1alpha1.RolloutHooks{
					PreRollout: []talosv1alpha1.Hook{{Name: "announce", Job: job}}, PostRollout: []talosv1alpha1.Hook{{Name: "all-clear", Job: job}},
					PreNode: []talosv1alpha1.Hook{{Name: "prepare", Job: job}}, PostNode: []talosv1alpha1.Hook{{Name: "release", Job: job}},
				}
			})

			upgradeKubernetes(patch)
			h.runUntil("the upgrade ends", func() bool { return len(kubernetesOperations()) == 1 })
			Expect(hooks).To(Equal([]string{talosv1alpha1.HookPreRollout, talosv1alpha1.HookPostRollout}))
		})
	})

	Context("version awareness", func() {
		var source *catalog.Fake

		versions := func() *talosv1alpha1.VersionsStatus {
			GinkgoHelper()
			Expect(h.tc.Status.Versions).NotTo(BeNil())

			return h.tc.Status.Versions
		}

		reachable := func() *metav1.Condition {
			GinkgoHelper()

			c := condition(h.tc, talosv1alpha1.ConditionVersionsReachable)
			Expect(c).NotTo(BeNil())

			return c
		}

		BeforeEach(func() {
			source = upstream()
			h.reconciler.Catalog = &catalog.Cached{Source: source, Now: func() time.Time { return h.clock }}
			h.settle(nil)
		})

		It("says what the cluster runs and what exists, and that there is nothing to do", func() {
			Expect(versions().Talos).To(Equal(talosVersion))
			Expect(versions().Kubernetes).To(Equal(kubernetesVersion))
			Expect(versions().Plan).To(BeEmpty())
			Expect(reachable().Status).To(Equal(metav1.ConditionTrue))
			Expect(reachable().Reason).To(Equal(talosv1alpha1.ReasonUpToDate))

			// Of Talos: the newest release of each minor this operator supports, nothing older than
			// what runs. Of Kubernetes: up to the newest minor one of those runs.
			Expect(versions().Available.Talos).To(Equal([]string{newerTalosVersion}))
			Expect(versions().Available.Kubernetes).To(Equal([]string{"1.37.1"}))
			Expect(versions().Available.Problem).To(BeEmpty())

			// What exists is information. Nothing is upgraded because of it.
			h.advance(time.Hour)
			Expect(h.operation()).To(BeNil())
			Expect(h.tc.Status.History).To(BeEmpty())
			Expect(h.scenario.CallsFor(fake.MethodImagePull)).To(BeEmpty())
		})

		It("asks upstream rarely, and not at all while an operation is in flight", func() {
			asked := source.Asked
			for range 20 {
				h.round()
			}

			Expect(source.Asked).To(Equal(asked), "an answer is kept for a while")

			h.setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
			h.settle(h.started)

			asked = source.Asked
			h.clock = h.clock.Add(catalog.DefaultKeep)
			h.settle(nil)
			Expect(h.started()).To(BeTrue())
			Expect(source.Asked).To(Equal(asked), "an operation in flight has deadlines to keep")
		})

		It("plans the way to what the spec asks for, and goes it in that order", func() {
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
				tc.Spec.Versions.Talos, tc.Spec.Versions.Kubernetes = newerTalosVersion, "1.37.1"
				tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
			})
			h.settle(h.started)

			Expect(versions().Plan).To(Equal([]talosv1alpha1.VersionStep{
				{Kind: plan.StepTalos, From: talosVersion, To: newerTalosVersion},
				{Kind: plan.StepKubernetes, From: kubernetesVersion, To: "1.37.1"},
			}))
			Expect(reachable().Reason).To(Equal(talosv1alpha1.ReasonPlanned))
			Expect(reachable().Message).To(ContainSubstring("2 upgrade(s)"))
			Expect(h.operation().Type).To(Equal(talosv1alpha1.OperationTalosUpgrade), "the first step of the plan is the first operation")
		})

		It("says why the versions a spec asks for cannot be reached", func() {
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
				tc.Spec.Versions.Kubernetes = "1.38.0"
				tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
			})
			h.advance(time.Minute)

			Expect(reachable().Status).To(Equal(metav1.ConditionFalse))
			Expect(reachable().Reason).To(Equal(talosv1alpha1.ReasonIncompatible))
			Expect(reachable().Message).To(ContainSubstring("the spec asks for Talos " + talosVersion + " with Kubernetes 1.38.0"))
			Expect(versions().Plan).To(BeEmpty())
			Expect(h.operation()).To(BeNil())

			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Versions.Kubernetes = "1.36.5" })
			h.advance(time.Minute)
			Expect(reachable().Message).To(ContainSubstring("not downgraded"))
		})

		It("keeps what it knew when upstream does not answer, and says so", func() {
			source.Err = errors.New("no route to host")
			h.clock = h.clock.Add(catalog.DefaultKeep)
			h.settle(nil)

			Expect(versions().Available.Talos).To(Equal([]string{newerTalosVersion}))
			Expect(versions().Available.Kubernetes).To(Equal([]string{"1.37.1"}))
			Expect(versions().Available.Problem).To(ContainSubstring("no route to host"))
			Expect(reachable().Status).To(Equal(metav1.ConditionTrue), "what the cluster runs and may be upgraded to does not depend on it")

			source.Err = nil
			h.clock = h.clock.Add(catalog.DefaultRetry)
			h.settle(nil)
			Expect(versions().Available.Problem).To(BeEmpty())
		})

		It("asks nobody when it was started without a source", func() {
			h.reconciler.Catalog = nil
			h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
			asked := source.Asked
			h.clock = h.clock.Add(2 * catalog.DefaultKeep)
			h.settle(nil)

			Expect(source.Asked).To(Equal(asked))
			Expect(reachable().Status).To(Equal(metav1.ConditionTrue))
		})
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
