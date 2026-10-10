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
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
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

// rolloutNode is one node of the cluster the rollout specs manage: a fake Talos node, its
// Kubernetes Node with a heartbeat Lease, and its TalosMachine.
type rolloutNode struct {
	machine string
	node    string
	addr    netip.Addr
	uuid    string
	role    talosv1alpha1.MachineRole
	etcd    uint64
}

var (
	worker2Addr = netip.MustParseAddr("10.5.0.4")

	rolloutNodes = []rolloutNode{
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

var _ = Describe("Config rollout", func() {
	var (
		ns                string
		scenario          *fake.Scenario
		recorder          *events.FakeRecorder
		reconciler        *TalosClusterReconciler
		machineReconciler *TalosMachineReconciler
		tc                *talosv1alpha1.TalosCluster
		// clock is the operator's clock; the specs move it.
		clock time.Time
		// silent kubelets renew no Lease although their node's Talos API answers.
		silent map[netip.Addr]bool
	)

	node := func(machine string) rolloutNode {
		GinkgoHelper()

		for _, n := range rolloutNodes {
			if n.machine == machine {
				return n
			}
		}

		Fail("no node " + machine)

		return rolloutNode{}
	}

	newReconciler := func() *TalosClusterReconciler {
		return &TalosClusterReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			Resolver:  &target.Resolver{Client: k8sClient, Factory: &fake.Factory{Scenario: scenario}},
			Recorder:  recorder,
			APIReader: k8sClient,
			Now:       func() time.Time { return clock },
		}
	}

	// heartbeat is the kubelets, and Kubernetes watching them: every node that can reach the API
	// server renews its Lease, and a node whose kubelet is down (see sicken) is not Ready.
	heartbeat := func() {
		GinkgoHelper()

		for _, n := range rolloutNodes {
			live := scenario.Node(n.addr)
			sick := live.UnhealthyWhen != nil && live.UnhealthyWhen(live.Config.Active)

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

			if sick || silent[n.addr] || (live.UnreachableWhen != nil && live.UnreachableWhen(live.Config.Active)) {
				continue
			}

			lease := &coordinationv1.Lease{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: nodeLeaseNamespace, Name: n.node}, lease)).To(Succeed())
			lease.Spec.RenewTime = &metav1.MicroTime{Time: clock}
			Expect(k8sClient.Update(ctx, lease)).To(Succeed())
		}
	}

	// applies lists every config the operator really applied, in order, as "<machine> try" or
	// "<machine> confirm".
	applies := func() []string {
		var out []string

		for _, c := range scenario.CallsFor(fake.MethodApplyConfiguration) {
			if c.DryRun {
				continue
			}

			name := c.Node.String()

			for _, n := range rolloutNodes {
				if n.addr == c.Node {
					name = n.machine
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

	// round lets the kubelets and both controllers run once, machines first.
	round := func() {
		GinkgoHelper()

		heartbeat()

		for _, n := range rolloutNodes {
			_, err := machineReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: n.machine}})
			Expect(err).NotTo(HaveOccurred())
		}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
	}

	// settle runs rounds until one changes nothing: no status, and nothing sent to Talos. It
	// stops early as soon as stop, if given, holds.
	settle := func(stop func() bool) {
		GinkgoHelper()

		for range 40 {
			version, sent := tc.ResourceVersion, len(applies())

			round()

			if stop != nil && stop() {
				return
			}

			if tc.ResourceVersion == version && len(applies()) == sent {
				return
			}
		}

		Fail("the controllers never settled")
	}

	// advance moves the clock and lets everything settle.
	advance := func(d time.Duration) {
		GinkgoHelper()

		clock = clock.Add(d)
		settle(nil)
	}

	// runEvery advances time in steps until done holds.
	runEvery := func(step time.Duration, what string, done func() bool) {
		GinkgoHelper()

		for range 80 {
			if done() {
				return
			}

			clock = clock.Add(step)
			settle(done)
		}

		Fail(fmt.Sprintf("never happened: %s; operation: %+v; ConfigInSync: %+v", what,
			tc.Status.Operation, condition(tc, talosv1alpha1.ConditionConfigInSync)))
	}

	// runUntil advances time the way a rollout usually sees it pass: a few seconds at a time.
	runUntil := func(what string, done func() bool) {
		GinkgoHelper()

		runEvery(10*time.Second, what, done)
	}

	// at sets the clock and lets everything settle.
	at := func(t time.Time) {
		GinkgoHelper()

		clock = t
		settle(nil)
	}

	operation := func() *talosv1alpha1.Operation { return tc.Status.Operation }
	started := func() bool { return operation() != nil }
	idle := func() bool { return operation() == nil }
	failed := func() bool { return operation() != nil && operation().Failure != nil }
	stepIs := func(machine, step string) func() bool {
		return func() bool {
			cur := tc.Status.Operation
			return cur != nil && cur.Current != nil && cur.Current.Machine == machine && cur.Current.Step == step
		}
	}

	inSync := func() *metav1.Condition {
		GinkgoHelper()

		c := condition(tc, talosv1alpha1.ConditionConfigInSync)
		Expect(c).NotTo(BeNil())

		return c
	}

	blocked := func() *metav1.Condition { return condition(tc, talosv1alpha1.ConditionRolloutBlocked) }

	getMachine := func(name string) *talosv1alpha1.TalosMachine {
		GinkgoHelper()

		tm := &talosv1alpha1.TalosMachine{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, tm)).To(Succeed())

		return tm
	}

	// setPoolConfig sets the config layer of a role's pool.
	setPoolConfig := func(role talosv1alpha1.MachineRole, documents ...string) {
		GinkgoHelper()

		pool := &talosv1alpha1.TalosMachinePool{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: poolFor(role)}, pool)).To(Succeed())

		pool.Spec.Config = nil
		for _, d := range documents {
			pool.Spec.Config = append(pool.Spec.Config, talosv1alpha1.ConfigSource{Inline: d})
		}

		Expect(k8sClient.Update(ctx, pool)).To(Succeed())
	}

	updateCluster := func(mutate func(*talosv1alpha1.TalosCluster)) {
		GinkgoHelper()

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc)).To(Succeed())
		mutate(tc)
		Expect(k8sClient.Update(ctx, tc)).To(Succeed())
	}

	annotate := func(key, value string) {
		GinkgoHelper()

		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			if tc.Annotations == nil {
				tc.Annotations = map[string]string{}
			}

			tc.Annotations[key] = value
		})
	}

	// cutOff makes a node unreachable for as long as its active config carries the dead resolver:
	// its Talos API stops answering and its kubelet stops renewing its Lease.
	cutOff := func(machine string) {
		scenario.UpdateNode(node(machine).addr, func(n *fake.Node) {
			n.UnreachableWhen = func(active []byte) bool { return bytes.Contains(active, []byte(deadResolver)) }
		})
	}

	// sicken makes a node unhealthy for as long as its active config contains marker, or for good
	// if marker is empty: the node answers, but its kubelet is down. Talos reports the node not
	// ready and fails the cluster's health check on it, and Kubernetes reports it not Ready.
	sicken := func(machine, marker string) {
		scenario.UpdateNode(node(machine).addr, func(n *fake.Node) {
			n.UnhealthyWhen = func(active []byte) bool { return bytes.Contains(active, []byte(marker)) }
		})
	}

	// expireTry is Talos's own timer: it restores the config a try replaced.
	expireTry := func(machine string) {
		GinkgoHelper()

		Expect(scenario.ExpireTry(node(machine).addr)).To(BeTrue(), "a try must be pending on "+machine)
	}

	BeforeEach(func() {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "rollout-"}}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name

		clock = time.Now().Truncate(time.Second)
		silent = map[netip.Addr]bool{}

		fakes := make([]fake.Node, 0, len(rolloutNodes))

		for _, n := range rolloutNodes {
			machineType := fake.TypeWorker
			if n.role == talosv1alpha1.MachineRoleControlPlane {
				machineType = fake.TypeControlPlane
			}

			fakes = append(fakes, fakeNode(n.addr, machineType, n.node, n.uuid, n.etcd))

			kube := kubeNode(n.node)
			kube.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			Expect(k8sClient.Create(ctx, kube)).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, kube)).To(Succeed())

			Expect(k8sClient.Create(ctx, &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{Name: n.node, Namespace: nodeLeaseNamespace},
				Spec:       coordinationv1.LeaseSpec{RenewTime: &metav1.MicroTime{Time: clock}},
			})).To(Succeed())
		}

		DeferCleanup(func() {
			Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())
			Expect(k8sClient.DeleteAllOf(ctx, &coordinationv1.Lease{}, client.InNamespace(nodeLeaseNamespace))).To(Succeed())
		})

		scenario = fake.NewScenario(fakes...)
		recorder = events.NewFakeRecorder(4096)

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: credentialsSecret, Namespace: ns},
			Data:       map[string][]byte{creds.DefaultTalosconfigKey: talostest.AdminTalosconfig(GinkgoT(), cpAddr.String())},
		})).To(Succeed())

		// A short soak keeps the specs short; the try window stays at its default.
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

		reconciler = newReconciler()
		machineReconciler = &TalosMachineReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Resolver: reconciler.Resolver}

		// The specs bring their own machines, under their own names, so the operator writes none.
		for _, n := range rolloutNodes {
			Expect(k8sClient.Create(ctx, machine(ns, n.machine, n.role, n.addr, n.uuid))).To(Succeed())
		}

		// Adopt: the first pass stores the bundle the machines render with.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tc)})
		Expect(err).NotTo(HaveOccurred())

		settle(nil)
		Expect(condition(tc, talosv1alpha1.ConditionConfigProven).Status).To(Equal(metav1.ConditionTrue), "the cluster must adopt cleanly")

		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Management.Config = talosv1alpha1.ConfigManagementManage })
		settle(nil)

		Expect(condition(tc, talosv1alpha1.ConditionConfigHandover).Status).To(Equal(metav1.ConditionTrue))
		Expect(inSync().Status).To(Equal(metav1.ConditionTrue), inSync().Message)
		Expect(applies()).To(BeEmpty(), "adopting and taking over must not apply anything")
	})

	It("takes over every node's config as the baseline drift is measured from", func() {
		for _, n := range rolloutNodes {
			cfg := getMachine(n.machine).Status.Config
			Expect(cfg.AppliedHash).To(Equal(cfg.LiveHash), n.machine)
			Expect(cfg.AppliedHash).To(Equal(cfg.DesiredHash), n.machine)
		}

		Expect(operation()).To(BeNil())
		Expect(blocked()).To(BeNil())
	})

	It("rolls a label out one node at a time, each confirmed only after staying healthy", func() {
		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		settle(started)

		// Planning takes the lock and sends nothing.
		Expect(operation()).NotTo(BeNil())
		op := operation().DeepCopy()
		Expect(op.Type).To(Equal(talosv1alpha1.OperationConfigRollout))
		Expect(op.Plan).To(Equal([]string{worker1, worker2}))
		Expect(op.Targets).To(HaveLen(2))
		Expect(op.Bases).To(HaveLen(2))
		Expect(applies()).To(BeEmpty())
		Expect(inSync().Reason).To(Equal(talosv1alpha1.ReasonRollingOut))

		// The step is persisted before the try is sent.
		runUntil("worker-1 is being tried", stepIs(worker1, plan.StepTrying))
		Expect(applies()).To(BeEmpty())

		step := operation().Current
		Expect(step.PreviousHash).To(Equal(op.Bases[worker1]))
		Expect(step.DesiredHash).To(Equal(op.Targets[worker1]))
		Expect(step.Deadline.Time).To(BeTemporally("==", step.Since.Add(plan.DefaultTryTimeout)))
		Expect(step.Class).To(Equal("LiveSafe"))

		runUntil("worker-1 is verified", stepIs(worker1, plan.StepVerifying))
		Expect(applies()).To(Equal([]string{tried(worker1)}))
		Expect(scenario.TryPending(workerAddr)).To(BeTrue(), "the config is only tried, not persistent")

		runUntil("the rollout ends", idle)

		Expect(applies()).To(Equal([]string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}),
			"one node at a time, a try never repeated, the control plane never touched")

		settle(nil)
		Expect(inSync().Status).To(Equal(metav1.ConditionTrue), inSync().Message)
		Expect(blocked()).To(BeNil())
		Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseManaged))

		Expect(tc.Status.History).To(HaveLen(1))
		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(tc.Status.History[0].Completed).To(Equal([]string{worker1, worker2}))

		for _, name := range []string{worker1, worker2} {
			live := scenario.Node(node(name).addr)
			Expect(string(live.Config.Active)).To(ContainSubstring("rollout: one"))
			Expect(live.Config.Persistent).To(Equal(live.Config.Active), "confirmed configs are persistent")
			Expect(scenario.TryPending(node(name).addr)).To(BeFalse())

			cfg := getMachine(name).Status.Config
			Expect(cfg.AppliedHash).To(Equal(cfg.DesiredHash))
			Expect(cfg.LiveHash).To(Equal(cfg.DesiredHash))
		}

		Expect(drain(recorder)).To(ContainElements(
			ContainSubstring("RolloutStarted"), ContainSubstring("MachineConfigured"), ContainSubstring("RolloutSucceeded")))
	})

	It("rolls a cluster-wide change out to workers first, then the control plane", func() {
		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: labelConfig("everywhere")}}
		})
		settle(started)
		Expect(operation().Plan).To(Equal([]string{worker1, worker2, cp1Machine}))

		runUntil("the rollout ends", idle)
		Expect(applies()).To(Equal([]string{
			tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2), tried(cp1Machine), confirmed(cp1Machine),
		}))

		settle(nil)
		Expect(inSync().Status).To(Equal(metav1.ConditionTrue), inSync().Message)
	})

	Context("a change that cuts a node off", func() {
		BeforeEach(func() {
			cutOff(worker1)
			setPoolConfig(talosv1alpha1.MachineRoleWorker, resolverConfig)
			runUntil("worker-1 is tried", func() bool { return len(applies()) > 0 })

			Expect(applies()).To(Equal([]string{tried(worker1)}))
			Expect(operation().Current.Class).To(Equal("Connectivity"))

			// The node went dark with the try, so the operator never even sees the config active.
			advance(time.Minute)
			Expect(operation().Current.Step).To(Equal(plan.StepTrying))
			Expect(inSync().Message).To(ContainSubstring("not reachable"))
		})

		// windowPasses lets the try window pass without the node ever answering.
		windowPasses := func() time.Time {
			GinkgoHelper()

			deadline := operation().Current.Deadline.Time

			at(deadline.Add(-time.Second))
			Expect(operation().Current.Step).To(Equal(plan.StepTrying), "it waits for the whole window")

			at(deadline)
			Expect(operation().Current.Step).To(Equal(plan.StepRollingBack))
			Expect(applies()).To(Equal([]string{tried(worker1)}))

			return deadline
		}

		// rollBack lets the window pass, then Talos revert, and waits for the operator to notice.
		rollBack := func() {
			GinkgoHelper()

			deadline := windowPasses()
			expireTry(worker1)

			at(deadline.Add(plan.RollbackGrace - time.Second))
			Expect(failed()).To(BeFalse(), "Talos gets a moment to revert")

			at(deadline.Add(plan.RollbackGrace))
			Expect(failed()).To(BeTrue())
		}

		It("is reverted by Talos, never confirmed, and halts the rollout before another node is touched", func() {
			rollBack()

			Expect(applies()).To(Equal([]string{tried(worker1)}), "no confirm, no second try, no other node")

			f := operation().Failure
			Expect(f.Machine).To(Equal(worker1))
			Expect(f.Reason).To(Equal(plan.FailRolledBack))
			Expect(f.Token).NotTo(BeEmpty())
			Expect(f.Remediation).To(ContainSubstring(talosv1alpha1.AnnotationRetry + "=" + f.Token))

			Expect(blocked().Status).To(Equal(metav1.ConditionTrue))
			Expect(blocked().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
			Expect(inSync().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed))
			Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))
			Expect(drain(recorder)).To(ContainElement(ContainSubstring("RolloutHalted")))

			live := scenario.Node(workerAddr)
			Expect(string(live.Config.Active)).NotTo(ContainSubstring(deadResolver), "the node is back on its previous config")
			Expect(getMachine(worker1).Status.Config.AppliedHash).To(Equal(operation().Bases[worker1]))

			// It stays halted, whatever time passes.
			advance(time.Hour)
			Expect(applies()).To(Equal([]string{tried(worker1)}))
			Expect(failed()).To(BeTrue())
		})

		It("is not confirmed by an operator that takes over in the middle of the try", func() {
			// A new process: nothing but status.operation tells it a try is in flight.
			reconciler = newReconciler()
			machineReconciler.Resolver = reconciler.Resolver

			rollBack()

			Expect(applies()).To(Equal([]string{tried(worker1)}))
			Expect(operation().Failure.Reason).To(Equal(plan.FailRolledBack))
		})

		It("escalates when the node does not come back on its previous config", func() {
			deadline := windowPasses()

			// Talos's revert never shows: the node stays dark.
			at(deadline.Add(plan.RollbackGrace + plan.DefaultStepTimeout - time.Second))
			Expect(failed()).To(BeFalse(), "a node gets stepTimeout to come back")

			at(deadline.Add(plan.RollbackGrace + plan.DefaultStepTimeout))
			Expect(failed()).To(BeTrue())
			Expect(operation().Failure.Reason).To(Equal(plan.FailNodeUnreachableAfterRollback))
			Expect(applies()).To(Equal([]string{tried(worker1)}))
		})

		It("ignores an annotation that does not carry the failure's token", func() {
			rollBack()

			for _, a := range []string{talosv1alpha1.AnnotationRetry, talosv1alpha1.AnnotationSkipMachine, talosv1alpha1.AnnotationAbort} {
				annotate(a, "true")
			}

			advance(time.Minute)
			Expect(failed()).To(BeTrue())
			Expect(applies()).To(Equal([]string{tried(worker1)}))
		})

		It("runs the step again on retry, from what the node runs now", func() {
			rollBack()

			// Whatever cut the node off is fixed.
			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.UnreachableWhen = nil })
			annotate(talosv1alpha1.AnnotationRetry, operation().Failure.Token)

			settle(func() bool { return !failed() })
			Expect(operation().Failure).To(BeNil())
			Expect(operation().Current.Attempt).To(Equal(int32(2)))
			Expect(operation().Current.Step).To(Equal(plan.StepTrying))

			runUntil("the rollout ends", idle)
			Expect(applies()).To(Equal([]string{tried(worker1), tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}))
			Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(drain(recorder)).To(ContainElement(ContainSubstring("RolloutResumed")))
		})

		It("leaves the machine out on skip-machine and goes on, and does not try the same config on it again", func() {
			rollBack()
			target := operation().Targets[worker1]

			annotate(talosv1alpha1.AnnotationSkipMachine, operation().Failure.Token)
			runUntil("the rollout ends", idle)

			Expect(applies()).To(Equal([]string{tried(worker1), tried(worker2), confirmed(worker2)}))
			Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
			Expect(tc.Status.History[0].Skipped).To(Equal([]string{worker1}))
			Expect(getMachine(worker1).Status.Config.SkippedHash).To(Equal(target))

			advance(time.Hour)
			Expect(operation()).To(BeNil(), "a skipped config is not rolled out to the machine again")
			Expect(inSync().Status).To(Equal(metav1.ConditionFalse))
			Expect(inSync().Reason).To(Equal(talosv1alpha1.ReasonHeld))
			Expect(inSync().Message).To(ContainSubstring(worker1))

			// A different config is a different matter.
			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.UnreachableWhen = nil })
			setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("instead"))
			runUntil("the new config is rolled out", func() bool {
				return idle() && len(tc.Status.History) == 2
			})

			settle(nil)
			Expect(inSync().Status).To(Equal(metav1.ConditionTrue), inSync().Message)
			Expect(getMachine(worker1).Status.Config.SkippedHash).To(BeEmpty())
		})

		It("ends the rollout on abort and does not start the same one again", func() {
			rollBack()

			annotate(talosv1alpha1.AnnotationAbort, operation().Failure.Token)
			settle(idle)

			Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
			Expect(tc.Status.AbortedTargets).To(HaveLen(2))

			advance(time.Hour)
			Expect(operation()).To(BeNil())
			Expect(applies()).To(Equal([]string{tried(worker1)}))
			Expect(blocked().Reason).To(Equal(talosv1alpha1.ReasonAborted))
			Expect(inSync().Reason).To(Equal(talosv1alpha1.ReasonAborted))
			Expect(drain(recorder)).To(ContainElement(ContainSubstring("RolloutAborted")))

			// Reverting the spec leaves nothing to roll out, and the abort is spent.
			setPoolConfig(talosv1alpha1.MachineRoleWorker)
			advance(time.Minute)

			Expect(inSync().Status).To(Equal(metav1.ConditionTrue), inSync().Message)
			Expect(blocked()).To(BeNil())
			Expect(tc.Status.AbortedTargets).To(BeEmpty())
			Expect(applies()).To(Equal([]string{tried(worker1)}))
		})
	})

	It("does not confirm a node whose kubelet stopped renewing its Lease, though Talos and its Ready condition look fine", func() {
		silent[workerAddr] = true

		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("worker-1 is verified", stepIs(worker1, plan.StepVerifying))

		advance(2 * time.Minute)
		Expect(operation().Current.HealthySince).To(BeNil(), "the soak does not start without a heartbeat")
		Expect(inSync().Message).To(ContainSubstring("Lease"))

		// It stops hoping while Talos's own timer still runs, so a confirm can never come late.
		deadline := operation().Current.Deadline.Time

		at(deadline.Add(-plan.ConfirmMargin - time.Second))
		Expect(operation().Current.Step).To(Equal(plan.StepVerifying))

		at(deadline.Add(-plan.ConfirmMargin))
		Expect(operation().Current.Step).To(Equal(plan.StepRollingBack))

		// Even a node that turns healthy now is not confirmed any more.
		silent[workerAddr] = false
		at(deadline.Add(-time.Second))
		Expect(applies()).To(Equal([]string{tried(worker1)}))

		expireTry(worker1)
		at(deadline.Add(plan.RollbackGrace))
		Expect(operation().Failure.Reason).To(Equal(plan.FailRolledBack))
		Expect(applies()).To(Equal([]string{tried(worker1)}))
	})

	It("starts the soak over when a check fails in the middle of it", func() {
		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("worker-1 soaks", func() bool {
			return stepIs(worker1, plan.StepVerifying)() && operation().Current.HealthySince != nil
		})

		first := operation().Current.HealthySince.Time

		scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{etcdUnhealthy}})
		advance(10 * time.Second)
		Expect(operation().Current.HealthySince).To(BeNil())
		Expect(applies()).To(Equal([]string{tried(worker1)}))

		scenario.SetHealth(talos.HealthResult{Healthy: true})
		runUntil("worker-1 soaks again", func() bool {
			return operation() != nil && operation().Current != nil && operation().Current.HealthySince != nil
		})
		Expect(operation().Current.HealthySince.Time).To(BeTemporally(">", first))

		runUntil("the rollout ends", idle)
		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
	})

	It("refuses to change a protected field", func() {
		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: tokenConfig}}
		})
		advance(time.Minute)

		Expect(operation()).To(BeNil())
		Expect(blocked().Status).To(Equal(metav1.ConditionTrue))
		Expect(blocked().Reason).To(Equal(talosv1alpha1.ReasonProtectedChange))
		Expect(blocked().Message).To(ContainSubstring("machine.token"))
		Expect(blocked().Message).NotTo(ContainSubstring("aaaaaa"), "the value of a protected field is never shown")
		Expect(inSync().Reason).To(Equal(talosv1alpha1.ReasonProtectedChange))
		Expect(getMachine(cp1Machine).Status.Config.ChangeClass).To(Equal("Protected"))
		Expect(drain(recorder)).To(ContainElement(ContainSubstring("RolloutBlocked is True")))

		advance(time.Hour)
		Expect(applies()).To(BeEmpty())

		// One blocked machine blocks the rollout for every machine.
		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		advance(time.Minute)
		Expect(operation()).To(BeNil())
		Expect(applies()).To(BeEmpty())

		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Config = nil })
		runUntil("the label is rolled out", func() bool { return idle() && len(tc.Status.History) == 1 })
		Expect(applies()).To(HaveLen(4))
	})

	It("waits for a human to approve a storage change, by the hash of exactly that change", func() {
		setPoolConfig(talosv1alpha1.MachineRoleWorker, volumeConfig)
		advance(time.Minute)

		Expect(operation()).To(BeNil())
		Expect(blocked().Reason).To(Equal(talosv1alpha1.ReasonAwaitingApproval))

		approval := tc.Status.Approval
		Expect(approval).NotTo(BeNil())
		Expect(approval.Machines).To(HaveLen(2))
		Expect(approval.Machines[0]).To(And(ContainSubstring(worker1), ContainSubstring("Storage"), ContainSubstring("VolumeConfig")))
		Expect(blocked().Message).To(ContainSubstring(approval.Hash))
		Expect(getMachine(worker1).Status.Config.Diff).To(ContainSubstring("maxSize"), "the diff to review is on the machine")

		annotate(talosv1alpha1.AnnotationApproveDiff, "not-the-hash")
		advance(time.Minute)
		Expect(operation()).To(BeNil())
		Expect(applies()).To(BeEmpty())

		annotate(talosv1alpha1.AnnotationApproveDiff, approval.Hash)
		runUntil("the approved change is rolled out", func() bool { return idle() && len(tc.Status.History) == 1 })

		Expect(applies()).To(HaveLen(4))
		Expect(tc.Status.Approval).To(BeNil())

		// The approval was for that change only.
		setPoolConfig(talosv1alpha1.MachineRoleWorker, strings.Replace(volumeConfig, "50GiB", "60GiB", 1))
		advance(time.Minute)
		Expect(operation()).To(BeNil())
		Expect(tc.Status.Approval.Hash).NotTo(Equal(approval.Hash))
		Expect(applies()).To(HaveLen(4))
	})

	Context("a node changed by hand", func() {
		BeforeEach(func() {
			edited := liveConfig(talosmachine.TypeWorker, editedLabel)
			scenario.UpdateNode(workerAddr, func(n *fake.Node) { n.Config.Active = edited })
		})

		It("is reported and left alone under driftPolicy Report", func() {
			advance(time.Hour)

			Expect(inSync().Status).To(Equal(metav1.ConditionFalse))
			Expect(inSync().Reason).To(Equal(talosv1alpha1.ReasonDrift))
			Expect(inSync().Message).To(ContainSubstring(worker1))
			Expect(blocked()).To(BeNil())
			Expect(operation()).To(BeNil())
			Expect(applies()).To(BeEmpty())
		})

		It("is put back under driftPolicy Enforce", func() {
			updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Rollout.DriftPolicy = talosv1alpha1.DriftPolicyEnforce })
			runUntil("the drift is undone", func() bool { return idle() && len(tc.Status.History) == 1 })

			Expect(applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}))
			Expect(string(scenario.Node(workerAddr).Config.Active)).NotTo(ContainSubstring("edited"))

			settle(nil)
			Expect(inSync().Status).To(Equal(metav1.ConditionTrue), inSync().Message)
		})

		It("becomes the baseline once the spec says the same", func() {
			tm := getMachine(worker1)
			tm.Spec.Config = []talosv1alpha1.ConfigSource{{Inline: editedLabel}}
			Expect(k8sClient.Update(ctx, tm)).To(Succeed())

			advance(time.Minute)
			Expect(inSync().Status).To(Equal(metav1.ConditionTrue), inSync().Message)
			Expect(applies()).To(BeEmpty())

			cfg := getMachine(worker1).Status.Config
			Expect(cfg.AppliedHash).To(Equal(cfg.LiveHash))
		})
	})

	// A config can pass its soak, be confirmed, and break the node later. The rollout that takes
	// it back has to touch a node that is unhealthy, which is the one case the gate allows.
	Context("a config that breaks a node after it was confirmed", func() {
		const bad = "bad"

		rolledOut := []string{tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2)}

		BeforeEach(func() {
			setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig(bad))
			runUntil("the config is rolled out", func() bool { return idle() && len(tc.Status.History) == 1 })
			Expect(applies()).To(Equal(rolledOut))
		})

		// breaks is the config doing its damage on the given machines, some time after the rollout.
		breaks := func(machines ...string) {
			GinkgoHelper()

			for _, m := range machines {
				sicken(m, "rollout: "+bad)
			}

			advance(time.Hour)

			Expect(condition(tc, talosv1alpha1.ConditionHealthy).Status).To(Equal(metav1.ConditionFalse))
			Expect(operation()).To(BeNil())
			Expect(applies()).To(Equal(rolledOut), "an unhealthy node alone is nothing the operator acts on")
		}

		It("is taken back by a rollout, which repairs the unhealthy node before it touches another", func() {
			breaks(worker2)

			setPoolConfig(talosv1alpha1.MachineRoleWorker)
			settle(started)
			Expect(operation()).NotTo(BeNil(), inSync().Message)
			Expect(operation().Plan).To(Equal([]string{worker2, worker1}), "the node to repair goes first")

			// The node is healthy again as soon as the config is gone, and is only then confirmed.
			runUntil("worker-2 is verified", stepIs(worker2, plan.StepVerifying))
			Expect(applies()).To(Equal(append(slices.Clone(rolledOut), tried(worker2))))
			Expect(string(scenario.Node(worker2Addr).Config.Active)).NotTo(ContainSubstring(bad))

			runUntil("the rollout ends", func() bool { return idle() && len(tc.Status.History) == 2 })
			Expect(applies()).To(Equal(append(slices.Clone(rolledOut),
				tried(worker2), confirmed(worker2), tried(worker1), confirmed(worker1))))
			Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))

			settle(nil)
			Expect(inSync().Status).To(Equal(metav1.ConditionTrue), inSync().Message)
			Expect(condition(tc, talosv1alpha1.ConditionHealthy).Status).To(Equal(metav1.ConditionTrue))
			Expect(blocked()).To(BeNil())
		})

		It("is not taken back while a second node is unhealthy", func() {
			breaks(worker1, worker2)

			setPoolConfig(talosv1alpha1.MachineRoleWorker)
			advance(time.Hour)

			Expect(operation()).To(BeNil())
			Expect(blocked().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
			Expect(blocked().Message).To(ContainSubstring(node(worker2).node), "the gate names the node that is not the one to repair")
			Expect(applies()).To(Equal(rolledOut))
		})

		It("keeps every healthy node untouched for as long as the broken one is not repaired", func() {
			breaks(worker2)

			// A change for the control plane only: the unhealthy worker is not part of it.
			setPoolConfig(talosv1alpha1.MachineRoleControlPlane, labelConfig("one"))
			advance(time.Hour)

			Expect(operation()).To(BeNil())
			Expect(blocked().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
			Expect(blocked().Message).To(ContainSubstring(node(worker2).node))
			Expect(applies()).To(Equal(rolledOut))

			// With the workers' config taken back as well, the repair goes first and the rest follows.
			setPoolConfig(talosv1alpha1.MachineRoleWorker)
			runUntil("the rollout ends", func() bool { return idle() && len(tc.Status.History) == 2 })
			Expect(applies()[len(rolledOut):]).To(Equal([]string{
				tried(worker2), confirmed(worker2), tried(worker1), confirmed(worker1), tried(cp1Machine), confirmed(cp1Machine),
			}))
		})
	})

	It("does not keep a config that leaves an unhealthy node unhealthy, and halts before another node is touched", func() {
		sicken(worker2, "")
		advance(time.Minute)

		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("worker-2 is tried", stepIs(worker2, plan.StepVerifying))
		Expect(operation().Plan).To(Equal([]string{worker2, worker1}))
		Expect(applies()).To(Equal([]string{tried(worker2)}))

		// Verification leaves nobody out: the node itself has to be healthy for the whole soak.
		deadline := operation().Current.Deadline.Time
		at(deadline.Add(-plan.ConfirmMargin))
		Expect(operation().Current.Step).To(Equal(plan.StepRollingBack))

		expireTry(worker2)
		at(deadline.Add(plan.RollbackGrace))

		Expect(failed()).To(BeTrue())
		Expect(operation().Failure.Machine).To(Equal(worker2))
		Expect(operation().Failure.Reason).To(Equal(plan.FailRolledBack))
		Expect(applies()).To(Equal([]string{tried(worker2)}), "never confirmed, and worker-1 never touched")

		advance(time.Hour)
		Expect(applies()).To(Equal([]string{tried(worker2)}))
	})

	It("touches no node while paused, and goes on when the pause is lifted", func() {
		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		advance(time.Hour)

		Expect(operation()).To(BeNil())
		Expect(blocked().Reason).To(Equal(talosv1alpha1.ReasonPaused))
		Expect(applies()).To(BeEmpty())

		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
		runUntil("worker-1 is done", func() bool { return operation() != nil && len(operation().Completed) == 1 })

		// Pausing between two nodes: the lock is kept, the next node is not started, and no
		// amount of waiting turns that into a failure.
		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		advance(time.Hour)

		Expect(operation()).NotTo(BeNil())
		Expect(operation().Failure).To(BeNil())
		Expect(operation().Current).To(BeNil())
		Expect(blocked().Reason).To(Equal(talosv1alpha1.ReasonPaused))
		Expect(applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}))

		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
		runUntil("the rollout ends", idle)
		Expect(applies()).To(HaveLen(4))
	})

	It("finishes a node whose config is being tried when paused, rather than leaving it half done", func() {
		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("worker-1 is verified", stepIs(worker1, plan.StepVerifying))

		updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })
		runUntil("worker-1 is done", func() bool { return operation() != nil && len(operation().Completed) == 1 })

		advance(time.Hour)
		Expect(applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}))
		Expect(operation().Current).To(BeNil())
	})

	It("does not start while the cluster is unhealthy, and says what is wrong", func() {
		scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{etcdUnhealthy}})
		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		advance(time.Hour)

		Expect(operation()).To(BeNil())
		Expect(blocked().Reason).To(Equal(talosv1alpha1.ReasonGateClosed))
		Expect(blocked().Message).To(ContainSubstring(etcdUnhealthy))
		Expect(applies()).To(BeEmpty())

		scenario.SetHealth(talos.HealthResult{Healthy: true})
		runUntil("the rollout ends", func() bool { return idle() && len(tc.Status.History) == 1 })
	})

	It("halts when the cluster does not get healthy for the next node, instead of waiting forever or going on", func() {
		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("worker-1 is done", func() bool { return operation() != nil && len(operation().Completed) == 1 })

		scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{etcdUnhealthy}})
		runEvery(time.Minute, "the rollout halts", failed)

		Expect(operation().Failure.Machine).To(Equal(worker2))
		Expect(operation().Failure.Reason).To(Equal(plan.FailTimeout))
		Expect(operation().Failure.Message).To(ContainSubstring(etcdUnhealthy))
		Expect(applies()).To(Equal([]string{tried(worker1), confirmed(worker1)}))
	})

	It("ends a rollout whose spec moved on, and plans the rest afresh", func() {
		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("worker-1 is done", func() bool { return operation() != nil && len(operation().Completed) == 1 })

		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("two"))
		runUntil("the first rollout ends", func() bool { return len(tc.Status.History) == 1 })

		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSuperseded))
		Expect(tc.Status.History[0].Completed).To(Equal([]string{worker1}))

		runUntil("the second rollout ends", func() bool { return idle() && len(tc.Status.History) == 2 })
		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))

		for _, name := range []string{worker1, worker2} {
			Expect(string(scenario.Node(node(name).addr).Config.Active)).To(ContainSubstring("rollout: two"))
		}

		Expect(string(scenario.Node(worker2Addr).Config.Active)).NotTo(ContainSubstring("rollout: one"))
		Expect(applies()).To(Equal([]string{
			tried(worker1), confirmed(worker1), tried(worker1), confirmed(worker1), tried(worker2), confirmed(worker2),
		}), "worker-2 never got the config that was superseded")
	})

	It("leaves a machine on hold out of the rollout", func() {
		tm := getMachine(worker1)
		tm.Spec.Hold = true
		Expect(k8sClient.Update(ctx, tm)).To(Succeed())

		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("the rollout ends", func() bool { return idle() && len(tc.Status.History) == 1 })

		Expect(applies()).To(Equal([]string{tried(worker2), confirmed(worker2)}))

		settle(nil)
		Expect(inSync().Reason).To(Equal(talosv1alpha1.ReasonHeld))
		Expect(inSync().Message).To(ContainSubstring(worker1))
	})

	It("halts at once when Talos refuses the config, having applied nothing", func() {
		scenario.FailNext(fake.MethodApplyConfiguration, workerAddr, status.Error(codes.InvalidArgument, "unknown field"))

		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("the rollout halts", failed)

		Expect(operation().Failure.Reason).To(Equal(plan.FailRejected))
		Expect(operation().Failure.Message).To(ContainSubstring("unknown field"))
		Expect(applies()).To(BeEmpty(), "the dry run caught it")
	})

	It("keeps trying through an error that may pass", func() {
		scenario.FailNext(fake.MethodApplyConfiguration, workerAddr, status.Error(codes.Unavailable, "connection refused"))

		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("worker-1 is being tried", stepIs(worker1, plan.StepTrying))
		advance(time.Minute)

		Expect(failed()).To(BeFalse())
		Expect(inSync().Message).To(ContainSubstring("connection refused"))

		scenario.FailNext(fake.MethodApplyConfiguration, workerAddr, nil)
		runUntil("the rollout ends", idle)
		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
	})

	It("lets go of a rollout when config management returns to Observe, confirming nothing", func() {
		setPoolConfig(talosv1alpha1.MachineRoleWorker, labelConfig("one"))
		runUntil("worker-1 is verified", stepIs(worker1, plan.StepVerifying))

		updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Management.Config = talosv1alpha1.ConfigManagementObserve
		})
		advance(time.Hour)

		Expect(operation()).To(BeNil())
		Expect(tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
		Expect(tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseAdopted))
		Expect(applies()).To(Equal([]string{tried(worker1)}), "observing writes nothing: Talos reverts the try by itself")
		Expect(scenario.TryPending(workerAddr)).To(BeTrue())
	})

	It("refuses a rollout whose timings could never confirm a try", func() {
		// The API server refuses these too; this is what an operator would do with a spec that
		// predates the rule.
		Expect(rolloutTiming(talosv1alpha1.Rollout{
			TryTimeout: &metav1.Duration{Duration: time.Minute}, Soak: &metav1.Duration{Duration: time.Minute},
		}).Validate()).NotTo(Succeed())

		Expect(rolloutTiming(talosv1alpha1.Rollout{}).Validate()).To(Succeed())
		Expect(rolloutTiming(talosv1alpha1.Rollout{Soak: &metav1.Duration{}}).Soak).To(BeZero(), "a soak set to zero is zero, not the default")
	})
})
