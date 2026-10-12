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
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
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
	var h *harness

	defragmented := func() []string {
		var out []string

		for _, call := range h.scenario.CallsFor(fake.MethodEtcdDefragment) {
			for _, m := range h.declared {
				if m.addr == call.Node {
					out = append(out, m.name)
				}
			}
		}

		return out
	}

	maintained := func() *metav1.Condition {
		cond := condition(h.tc, talosv1alpha1.ConditionEtcdMaintained)
		if cond == nil {
			return &metav1.Condition{}
		}

		return cond
	}

	// sizes sets the size of a member's database file and what of it holds data, in MiB.
	sizes := func(m member, size, inUse int64) {
		h.scenario.UpdateNode(m.addr, func(n *fake.Node) { n.EtcdDBSize, n.EtcdDBInUse = size*mebibyte, inUse*mebibyte })
	}

	idle := func() bool { return h.tc.Status.Operation == nil }

	// rested is the cluster after a defragmentation: the pass after the one that ended it says so.
	rested := func() bool { return idle() && maintained().Status == metav1.ConditionTrue }

	halted := func() bool { return h.tc.Status.Operation != nil && h.tc.Status.Operation.Failure != nil }

	// start brings up a cluster of the given machines, adopted, with its config managed and,
	// unless told otherwise, its upkeep enabled.
	start := func(maintenance talosv1alpha1.CapabilityState, members ...member) {
		GinkgoHelper()

		h.start(func(m *talosv1alpha1.Management) { m.Maintenance = maintenance }, members...)
		Expect(maintained().Status).To(Equal(metav1.ConditionTrue), maintained().Message)
		Expect(defragmented()).To(BeEmpty(), "an etcd that is not fragmented is left alone")
	}

	BeforeEach(func() {
		// settle watches the cluster's status alone here, and nothing that was sent: a
		// defragmentation that succeeds moves its step on, so a pass that leaves the cluster as it
		// was did nothing, or tried again what failed before.
		h = newHarness("upkeep-", 120, talosv1alpha1.ConditionEtcdMaintained, talosv1alpha1.ConditionRolloutBlocked)

		// The nodes report what status holds of a node's health, and have the logs a bundle keeps.
		h.dress = func(m member, n *fake.Node) {
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
		}
		h.complete = func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Rollout.StepTimeout = &metav1.Duration{Duration: 5 * time.Minute}
		}
	})

	It("reports what Talos says about a node, and nothing of it once the node does not answer", func() {
		start(talosv1alpha1.CapabilityDisabled, cpA, wOne)

		health := h.machine(wOne.name).Status.Health
		Expect(health).NotTo(BeNil())
		Expect(health.Services).To(ConsistOf(
			talosv1alpha1.ServiceHealth{Name: "apid", Running: true, Healthy: true},
			talosv1alpha1.ServiceHealth{Name: "kubelet", Running: true, Healthy: true},
		))
		Expect(health.TimeSynced).To(HaveValue(BeTrue()))
		Expect(health.Diagnostics).To(BeEmpty())
		Expect(health.Links).To(Equal([]talosv1alpha1.LinkHealth{{Name: "eth0", Up: true, SpeedMbps: 1000}}))
		Expect(health.Volumes).To(HaveLen(2))

		etcd := h.machine(cpA.name).Status.Etcd
		Expect(etcd.Leader).To(BeTrue())
		Expect(etcd.DBSize).To(Equal(200 * mebibyte))
		Expect(etcd.DBSizeInUse).To(Equal(150 * mebibyte))
		Expect(h.machine(wOne.name).Status.Etcd).To(BeNil())

		h.scenario.UpdateNode(wOne.addr, func(n *fake.Node) {
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
		version := h.machine(cpA.name).ResourceVersion
		h.round()

		health = h.machine(wOne.name).Status.Health
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
		Expect(h.machine(cpA.name).ResourceVersion).To(Equal(version), "a database that moved by a megabyte must not write status")

		// A node that does not answer is not said to be anything: not healthy, and not leading.
		h.scenario.Kill(wOne.addr)
		h.scenario.FailNext(fake.MethodEtcdStatus, cpA.addr, errors.New("etcd status: context deadline exceeded"))
		h.round()

		Expect(h.machine(wOne.name).Status.Health).To(BeNil())

		etcd = h.machine(cpA.name).Status.Etcd
		Expect(etcd.MemberID).NotTo(BeEmpty(), "which member it is does not change while nobody looks")
		Expect(etcd.Leader).To(BeFalse())
		Expect(etcd.DBSize).To(BeZero())
		Expect(h.machine(cpA.name).Status.Health).NotTo(BeNil(), "what was read of the node is still reported")
	})

	It("says when the certificates the cluster depends on end", func() {
		start(talosv1alpha1.CapabilityDisabled, cpA, wOne)

		names := make([]string, 0, len(h.tc.Status.Certificates))

		for _, cert := range h.tc.Status.Certificates {
			names = append(names, cert.Name)

			Expect(cert.ExpiresAt.After(cert.IssuedAt.Time)).To(BeTrue(), cert.Name)
			Expect(cert.ExpiresAt.After(h.clock)).To(BeTrue(), cert.Name)
		}

		Expect(names).To(Equal([]string{"etcd", "kubernetes", "kubernetes-aggregator", "operator", "os"}))

		// Credentials that can no longer be read keep the date they had: it is the one that matters.
		before := h.tc.Status.Certificates
		Expect(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecret, Namespace: h.ns}})).To(Succeed())
		h.round()

		Expect(condition(h.tc, talosv1alpha1.ConditionCredentialsValid).Status).To(Equal(metav1.ConditionFalse))
		Expect(h.tc.Status.Certificates).To(Equal(before))
	})

	It("defragments every member when one is fragmented, the leader last, and not again within the interval", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		// More than half of one member's file is free space.
		sizes(cpB, 400, 100)
		h.runUntil("etcd is defragmented", func() bool { return rested() && len(defragmented()) == 3 })

		// The first control plane leads etcd, and gives the lead away before its turn.
		Expect(defragmented()).To(Equal([]string{cpB.name, cpC.name, cpA.name}))
		Expect(h.scenario.CallsFor(fake.MethodEtcdForfeitLeadership)).To(HaveLen(1))
		Expect(h.scenario.CallsFor(fake.MethodEtcdForfeitLeadership)[0].Node).To(Equal(cpA.addr))

		record := h.tc.Status.History[0]
		Expect(record.Type).To(Equal(talosv1alpha1.OperationEtcdDefragment))
		Expect(record.Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(record.Completed).To(Equal([]string{cpB.name, cpC.name, cpA.name}))

		Expect(h.scenario.Node(cpB.addr).EtcdDBSize).To(Equal(100 * mebibyte))
		Expect(h.tc.Status.Maintenance.EtcdDefragmentedAt).NotTo(BeNil())
		Expect(maintained().Status).To(Equal(metav1.ConditionTrue), maintained().Message)
		Expect(h.factory.KeeperTargets()).NotTo(BeEmpty())

		// It is fragmented again an hour later. That waits for the interval.
		h.clock = h.clock.Add(time.Hour)
		sizes(cpC, 400, 100)
		h.settle(nil)

		Expect(idle()).To(BeTrue())
		Expect(maintained().Status).To(Equal(metav1.ConditionFalse))
		Expect(maintained().Reason).To(Equal(talosv1alpha1.ReasonTooSoon), maintained().Message)
		Expect(maintained().Message).To(ContainSubstring(cpC.name))
		Expect(defragmented()).To(HaveLen(3))

		h.clock = h.clock.Add(24 * time.Hour)
		h.runUntil("etcd is defragmented again", func() bool { return rested() && len(defragmented()) == 6 })
		Expect(maintained().Status).To(Equal(metav1.ConditionTrue), maintained().Message)
	})

	It("leaves a fragmented etcd alone while maintenance is not enabled", func() {
		start(talosv1alpha1.CapabilityDisabled, cpA, wOne)

		sizes(cpA, 400, 100)
		h.clock = h.clock.Add(time.Minute)
		h.settle(nil)

		Expect(idle()).To(BeTrue())
		Expect(maintained().Status).To(Equal(metav1.ConditionFalse))
		Expect(maintained().Reason).To(Equal(talosv1alpha1.ReasonMaintenanceDisabled), maintained().Message)
		Expect(defragmented()).To(BeEmpty())
		Expect(h.factory.KeeperTargets()).To(BeEmpty(), "no connection that may defragment is even made")
	})

	It("does not defragment a small database, however empty it is", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, wOne)

		sizes(cpA, 60, 5)
		h.clock = h.clock.Add(time.Minute)
		h.settle(nil)

		Expect(maintained().Status).To(Equal(metav1.ConditionTrue), maintained().Message)
		Expect(defragmented()).To(BeEmpty())

		// Unless the spec says that is big enough.
		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Maintenance.Etcd.MinimumSize = new(resource.MustParse("50Mi"))
		})
		h.runUntil("etcd is defragmented", func() bool { return rested() && len(defragmented()) == 1 })
	})

	It("starts no defragmentation while the cluster is not healthy, and no member before the last one is verified", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		h.scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{"a node is not ready"}})
		sizes(cpB, 400, 100)
		h.clock = h.clock.Add(time.Minute)
		h.settle(nil)

		Expect(idle()).To(BeTrue())
		Expect(maintained().Reason).To(Equal(plan.BlockGateClosed), maintained().Message)
		Expect(defragmented()).To(BeEmpty())

		// It is healthy again, and the operation starts: the first member's turn is written down,
		// and nothing was sent yet. The cluster stops being healthy before it is.
		h.scenario.SetHealth(talos.HealthResult{Healthy: true})
		h.runUntil("the first member's turn begins", func() bool {
			return h.tc.Status.Operation != nil && h.tc.Status.Operation.Current != nil
		})
		Expect(defragmented()).To(BeEmpty(), "a turn is written down before anything is sent")

		h.scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{"a node is not ready"}})

		for range 6 {
			h.clock = h.clock.Add(20 * time.Second)
			h.settle(nil)
		}

		Expect(defragmented()).To(BeEmpty(), "a member whose turn it is waits for the gate like one whose turn has not come")
		Expect(h.tc.Status.Operation.Current.Step).To(Equal(plan.StepDefragmenting))

		h.scenario.SetHealth(talos.HealthResult{Healthy: true})
		h.runUntil("the first member is defragmented", func() bool { return len(defragmented()) == 1 })

		// The cluster stops being healthy while the first member is verified: the second waits.
		h.scenario.SetHealth(talos.HealthResult{Healthy: false, Messages: []string{"etcd is not healthy"}})

		for range 6 {
			h.clock = h.clock.Add(30 * time.Second)
			h.settle(nil)
		}

		Expect(defragmented()).To(Equal([]string{cpB.name}))
		Expect(h.tc.Status.Operation.Completed).To(BeEmpty())

		h.scenario.SetHealth(talos.HealthResult{Healthy: true})
		h.runUntil("etcd is defragmented", func() bool { return rested() && len(defragmented()) == 3 })
	})

	It("does not begin the next member while paused", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		sizes(cpB, 400, 100)
		h.runUntil("the first member is defragmented", func() bool { return len(defragmented()) == 1 })

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = true })

		for range 12 {
			h.clock = h.clock.Add(30 * time.Second)
			h.settle(nil)
		}

		// The member that was defragmented is verified; nobody else is touched.
		Expect(h.tc.Status.Operation.Completed).To(Equal([]string{cpB.name}))
		Expect(defragmented()).To(Equal([]string{cpB.name}))
		Expect(condition(h.tc, talosv1alpha1.ConditionRolloutBlocked).Reason).To(Equal(talosv1alpha1.ReasonPaused))

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) { tc.Spec.Paused = false })
		h.runUntil("etcd is defragmented", func() bool { return rested() && len(defragmented()) == 3 })
	})

	It("halts at a member that cannot be defragmented, keeps what the node said, and goes on when told to leave it out", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		h.scenario.FailNext(fake.MethodEtcdDefragment, cpB.addr, errors.New("defragment: rpc error: code = Unavailable"))
		sizes(cpB, 400, 100)
		h.runUntil("the operation halts", halted)

		failure := h.tc.Status.Operation.Failure
		Expect(failure.Machine).To(Equal(cpB.name))
		Expect(failure.Step).To(Equal(plan.StepDefragmenting))
		Expect(failure.Reason).To(Equal(plan.FailTimeout))
		Expect(maintained().Reason).To(Equal(talosv1alpha1.ReasonOperationFailed), maintained().Message)
		Expect(h.tc.Status.Phase).To(Equal(talosv1alpha1.ClusterPhaseDegraded))

		// The node answered, so what it said when the operation halted is kept.
		Expect(failure.Bundle).To(Equal(clusterName + "-support-bundle"))

		bundle := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: h.ns, Name: failure.Bundle}, bundle)).To(Succeed())
		Expect(metav1.IsControlledBy(bundle, h.tc)).To(BeTrue())
		Expect(string(bundle.Data["services.txt"])).To(ContainSubstring("kubelet running=true healthy=true"))
		Expect(unzipped(bundle.Data["kubelet.log.gz"])).To(Equal("the kubelet of " + cpB.name + "\n"))
		Expect(unzipped(bundle.Data["etcd.log.gz"])).To(Equal("etcd of " + cpB.name + "\n"))
		Expect(unzipped(bundle.Data["dmesg.log.gz"])).To(Equal("the kernel of " + cpB.name + "\n"))
		Expect(string(bundle.Data["failure.txt"])).To(And(
			ContainSubstring("machine: "+cpB.name), ContainSubstring("reason: "+plan.FailTimeout), ContainSubstring("not read: none"),
		))

		// It is collected once: the logs are not read again while the operation waits.
		logs := len(h.scenario.CallsFor(fake.MethodLogs))
		h.clock = h.clock.Add(time.Minute)
		h.settle(nil)
		Expect(h.scenario.CallsFor(fake.MethodLogs)).To(HaveLen(logs))

		h.annotate(talosv1alpha1.AnnotationSkipMachine, failure.Token)
		h.runUntil("the others are defragmented", idle)

		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationSucceeded))
		Expect(h.tc.Status.History[0].Skipped).To(Equal([]string{cpB.name}))
		Expect(h.tc.Status.History[0].Completed).To(Equal([]string{cpC.name, cpA.name}))
	})

	It("says so when nothing could be collected from a node that does not answer", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		sizes(cpB, 400, 100)
		h.runUntil("the first member's turn begins", func() bool {
			return h.tc.Status.Operation != nil && h.tc.Status.Operation.Current != nil
		})

		h.scenario.Kill(cpB.addr)
		h.runUntil("the operation halts", halted)

		// For a while the node may come back and still say something.
		Expect(h.tc.Status.Operation.Failure.Bundle).To(BeEmpty())

		h.clock = h.clock.Add(6 * time.Minute)
		h.settle(nil)

		Expect(h.tc.Status.Operation.Failure.Bundle).To(HavePrefix("not collected: "))
		Expect(defragmented()).To(BeEmpty())

		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: h.ns, Name: clusterName + "-support-bundle"}, &corev1.Secret{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "%v", err)

		h.annotate(talosv1alpha1.AnnotationAbort, h.tc.Status.Operation.Failure.Token)
		h.runUntil("the operation ends", idle)
		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
	})

	It("does not write its bundle over a Secret that is not its own", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		theirs := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: h.ns, Name: clusterName + "-support-bundle"},
			Data:       map[string][]byte{"theirs": []byte("something else")},
		}
		Expect(k8sClient.Create(ctx, theirs)).To(Succeed())

		h.scenario.FailNext(fake.MethodEtcdDefragment, cpB.addr, errors.New("defragment: rpc error: code = Unavailable"))
		sizes(cpB, 400, 100)
		h.runUntil("the operation halts", halted)

		Expect(h.tc.Status.Operation.Failure.Bundle).To(And(HavePrefix("not collected: "), ContainSubstring("is not this cluster's")))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(theirs), theirs)).To(Succeed())
		Expect(theirs.Data).To(Equal(map[string][]byte{"theirs": []byte("something else")}))
	})

	It("ends a defragmentation when maintenance is disabled, and touches no further member", func() {
		start(talosv1alpha1.CapabilityEnabled, cpA, cpB, cpC, wOne)

		sizes(cpB, 400, 100)
		h.runUntil("the first member is defragmented", func() bool { return len(defragmented()) == 1 })

		h.updateCluster(func(tc *talosv1alpha1.TalosCluster) {
			tc.Spec.Management.Maintenance = talosv1alpha1.CapabilityDisabled
		})
		h.runUntil("the operation ends", idle)

		Expect(h.tc.Status.History[0].Outcome).To(Equal(talosv1alpha1.OperationAborted))
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
