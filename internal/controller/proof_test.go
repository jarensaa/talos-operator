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
	talosmachine "github.com/siderolabs/talos/pkg/machinery/config/machine"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

var _ = Describe("adoption proof gate", func() {
	// ready returns a cluster, its facts and a machine that pass every gate; entries break one.
	pools := map[string]*talosv1alpha1.TalosMachinePool{
		controlPlanePool: {Spec: talosv1alpha1.TalosMachinePoolSpec{Role: talosv1alpha1.MachineRoleControlPlane}},
	}

	ready := func() (*talosv1alpha1.TalosCluster, *clusterFacts, []talosv1alpha1.TalosMachine) {
		live := liveConfig(talosmachine.TypeControlPlane)
		hash := normalizedHash(live)

		tc := &talosv1alpha1.TalosCluster{}
		for _, c := range []string{talosv1alpha1.ConditionAdoptionPreconditionsMet, talosv1alpha1.ConditionSecretsMatchLive} {
			setCondition(&tc.Status, 0, c, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "")
		}

		facts := &clusterFacts{nodes: []nodeFacts{{hostname: cpNodename, address: cpAddr, systemUUID: cpUUID, activeConfig: live}}}

		tm := *machine("ns", "cp-1", talosv1alpha1.MachineRoleControlPlane, cpAddr, cpUUID)
		tm.Generation = 2
		tm.Status.ObservedGeneration = 2
		tm.Status.Config = &talosv1alpha1.ConfigState{LiveHash: hash, DesiredHash: hash}
		setConditionIn(&tm.Status.Conditions, 2, talosv1alpha1.ConditionMatched, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "")
		setConditionIn(&tm.Status.Conditions, 2, talosv1alpha1.ConditionConfigMatchesLive, metav1.ConditionTrue,
			talosv1alpha1.ReasonSucceeded, "")

		return tc, facts, []talosv1alpha1.TalosMachine{tm}
	}

	It("passes when every machine is current and matches", func() {
		tc, facts, machines := ready()

		nodes, why := proofGate(tc, facts, []string{}, pools, machines)
		Expect(why).To(BeEmpty())
		Expect(nodes).To(HaveLen(1))
		Expect(nodes[0].node.address).To(Equal(cpAddr))
	})

	DescribeTable("fails closed",
		func(breakIt func(*talosv1alpha1.TalosCluster, *clusterFacts, []talosv1alpha1.TalosMachine) []string, want string) {
			tc, facts, machines := ready()
			unmatched := breakIt(tc, facts, machines)

			nodes, why := proofGate(tc, facts, unmatched, pools, machines)
			Expect(nodes).To(BeNil())
			Expect(why).To(ContainSubstring(want))
		},
		Entry("secrets not verified", func(tc *talosv1alpha1.TalosCluster, _ *clusterFacts, _ []talosv1alpha1.TalosMachine) []string {
			setCondition(&tc.Status, 0, talosv1alpha1.ConditionSecretsMatchLive, metav1.ConditionFalse, talosv1alpha1.ReasonMismatch, "")
			return []string{}
		}, talosv1alpha1.ConditionSecretsMatchLive),
		Entry("matching unknown", func(*talosv1alpha1.TalosCluster, *clusterFacts, []talosv1alpha1.TalosMachine) []string {
			return nil
		}, "could not all be matched"),
		Entry("machine status older than its spec", func(
			_ *talosv1alpha1.TalosCluster, _ *clusterFacts, m []talosv1alpha1.TalosMachine,
		) []string {
			m[0].Generation = 3
			return []string{}
		}, "not been observed since its last change"),
		Entry("machine compared against an older live config", func(
			_ *talosv1alpha1.TalosCluster, _ *clusterFacts, m []talosv1alpha1.TalosMachine,
		) []string {
			m[0].Status.Config.LiveHash = "sha256:old"
			return []string{}
		}, "older live config"),
		Entry("machine renders something else", func(_ *talosv1alpha1.TalosCluster, _ *clusterFacts, m []talosv1alpha1.TalosMachine) []string {
			m[0].Status.Config.DesiredHash = "sha256:other"
			return []string{}
		}, "older live config"),
		Entry("live config unreadable", func(_ *talosv1alpha1.TalosCluster, f *clusterFacts, _ []talosv1alpha1.TalosMachine) []string {
			f.nodes[0].activeConfig = nil
			return []string{}
		}, "not read"),
		Entry("pool gone", func(_ *talosv1alpha1.TalosCluster, _ *clusterFacts, m []talosv1alpha1.TalosMachine) []string {
			m[0].Spec.PoolRef.Name = "gone"
			return []string{}
		}, "TalosMachinePool gone"),
		Entry("hardware not live", func(_ *talosv1alpha1.TalosCluster, f *clusterFacts, _ []talosv1alpha1.TalosMachine) []string {
			f.nodes[0].systemUUID = workerUUID
			return []string{}
		}, "no live node has the systemUUID"),
	)
})
