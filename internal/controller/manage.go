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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// managing reports whether the operator owns the cluster's config.
func managing(tc *talosv1alpha1.TalosCluster) bool {
	return tc.Spec.Management.Config == talosv1alpha1.ConfigManagementManage
}

// manage runs while config is Manage (docs/scope.md §7.1 stage 4).
//
// The first reconcile after entering Manage is the handover: it must find that taking ownership
// changes nothing, by the proof's own gate and a fresh Talos dry run on every node, because the
// adoption proof stops being re-evaluated once the cluster is managed. Until a handover is
// verified the cluster is Degraded and rollouts are blocked; the check is repeated every
// reconcile, so a spec that renders the live config again lifts the block.
//
// Once handed over, the rollout executor takes it from there: it reports whether every node
// runs its rendered config and rolls out the ones that do not (rollout.go).
func (r *TalosClusterReconciler) manage(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation) error {
	st := &tc.Status
	gen := tc.Generation

	pools, machines, err := membersOf(ctx, r.Client, tc.Namespace, tc.Name)
	if err != nil {
		return err
	}

	if obs.facts == nil || obs.reader == nil {
		// The cluster was not reached, so nothing can be compared or planned. A node step in flight
		// still has deadlines to keep: it goes on, with its node counted as unreachable.
		if st.Operation != nil {
			return r.rollout(ctx, tc, obs, pools, machines)
		}

		return nil
	}

	if !meta.IsStatusConditionTrue(st.Conditions, talosv1alpha1.ConditionConfigHandover) {
		nodes, why := proofGate(tc, obs.facts, obs.unmatched, pools, machines)
		problem := &configProblem{metav1.ConditionFalse, talosv1alpha1.ReasonHandoverUnverified, why}

		if why == "" {
			if problem, err = r.verifyNoChanges(ctx, tc, obs.reader, nodes); err != nil {
				return err
			}
		}

		if problem != nil {
			setCondition(st, gen, talosv1alpha1.ConditionConfigHandover, problem.status, problem.reason, problem.msg)
			setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonHandoverUnverified,
				"entering config: Manage is not verified to change nothing; no rollout starts until every node's rendered config "+
					"equals its live config again, or config returns to Observe: "+problem.msg)
			setCondition(st, gen, talosv1alpha1.ConditionConfigInSync, metav1.ConditionFalse, problem.reason, problem.msg)
			st.Phase = talosv1alpha1.ClusterPhaseDegraded

			return nil //nolint:nilerr // err is nil here: a problem is a verdict, reported in ConfigHandover
		}

		setCondition(st, gen, talosv1alpha1.ConditionConfigHandover, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			fmt.Sprintf("taking ownership changes nothing: Talos's dry run reports no changes on all %d nodes", len(nodes)))
		meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionRolloutBlocked)
	}

	return r.rollout(ctx, tc, obs, pools, machines)
}

// forgetHandover drops the handover state when config returns to Observe, so entering Manage
// again is verified again.
func forgetHandover(st *talosv1alpha1.TalosClusterStatus) {
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionConfigHandover)
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionConfigInSync)

	blocked := meta.FindStatusCondition(st.Conditions, talosv1alpha1.ConditionRolloutBlocked)
	if blocked != nil && blocked.Reason == talosv1alpha1.ReasonHandoverUnverified {
		meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionRolloutBlocked)
	}
}
