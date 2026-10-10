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
	"errors"
	"fmt"
	"maps"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// proofNode is a TalosMachine bound to its live node, ready to be proven.
type proofNode struct {
	machine *talosv1alpha1.TalosMachine
	pool    *talosv1alpha1.TalosMachinePool
	node    nodeFacts
	live    *render.Config
}

// proofGate decides whether the adoption proof can run (docs/scope.md §7.1 stage 3). It is pure
// so every gate can be table-tested. On success it returns one proofNode per live node; otherwise
// it says what is missing. Machine status must be current: a TalosMachine compared against an
// older live config, or not observed since its last change, blocks the proof.
func proofGate(tc *talosv1alpha1.TalosCluster, facts *clusterFacts, unmatched []string, pools map[string]*talosv1alpha1.TalosMachinePool,
	machines []talosv1alpha1.TalosMachine,
) ([]proofNode, string) {
	for _, c := range []string{talosv1alpha1.ConditionAdoptionPreconditionsMet, talosv1alpha1.ConditionSecretsMatchLive} {
		if !meta.IsStatusConditionTrue(tc.Status.Conditions, c) {
			return nil, c + " is not True"
		}
	}

	if unmatched == nil {
		return nil, "live members could not all be matched against TalosMachines"
	}

	if len(unmatched) > 0 {
		return nil, "live members without a TalosMachine: " + strings.Join(unmatched, ", ")
	}

	if len(machines) == 0 {
		return nil, "the cluster has no TalosMachines"
	}

	byUUID := make(map[string]nodeFacts, len(facts.nodes))
	for _, n := range facts.nodes {
		byUUID[strings.ToLower(n.systemUUID)] = n
	}

	nodes := make([]proofNode, 0, len(machines))

	for i := range machines {
		tm := &machines[i]
		st := tm.Status

		pool := pools[tm.Spec.PoolRef.Name]
		if pool == nil {
			return nil, fmt.Sprintf("TalosMachinePool %s of TalosMachine %s not found", tm.Spec.PoolRef.Name, tm.Name)
		}

		if !meta.IsStatusConditionTrue(st.Conditions, talosv1alpha1.ConditionMatched) {
			return nil, fmt.Sprintf("TalosMachine %s is not matched to a live node", tm.Name)
		}

		if st.ObservedGeneration != tm.Generation {
			return nil, fmt.Sprintf("TalosMachine %s has not been observed since its last change", tm.Name)
		}

		if !meta.IsStatusConditionTrue(st.Conditions, talosv1alpha1.ConditionConfigMatchesLive) {
			return nil, fmt.Sprintf("TalosMachine %s does not match its live config (see its ConfigMatchesLive)", tm.Name)
		}

		node, ok := byUUID[strings.ToLower(tm.Spec.Identity.SystemUUID)]
		if !ok {
			return nil, fmt.Sprintf("no live node has the systemUUID of TalosMachine %s", tm.Name)
		}

		live, err := normalizedLive(node)
		if err != nil {
			return nil, fmt.Sprintf("live config of %s: %v", node.hostname, err)
		}

		if st.Config == nil || st.Config.LiveHash != live.Hash || st.Config.DesiredHash != live.Hash {
			return nil, fmt.Sprintf("TalosMachine %s was compared against an older live config", tm.Name)
		}

		nodes = append(nodes, proofNode{machine: tm, pool: pool, node: node, live: live})
	}

	return nodes, ""
}

func normalizedLive(n nodeFacts) (*render.Config, error) {
	if len(n.activeConfig) == 0 {
		return nil, errors.New("not read")
	}

	parsed, err := talos.ParseMachineConfig(n.activeConfig)
	if err != nil {
		return nil, err
	}

	return render.Normalize(parsed)
}

// prove sets Adopted (docs/scope.md §7.1 stage 3). Once every machine matches its live config,
// it asks every node's Talos for a dry-run verdict on the rendered config: only "no changes"
// everywhere proves adoption. The proof is recorded as each machine's proven hash, and any later
// change to a live config or to what the spec renders invalidates it. It never applies anything:
// the Reader only exposes dry runs.
func (r *TalosClusterReconciler) prove(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation) error {
	st := &tc.Status
	gen := tc.Generation
	proven := meta.IsStatusConditionTrue(st.Conditions, talosv1alpha1.ConditionConfigProven)

	pools, machines, err := membersOf(ctx, r.Client, tc.Namespace, tc.Name)
	if err != nil {
		return err
	}

	nodes, why := proofGate(tc, obs.facts, obs.unmatched, pools, machines)
	if why != "" {
		// A cluster that was proven before has lost its proof; one that never was has not met it yet.
		reason := talosv1alpha1.ReasonPreconditionUnmet
		if proven || adopted(tc) {
			reason = talosv1alpha1.ReasonProofInvalidated
		}

		setCondition(st, gen, talosv1alpha1.ConditionConfigProven, metav1.ConditionFalse, reason, why)

		return nil
	}

	hashes := make(map[string]string, len(nodes))
	for _, n := range nodes {
		hashes[n.machine.Name] = n.live.Hash
	}

	// Desired equals live everywhere (the gate), so the hashes alone tell whether anything moved
	// since the last proof; the dry runs only run again when something did.
	if proven && st.Adoption != nil && maps.Equal(st.Adoption.LiveConfigHashes, hashes) {
		return nil
	}

	if verdict, err := r.verifyNoChanges(ctx, tc, obs.reader, nodes); err != nil {
		return err
	} else if verdict != nil {
		setCondition(st, gen, talosv1alpha1.ConditionConfigProven, verdict.status, verdict.reason, verdict.msg)

		return nil //nolint:nilerr // err is nil here: a verdict is reported in Adopted
	}

	if st.Adoption == nil {
		st.Adoption = &talosv1alpha1.AdoptionStatus{}
	}

	now := metav1.NewTime(r.now())
	st.Adoption.LiveConfigHashes = hashes
	st.Adoption.ProvenAt = &now

	// The first proof is what adopts the cluster. From here on the resources are the user's, and
	// nothing takes that back (docs/scope.md §12.14).
	if st.Adoption.AdoptedAt == nil {
		st.Adoption.AdoptedAt = &now
	}

	setCondition(st, gen, talosv1alpha1.ConditionConfigProven, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
		fmt.Sprintf("Talos's dry run reports no changes on all %d nodes", len(nodes)))

	return nil
}

// verifyNoChanges asks every node's Talos for a dry-run verdict on its rendered config, sent in the
// live config's document order. It returns nil when every node reports no changes, and otherwise
// why not: a node reporting changes, a config that no longer renders to what was compared, or a
// node that could not be asked. The server's diff is never shown: it is an unredacted text diff of
// the whole config. Other errors are API failures of the management cluster.
func (r *TalosClusterReconciler) verifyNoChanges(ctx context.Context, tc *talosv1alpha1.TalosCluster, reader talos.Reader,
	nodes []proofNode,
) (*configProblem, error) {
	var changed []string

	for _, n := range nodes {
		desired, _, err := renderMachine(ctx, r.Client, tc, n.pool, n.machine)
		if err != nil {
			var problem *configProblem
			if errors.As(err, &problem) {
				return &configProblem{problem.status, problem.reason, n.machine.Name + ": " + problem.msg}, nil
			}

			return nil, err
		}

		if desired.Hash != n.live.Hash {
			return &configProblem{metav1.ConditionFalse, talosv1alpha1.ReasonPreconditionUnmet,
				fmt.Sprintf("TalosMachine %s renders differently than when it was last compared", n.machine.Name)}, nil
		}

		result, err := reader.DryRunApply(ctx, n.node.address, desired.BytesOrderedLike(n.live))
		if err != nil {
			return &configProblem{metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable,
				fmt.Sprintf("dry run on %s: %v", n.machine.Name, err)}, nil
		}

		if !result.NoChanges {
			changed = append(changed, n.machine.Name)
		}
	}

	if len(changed) > 0 {
		return &configProblem{metav1.ConditionFalse, talosv1alpha1.ReasonServerReportsChanges,
			fmt.Sprintf("Talos's dry run reports changes on %s that the per-document comparison does not show",
				strings.Join(changed, ", "))}, nil
	}

	return nil, nil
}
