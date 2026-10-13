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
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/jarensaa/talos-operator/internal/plan"
)

const (
	// nodeLeaseNamespace holds one Lease per Node, which its kubelet renews every few seconds.
	nodeLeaseNamespace = "kube-node-lease"
	// leaseFreshness is how recent a node's heartbeat must be. It is the grace period after which
	// Kubernetes itself has long declared a node NotReady.
	leaseFreshness = 40 * time.Second
)

// The chart binds the manager's rules in the release namespace only, and grants this one in
// kube-node-lease through a Role of its own (charts/talos-operator/templates/rbac.yaml).
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get

// verifyNode checks a node whose config was just changed, from outside the node
// (docs/scope.md §8.5.1). The node's Talos API has already answered through a different node by
// the time this runs, which is what makes it reachable to the planner. On top of that:
//
//   - the cluster is whole, this node included: every member runs and is Ready to Kubernetes,
//     etcd has exactly the control planes as healthy voters, and Talos's own health checks pass
//     (plan.VerifyGate);
//   - Kubernetes sees the node Ready, with a network;
//   - the node's kubelet has renewed its Lease since verification began, and recently. The
//     Ready condition alone is the control plane's last word on the node, which stays True for
//     most of a minute after a node went dark.
//
// The planner requires all of it to hold without interruption for the soak. Only API errors of
// the management cluster are returned.
func (r *TalosClusterReconciler) verifyNode(ctx context.Context, obs observation, m boundMachine, since, now time.Time) (plan.Gate, error) {
	closed := func(format string, args ...any) (plan.Gate, error) {
		return plan.Gate{Reason: fmt.Sprintf(format, args...)}, nil
	}

	if gate := plan.VerifyGate(passOf(obs)); !gate.Open {
		return gate, nil
	}

	name := m.kubernetesNode()

	node := &corev1.Node{}
	if err := r.Get(ctx, client.ObjectKey{Name: name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return closed("Kubernetes has no Node %s", name)
		}

		return plan.Gate{}, fmt.Errorf("get node %s: %w", name, err)
	}

	if !nodeConditionTrue(node, corev1.NodeReady) {
		return closed("Kubernetes Node %s is not Ready", name)
	}

	if nodeConditionTrue(node, corev1.NodeNetworkUnavailable) {
		return closed("Kubernetes Node %s has no network", name)
	}

	lease := &coordinationv1.Lease{}
	if err := r.apiReader().Get(ctx, client.ObjectKey{Namespace: nodeLeaseNamespace, Name: name}, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return closed("Node %s has no heartbeat Lease", name)
		}

		return plan.Gate{}, fmt.Errorf("get lease of node %s: %w", name, err)
	}

	renewed := lease.Spec.RenewTime
	if renewed == nil || !renewed.After(since) {
		return closed("the kubelet of %s has not renewed its Lease since the config was applied", name)
	}

	if now.Sub(renewed.Time) > leaseFreshness {
		return closed("the kubelet of %s has not renewed its Lease for more than %s", name, leaseFreshness)
	}

	return plan.Gate{Open: true}, nil
}

// nodeConditionTrue reports whether the Node has the condition, and it is True. A condition the
// Node does not report is not True.
func nodeConditionTrue(node *corev1.Node, condType corev1.NodeConditionType) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == condType {
			return c.Status == corev1.ConditionTrue
		}
	}

	return false
}

// apiReader reads objects the manager's cache does not hold.
func (r *TalosClusterReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}

	return r.Client
}
