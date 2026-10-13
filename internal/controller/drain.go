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

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
)

// Draining a node before an upgrade reboots it (docs/scope.md §7.3). The operator does what
// `kubectl drain` does, and nothing a node's own tooling does as well: Talos's upgrade since 1.13
// leaves draining to the client, so there is exactly one drain.
//
// The chart binds these cluster-wide, in a ClusterRole of their own (templates/rbac.yaml): pods
// live in every namespace, and Nodes in none.
// +kubebuilder:rbac:groups="",resources=nodes,verbs=patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=list
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create

// mirrorPodAnnotation marks a static pod's reflection in the API. The kubelet runs the pod from a
// file on the node, so evicting the reflection moves nothing.
const mirrorPodAnnotation = "kubernetes.io/config.mirror"

// setUnschedulable cordons or uncordons a node.
func (r *TalosClusterReconciler) setUnschedulable(ctx context.Context, node *corev1.Node, unschedulable bool) error {
	if node.Spec.Unschedulable == unschedulable {
		return nil
	}

	original := node.DeepCopy()
	node.Spec.Unschedulable = unschedulable

	if err := r.Patch(ctx, node, client.MergeFrom(original)); err != nil {
		return fmt.Errorf("set unschedulable=%t on node %s: %w", unschedulable, node.Name, err)
	}

	return nil
}

// podsToDrain lists the pods that have to leave a node before it reboots: everything but static
// pods, pods of a DaemonSet (their controller would put them straight back) and pods that have
// finished. A pod that is already terminating still counts: the node is drained when it is gone.
func (r *TalosClusterReconciler) podsToDrain(ctx context.Context, nodeName string) ([]corev1.Pod, error) {
	var pods corev1.PodList

	// Uncached: the manager only watches its own namespace, and pods live in all of them.
	if err := r.apiReader().List(ctx, &pods, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return nil, fmt.Errorf("list pods on node %s: %w", nodeName, err)
	}

	left := make([]corev1.Pod, 0, len(pods.Items))

	for _, pod := range pods.Items {
		switch {
		case pod.Annotations[mirrorPodAnnotation] != "":
		case pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed:
		case metav1.GetControllerOf(&pod) != nil && metav1.GetControllerOf(&pod).Kind == "DaemonSet":
		default:
			left = append(left, pod)
		}
	}

	return left, nil
}

// PodEvictor evicts a pod through the eviction API.
type PodEvictor interface {
	Evict(ctx context.Context, pod *corev1.Pod) error
}

// restEvictor evicts without the client's own retries. A budget that has nothing to give answers
// 429 with a Retry-After header, and the Kubernetes client by default sleeps and asks again, ten
// times over (rest.Request.MaxRetries): well over a minute for every pod that may not go, inside
// a reconcile. The operator's own loop is what tries again, with everything else it has to watch.
type restEvictor struct {
	pods rest.Interface
}

// NewPodEvictor returns the PodEvictor of a cluster.
func NewPodEvictor(cfg *rest.Config) (PodEvictor, error) {
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("client for evictions: %w", err)
	}

	return &restEvictor{pods: clientset.CoreV1().RESTClient()}, nil
}

func (e *restEvictor) Evict(ctx context.Context, pod *corev1.Pod) error {
	eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}

	return e.pods.Post().Namespace(pod.Namespace).Resource("pods").Name(pod.Name).SubResource("eviction").
		Body(eviction).MaxRetries(0).Do(ctx).Error()
}

// evict asks Kubernetes to evict the pods, which is what makes PodDisruptionBudgets count: a pod
// whose budget does not allow it now is refused, and tried again in the next pass. It returns
// how many pods a budget held back.
func (r *TalosClusterReconciler) evict(ctx context.Context, pods []corev1.Pod) (int, error) {
	if r.Evictor == nil {
		return 0, errors.New("the operator was started without a way to evict pods")
	}

	held := 0

	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil {
			continue
		}

		err := r.Evictor.Evict(ctx, pod)

		switch {
		case err == nil, apierrors.IsNotFound(err):
		case apierrors.IsTooManyRequests(err):
			// The answer of a disruption budget that has nothing to give right now.
			held++
		default:
			return held, fmt.Errorf("evict pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}

	return held, nil
}

// nodeToDrain reads the Node of that name and, while the step in flight drains it and it is
// cordoned, the pods that have to leave it. The Node is nil when there is none of that name, and
// podsLeft negative when the pods were not counted.
func (r *TalosClusterReconciler) nodeToDrain(ctx context.Context, name string, cur *talosv1alpha1.OperationStep,
) (*corev1.Node, []corev1.Pod, int, error) {
	if name == "" {
		return nil, nil, -1, nil
	}

	kube := &corev1.Node{}
	if err := r.Get(ctx, client.ObjectKey{Name: name}, kube); err != nil {
		return nil, nil, -1, client.IgnoreNotFound(err)
	}

	if cur.Step != plan.StepDraining || !kube.Spec.Unschedulable {
		return kube, nil, -1, nil
	}

	pods, err := r.podsToDrain(ctx, name)
	if err != nil {
		logf.FromContext(ctx).Info("listing the pods to drain failed, will retry", "node", name, "error", err.Error())

		return kube, nil, -1, nil
	}

	return kube, pods, len(pods), nil
}

// drained carries out what a planner answered for a node that is being drained, or made
// schedulable again: cordon it, evict what is left on it, uncordon it. It reports whether the
// answer was one of those. Each is only done from the TalosCluster as the API server has it now.
func (r *TalosClusterReconciler) drained(ctx context.Context, tc *talosv1alpha1.TalosCluster, action plan.Action, kube *corev1.Node,
	pods []corev1.Pod,
) (string, bool, error) {
	switch action.Kind {
	case plan.Cordon, plan.Uncordon, plan.Evict:
	default:
		return "", false, nil
	}

	if !r.latest(ctx, tc) {
		return noteCatchingUp, true, nil
	}

	if action.Kind != plan.Evict {
		return "", true, r.setUnschedulable(ctx, kube, action.Kind == plan.Cordon)
	}

	held, err := r.evict(ctx, pods)
	if err != nil {
		return "", true, err
	}

	if held > 0 {
		return fmt.Sprintf("%s; a PodDisruptionBudget holds %d of them back for now", action.Message, held), true, nil
	}

	return action.Message, true, nil
}
