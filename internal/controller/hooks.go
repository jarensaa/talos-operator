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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
)

// Hooks: the Jobs a user has the operator run around an operation and around each node's turn
// (docs/scope.md §6.4). plan.NextTurn decides which hook runs when, and plan.NextHook what a
// running one needs; this file creates the Job and reads what became of it.
//
// The chart binds this in the release namespace, with the manager's other rules.
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create

// hookJobTTL is how long a finished hook Job is kept, where its spec does not say: long enough to
// read the log of one that failed.
const hookJobTTL = 24 * time.Hour

// hooksAt is the list of hooks of a stage.
func hooksAt(tc *talosv1alpha1.TalosCluster, stage string) []talosv1alpha1.Hook {
	hooks := tc.Spec.Rollout.Hooks
	if hooks == nil {
		return nil
	}

	switch stage {
	case talosv1alpha1.HookPreRollout:
		return hooks.PreRollout
	case talosv1alpha1.HookPostRollout:
		return hooks.PostRollout
	case talosv1alpha1.HookPreNode:
		return hooks.PreNode
	case talosv1alpha1.HookPostNode:
		return hooks.PostNode
	}

	return nil
}

// hookCounts is how many hooks each stage has. An operation that takes no node out of the cluster
// runs none around its nodes.
func hookCounts(tc *talosv1alpha1.TalosCluster, nodeHooks bool) plan.Hooks {
	counts := plan.Hooks{
		PreRollout:  len(hooksAt(tc, talosv1alpha1.HookPreRollout)),
		PostRollout: len(hooksAt(tc, talosv1alpha1.HookPostRollout)),
	}
	if nodeHooks {
		counts.PreNode = len(hooksAt(tc, talosv1alpha1.HookPreNode))
		counts.PostNode = len(hooksAt(tc, talosv1alpha1.HookPostNode))
	}

	return counts
}

// hookJobName names the Job of one run of a hook. It is derived from everything that tells one
// run from another, so a replica that takes over finds the Job that was already started, and a
// retry gets a Job of its own.
func hookJobName(tc *talosv1alpha1.TalosCluster, opID, stage string, index int32, machine string, attempt int32) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s/%s/%s/%d/%s/%d", tc.UID, opID, stage, index, machine, attempt))
	prefix := tc.Name

	// A Job's name becomes a label of its pods: 63 characters.
	const maxPrefix = 63 - 1 - 23 - 1 - 10
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
	}

	return fmt.Sprintf("%s-%s-%s", prefix, hooksAt(tc, stage)[index].Name, hex.EncodeToString(sum[:5]))
}

// startHook persists the hook that runs next. Its Job is created by a later reconcile.
func (r *TalosClusterReconciler) startHook(tc *talosv1alpha1.TalosCluster, stage string, index int32, machine string, attempt int32) {
	op := tc.Status.Operation
	op.Hook = &talosv1alpha1.HookRun{
		Stage:   stage,
		Index:   index,
		Name:    hooksAt(tc, stage)[index].Name,
		Machine: machine,
		Job:     hookJobName(tc, op.ID, stage, index, machine, attempt),
		Attempt: attempt,
		Since:   metav1.NewTime(r.now()),
	}
}

// runHook carries the hook that is running further. It reports whether the hook is through, and
// otherwise what it waits for. A hook that failed halts the operation.
func (r *TalosClusterReconciler) runHook(ctx context.Context, tc *talosv1alpha1.TalosCluster, timing plan.Timing) (bool, string, error) {
	op := tc.Status.Operation
	h := op.Hook
	now := r.now()

	declared := hooksAt(tc, h.Stage)
	if int(h.Index) >= len(declared) || declared[h.Index].Name != h.Name {
		// The spec no longer has it. What is not asked for is not waited for.
		return true, "", nil
	}

	hook := declared[h.Index]

	timeout := timing.StepTimeout
	if hook.Timeout != nil {
		timeout = hook.Timeout.Duration
	}

	job := &batchv1.Job{}
	observed := plan.HookJob{Exists: true}

	if err := r.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: h.Job}, job); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, "", fmt.Errorf("%w: get Job %s: %w", errKubernetes, h.Job, err)
		}

		observed.Exists = false
	}

	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}

		switch c.Type { //nolint:exhaustive // a Job is through when it is complete or failed
		case batchv1.JobComplete:
			observed.Succeeded = true
		case batchv1.JobFailed:
			observed.Failed, observed.Message = true, c.Reason+": "+c.Message
		}
	}

	action := plan.NextHook(observed, h.Since.Time, timeout, now)
	what := fmt.Sprintf("%s hook %s", h.Stage, h.Name)

	switch action.Kind { //nolint:exhaustive // the actions of a node step never come out of a hook
	case plan.EndHook:
		logf.FromContext(ctx).Info("hook succeeded", "stage", h.Stage, "hook", h.Name, "machine", h.Machine, "job", h.Job)

		return true, "", nil
	case plan.Fail:
		failHook(tc, action.Reason, fmt.Sprintf("%s (Job %s): %s", what, h.Job, action.Message), now)

		return false, "", nil
	case plan.Run:
		if !r.latest(ctx, tc) {
			return false, noteCatchingUp, nil
		}

		if err := r.createHookJob(ctx, tc, hook, timeout); err != nil {
			if apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsForbidden(err) {
				failHook(tc, plan.FailHook, fmt.Sprintf("%s: its Job could not be created: %v", what, err), now)

				return false, "", nil
			}

			return false, "", fmt.Errorf("%w: create Job %s: %w", errKubernetes, h.Job, err)
		}
	}

	return false, fmt.Sprintf("%s: waiting for Job %s", what, h.Job), nil
}

// createHookJob creates the Job of the hook that is running. One that exists already is the one.
func (r *TalosClusterReconciler) createHookJob(ctx context.Context, tc *talosv1alpha1.TalosCluster, hook talosv1alpha1.Hook,
	timeout time.Duration,
) error {
	op := tc.Status.Operation
	h := op.Hook

	var spec batchv1.JobSpec
	if err := json.Unmarshal(hook.Job.Raw, &spec); err != nil {
		return apierrors.NewBadRequest("spec.rollout.hooks: job is not a batch/v1 JobSpec: " + err.Error())
	}

	// A hook runs once, and its failure is the operation's to report: nothing here tries again
	// behind the operator's back unless the spec says so.
	if spec.BackoffLimit == nil {
		spec.BackoffLimit = ptr.To(int32(0))
	}

	if spec.Template.Spec.RestartPolicy == "" {
		spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	}

	if spec.ActiveDeadlineSeconds == nil {
		spec.ActiveDeadlineSeconds = ptr.To(int64(timeout.Seconds()))
	}

	if spec.TTLSecondsAfterFinished == nil {
		spec.TTLSecondsAfterFinished = ptr.To(int32(hookJobTTL.Seconds()))
	}

	env := []corev1.EnvVar{
		{Name: "TALOS_CLUSTER", Value: tc.Name},
		{Name: "TALOS_OPERATION", Value: string(op.Type)},
		{Name: "TALOS_OPERATION_ID", Value: op.ID},
		{Name: "TALOS_HOOK", Value: h.Stage},
	}

	if h.Machine != "" {
		env = append(env, corev1.EnvVar{Name: "TALOS_MACHINE", Value: h.Machine})

		tm := &talosv1alpha1.TalosMachine{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: h.Machine}, tm); err == nil &&
			tm.Status.Identity != nil && tm.Status.Identity.KubernetesNodeName != "" {
			env = append(env, corev1.EnvVar{Name: "TALOS_NODE", Value: tm.Status.Identity.KubernetesNodeName})
		}
	}

	for _, containers := range [][]corev1.Container{spec.Template.Spec.InitContainers, spec.Template.Spec.Containers} {
		for i := range containers {
			containers[i].Env = append(containers[i].Env, env...)
		}
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: tc.Namespace,
			Name:      h.Job,
			Labels:    map[string]string{talosv1alpha1.LabelCluster: tc.Name, talosv1alpha1.LabelHook: h.Stage},
		},
		Spec: spec,
	}

	if err := controllerutil.SetControllerReference(tc, job, r.Scheme); err != nil {
		return fmt.Errorf("own Job %s: %w", job.Name, err)
	}

	if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	logf.FromContext(ctx).Info("hook started", "stage", h.Stage, "hook", h.Name, "machine", h.Machine, "job", h.Job)

	return nil
}

// failHook halts the operation at the hook that is running until a human acts (docs/scope.md §8.3).
func failHook(tc *talosv1alpha1.TalosCluster, reason, message string, now time.Time) {
	op := tc.Status.Operation
	h := op.Hook
	at := metav1.NewTime(now)

	sum := sha256.Sum256(fmt.Appendf(nil, "%s/%s/%d/%s/%d/%d", op.ID, h.Stage, h.Index, h.Machine, h.Attempt, now.UnixNano()))
	token := hex.EncodeToString(sum[:4])

	first := fmt.Sprintf("Read the Job's log: kubectl logs -n %s job/%s.", tc.Namespace, h.Job)
	retry := fmt.Sprintf("annotate the TalosCluster with %s=%s to run the hook again", talosv1alpha1.AnnotationRetry, token)
	abort := fmt.Sprintf("%s=%s to end the operation", talosv1alpha1.AnnotationAbort, token)

	var skip string

	switch h.Stage {
	case talosv1alpha1.HookPreNode:
		skip = fmt.Sprintf(", %s=%s to leave this machine out", talosv1alpha1.AnnotationSkipMachine, token)
	case talosv1alpha1.HookPostNode:
		skip = fmt.Sprintf(", %s=%s to go on without this machine's remaining hooks", talosv1alpha1.AnnotationSkipMachine, token)
	case talosv1alpha1.HookPostRollout:
		abort = fmt.Sprintf("%s=%s to end the operation without its remaining hooks", talosv1alpha1.AnnotationAbort, token)
	}

	op.Failure = &talosv1alpha1.OperationFailure{
		Machine:     h.Machine,
		Step:        h.Stage + "/" + h.Name,
		Reason:      reason,
		Message:     message,
		Remediation: fmt.Sprintf("%s Then %s%s, or %s.", first, retry, skip, abort),
		Hook:        h.Stage,
		Token:       token,
		At:          &at,
	}
}
