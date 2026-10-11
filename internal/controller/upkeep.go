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
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/adopt"
	"github.com/jarensaa/talos-operator/internal/plan"
)

// The upkeep the operator does by itself (docs/scope.md §14.6): it defragments etcd before a
// database reaches its quota, and says when the certificates the cluster depends on end.

// defragTimeout bounds the defragmentation of one member. It takes a moment for a database of a
// few hundred megabytes and minutes for one at its quota on a slow disk; the member answers
// nobody meanwhile, so it is not left hanging for longer.
const defragTimeout = 5 * time.Minute

// certificateOperator is the name status.certificates gives the client certificate the operator
// reaches the Talos API with. The others are the authorities of the secrets bundle.
const certificateOperator = "operator"

func maintenanceEnabled(tc *talosv1alpha1.TalosCluster) bool {
	return managing(tc) && tc.Spec.Management.Maintenance == talosv1alpha1.CapabilityEnabled
}

// observeCertificates owns status.certificates: when the operator's own client certificate and
// the cluster's certificate authorities stop being valid. A certificate that could not be read
// in this pass keeps the date it had: a date does not change while nobody looks, and the one
// that matters most is the one of a certificate that has just stopped working.
func (r *TalosClusterReconciler) observeCertificates(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation) {
	known := map[string]talosv1alpha1.Certificate{}
	for _, c := range tc.Status.Certificates {
		known[c.Name] = c
	}

	note := func(name string, notBefore, notAfter time.Time) {
		if !notAfter.IsZero() {
			known[name] = talosv1alpha1.Certificate{Name: name, IssuedAt: metav1.NewTime(notBefore), ExpiresAt: metav1.NewTime(notAfter)}
		}
	}

	if c := obs.credentials; c != nil {
		note(certificateOperator, c.NotBefore, c.NotAfter)
	}

	ref := tc.SecretsReference()
	secret := &corev1.Secret{}

	// Without the bundle there are no authorities to report yet: adoption writes it.
	if err := r.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: ref.Name}, secret); err == nil {
		if bundle, err := adopt.DecodeBundle(secret.Data[ref.Key]); err == nil {
			for name, v := range adopt.AuthorityValidity(bundle) {
				note(name, v.NotBefore, v.NotAfter)
			}
		}
	}

	certs := make([]talosv1alpha1.Certificate, 0, len(known))
	for _, c := range known {
		certs = append(certs, c)
	}

	slices.SortFunc(certs, func(a, b talosv1alpha1.Certificate) int { return strings.Compare(a.Name, b.Name) })

	tc.Status.Certificates = certs
	if len(certs) == 0 {
		tc.Status.Certificates = nil
	}
}

// defragSettings is spec.maintenance.etcd with its defaults, and what this pass adds to it.
func defragSettings(tc *talosv1alpha1.TalosCluster, obs observation, now time.Time) plan.DefragSettings {
	s := plan.DefragSettings{
		Paused: tc.Spec.Paused, Policy: obs.policy, Now: now,
		FreePercent: plan.DefaultDefragmentFreePercent,
		MinimumSize: plan.DefaultDefragmentMinimumSize,
		Interval:    plan.DefaultDefragmentInterval,
	}

	etcd := tc.Spec.Maintenance.Etcd

	if etcd.FreePercent != nil {
		s.FreePercent = int64(*etcd.FreePercent)
	}

	if etcd.MinimumSize != nil {
		s.MinimumSize = etcd.MinimumSize.Value()
	}

	if etcd.Interval != nil {
		s.Interval = etcd.Interval.Duration
	}

	if done := tc.Status.Maintenance; done != nil {
		s.Last = timeOf(done.EtcdDefragmentedAt)
	}

	return s
}

// defragMembers is what this pass read of the etcd member of every control plane. The numbers
// are the ones read now, not the ones in a machine's status, which follow at a distance.
func defragMembers(obs observation, pools map[string]*talosv1alpha1.TalosMachinePool, machines []talosv1alpha1.TalosMachine,
) []plan.DefragMember {
	var out []plan.DefragMember

	for i := range machines {
		tm := &machines[i]
		if !controlPlane(pools[tm.Spec.PoolRef.Name]) {
			continue
		}

		m := plan.DefragMember{Name: tm.Name, Node: nodeOf(obs, tm)}

		if live := nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID); live != nil && live.etcdStatusRead {
			m.Known, m.Size, m.InUse, m.Leader = true, live.etcdSize, live.etcdInUse, live.etcdLeader
		}

		out = append(out, m)
	}

	return out
}

// defragment continues the defragmentation of etcd in flight, or decides whether one is due. It
// runs after everything else: a cluster that has something to roll out does that first, and is
// defragmented while nothing else holds it.
func (r *TalosClusterReconciler) defragment(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	pools map[string]*talosv1alpha1.TalosMachinePool, machines []talosv1alpha1.TalosMachine,
) error {
	st, gen := &tc.Status, tc.Generation
	timing := rolloutTiming(tc.Spec.Rollout)

	if op := st.Operation; op != nil {
		if op.Type != talosv1alpha1.OperationEtcdDefragment {
			return nil
		}

		if !maintenanceEnabled(tc) {
			r.finish(tc, talosv1alpha1.OperationAborted, "maintenance was disabled")

			return nil
		}

		note, err := r.continueDefragment(ctx, tc, obs, machines, timing)
		reportDefragmentation(tc, obs, note)

		return err
	}

	if obs.facts == nil {
		return nil
	}

	maintained := func(status metav1.ConditionStatus, reason, format string, args ...any) error {
		setCondition(st, gen, talosv1alpha1.ConditionEtcdMaintained, status, reason, fmt.Sprintf(format, args...))

		return nil
	}

	members := defragMembers(obs, pools, machines)
	result := plan.PlanDefragment(members, defragSettings(tc, obs, r.now()), passOf(obs))
	fragmented := strings.Join(result.Fragmented, ", ")

	switch {
	case !slices.ContainsFunc(members, func(m plan.DefragMember) bool { return m.Known }):
		return maintained(metav1.ConditionUnknown, talosv1alpha1.ReasonAwaitingStatus, "the size of no etcd member's database was read")
	case len(result.Pending) == 0:
		return maintained(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "no etcd member's database is due for defragmenting")
	case !maintenanceEnabled(tc):
		return maintained(metav1.ConditionFalse, talosv1alpha1.ReasonMaintenanceDisabled,
			"the etcd database of %s is fragmented, and is left so while spec.management.maintenance is not Enabled", fragmented)
	}

	if result.MayStart() {
		if err := timing.Validate(); err != nil {
			result.BlockReason, result.BlockMessage = talosv1alpha1.ReasonInvalidRollout, "spec.rollout: "+err.Error()
		}
	}

	// The reason is what it waits for, as it is for a change that is not rolled out: waiting for
	// the interval, a pause or a window is waiting as told.
	if !result.MayStart() {
		return maintained(metav1.ConditionFalse, result.BlockReason,
			"the etcd database of %s is fragmented, and is not defragmented yet: %s", fragmented, result.BlockMessage)
	}

	now := metav1.NewTime(r.now())
	st.Operation = &talosv1alpha1.Operation{
		ID:        fmt.Sprintf("%s-%d", result.Change, now.Unix()),
		Type:      talosv1alpha1.OperationEtcdDefragment,
		Target:    result.Change,
		Plan:      result.Pending,
		StartedAt: now,
	}

	// Recorded as it starts, not as it ends: one that fails or is aborted is not started again
	// before the interval has passed either.
	if st.Maintenance == nil {
		st.Maintenance = &talosv1alpha1.MaintenanceStatus{}
	}

	st.Maintenance.EtcdDefragmentedAt = &now

	logf.FromContext(ctx).Info("etcd is to be defragmented", "fragmented", result.Detail, "order", result.Pending)

	return maintained(metav1.ConditionFalse, talosv1alpha1.ReasonDefragmenting,
		"the etcd database of %s is fragmented: every member is defragmented, one at a time", fragmented)
}

// reportDefragmentation sets EtcdMaintained and RolloutBlocked for the defragmentation in flight.
func reportDefragmentation(tc *talosv1alpha1.TalosCluster, obs observation, note string) {
	st, gen := &tc.Status, tc.Generation

	op := st.Operation
	if op == nil {
		return
	}

	if f := op.Failure; f != nil {
		msg := fmt.Sprintf("defragmenting etcd halted %s: %s", haltedAt(f), f.Message)

		setCondition(st, gen, talosv1alpha1.ConditionEtcdMaintained, metav1.ConditionFalse, talosv1alpha1.ReasonOperationFailed, msg)
		setCondition(st, gen, talosv1alpha1.ConditionRolloutBlocked, metav1.ConditionTrue, talosv1alpha1.ReasonOperationFailed,
			msg+" "+f.Remediation)
		st.Phase = talosv1alpha1.ClusterPhaseDegraded

		return
	}

	st.Phase = talosv1alpha1.ClusterPhaseManaged

	setCondition(st, gen, talosv1alpha1.ConditionEtcdMaintained, metav1.ConditionFalse, talosv1alpha1.ReasonDefragmenting, progressOf(op, note))
	reportHeld(tc, obs.policy, "a member that was defragmented is still verified, no other member is defragmented")
}

func (r *TalosClusterReconciler) continueDefragment(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	machines []talosv1alpha1.TalosMachine, timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation

	begin := func(name string, attempt int32) error {
		// A control plane that left the cluster since the plan was made has no member any more.
		if machineNamed(machines, name) == nil {
			skipped(op, name)

			return nil
		}

		op.Current = &talosv1alpha1.OperationStep{
			Machine: name, Step: plan.StepDefragmenting, Attempt: attempt, Since: metav1.NewTime(r.now()),
		}

		return nil
	}

	return r.drive(ctx, tc, obs, timing, turns{
		begin: begin,
		step: func() (string, error) {
			return r.defragStep(ctx, tc, obs, machines, timing)
		},
		failed: func() error {
			f := op.Failure
			asked := func(annotation string) bool { return f.Token != "" && tc.Annotations[annotation] == f.Token }

			switch {
			case asked(talosv1alpha1.AnnotationAbort):
				r.end(tc, talosv1alpha1.OperationAborted, fmt.Sprintf("aborted after failing at %s (%s): %s", f.Machine, f.Reason, f.Message))
			case asked(talosv1alpha1.AnnotationSkipMachine):
				skipped(op, f.Machine)
			case asked(talosv1alpha1.AnnotationRetry):
				attempt := int32(1)
				if op.Current != nil {
					attempt = op.Current.Attempt + 1
				}

				// Defragmenting a member twice does no harm: the turn starts over.
				op.Failure, op.Current = nil, nil

				return begin(f.Machine, attempt)
			}

			return nil
		},
		// Nothing has to be remembered about a member that was left out, or about an operation
		// that was ended: the interval keeps the next one away.
		skip:  func(string) error { return nil },
		abort: func() {},
	})
}

// defragStep asks the planner what the member that is being defragmented needs next, and does it.
func (r *TalosClusterReconciler) defragStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation,
	machines []talosv1alpha1.TalosMachine, timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	tm := machineNamed(machines, cur.Machine)
	if tm == nil {
		failOperation(op, plan.FailTimeout, "the TalosMachine is no longer a member of the cluster", now)

		return "", nil
	}

	live := nodeWithIdentity(obs.facts, tm.Spec.Identity.SystemUUID)
	reachable := live != nil && live.etcdStatusRead

	// Every node has to be in order, the member's own included: nothing here repairs it.
	pre := plan.TouchGate(passOf(obs), "").And(obs.policy.Gates())
	checks := plan.Gate{Reason: notVerifiedYet}

	if cur.Step == plan.StepVerifying {
		checks = plan.VerifyGate(passOf(obs)).And(obs.policy.Gates())
	}

	step := plan.DefragStep{Name: cur.Step, Since: cur.Since.Time, HealthySince: timeOf(cur.HealthySince)}
	action := plan.NextDefragStep(step, reachable, pre, checks, timing, now)
	stamp := metav1.NewTime(now)

	if action.Kind != plan.Wait {
		logf.FromContext(ctx).Info("defragmentation step", "machine", cur.Machine, "step", cur.Step, "action", action.Kind.String(),
			"reason", action.Reason, "detail", action.Message, "gate", pre.Note)
	}

	switch action.Kind { //nolint:exhaustive // the actions of the other operations never come out of this one
	case plan.Wait:
		return action.Message, nil
	case plan.MarkHealthy:
		cur.HealthySince = &stamp
	case plan.MarkUnhealthy:
		cur.HealthySince = nil

		return action.Message, nil
	case plan.Defrag:
		if !r.latest(ctx, tc) {
			return noteCatchingUp, nil
		}

		keeper, err := r.Resolver.ResolveKeeper(ctx, tc, obs.endpoints)
		if err != nil {
			//nolint:nilerr // what the step waits for; the planner's timeouts end it if this lasts
			return "no Talos client to defragment with: " + err.Error(), nil
		}

		if live.etcdLeader {
			// A courtesy to the cluster, not a condition: a leader that is busy defragmenting is
			// replaced either way, only later. A single control plane has nobody to hand over to.
			if err := keeper.ForfeitEtcdLeadership(ctx, live.address); err != nil {
				logf.FromContext(ctx).Info("etcd leadership was not handed over before the defragmentation", "machine", cur.Machine,
					"error", err.Error())
			}
		}

		defragCtx, cancel := context.WithTimeout(ctx, defragTimeout)
		defer cancel()

		if err := keeper.DefragmentEtcd(defragCtx, live.address); err != nil {
			logf.FromContext(ctx).Info("the defragmentation failed, will retry", "machine", cur.Machine, "error", err.Error())

			return "the last attempt to defragment the member failed: " + err.Error(), nil
		}

		cur.Step, cur.Since, cur.HealthySince = plan.StepVerifying, stamp, nil
	case plan.Complete:
		verified(tc, false)
	case plan.Fail:
		failOperation(op, action.Reason, action.Message, now)
	}

	return "", nil
}

// forgetUpkeep drops what status says about upkeep that is no longer looked after, when config
// management returns to Observe. When etcd was last defragmented stays: it happened.
func forgetUpkeep(st *talosv1alpha1.TalosClusterStatus) {
	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionEtcdMaintained)
}
