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
		Policy: obs.policy, Now: now,
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
func defragMembers(f *fleet) []plan.DefragMember {
	var out []plan.DefragMember

	for i := range f.members {
		m := f.of(&f.members[i])
		if !m.controlPlane() {
			continue
		}

		member := plan.DefragMember{Name: m.tm.Name, Node: m.knownAs()}

		if live := m.node; live != nil && live.etcdStatusRead {
			member.Known, member.Size, member.InUse, member.Leader = true, live.etcdSize, live.etcdInUse, live.etcdLeader
		}

		out = append(out, member)
	}

	return out
}

// planDefragment owns EtcdMaintained while no operation is in flight: it asks the planner whether
// etcd is to be defragmented, and starts that by taking the lock. It runs after everything else:
// a cluster that has something to roll out does that first, and is defragmented while nothing
// else holds it.
func (r *TalosClusterReconciler) planDefragment(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet) {
	st, gen := &tc.Status, tc.Generation

	maintained := func(status metav1.ConditionStatus, reason, format string, args ...any) {
		setCondition(st, gen, talosv1alpha1.ConditionEtcdMaintained, status, reason, fmt.Sprintf(format, args...))
	}

	members := defragMembers(f)
	result := plan.PlanDefragment(members, defragSettings(tc, obs, r.now()), passOf(obs))
	fragmented := strings.Join(result.Fragmented, ", ")

	switch {
	case !slices.ContainsFunc(members, func(m plan.DefragMember) bool { return m.Known }):
		maintained(metav1.ConditionUnknown, talosv1alpha1.ReasonAwaitingStatus, "the size of no etcd member's database was read")
	case len(result.Pending) == 0:
		maintained(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "no etcd member's database is due for defragmenting")
	case !tc.Spec.Management.MayMaintain():
		maintained(metav1.ConditionFalse, talosv1alpha1.ReasonMaintenanceDisabled,
			"the etcd database of %s is fragmented, and is left so while spec.management.maintenance is not Enabled", fragmented)
	case !result.MayStart():
		// The reason is what it waits for, as it is for a change that is not rolled out: waiting
		// for the interval, a pause or a window is waiting as told.
		maintained(metav1.ConditionFalse, result.BlockReason,
			"the etcd database of %s is fragmented, and is not defragmented yet: %s", fragmented, result.BlockMessage)
	default:
		op := r.start(tc, talosv1alpha1.OperationEtcdDefragment, result.Due)

		// Recorded as it starts, not as it ends: one that fails or is aborted is not started
		// again before the interval has passed either.
		if st.Maintenance == nil {
			st.Maintenance = &talosv1alpha1.MaintenanceStatus{}
		}

		st.Maintenance.EtcdDefragmentedAt = &op.StartedAt

		logf.FromContext(ctx).Info("etcd is to be defragmented", "fragmented", result.Detail, "order", result.Pending)

		maintained(metav1.ConditionFalse, talosv1alpha1.ReasonDefragmenting,
			"the etcd database of %s is fragmented: every member is defragmented, one at a time", fragmented)
	}
}

// defragTurns is what the turns of a defragmentation do.
func (r *TalosClusterReconciler) defragTurns(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) turns {
	op := tc.Status.Operation

	return turns{
		// Defragmenting a member twice does no harm: a turn that is run again starts over.
		begin: func(name string, attempt int32) error {
			// A control plane that left the cluster since the plan was made has no member any more.
			if f.member(name) == nil {
				skipped(op, name)

				return nil
			}

			op.Current = &talosv1alpha1.OperationStep{Machine: name, Step: plan.StepDefragmenting, Attempt: attempt, Since: r.stamp()}

			return nil
		},
		step: func() (string, error) { return r.defragStep(ctx, tc, obs, f, timing) },
	}
}

// defragStep asks the planner what the member that is being defragmented needs next, and does it.
func (r *TalosClusterReconciler) defragStep(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation, f *fleet,
	timing plan.Timing,
) (string, error) {
	op := tc.Status.Operation
	cur := op.Current
	now := r.now()

	tm := f.member(cur.Machine)
	if tm == nil {
		failOperation(op, plan.FailTimeout, "the TalosMachine is no longer a member of the cluster", now)

		return "", nil
	}

	live := f.of(tm).node
	reachable := live != nil && live.etcdStatusRead

	// Every node has to be in order, the member's own included: nothing here repairs it.
	pre := plan.TouchGate(passOf(obs), "").And(obs.policy.Gates())
	checks := plan.Gate{Reason: notVerifiedYet}

	if cur.Step == plan.StepVerifying {
		checks = plan.VerifyGate(passOf(obs)).And(obs.policy.Gates())
	}

	action := plan.NextDefragStep(stepOf(cur), reachable, pre, checks, timing, now)
	logAnswer(ctx, op, action, pre)

	if note, done := answered(op, action, now); done {
		return note, nil
	}

	switch action.Kind {
	case plan.Defrag:
		keeper, note := clientTo(ctx, r, tc, obs, "defragment with", r.Resolver.ResolveKeeper)
		if note != "" {
			return note, nil
		}

		// A leader that is busy defragmenting is replaced either way, only later.
		if live.etcdLeader {
			handOver(ctx, keeper, live.address, cur.Machine)
		}

		defragCtx, cancel := context.WithTimeout(ctx, defragTimeout)
		defer cancel()

		if err := keeper.DefragmentEtcd(defragCtx, live.address); err != nil {
			return retried(ctx, op, "defragment the member", err), nil
		}

		moveTo(cur, plan.StepVerifying, now)
	case plan.Complete:
		completed(tc)
	default:
		return "", unexpected(op, action)
	}

	return "", nil
}
