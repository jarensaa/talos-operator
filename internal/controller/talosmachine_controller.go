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
	"net/netip"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/target"
)

// Machine phases set in observe mode.
const (
	machinePhasePending talosv1alpha1.MachinePhase = "Pending"
	machinePhaseReady   talosv1alpha1.MachinePhase = "Ready"
)

// Machine phases of a machine that is being added or removed, or is no member.
const (
	machinePhaseAwaitingIdentity talosv1alpha1.MachinePhase = "AwaitingIdentity"
	machinePhaseProvisioning     talosv1alpha1.MachinePhase = "Provisioning"
	machinePhaseJoining          talosv1alpha1.MachinePhase = "Joining"
	machinePhaseDraining         talosv1alpha1.MachinePhase = "Draining"
	machinePhaseRemoving         talosv1alpha1.MachinePhase = "Removing"
	machinePhaseRemoved          talosv1alpha1.MachinePhase = "Removed"
)

// Condition reasons specific to TalosMachine binding.
const (
	reasonIdentityMismatch = "IdentityMismatch"
	reasonRoleMismatch     = "RoleMismatch"
)

// TalosMachineReconciler binds TalosMachines to live nodes by hardware identity and reports
// their state. The operator never creates TalosMachines; they come from Git (docs/scope.md §7.1).
type TalosMachineReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Resolver *target.Resolver

	ObserveInterval time.Duration
}

// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachines,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosmachines/finalizers,verbs=update

// Reconcile observes one TalosMachine and updates its status.
func (r *TalosMachineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	tm := &talosv1alpha1.TalosMachine{}
	if err := r.Get(ctx, req.NamespacedName, tm); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	original := tm.DeepCopy()

	if err := r.observe(ctx, tm); err != nil {
		return ctrl.Result{}, err
	}

	if !equality.Semantic.DeepEqual(original.Status, tm.Status) {
		// A merge patch of what this pass changed. The cluster's rollout writes status.config's
		// appliedHash and skippedHash, which this controller never sets, so neither needs to
		// yield to the other and no conflict can arise.
		if err := r.Status().Patch(ctx, tm, client.MergeFrom(original)); err != nil {
			return ctrl.Result{}, fmt.Errorf("patch status: %w", err)
		}

		logTransitions(ctx, original.Status.Conditions, tm.Status.Conditions)
	}

	return ctrl.Result{RequeueAfter: r.interval()}, nil
}

// observe fills tm.Status. Only API errors of the management cluster are returned; problems
// with the Talos cluster are reported in conditions.
func (r *TalosMachineReconciler) observe(ctx context.Context, tm *talosv1alpha1.TalosMachine) error {
	st := &tm.Status
	gen := tm.Generation
	st.ObservedGeneration = gen

	if st.Phase == "" {
		st.Phase = machinePhasePending
	}

	// The pool decides the cluster and the role.
	pool := &talosv1alpha1.TalosMachinePool{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: tm.Namespace, Name: tm.Spec.PoolRef.Name}, pool); err != nil {
		if apierrors.IsNotFound(err) {
			r.unmatched(tm, metav1.ConditionFalse, talosv1alpha1.ReasonNotFound,
				fmt.Sprintf("TalosMachinePool %s not found", tm.Spec.PoolRef.Name))

			return nil
		}

		return fmt.Errorf("get pool: %w", err)
	}

	tc := &talosv1alpha1.TalosCluster{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: tm.Namespace, Name: pool.Spec.ClusterRef.Name}, tc); err != nil {
		if apierrors.IsNotFound(err) {
			r.unmatched(tm, metav1.ConditionFalse, talosv1alpha1.ReasonNotFound,
				fmt.Sprintf("TalosCluster %s of pool %s not found", pool.Spec.ClusterRef.Name, pool.Name))

			return nil
		}

		return fmt.Errorf("get cluster: %w", err)
	}

	if conflict, err := r.identityConflict(ctx, tm, tc.Name); err != nil {
		return err
	} else if conflict != "" {
		r.unmatched(tm, metav1.ConditionFalse, talosv1alpha1.ReasonConflict,
			fmt.Sprintf("TalosMachine %s claims the same systemUUID", conflict))

		return nil
	}

	// The same endpoints as the cluster's controller: never a node whose config is being changed.
	tgt, _, err := resolveOutside(ctx, r.Resolver, tc, operationEndpoints(ctx, r.Client, tc))
	if errors.Is(err, target.ErrKubernetesAPI) {
		// Nothing was learned about the cluster: the pass is retried, and status stays as it is.
		return err
	}

	if err != nil {
		r.unmatched(tm, metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable, "cluster not reachable: "+err.Error())

		return nil //nolint:nilerr // reported in the Matched condition
	}

	node, err := netip.ParseAddr(tm.Spec.Address)
	if err != nil {
		// CRD validation guarantees an IP; this only guards against bypassed validation.
		r.unmatched(tm, metav1.ConditionFalse, talosv1alpha1.ReasonParseError, "address is not an IP")

		return nil //nolint:nilerr // reported in the Matched condition
	}

	reads := newNodeReads(ctx, node)

	// Identity first: nothing else about a node is trusted until its hardware matches the spec.
	var si talos.SystemInformation

	if !reads.read("system information", func(ctx context.Context) (err error) {
		si, err = tgt.Talos.SystemInformation(ctx, node)

		return err
	}) {
		if !isMember(tm) {
			// Nothing answers as a node of this cluster, and none was expected to: the machine is
			// one that is to be added, or one that has left.
			return r.observeCandidate(ctx, tc, pool, tm, node)
		}

		r.unmatched(tm, metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable, reads.reason())

		return nil
	}

	if !strings.EqualFold(si.UUID, tm.Spec.Identity.SystemUUID) {
		r.unmatched(tm, metav1.ConditionFalse, reasonIdentityMismatch,
			fmt.Sprintf("node %s has systemUUID %s, not %s", node, si.UUID, tm.Spec.Identity.SystemUUID))

		return nil
	}

	var machineType string

	if !reads.read("machine type", func(ctx context.Context) (err error) {
		machineType, err = tgt.Talos.MachineType(ctx, node)

		return err
	}) {
		r.unmatched(tm, metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable, reads.reason())

		return nil
	}

	if want := roleMachineType(pool.Spec.Role); machineType != want {
		r.unmatched(tm, metav1.ConditionFalse, reasonRoleMismatch,
			fmt.Sprintf("node %s is a %s, but pool %s is %s", node, machineType, pool.Name, pool.Spec.Role))

		return nil
	}

	setMachineCondition(tm, talosv1alpha1.ConditionMatched, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
		fmt.Sprintf("bound to %s by systemUUID", node))

	// A node that answers with the cluster's credentials and carries the pinned identity is a
	// member, however it became one. That is recorded once and stays.
	if st.JoinedAt == nil {
		now := metav1.Now()
		st.JoinedAt = &now
	}

	meta.RemoveStatusCondition(&st.Conditions, talosv1alpha1.ConditionProvisionable)

	if err := r.observeNode(ctx, tgt, reads, tc, pool, tm, si); err != nil {
		return err
	}

	membershipPhase(tc, tm)

	return nil
}

// isMember reports whether a machine counts as a node of its cluster: it was found to be one, and
// has not been taken out since. A machine that is not is one to be added, or one that has left;
// no rollout, no upgrade and no proof waits for it (docs/scope.md §7.6).
func isMember(tm *talosv1alpha1.TalosMachine) bool {
	return tm.Status.JoinedAt != nil && tm.Status.RemovedAt == nil
}

// membershipEnabled reports whether the operator may add and remove machines of tc.
func membershipEnabled(tc *talosv1alpha1.TalosCluster) bool {
	return managing(tc) && tc.Spec.Management.Membership == talosv1alpha1.CapabilityEnabled
}

// observeCandidate reports on a machine that is no member of the cluster: whether it could be
// added. It asks the machine itself, over a maintenance connection, which carries none of the
// cluster's credentials and is only made while membership is enabled. Nothing is sent to the
// machine but questions; handing it its config is the cluster's operation (docs/scope.md §7.6).
func (r *TalosMachineReconciler) observeCandidate(ctx context.Context, tc *talosv1alpha1.TalosCluster, pool *talosv1alpha1.TalosMachinePool,
	tm *talosv1alpha1.TalosMachine, addr netip.Addr,
) error {
	const notAMember = "the machine is not a member of the cluster"

	r.unmatched(tm, metav1.ConditionFalse, talosv1alpha1.ReasonNotAMember, notAMember)
	setMachineCondition(tm, talosv1alpha1.ConditionConfigMatchesLive, metav1.ConditionUnknown, talosv1alpha1.ReasonNotAMember, notAMember)
	setMachineCondition(tm, talosv1alpha1.ConditionNodeObserved, metav1.ConditionFalse, talosv1alpha1.ReasonNotAMember, notAMember)

	if tm.Status.RemovedAt != nil {
		tm.Status.Phase = machinePhaseRemoved
	}

	defer membershipPhase(tc, tm)

	provisionable := func(status metav1.ConditionStatus, reason, format string, args ...any) error {
		setMachineCondition(tm, talosv1alpha1.ConditionProvisionable, status, reason, fmt.Sprintf(format, args...))

		return nil
	}

	switch {
	case tm.Spec.DesiredState == talosv1alpha1.DesiredStateRemoved:
		meta.RemoveStatusCondition(&tm.Status.Conditions, talosv1alpha1.ConditionProvisionable)

		return nil
	case !membershipEnabled(tc):
		return provisionable(metav1.ConditionFalse, talosv1alpha1.ReasonMembershipDisabled,
			"nothing is asked of a machine that is not a member while spec.management.membership is not Enabled")
	}

	ctx, cancel := context.WithTimeout(ctx, nodeReadTimeout)
	defer cancel()

	maintenance, err := r.Resolver.ResolveMaintenance(ctx, tc, addr)
	if err != nil {
		return provisionable(metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable, "no maintenance connection to %s: %s", addr, err)
	}

	defer maintenance.Close() //nolint:errcheck // nothing depends on how a connection that was only read from ends

	found, err := maintenance.Probe(ctx)

	switch {
	case err != nil:
		return provisionable(metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable,
			"nothing answers in maintenance mode at %s: boot the machine without a config", addr)
	case found.Stage != talos.StageMaintenance:
		return provisionable(metav1.ConditionFalse, talosv1alpha1.ReasonNotInMaintenance,
			"what answers at %s is in stage %q, not waiting in maintenance mode", addr, found.Stage)
	case !strings.EqualFold(found.UUID, tm.Spec.Identity.SystemUUID):
		tm.Status.Phase = machinePhaseAwaitingIdentity

		// The config carries every secret of the cluster, and this machine is not the one it was
		// meant for.
		return provisionable(metav1.ConditionFalse, talosv1alpha1.ReasonIdentityMismatch,
			"the machine in maintenance mode at %s has systemUUID %s, not %s; it is sent nothing", addr, found.UUID, tm.Spec.Identity.SystemUUID)
	}

	rendered, _, err := renderMachine(ctx, r.Client, tc, pool, tm)
	if err != nil {
		var problem *configProblem
		if !errors.As(err, &problem) {
			return err
		}

		return provisionable(problem.status, problem.reason, "%s", problem.msg)
	}

	problems, err := maintenance.Fits(ctx, rendered.Bytes)

	switch {
	case err != nil:
		return provisionable(metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable,
			"whether the rendered config fits the machine at %s could not be told: %s", addr, err)
	case len(problems) > 0:
		return provisionable(metav1.ConditionFalse, talosv1alpha1.ReasonConfigDoesNotFit, "%s", strings.Join(problems, "; "))
	}

	return provisionable(metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
		"waits in maintenance mode at %s with the pinned identity, and the rendered config fits it", addr)
}

// membershipPhase shows the machine's part in the cluster's operation, while it is the one being
// added or removed.
func membershipPhase(tc *talosv1alpha1.TalosCluster, tm *talosv1alpha1.TalosMachine) {
	op := tc.Status.Operation
	if op == nil || op.Current == nil || op.Current.Machine != tm.Name {
		return
	}

	switch op.Type { //nolint:exhaustive // the other operations leave the phase to what the node says
	case talosv1alpha1.OperationAddMachine:
		tm.Status.Phase = machinePhaseJoining

		if op.Current.Step == plan.StepProvisioning {
			tm.Status.Phase = machinePhaseProvisioning
		}
	case talosv1alpha1.OperationRemoveMachine:
		tm.Status.Phase = machinePhaseRemoving

		if op.Current.Step == plan.StepDraining {
			tm.Status.Phase = machinePhaseDraining
		}
	}
}

// observeNode records the bound node's state.
//
// A read that fails is logged and named in the NodeObserved condition. What it would have set is
// then one of two things. Facts that change under the operator's hands (the stage, readiness,
// the boot ID, the live config hash) are cleared: an old value would look like a current one,
// and a boot ID that survives a failed read would say that no reboot happened. Everything else
// keeps the last value read, which is more use than nothing while the node is away.
func (r *TalosMachineReconciler) observeNode(ctx context.Context, tgt *target.Target, reads *nodeReads,
	tc *talosv1alpha1.TalosCluster, pool *talosv1alpha1.TalosMachinePool, tm *talosv1alpha1.TalosMachine, si talos.SystemInformation,
) error {
	st := &tm.Status
	node := reads.node

	identity := &talosv1alpha1.ObservedIdentity{SystemUUID: si.UUID}
	if st.Identity != nil {
		*identity = *st.Identity
		identity.SystemUUID = si.UUID
	}

	reads.read("members", func(ctx context.Context) error {
		members, err := tgt.Talos.Members(ctx, node)

		for _, m := range members {
			if containsAddr(m.Addresses, node) {
				identity.NodeID = m.NodeID
				identity.Hostname = m.Hostname
			}
		}

		return err
	})

	talosState := &talosv1alpha1.TalosState{}
	if st.Talos != nil {
		talosState.Version, talosState.SchematicID = st.Talos.Version, st.Talos.SchematicID
	}

	reads.read("version", func(ctx context.Context) error {
		v, err := tgt.Talos.Version(ctx, node)
		if err == nil {
			talosState.Version = v.Tag
		}

		return err
	})

	reads.read("boot ID", func(ctx context.Context) (err error) {
		talosState.BootID, err = tgt.Talos.BootID(ctx, node)

		return err
	})

	reads.read("schematic", func(ctx context.Context) error {
		sc, err := tgt.Talos.ImageFactorySchematic(ctx, node)

		// A node that was not installed from Image Factory has none: that is an answer.
		if err == nil || errors.Is(err, talos.ErrNotFound) {
			talosState.SchematicID = sc.ID

			return nil
		}

		return err
	})

	reads.read("machine status", func(ctx context.Context) error {
		ms, err := tgt.Talos.MachineStatus(ctx, node)
		talosState.Stage, talosState.Ready = ms.Stage, ms.Ready

		return err
	})

	st.Talos = talosState

	var nodename string

	if reads.read("nodename", func(ctx context.Context) (err error) {
		nodename, err = tgt.Talos.Nodename(ctx, node)

		return err
	}) {
		identity.KubernetesNodeName = nodename

		kubelet, err := kubeletVersion(ctx, tgt.Kube, nodename)
		if err != nil {
			return err
		}

		if kubelet != "" {
			st.Kubelet = &talosv1alpha1.KubeletState{Version: kubelet}
		}
	}

	st.Identity = identity

	var cfgs talos.MachineConfigs

	if reads.read("machine config", func(ctx context.Context) (err error) {
		cfgs, err = tgt.Talos.MachineConfig(ctx, node)

		return err
	}) {
		if err := r.observeConfig(ctx, tc, pool, tm, cfgs.Active); err != nil {
			return err
		}
	} else {
		forgetLiveConfig(tm)
		setMachineCondition(tm, talosv1alpha1.ConditionConfigMatchesLive, metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable,
			"the node's machine config could not be read")
	}

	if pool.Spec.Role == talosv1alpha1.MachineRoleControlPlane {
		if !reads.read("etcd status", func(ctx context.Context) error {
			es, err := tgt.Talos.EtcdStatus(ctx, node)
			if err == nil {
				etcd := &talosv1alpha1.EtcdState{
					MemberID:  fmt.Sprintf("%x", es.MemberID),
					IsLearner: es.IsLearner,
					Leader:    es.Leader != 0 && es.Leader == es.MemberID,
					DBSize:    es.DBSize, DBSizeInUse: es.DBSizeInUse,
				}

				if was := st.Etcd; was != nil {
					etcd.DBSize = followedSize(was.DBSize, es.DBSize, es.DBSize)
					etcd.DBSizeInUse = followedSize(was.DBSizeInUse, es.DBSizeInUse, es.DBSize)
				}

				st.Etcd = etcd
			}

			return err
		}) {
			forgetEtcdMoment(tm)
		}
	}

	st.Health = observeHealth(tgt.Talos, reads)

	if reads.ok() {
		setMachineCondition(tm, talosv1alpha1.ConditionNodeObserved, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			"the node's state was read in full")
	} else {
		setMachineCondition(tm, talosv1alpha1.ConditionNodeObserved, metav1.ConditionFalse, talosv1alpha1.ReasonUnreachable,
			"not read in the last pass: "+reads.missing())
	}

	// Only what the node said in this pass makes a machine Ready.
	if talosState.Stage == stageRunning && talosState.Ready {
		st.Phase = machinePhaseReady
	} else {
		st.Phase = machinePhasePending
	}

	return nil
}

// forgetLiveConfig drops what status says about the node's live config, when it could not be
// read: a hash that is no longer known to be the node's must not look like one.
func forgetLiveConfig(tm *talosv1alpha1.TalosMachine) {
	if cfg := tm.Status.Config; cfg != nil {
		cfg.LiveHash, cfg.Diff, cfg.ChangeClass, cfg.ChangeReasons = "", "", "", nil
	}
}

// forgetEtcdMoment drops what status says about the member's etcd as it was when last read. Which
// member the machine is stays: that does not change while nobody looks.
func forgetEtcdMoment(tm *talosv1alpha1.TalosMachine) {
	if etcd := tm.Status.Etcd; etcd != nil {
		etcd.Leader, etcd.DBSize, etcd.DBSizeInUse = false, 0, 0
	}
}

// identityConflict returns the name of another TalosMachine of the same cluster claiming the
// same systemUUID, or "".
func (r *TalosMachineReconciler) identityConflict(ctx context.Context, tm *talosv1alpha1.TalosMachine, cluster string) (string, error) {
	siblings, err := machinesOf(ctx, r.Client, tm.Namespace, cluster)
	if err != nil {
		return "", err
	}

	for _, other := range siblings {
		if other.Name != tm.Name && strings.EqualFold(other.Spec.Identity.SystemUUID, tm.Spec.Identity.SystemUUID) {
			return other.Name, nil
		}
	}

	return "", nil
}

// unmatched records that the machine is not bound to a node, so nothing was read from one. What
// was known of the node's identity and version stays visible; what only ever describes a moment
// (stage, readiness, boot ID, live config) is cleared, as in observeNode.
func (r *TalosMachineReconciler) unmatched(tm *talosv1alpha1.TalosMachine, status metav1.ConditionStatus, reason, message string) {
	const notMatched = "the machine is not matched to a live node"

	setMachineCondition(tm, talosv1alpha1.ConditionMatched, status, reason, message)
	// A config comparison is only meaningful for the node the machine is bound to.
	for condType, status := range map[string]metav1.ConditionStatus{
		talosv1alpha1.ConditionConfigMatchesLive: metav1.ConditionUnknown,
		talosv1alpha1.ConditionNodeObserved:      metav1.ConditionFalse,
	} {
		setMachineCondition(tm, condType, status, talosv1alpha1.ReasonPreconditionUnmet, notMatched)
	}

	if state := tm.Status.Talos; state != nil {
		state.Stage, state.Ready, state.BootID = "", false, ""
	}

	forgetLiveConfig(tm)
	forgetEtcdMoment(tm)

	tm.Status.Health = nil
	tm.Status.Phase = machinePhasePending
}

func setMachineCondition(tm *talosv1alpha1.TalosMachine, condType string, status metav1.ConditionStatus, reason, message string) {
	setConditionIn(&tm.Status.Conditions, tm.Generation, condType, status, reason, message)
}

func roleMachineType(role talosv1alpha1.MachineRole) string {
	if role == talosv1alpha1.MachineRoleControlPlane {
		return machineTypeControlPlane
	}

	return machineTypeWorker
}

// kubeletVersion returns the kubelet version Kubernetes reports for node name, without the
// leading "v"; empty if the Node does not exist.
func kubeletVersion(ctx context.Context, c client.Client, name string) (string, error) {
	node := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}

		return "", fmt.Errorf("get node %s: %w", name, err)
	}

	return strings.TrimPrefix(node.Status.NodeInfo.KubeletVersion, "v"), nil
}

func containsAddr(addrs []netip.Addr, addr netip.Addr) bool {
	for _, a := range addrs {
		if a == addr {
			return true
		}
	}

	return false
}

func (r *TalosMachineReconciler) interval() time.Duration {
	if r.ObserveInterval > 0 {
		return r.ObserveInterval
	}

	return DefaultObserveInterval
}

// membersOf returns a cluster's pools by name and the machines in them. Clusters have a handful of
// each, so filtering namespace lists works with any client, cached or direct, without an index.
func membersOf(ctx context.Context, c client.Reader, namespace, cluster string) (map[string]*talosv1alpha1.TalosMachinePool,
	[]talosv1alpha1.TalosMachine, error,
) {
	var pools talosv1alpha1.TalosMachinePoolList
	if err := c.List(ctx, &pools, client.InNamespace(namespace)); err != nil {
		return nil, nil, fmt.Errorf("list pools: %w", err)
	}

	byName := map[string]*talosv1alpha1.TalosMachinePool{}

	for i := range pools.Items {
		if pools.Items[i].Spec.ClusterRef.Name == cluster {
			byName[pools.Items[i].Name] = &pools.Items[i]
		}
	}

	var list talosv1alpha1.TalosMachineList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, nil, fmt.Errorf("list machines: %w", err)
	}

	machines := make([]talosv1alpha1.TalosMachine, 0, len(list.Items))
	for _, tm := range list.Items {
		if byName[tm.Spec.PoolRef.Name] != nil {
			machines = append(machines, tm)
		}
	}

	return byName, machines, nil
}

// machinesOf lists the TalosMachines of one cluster: the members of its pools.
func machinesOf(ctx context.Context, c client.Reader, namespace, cluster string) ([]talosv1alpha1.TalosMachine, error) {
	_, machines, err := membersOf(ctx, c, namespace, cluster)

	return machines, err
}

// machinesOfPool lists a pool's members.
func machinesOfPool(ctx context.Context, c client.Reader, namespace, pool string) ([]talosv1alpha1.TalosMachine, error) {
	var list talosv1alpha1.TalosMachineList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list machines: %w", err)
	}

	return slices.DeleteFunc(list.Items, func(tm talosv1alpha1.TalosMachine) bool { return tm.Spec.PoolRef.Name != pool }), nil
}

// machinesForPool re-reconciles a pool's members when the pool changes; on a membership change
// the old and the new pool are both mapped.
func machinesForPool(c client.Reader) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		machines, err := machinesOfPool(ctx, c, obj.GetNamespace(), obj.GetName())
		if err != nil {
			mapFailed(ctx, err, "machines of the pool", obj)

			return nil
		}

		return requestsFor(machines)
	}
}

func requestsFor(machines []talosv1alpha1.TalosMachine) []reconcile.Request {
	reqs := make([]reconcile.Request, 0, len(machines))
	for i := range machines {
		reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&machines[i])})
	}

	return reqs
}

// machinesForCluster re-reconciles a cluster's machines when the cluster changes, e.g. when
// its credentials are fixed.
func machinesForCluster(c client.Reader) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		machines, err := machinesOf(ctx, c, obj.GetNamespace(), obj.GetName())
		if err != nil {
			mapFailed(ctx, err, "machines of the cluster", obj)

			return nil
		}

		return requestsFor(machines)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *TalosMachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&talosv1alpha1.TalosMachine{}).
		Watches(&talosv1alpha1.TalosCluster{}, handler.EnqueueRequestsFromMapFunc(machinesForCluster(mgr.GetClient()))).
		Watches(&talosv1alpha1.TalosMachinePool{}, handler.EnqueueRequestsFromMapFunc(machinesForPool(mgr.GetClient()))).
		Watches(&talosv1alpha1.TalosMachineConfig{}, handler.EnqueueRequestsFromMapFunc(machinesForObject(mgr.GetClient()))).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(machinesForObject(mgr.GetClient()))).
		Named("talosmachine").
		Complete(r)
}
