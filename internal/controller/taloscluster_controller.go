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
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/siderolabs/talos/pkg/machinery/role"

	"github.com/jarensaa/talos-operator/internal/plan"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
	"github.com/jarensaa/talos-operator/internal/target"
)

const (
	// DefaultObserveInterval is how often a cluster is re-observed when nothing triggers a reconcile.
	DefaultObserveInterval = time.Minute
	// DefaultHealthTimeout bounds the server-side health check per reconcile.
	DefaultHealthTimeout = 30 * time.Second

	machineTypeControlPlane = "controlplane"
	machineTypeWorker       = "worker"

	// stageRunning is the Talos machine stage of a booted, configured node.
	stageRunning = plan.StageRunning
)

// TalosClusterReconciler observes Talos clusters and reports their state. In observe mode it
// never writes to Talos: it only holds a talos.Reader (docs/scope.md §7.1 stage 1).
type TalosClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Resolver *target.Resolver
	Recorder events.EventRecorder

	ObserveInterval time.Duration
	HealthTimeout   time.Duration
	// ObserveBudget bounds the reading of all nodes in one pass; zero means DefaultObserveBudget.
	ObserveBudget time.Duration
	// Now is the clock for status timestamps; nil means time.Now.
	Now func() time.Time
	// APIReader reads objects outside the namespaces the manager caches: the node leases a
	// rollout verifies heartbeats with. Nil means Client.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=talos.jarensaa.github.io,resources=talosclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile observes one TalosCluster and updates its status.
func (r *TalosClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	tc := &talosv1alpha1.TalosCluster{}
	if err := r.Get(ctx, req.NamespacedName, tc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	original := tc.DeepCopy()

	var obs observation

	if err := r.requestCredentials(ctx, tc); err != nil {
		var credsErr *creds.Error
		if !errors.As(err, &credsErr) {
			return ctrl.Result{}, err
		}

		obs.credsErr = err
	} else {
		obs = r.observe(ctx, tc, operationEndpoints(ctx, r.Client, tc))
	}

	r.applyObservation(tc, obs)

	var adoptErr error
	if proposing(tc, obs) {
		adoptErr = r.propose(ctx, tc, obs.facts)
	}

	if adoptErr == nil && proving(tc, obs) {
		adoptErr = r.prove(ctx, tc, obs)
	}

	if adoptErr == nil && managing(tc) {
		adoptErr = r.manage(ctx, tc, obs)
	}

	if !equality.Semantic.DeepEqual(original.Status, tc.Status) {
		// The status holds the operation lock and the step in flight, so it is only ever written
		// on top of the version it was decided from.
		patch := client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})

		if err := r.Status().Patch(ctx, tc, patch); err != nil {
			// The object changed since it was read; that change already queued a new reconcile.
			if apierrors.IsConflict(err) {
				return ctrl.Result{}, nil
			}

			return ctrl.Result{}, fmt.Errorf("patch status: %w", err)
		}

		logTransitions(ctx, original.Status.Conditions, tc.Status.Conditions)
		r.emitTransitions(ctx, original, tc)
	}

	if obs.err != nil {
		log.Info("observation incomplete", "reason", obs.errReason, "error", obs.err.Error())
	}

	if adoptErr != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile %s: %w", tc.Name, adoptErr)
	}

	// Not a fact about the Talos cluster: nothing in status says so, and the pass is retried.
	if obs.kubeErr != nil {
		return ctrl.Result{}, fmt.Errorf("observe %s: Kubernetes API: %w", tc.Name, obs.kubeErr)
	}

	// A node step in flight is driven by time (the try window, the soak), so it is re-evaluated
	// often rather than on the observe interval.
	if op := tc.Status.Operation; op != nil && op.Failure == nil {
		return ctrl.Result{RequeueAfter: min(r.interval(), rolloutInterval)}, nil
	}

	return ctrl.Result{RequeueAfter: r.interval()}, nil
}

// proving reports whether the adoption proof is evaluated: the cluster is observed only and was
// reached. The proof's own gate decides whether it can succeed (docs/scope.md §7.1 stage 3).
func proving(tc *talosv1alpha1.TalosCluster, obs observation) bool {
	observeOnly := tc.Spec.Management.Config == talosv1alpha1.ConfigManagementObserve || tc.Spec.Management.Config == ""

	return observeOnly && obs.facts != nil && obs.reader != nil
}

// proposing reports whether an adoption proposal is due: the cluster is observed only and every
// adoption precondition holds (docs/scope.md §7.1 stage 1).
func proposing(tc *talosv1alpha1.TalosCluster, obs observation) bool {
	observeOnly := tc.Spec.Management.Config == talosv1alpha1.ConfigManagementObserve || tc.Spec.Management.Config == ""

	return observeOnly && obs.facts != nil &&
		meta.IsStatusConditionTrue(tc.Status.Conditions, talosv1alpha1.ConditionAdoptionPreconditionsMet)
}

// observation is one pass over the live cluster. A failed step leaves later fields unset and
// records why; conditions are derived from what was reached.
type observation struct {
	credentials *creds.Credentials
	credsErr    error

	reachable bool
	err       error
	errReason string
	// kubeErr is a failure of the Kubernetes API the operator itself runs against. It says nothing
	// about Talos: the reconcile fails with it and is retried, and until then nothing that
	// depends on what was not read is decided.
	kubeErr error

	controlPlanes []netip.Addr
	workers       []netip.Addr
	talosVersions map[string]int32
	kubernetes    string
	etcdMembers   []talosv1alpha1.EtcdMember
	// unmatched lists hostnames of live members no TalosMachine claims; nil if not determined.
	unmatched []string
	// facts feed the adoption preconditions; nil if the cluster was not reached.
	facts *clusterFacts
	// reader is the cluster's Talos client; nil if the cluster was not reached.
	reader talos.Reader
	// endpoints is how the reader's endpoints were selected; a write client uses the same.
	endpoints      talos.SelectOptions
	health         *talos.HealthResult
	healthErrorMsg string
}

func (r *TalosClusterReconciler) observe(ctx context.Context, tc *talosv1alpha1.TalosCluster, opts talos.SelectOptions) observation {
	var obs observation

	tgt, opts, err := resolveOutside(ctx, r.Resolver, tc, opts)
	obs.endpoints = opts

	if err != nil {
		var credsErr *creds.Error

		switch {
		case errors.As(err, &credsErr):
			obs.credsErr = err
		case errors.Is(err, target.ErrKubernetesAPI):
			obs.kubeErr = err
		default:
			obs.err, obs.errReason = err, talosv1alpha1.ReasonUnreachable
		}

		return obs
	}

	obs.credentials = tgt.Credentials
	endpoint := tgt.Endpoints[0].Addr

	members, err := tgt.Talos.Members(ctx, endpoint)
	if err != nil {
		obs.err, obs.errReason = err, talosv1alpha1.ReasonUnreachable

		return obs
	}

	obs.reachable = true
	obs.reader = tgt.Talos
	obs.talosVersions = map[string]int32{}
	obs.facts = &clusterFacts{credentialsAdmin: tgt.Credentials.Roles.IncludesAny(role.MakeSet(role.Admin))}

	// One budget for reading every node, so that a pass over a cluster in trouble still ends.
	nodesCtx, cancel := context.WithTimeout(ctx, r.observeBudget())
	defer cancel()

	// etcdVia is a control plane that answered, to ask for the etcd member list.
	var etcdVia netip.Addr

	for _, m := range members {
		if len(m.Addresses) == 0 {
			continue
		}

		addr := m.Addresses[0]

		switch m.MachineType {
		case machineTypeControlPlane:
			obs.controlPlanes = append(obs.controlPlanes, addr)
		case machineTypeWorker:
			obs.workers = append(obs.workers, addr)
		}

		facts := observeNodeFacts(nodesCtx, tgt.Talos, m, addr)
		obs.facts.nodes = append(obs.facts.nodes, facts)

		if facts.version == "" {
			obs.err, obs.errReason = fmt.Errorf("%s (%s): %s", m.Hostname, addr, facts.readErr), talosv1alpha1.ReasonUnreachable

			continue
		}

		obs.talosVersions[facts.version]++

		if m.MachineType == machineTypeControlPlane && facts.read && !etcdVia.IsValid() {
			etcdVia = addr
		}
	}

	slices.SortFunc(obs.controlPlanes, netip.Addr.Compare)
	slices.SortFunc(obs.workers, netip.Addr.Compare)

	r.observeUnmatched(ctx, tc, &obs)

	if etcdVia.IsValid() {
		r.observeEtcd(nodesCtx, tgt, etcdVia, &obs)
	}

	var nodes corev1.NodeList
	if err := tgt.Kube.List(ctx, &nodes); err != nil {
		obs.kubeErr = fmt.Errorf("list nodes: %w", err)
	} else {
		obs.kubernetes = lowestKubeletVersion(nodes.Items)
		obs.facts.observeKubeNodes(nodes.Items)
	}

	health, err := tgt.Talos.HealthCheck(ctx, endpoint,
		talos.ClusterInfo{ControlPlanes: obs.controlPlanes, Workers: obs.workers}, r.healthTimeout())
	if err != nil {
		obs.healthErrorMsg = err.Error()
	} else {
		obs.health = &health
	}

	return obs
}

func (r *TalosClusterReconciler) observeEtcd(ctx context.Context, tgt *target.Target, node netip.Addr, obs *observation) {
	var members []talos.EtcdMember

	reads := newNodeReads(ctx, node)
	if !reads.read("etcd members", func(ctx context.Context) (err error) {
		members, err = tgt.Talos.EtcdMembers(ctx, node)

		return err
	}) {
		obs.err, obs.errReason = reads.firstErr, talosv1alpha1.ReasonUnreachable

		return
	}

	obs.facts.etcdMembers = members

	for _, m := range members {
		obs.etcdMembers = append(obs.etcdMembers, talosv1alpha1.EtcdMember{
			ID:        fmt.Sprintf("%x", m.ID),
			Hostname:  m.Hostname,
			IsLearner: m.IsLearner,
		})
	}

	slices.SortFunc(obs.etcdMembers, func(a, b talosv1alpha1.EtcdMember) int { return strings.Compare(a.ID, b.ID) })
}

// observeNodeFacts reads what the gates need from one member. A read that fails leaves its fact
// unknown, never false, and is logged. A node that does not answer is not asked again in this
// pass (see nodeReads).
func observeNodeFacts(ctx context.Context, r talos.Reader, m talos.Member, addr netip.Addr) nodeFacts {
	facts := nodeFacts{hostname: m.Hostname, address: addr, machineType: m.MachineType}

	if ctx.Err() != nil {
		facts.readErr = "not read: the observation of the cluster ran out of time"

		return facts
	}

	reads := newNodeReads(ctx, addr)

	reads.read("system information", func(ctx context.Context) error {
		si, err := r.SystemInformation(ctx, addr)
		facts.systemUUID = si.UUID

		return err
	})

	reads.read("version", func(ctx context.Context) error {
		v, err := r.Version(ctx, addr)
		facts.version = v.Tag

		return err
	})

	reads.read("machine status", func(ctx context.Context) error {
		ms, err := r.MachineStatus(ctx, addr)
		if err == nil {
			facts.status = &ms
		}

		return err
	})

	reads.read("schematic", func(ctx context.Context) error {
		sc, err := r.ImageFactorySchematic(ctx, addr)

		// A node that was not installed from Image Factory has none: that is an answer.
		if err == nil || errors.Is(err, talos.ErrNotFound) {
			facts.schematicID = &sc.ID

			return nil
		}

		return err
	})

	reads.read("machine config", func(ctx context.Context) error {
		cfgs, err := r.MachineConfig(ctx, addr)
		if err == nil {
			settled := !cfgs.PendingConfigChange()
			facts.configSettled = &settled
			facts.activeConfig = cfgs.Active
			facts.persistentConfig = cfgs.Persistent
		}

		return err
	})

	if m.MachineType == machineTypeControlPlane {
		reads.read("etcd status", func(ctx context.Context) error {
			es, err := r.EtcdStatus(ctx, addr)
			if err == nil {
				facts.etcdErrors = es.Errors
				facts.etcdStatusRead = true
			}

			return err
		})
	}

	facts.read = reads.ok()
	facts.readErr = reads.reason()

	return facts
}

// observeUnmatched finds live members whose hardware identity no TalosMachine claims, so the
// user knows which TalosMachines are still missing during adoption (docs/scope.md §7.1 stage 2).
func (r *TalosClusterReconciler) observeUnmatched(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs *observation) {
	machines, err := machinesOf(ctx, r.Client, tc.Namespace, tc.Name)
	if err != nil {
		obs.kubeErr = err

		return
	}

	claimed := map[string]bool{}
	for _, tm := range machines {
		claimed[strings.ToLower(tm.Spec.Identity.SystemUUID)] = true
	}

	unmatched := []string{}

	for _, n := range obs.facts.nodes {
		if n.systemUUID == "" {
			// Unreadable identity: the node cannot be reported as matched or unmatched.
			return
		}

		if !claimed[strings.ToLower(n.systemUUID)] {
			unmatched = append(unmatched, n.hostname)
		}
	}

	slices.Sort(unmatched)
	obs.unmatched = unmatched
}

// lowestKubeletVersion returns the lowest kubelet version across Kubernetes nodes, without the
// leading "v"; empty when no node reports one.
func lowestKubeletVersion(nodes []corev1.Node) string {
	var lowest *version.Version

	for _, n := range nodes {
		v, err := version.ParseSemantic(n.Status.NodeInfo.KubeletVersion)
		if err != nil {
			continue
		}

		if lowest == nil || v.LessThan(lowest) {
			lowest = v
		}
	}

	if lowest == nil {
		return ""
	}

	return lowest.String()
}

// applyObservation updates tc.Status from obs. Fields that were not observed keep their
// previous value, so a transient failure does not erase what is known.
func (r *TalosClusterReconciler) applyObservation(tc *talosv1alpha1.TalosCluster, obs observation) {
	st := &tc.Status
	gen := tc.Generation

	st.ObservedGeneration = gen

	if tc.Spec.Management.Config == talosv1alpha1.ConfigManagementObserve || tc.Spec.Management.Config == "" {
		st.Phase = talosv1alpha1.ClusterPhaseObserving
		forgetHandover(st)
		r.abandonOperation(tc)
	}

	if obs.credsErr != nil {
		setCondition(st, gen, talosv1alpha1.ConditionCredentialsValid, metav1.ConditionFalse, creds.Reason(obs.credsErr), obs.credsErr.Error())
		setCondition(st, gen, talosv1alpha1.ConditionTalosAPIReachable, metav1.ConditionUnknown, talosv1alpha1.ReasonPreconditionUnmet,
			"credentials are not usable")

		return
	}

	if obs.credentials != nil {
		setCondition(st, gen, talosv1alpha1.ConditionCredentialsValid, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			fmt.Sprintf("client certificate with roles %s valid until %s",
				strings.Join(obs.credentials.Roles.Strings(), ","), obs.credentials.NotAfter.UTC().Format(time.RFC3339)))
	}

	if !obs.reachable {
		msg := "Talos API not reachable"
		if obs.err != nil {
			msg = obs.err.Error()
		}

		setCondition(st, gen, talosv1alpha1.ConditionTalosAPIReachable, metav1.ConditionFalse, obs.errReason, msg)

		return
	}

	if obs.err != nil {
		setCondition(st, gen, talosv1alpha1.ConditionTalosAPIReachable, metav1.ConditionFalse, obs.errReason, obs.err.Error())
	} else {
		setCondition(st, gen, talosv1alpha1.ConditionTalosAPIReachable, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded,
			fmt.Sprintf("%d control plane(s), %d worker(s)", len(obs.controlPlanes), len(obs.workers)))
	}

	switch {
	case obs.health != nil && obs.health.Healthy:
		setCondition(st, gen, talosv1alpha1.ConditionHealthy, metav1.ConditionTrue, talosv1alpha1.ReasonSucceeded, "all Talos health checks pass")
	case obs.health != nil:
		setCondition(st, gen, talosv1alpha1.ConditionHealthy, metav1.ConditionFalse, talosv1alpha1.ReasonFailed, lastMessage(obs.health.Messages))
	default:
		setCondition(st, gen, talosv1alpha1.ConditionHealthy, metav1.ConditionUnknown, talosv1alpha1.ReasonUnreachable, obs.healthErrorMsg)
	}

	if obs.facts != nil {
		if st.Adoption == nil {
			st.Adoption = &talosv1alpha1.AdoptionStatus{}
		}

		st.Adoption.Preconditions = plan.Preconditions(obs.facts.judged())
		status, message := plan.PreconditionsMet(st.Adoption.Preconditions)

		reason := talosv1alpha1.ReasonPreconditionUnmet
		if status == metav1.ConditionTrue {
			reason = talosv1alpha1.ReasonSucceeded
		}

		setCondition(st, gen, talosv1alpha1.ConditionAdoptionPreconditionsMet, status, reason, message)
	}

	if obs.unmatched != nil {
		if st.Adoption == nil {
			st.Adoption = &talosv1alpha1.AdoptionStatus{}
		}

		st.Adoption.UnmatchedMembers = nil
		if len(obs.unmatched) > 0 {
			st.Adoption.UnmatchedMembers = obs.unmatched
		}
	}

	observed := &talosv1alpha1.ObservedState{
		TalosVersions:     obs.talosVersions,
		KubernetesVersion: obs.kubernetes,
		EtcdMembers:       obs.etcdMembers,
	}

	// What the Kubernetes API did not say keeps its last known value.
	if obs.kubeErr != nil && st.Observed != nil {
		observed.KubernetesVersion = st.Observed.KubernetesVersion
	}

	// ObservedAt marks when the observation last changed, so an unchanged cluster produces no
	// status writes at all.
	if st.Observed != nil {
		observed.ObservedAt = st.Observed.ObservedAt

		previous := st.Observed.DeepCopy()
		previous.ObservedAt = nil

		probe := observed.DeepCopy()
		probe.ObservedAt = nil

		if equality.Semantic.DeepEqual(previous, probe) {
			return
		}
	}

	now := metav1.NewTime(r.now())
	observed.ObservedAt = &now
	st.Observed = observed
}

// emitTransitions records an Event for every condition whose status changed, and for every
// change of the operation in flight. Events are derived from the status that was just written,
// so they never announce something that was not persisted.
func (r *TalosClusterReconciler) emitTransitions(ctx context.Context, before, after *talosv1alpha1.TalosCluster) {
	for _, c := range after.Status.Conditions {
		if r.Recorder == nil {
			break
		}

		prev := meta.FindStatusCondition(before.Status.Conditions, c.Type)
		if prev != nil && prev.Status == c.Status {
			continue
		}

		// Every condition is good news when True, except the one that reports a halt.
		good := metav1.ConditionTrue
		if c.Type == talosv1alpha1.ConditionRolloutBlocked {
			good = metav1.ConditionFalse
		}

		eventType := corev1.EventTypeNormal
		if c.Status != good {
			eventType = corev1.EventTypeWarning
		}

		r.Recorder.Eventf(after, nil, eventType, c.Type+string(c.Status), "Observe", "%s is %s: %s", c.Type, c.Status, c.Message)
	}

	r.emitOperation(ctx, before, after)
}

const eventActionRollout = "Rollout"

// emitOperation records the life of an operation: started, each machine done or skipped,
// halted, resumed, ended.
func (r *TalosClusterReconciler) emitOperation(ctx context.Context, before, after *talosv1alpha1.TalosCluster) {
	log := logf.FromContext(ctx)

	// Every event is also a log line: Events expire, and the log is what is left of a rollout.
	emit := func(eventType, reason, format string, args ...any) {
		log.Info(fmt.Sprintf(format, args...), "event", reason)

		if r.Recorder != nil {
			r.Recorder.Eventf(after, nil, eventType, reason, eventActionRollout, format, args...)
		}
	}
	normal := func(reason, format string, args ...any) { emit(corev1.EventTypeNormal, reason, format, args...) }
	warning := func(reason, format string, args ...any) { emit(corev1.EventTypeWarning, reason, format, args...) }

	was, is := before.Status.Operation, after.Status.Operation
	if was != nil && is != nil && was.ID != is.ID {
		was = nil
	}

	if is != nil {
		if was == nil {
			was = &talosv1alpha1.Operation{}

			normal("RolloutStarted", "%s %s started for %s", is.Type, is.ID, strings.Join(is.Plan, ", "))
		}

		for _, name := range is.Completed {
			if !slices.Contains(was.Completed, name) {
				normal("MachineConfigured", "%s runs its rendered config, confirmed after staying healthy", name)
			}
		}

		for _, name := range is.Skipped {
			if !slices.Contains(was.Skipped, name) {
				warning("MachineSkipped", "%s was left out of %s %s", name, is.Type, is.ID)
			}
		}

		switch {
		case is.Failure != nil && (was.Failure == nil || was.Failure.Token != is.Failure.Token):
			warning("RolloutHalted", "%s, step %s (%s): %s %s",
				is.Failure.Machine, is.Failure.Step, is.Failure.Reason, is.Failure.Message, is.Failure.Remediation)
		case is.Failure == nil && was.Failure != nil:
			normal("RolloutResumed", "%s %s continues after %s", is.Type, is.ID, was.Failure.Reason)
		}

		return
	}

	// The operation ended: its record is the newest in the history.
	if was == nil || len(after.Status.History) == 0 || after.Status.History[0].ID != was.ID {
		return
	}

	record := after.Status.History[0]
	if record.Outcome == talosv1alpha1.OperationSucceeded {
		normal("Rollout"+record.Outcome, "%s %s: %d machine(s) configured, %d skipped",
			record.Type, record.ID, len(record.Completed), len(record.Skipped))

		return
	}

	warning("Rollout"+record.Outcome, "%s %s: %s", record.Type, record.ID, record.Message)
}

func setCondition(st *talosv1alpha1.TalosClusterStatus, gen int64, condType string, status metav1.ConditionStatus, reason, message string) {
	setConditionIn(&st.Conditions, gen, condType, status, reason, message)
}

// setConditionIn sets a condition; LastTransitionTime only moves when the status changes.
func setConditionIn(conds *[]metav1.Condition, gen int64, condType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
	})
}

func lastMessage(msgs []string) string {
	if len(msgs) == 0 {
		return "health check failed"
	}

	return msgs[len(msgs)-1]
}

func (r *TalosClusterReconciler) interval() time.Duration {
	if r.ObserveInterval > 0 {
		return r.ObserveInterval
	}

	return DefaultObserveInterval
}

func (r *TalosClusterReconciler) observeBudget() time.Duration {
	if r.ObserveBudget > 0 {
		return r.ObserveBudget
	}

	return DefaultObserveBudget
}

func (r *TalosClusterReconciler) healthTimeout() time.Duration {
	if r.HealthTimeout > 0 {
		return r.HealthTimeout
	}

	return DefaultHealthTimeout
}

func (r *TalosClusterReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

// clusterOfMachine re-reconciles a machine's cluster, found through its pool, e.g. to refresh
// unmatched members or re-evaluate the adoption proof.
func clusterOfMachine(c client.Reader) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		tm, ok := obj.(*talosv1alpha1.TalosMachine)
		if !ok {
			return nil
		}

		pool := &talosv1alpha1.TalosMachinePool{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: tm.Namespace, Name: tm.Spec.PoolRef.Name}, pool); err != nil {
			// A machine whose pool does not exist belongs to no cluster.
			if !apierrors.IsNotFound(err) {
				mapFailed(ctx, err, "pool of the machine", obj)
			}

			return nil
		}

		return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: tm.Namespace, Name: pool.Spec.ClusterRef.Name}}}
	}
}

// clusterOfPool re-reconciles a pool's cluster.
func clusterOfPool(_ context.Context, obj client.Object) []reconcile.Request {
	pool, ok := obj.(*talosv1alpha1.TalosMachinePool)
	if !ok {
		return nil
	}

	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: pool.Namespace, Name: pool.Spec.ClusterRef.Name}}}
}

// SetupWithManager sets up the controller with the Manager.
func (r *TalosClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &talosv1alpha1.TalosCluster{},
		target.SecretRefIndex, target.IndexSecretRefs); err != nil {
		return fmt.Errorf("index secret references: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&talosv1alpha1.TalosCluster{}).
		Owns(&corev1.ConfigMap{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(target.ClustersForSecret(mgr.GetClient()))).
		Watches(&talosv1alpha1.TalosMachine{}, handler.EnqueueRequestsFromMapFunc(clusterOfMachine(mgr.GetClient()))).
		Watches(&talosv1alpha1.TalosMachinePool{}, handler.EnqueueRequestsFromMapFunc(clusterOfPool)).
		Named("taloscluster").
		Complete(r)
}
