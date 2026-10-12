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

package v1alpha1

// Condition types reported on TalosCluster. See docs/scope.md §5.1 and §7.1.
const (
	// ConditionCredentialsValid reports whether the operator holds a usable admin Talos identity.
	ConditionCredentialsValid = "CredentialsValid"
	// ConditionTalosAPIReachable reports whether the Talos API answers through the selected endpoints.
	ConditionTalosAPIReachable = "TalosAPIReachable"
	// ConditionHealthy reports the result of the Talos cluster health check.
	ConditionHealthy = "Healthy"
	// ConditionAdoptionPreconditionsMet aggregates status.adoption.preconditions.
	ConditionAdoptionPreconditionsMet = "AdoptionPreconditionsMet"
	// ConditionSecretsMatchLive reports whether the secrets bundle matches every node's live config.
	ConditionSecretsMatchLive = "SecretsMatchLive"
	// ConditionSecretsBackedUp reports whether the user confirmed an off-cluster export of the bundle.
	ConditionSecretsBackedUp = "SecretsBackedUp"
	// ConditionResourcesDerived reports, while a cluster is being adopted, whether the operator
	// wrote the TalosMachinePools and TalosMachines that describe it (docs/scope.md §7.1 stage 1).
	// Once the cluster is adopted the operator writes them no more, and the condition says so.
	ConditionResourcesDerived = "ResourcesDerived"
	// ConditionConfigProven reports whether the resources render exactly what every node runs right
	// now, confirmed by Talos's dry run. It turns false again when either side changes, and it is
	// what management.config: Manage is gated on. Whether the cluster was ever adopted is the
	// phase, which never goes back.
	ConditionConfigProven = "ConfigProven"
	// ConditionConfigHandover reports whether entering config: Manage was verified to change
	// nothing: the first reconcile in Manage found every node's rendered config equal to its live
	// one, confirmed by Talos's dry run (docs/scope.md §7.1 stage 4).
	ConditionConfigHandover = "ConfigHandover"
	// ConditionConfigInSync reports whether every node runs its rendered config.
	ConditionConfigInSync = "ConfigInSync"
	// ConditionTalosUpToDate reports whether every node runs the Talos version and installer its
	// spec asks for (docs/scope.md §7.3).
	ConditionTalosUpToDate = "TalosUpToDate"
	// ConditionKubernetesUpToDate reports whether every node's config names, and every node runs,
	// the Kubernetes version its spec asks for (docs/scope.md §7.4).
	ConditionKubernetesUpToDate = "KubernetesUpToDate"
	// ConditionVersionsReachable reports whether there is a sequence of upgrades from the versions
	// the cluster runs to the versions its spec asks for (docs/scope.md §7.5).
	ConditionVersionsReachable = "VersionsReachable"
	// ConditionMembershipCurrent reports whether the cluster's machines are the ones that are
	// declared: every TalosMachine that is Present is a member, and none that is Removed still is
	// (docs/scope.md §7.6, §7.7).
	ConditionMembershipCurrent = "MembershipCurrent"
	// ConditionEtcdMaintained reports on the upkeep of etcd: whether it is fragmented enough to be
	// defragmented, and whether it has raised an alarm (docs/scope.md §14.6).
	ConditionEtcdMaintained = "EtcdMaintained"
	// ConditionRolloutBlocked reports that a disruptive operation is halted and needs attention.
	ConditionRolloutBlocked = "RolloutBlocked"
)

// Condition types reported on TalosMachinePool and TalosMachineConfig. See docs/scope.md §5.2-5.3.
const (
	// ConditionValid reports whether a TalosMachineConfig's documents parse as strategic-merge config.
	ConditionValid = "Valid"
	// ConditionInUse reports whether a pool has members or a TalosMachineConfig is referenced. While
	// it is True, deletion waits on FinalizerInUse.
	ConditionInUse = "InUse"
)

// FinalizerInUse keeps a TalosMachinePool with members, or a referenced TalosMachineConfig, from
// being deleted until nothing depends on it, the way Kubernetes protects bound PVCs.
const FinalizerInUse = "talos.jarensaa.github.io/in-use"

// Condition types reported on TalosMachine. See docs/scope.md §5.4.
const (
	// ConditionMatched reports whether the machine is bound to a live node by hardware identity.
	ConditionMatched = "Matched"
	// ConditionConfigMatchesLive reports whether the rendered config equals the node's live config.
	ConditionConfigMatchesLive = "ConfigMatchesLive"
	// ConditionNodeObserved reports whether the node's state in status was read in the last pass.
	// While it is not True, status.talos.stage, ready and bootID are empty rather than stale, and
	// everything else keeps the last value that was read.
	ConditionNodeObserved = "NodeObserved"
	// ConditionProvisionable reports, for a machine that is not a member of the cluster, whether
	// it can be added: it waits in maintenance mode at its address, carries the pinned identity,
	// and the config rendered for it fits its disks and links (docs/scope.md §7.6).
	ConditionProvisionable = "Provisionable"
)

// Condition reasons shared across types.
const (
	ReasonSecretNotFound    = "SecretNotFound"
	ReasonKeyMissing        = "KeyMissing"
	ReasonParseError        = "ParseError"
	ReasonExpired           = "Expired"
	ReasonInsufficientRole  = "InsufficientRole"
	ReasonUnreachable       = "Unreachable"
	ReasonNotFound          = "NotFound"
	ReasonConflict          = "Conflict"
	ReasonSucceeded         = "Succeeded"
	ReasonFailed            = "Failed"
	ReasonPreconditionUnmet = "PreconditionUnmet"
	ReasonMismatch          = "Mismatch"
	// ReasonChangedElsewhere: a resource the operator would write during adoption carries a value
	// someone else set, so it is left as it is.
	ReasonChangedElsewhere = "ChangedElsewhere"
	// ReasonHandedOver: the cluster is adopted, and the resources are the user's from here on.
	ReasonHandedOver       = "HandedOver"
	ReasonAwaitingBackup   = "AwaitingBackup"
	ReasonConfigUnresolved = "ConfigUnresolved"

	// ReasonTalosAPIAccessDisabled: the talos.dev ServiceAccount CRD is missing, so no
	// KubeTalosAPIAccessConfig is active on the control planes.
	ReasonTalosAPIAccessDisabled = "TalosAPIAccessDisabled"
	// ReasonTalosAPIAccessDenied: Talos refused the ServiceAccount, e.g. a role is not allowed.
	ReasonTalosAPIAccessDenied = "TalosAPIAccessDenied"
	// ReasonAwaitingCredentials: Talos has not issued the ServiceAccount's Secret yet.
	ReasonAwaitingCredentials = "AwaitingCredentials"

	// ReasonProofInvalidated: live state or the spec changed after the adoption proof.
	ReasonProofInvalidated = "ProofInvalidated"
	// ReasonServerReportsChanges: Talos's dry run reports changes the per-document comparison
	// does not show.
	ReasonServerReportsChanges = "ServerReportsChanges"
	// ReasonHandoverUnverified: entering config: Manage could not be verified to change nothing.
	ReasonHandoverUnverified = "HandoverUnverified"
	// ReasonDrift: a machine's live config no longer equals its rendered one.
	ReasonDrift = "Drift"

	// Reasons of RolloutBlocked and ConfigInSync while config is Manage (docs/scope.md §6.3, §8.3).
	ReasonRollingOut        = "RollingOut"
	ReasonPaused            = "Paused"
	ReasonOperationFailed   = "OperationFailed"
	ReasonProtectedChange   = "ProtectedChange"
	ReasonRequiresReinstall = "RequiresReinstall"
	ReasonAwaitingApproval  = "AwaitingApproval"
	ReasonAborted           = "Aborted"
	ReasonGateClosed        = "GateClosed"
	ReasonInvalidRollout    = "InvalidRollout"
	// ReasonAwaitingUpgrade: the config names another installer image than a node runs, which a
	// Talos upgrade brings about, not a config rollout.
	ReasonAwaitingUpgrade = "AwaitingUpgrade"
	// ReasonAwaitingKubernetesUpgrade: the config names another version of a Kubernetes component
	// than a node runs, which a Kubernetes upgrade brings about, in its own order.
	ReasonAwaitingKubernetesUpgrade = "AwaitingKubernetesUpgrade"
	// ReasonOutsideWindow: no maintenance window is open (docs/scope.md §6.4).
	ReasonOutsideWindow = "OutsideWindow"
	// Reasons of TalosUpToDate and KubernetesUpToDate.
	ReasonUpgrading        = "Upgrading"
	ReasonUpgradesDisabled = "UpgradesDisabled"
	ReasonIncompatible     = "Incompatible"
	// ReasonUpToDate: the cluster runs the versions its spec asks for.
	ReasonUpToDate = "UpToDate"
	// ReasonPlanned: there is a way to the versions the spec asks for, in status.versions.plan.
	ReasonPlanned = "Planned"
	// ReasonAwaitingStatus: a TalosMachine's status does not reflect its spec and node yet.
	ReasonAwaitingStatus = "AwaitingStatus"
	// ReasonHeld: a machine does not run its rendered config and is on hold, or was skipped.
	ReasonHeld = "Held"
	// Reasons of Provisionable, and of the conditions of a machine that is no member.
	// ReasonNotAMember: the machine is not a member of the cluster, so there is no node to read.
	ReasonNotAMember = "NotAMember"
	// ReasonMembershipDisabled: spec.management.membership is not Enabled, so nothing is asked of
	// a machine that is not a member.
	ReasonMembershipDisabled = "MembershipDisabled"
	// ReasonNotInMaintenance: something answers at the machine's address, and it is not a machine
	// waiting in maintenance mode.
	ReasonNotInMaintenance = "NotInMaintenance"
	// ReasonIdentityMismatch: the machine at the address is not the one that was declared.
	ReasonIdentityMismatch = "IdentityMismatch"
	// ReasonRoleMismatch: the node at the address is not of the role its machine's pool declares.
	ReasonRoleMismatch = "RoleMismatch"
	// ReasonConfigDoesNotFit: the rendered config names a disk or a link the machine does not have.
	ReasonConfigDoesNotFit = "ConfigDoesNotFit"
	// Reasons of EtcdMaintained. A defragmentation that is due and does not start has the reason
	// of what it waits for, as RolloutBlocked has; TooSoon is its own.
	ReasonDefragmenting       = "Defragmenting"
	ReasonMaintenanceDisabled = "MaintenanceDisabled"
	ReasonTooSoon             = "TooSoon"
	// Reasons of MembershipCurrent.
	ReasonAdding          = "Adding"
	ReasonRemoving        = "Removing"
	ReasonAwaitingMachine = "AwaitingMachine"
	// Reasons of RolloutBlocked for machines to be added or removed (docs/scope.md §7.6, §7.7).
	ReasonNotAtRest        = "NotAtRest"
	ReasonQuorumAtRisk     = "QuorumAtRisk"
	ReasonHostnameInUse    = "HostnameInUse"
	ReasonLastControlPlane = "LastControlPlane"
)

// Annotations and labels used by the operator.
const (
	// AnnotationSecretsBackedUp is set by the user on the secrets bundle Secret, to the bundle hash
	// shown in the SecretsBackedUp condition, once the bundle is exported off-cluster.
	AnnotationSecretsBackedUp = "talos.jarensaa.github.io/secrets-backed-up"
	// AnnotationApproveDiff approves the change whose hash is in status.approval.
	AnnotationApproveDiff = "talos.jarensaa.github.io/approve-diff"
	// AnnotationRetry, AnnotationSkipMachine and AnnotationAbort act on a failed operation. Their
	// value must be the failure's token from status.operation.failure (docs/scope.md §8.3).
	AnnotationRetry       = "talos.jarensaa.github.io/retry"
	AnnotationSkipMachine = "talos.jarensaa.github.io/skip-machine"
	AnnotationAbort       = "talos.jarensaa.github.io/abort"
	// AnnotationConfirmDead, on the TalosCluster and carrying the token of a removal that halted
	// because the machine does not answer, confirms that the machine is gone for good. The
	// removal then goes on without it: its etcd member is removed through another control plane
	// and its Node is deleted. Nothing on the machine is wiped, because nothing can be.
	AnnotationConfirmDead = "talos.jarensaa.github.io/confirm-dead"
	// AnnotationOrphan, set to "true" on a TalosMachine, lets it be deleted while desiredState is
	// Present: the operator stops managing the machine and leaves the node running.
	AnnotationOrphan = "talos.jarensaa.github.io/orphan"
	// LabelCluster names the TalosCluster an operator-created object belongs to.
	LabelCluster = "talos.jarensaa.github.io/cluster"
	// LabelHook marks the Job of a hook with the stage it ran at (docs/scope.md §6.4).
	LabelHook = "talos.jarensaa.github.io/hook"
)
