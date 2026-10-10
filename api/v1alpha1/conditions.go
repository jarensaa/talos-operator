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
	// ConditionAdoptionProposed reports that a proven proposal of TalosCluster and TalosMachine specs
	// is available in the ConfigMap named by status.adoption.proposalRef.
	ConditionAdoptionProposed = "AdoptionProposed"
	// ConditionAdopted reports that rendered configs were proven equal to live configs on every node.
	ConditionAdopted = "Adopted"
	// ConditionConfigHandover reports whether entering config: Manage was verified to change
	// nothing: the first reconcile in Manage found every node's rendered config equal to its live
	// one, confirmed by Talos's dry run (docs/scope.md §7.1 stage 4).
	ConditionConfigHandover = "ConfigHandover"
	// ConditionConfigInSync reports whether every node runs its rendered config.
	ConditionConfigInSync = "ConfigInSync"
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
	ReasonAwaitingBackup    = "AwaitingBackup"
	ReasonConfigUnresolved  = "ConfigUnresolved"

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
	// ReasonAwaitingStatus: a TalosMachine's status does not reflect its spec and node yet.
	ReasonAwaitingStatus = "AwaitingStatus"
	// ReasonHeld: a machine does not run its rendered config and is on hold, or was skipped.
	ReasonHeld = "Held"
)

// Annotations and labels used by the operator.
const (
	// AnnotationSecretsBackedUp is set by the user on the secrets bundle Secret, to the bundle hash
	// shown in the SecretsBackedUp condition, once the bundle is exported off-cluster.
	AnnotationSecretsBackedUp = "talos.jarensaa.github.io/secrets-backed-up"
	// AnnotationProposalSource fingerprints the live state a proposal was built from.
	AnnotationProposalSource = "talos.jarensaa.github.io/proposal-source"
	// AnnotationApproveDiff approves the change whose hash is in status.approval.
	AnnotationApproveDiff = "talos.jarensaa.github.io/approve-diff"
	// AnnotationRetry, AnnotationSkipMachine and AnnotationAbort act on a failed operation. Their
	// value must be the failure's token from status.operation.failure (docs/scope.md §8.3).
	AnnotationRetry       = "talos.jarensaa.github.io/retry"
	AnnotationSkipMachine = "talos.jarensaa.github.io/skip-machine"
	AnnotationAbort       = "talos.jarensaa.github.io/abort"
	// AnnotationOrphan, set to "true" on a TalosMachine, lets it be deleted while desiredState is
	// Present: the operator stops managing the machine and leaves the node running.
	AnnotationOrphan = "talos.jarensaa.github.io/orphan"
	// LabelCluster names the TalosCluster an operator-created object belongs to.
	LabelCluster = "talos.jarensaa.github.io/cluster"
)
