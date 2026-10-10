# talos-operator — scope and design

Status: **draft for review** · Target: Talos **v1.14.x**, Kubernetes **1.37.x** · Date: 2026-10-08

A Kubernetes operator that manages Talos Linux clusters declaratively through CRDs.

The four headline requirements:

| # | Requirement | Where it is designed |
|---|-------------|----------------------|
| R1 | Declarative cluster state (machine config, versions, membership) | §5, §6 |
| R2 | Automatic upgrades (Talos OS and Kubernetes) | §7.3–7.5 |
| R3 | Safe adoption of an existing talosctl-created cluster | §7.1 |
| R4 | Expanding an existing cluster with new nodes | §7.6 |

Removing and replacing nodes (§7.7) and greenfield bootstrap (§7.8) fall out of R1/R4 and are scoped too, at lower priority. Day-2 operations — observability, upgrade-path awareness, version-bump automation and maintenance — are scoped in §14.

All code references are to Talos source in the Go module cache:
- `T` = `github.com/siderolabs/talos@v1.14.0`
- `M` = `github.com/siderolabs/talos/pkg/machinery@v1.14.0`

---

## 1. Goals and non-goals

### Goals
- **One source of truth.** Desired state lives in CRs, which can be kept in Git. The operator renders every node's machine config from that state, plus a secrets bundle, plus layered config documents. It then converges the nodes to it.
- **Safety first.** Every disruptive action (config apply, reboot, upgrade, etcd membership change, reset) goes through:
  - a single cluster-wide disruption lock;
  - explicit health gates;
  - bounded timeouts;
  - **fail-closed** behaviour: on failure, stop and surface the problem, never "try the next node".
- **Talk to Talos directly over its gRPC API** using `pkg/machinery`. Never shell out to `talosctl`.
- **Adoption is provably non-disruptive.** The operator takes ownership only after it has shown that its rendered config equals the live config on every node. The proof is an empty semantic diff plus a server-side dry-run returning "No changes".
- **Restart-safe.** Any step can be interrupted by an operator restart, including the operator evicting itself while draining its own node, and resumes from persisted state.

### Non-goals (v1)
- **Hardware provisioning** (PXE, IPMI, cloud VMs, Terraform). Machines are assumed to exist and to boot Talos, either into maintenance mode or already configured.
- **Disaster recovery from total quorum loss.** The operator cannot fix the cluster it runs in once that cluster's API is gone. Rebuilding the cluster from its owner's own runbook remains the DR path. The operator's job is to never cause quorum loss and to keep DR inputs fresh (§9).
- **Managing workloads or CNI.** A CNI, a storage system and so on stay with whatever installs them.
- **Replacing Omni.** No SideroLink or multi-tenant SaaS features in v1. These are listed as stretch goals.
- **Supporting Talos older than 1.13.** The new `LifecycleService` and `ImageService` APIs, and the multi-document config, are the baseline. 1.12 nodes are only supported as the *source* of an upgrade.

---

## 2. Target environment

The design is shaped by one kind of cluster: a small bare-metal cluster that someone built by hand with `talosctl` and now wants managed. Its properties, and what each asks of the operator:

| Property | Consequence for the operator |
|----------|------------------------------|
| Three control-plane nodes that also run workloads, no workers | Every rolling operation eventually targets the node the operator itself runs on (§4.2). Only one failure is tolerated, ever. |
| Config generated with `talosctl gen config --with-secrets` and then `machineconfig patch` per node | Fully multi-document config. Adoption must derive per-node patches (an install-disk selector, network documents, deleted or disabled defaults). |
| Installer images from Image Factory | Each node's schematic must be tracked and preserved across upgrades. |
| Storage that lives on the nodes (Ceph, for one) | Node disruption must be gated on the storage system's health. Never wipe user disks. |
| A CNI installed by something else, kube-proxy disabled | A Kubernetes upgrade must not fight whoever owns those manifests. Syncing Talos's bootstrap manifests is off by default for adopted clusters. |
| The Kubernetes endpoint is a DNS name resolving to every node | Talos API endpoints used during upgrades must be **IPs of healthy, non-target nodes**, never the DNS name. |
| The secrets bundle may or may not have been kept | Recovery from cluster loss is a rebuild with fresh PKI (§12.7), so an off-cluster copy is optional break-glass, not DR. The operator holds an in-cluster copy (needed to render configs) and must never regenerate it for an existing cluster. |
| **The files the cluster was built from are stale.** A live cluster diverges from its generated configs and patches. | The live cluster is the only source of truth for adoption. The operator derives the spec from live state, and files are at most cross-checked (§7.1). |

Expected adoption result for such a cluster: cluster-wide documents on the TalosCluster, one control-plane pool, and one disk selector per machine.

---

## 3. What talosctl taught us (findings that drive the design)

1. **The upgrade is now orchestrated by the client.** Since 1.13, `talosctl upgrade` does these steps itself:
   1. `ImageService.Pull`
   2. `LifecycleService.Upgrade`, which writes the inactive A/B slot and streams progress. Check `exit_code`; it is not a gRPC error.
   3. Cordon and drain on the client (`go-kubernetes/nodedrain`).
   4. `MachineService.Reboot`
   5. Wait for a boot-ID change and `MachineStatus` Running+Ready.
   6. Uncordon.

   `MachineService.Upgrade` is deprecated, with removal planned for v1.18 (`T/api/machine/machine.proto:93`, `T/cmd/talosctl/cmd/talos/upgrade.go:93-174`).
2. **The new upgrade path has no etcd safety check.** The quorum check (`etcd.ValidateForUpgrade`: never 2 members, all started, quorum read OK) only exists in the legacy `Server.Upgrade` (`T/internal/app/machined/internal/server/v1alpha1/v1alpha1_server.go:487-600`). Docs still say "Talos refuses to upgrade a CP if quorum would be lost", but that is not true of `LifecycleService` (`T/internal/app/lifecycle/lifecycle.go:159-246`). **The operator must enforce it.**
3. **`apply-config` never reboots in 1.14.** `REBOOT` mode is rejected and `AUTO` becomes `NO_REBOOT` (`v1alpha1_server.go:223,240`). Every apply takes effect live. Settings that only matter at boot (kernel args, schematic) wait for the next install or upgrade. `TRY` mode applies with an automatic rollback (default 1 minute). Any later non-TRY apply confirms it.
4. **Talos guards almost nothing on apply.** It does not stop changes to CAs, cluster ID or secret, endpoint, or machine type. Network documents apply live and can cut a node off. The operator needs a protected-field policy.
5. **The secrets bundle can be fully recovered from a control-plane config**, including in 1.14 multi-document form (`M/config/generate/secrets/bundle.go:177`, round-trip tested). It **cannot** be recovered from a worker config, and on 1.14 that call nil-panics. Not carried over: `acceptedCAs`, and any extra etcd encryption keys.
6. **The version contract must be pinned.** `TalosVersionCurrent` means "everything this machinery knows about". Building against newer machinery silently changes the generated documents (`M/config/contract.go`).
7. **JSON6902 patches do not work on multi-document configs.** Only strategic merge (plus `$patch: delete`) is usable. `configdiff.Patch(original, modified)` (`M/config/configdiff/mergepatch.go:27`) produces strategic patches and is the core of adoption.
8. **`talosctl upgrade-k8s` writes machine config directly.** It patches each node's config component by component and waits on COSI resources and Kubernetes pods (`T/pkg/cluster/kubernetes/talos_managed.go:84-169`). An operator that owns the config must **re-implement this algorithm**, not call it. Otherwise the two writers fight over the config. Kubernetes minor versions cannot be skipped (go-kubernetes `upgrade.Path`).
9. **Maintenance mode is trust-on-first-use.** The node's TLS certificate comes from an in-memory CA that is new on every boot. Insecure clients get the Reader role plus `ApplyConfiguration`. Whoever answers on that IP receives the full config, including secrets. Adding a node needs identity pinning (§7.6).
10. **Bootstrap must run exactly once.** `codes.AlreadyExists` means the cluster is already bootstrapped. A second bootstrap on an unjoined CP creates a split-brain etcd (`v1alpha1_server.go:411-451`).
11. **Graceful reset handles etcd leave.** It calls `MemberRemove(self)` and stops etcd. It does **not** check quorum and only logs drain failures (`v1alpha1_sequencer.go:278-353`). Removing a dead CP is `EtcdRemoveMemberByID` from a healthy peer, also unguarded.
12. **Health checks are reusable without heavy dependencies.** `ClusterService.HealthCheck` runs `pkg/cluster/check` on the node itself, so the operator only needs the machinery client. Caveat: the "all nodes schedulable" check fails while a node is cordoned.

---

## 4. Architecture

### 4.1 Components

```
cmd/manager                 controller-runtime manager (leader-elected, 2 replicas)
api/v1alpha1                TalosCluster, TalosMachine (CEL rules; admission policies in config/admission)
internal/controller
  taloscluster_controller   observes cluster, owns the disruption lock, drives rollouts
  talosmachine_controller   per-node observe, provision, remove (finalizer)
internal/talos              Talos access layer (only package that imports machinery/client)
  clientfactory             cert minting from bundle, endpoint selection, WithNode targeting
  observe                   typed COSI reads: Version, MachineStatus, BootID, SystemInformation,
                            ImageFactorySchematic, MachineConfig, Members, Nodename, UnattendedInstallStatus
  ops                       mutating calls (ApplyConfiguration, ImageService.Pull, LifecycleService.Upgrade,
                            Reboot, Reset, Bootstrap, Etcd*), each idempotent and with a timeout
internal/render             config generation + patch layering + normalisation + hashing + semantic diff
internal/adopt              secrets recovery/verification, input inference, patch derivation, exactness proof
internal/plan               PURE planners: (observed state, desired state) -> next step; holds all safety invariants
internal/k8supgrade         re-implementation of talos_managed.go steps on top of go-kubernetes
internal/gates              built-in gates (etcd, Talos health, node Ready) + CEL gates on arbitrary objects + hook Jobs
```

**Key structural decision:** planners in `internal/plan` are pure functions with no I/O. Controllers do observe → plan → execute one step → persist → requeue. Every safety invariant lives in one place and can be table-tested exhaustively. Examples:
- never more than one CP not-Ready;
- never fewer than 3 etcd voters without an override;
- never two disruptive operations at once.

What is in `internal/plan` today: the cluster preconditions and the cluster gate of a rollout (`Preconditions`, `ClusterGate`), whether a rollout is due and may start (`PlanRollout`), and what a node step does next (`NextStep`). The controllers hand them plain facts: what a pass read from each node, with everything that was not read marked as unknown, never as fine. The adoption proof's gate (`proofGate`) is the one exception: it is pure and table-tested, but it judges TalosMachine objects directly and so sits with the controller that reads them.

**A failure is reported as what it is.** A node that does not answer makes its own facts unknown. A failure of the Kubernetes API the operator runs against is not a fact about Talos at all: the pass fails and is retried, and no condition claims the Talos cluster is unreachable. Every read that fails is logged with the node and what was asked; conditions carry the names of what was not read, which do not change from pass to pass.

### 4.2 Topology

**Recommendation:** run self-hosted first, but code against a target-cluster abstraction:
- endpoints + credentials, where "self" is just one target;
- a management-cluster mode becomes configuration, not a rewrite.

Self-hosted on a 3-CP-only cluster means the operator will drain and reboot its own node. That is handled by design:
- **2 replicas**, leader election, required pod anti-affinity, PDB `maxUnavailable: 1`.
- **Every step is persisted before its side effect** (CACPPT's "annotate before etcd-leave" pattern). Example: `status.operation.step = Draining(node-2)` is written before the drain starts. If the leader is evicted, the standby resumes at that step.
- **Steps are keyed on observed facts, not memory.** A reboot step stores the pre-reboot boot ID. On resume, a changed boot ID means "reboot happened".
- **Talos calls are never routed through the target node.** `WithEndpoints` uses the IPs of healthy non-target CPs, and `client.WithNode(target)` reaches the target through apid proxying. Endpoints are re-resolved after every reboot.
- **The node hosting the current leader is upgraded last** in a rollout. This minimises failovers; it is not required for correctness.

What self-hosted cannot do: recover from quorum loss, or bootstrap the cluster it lives in. Both remain out-of-band (talosctl + the existing runbook) and are documented, not automated.

### 4.3 Dependencies

| Dependency | Use | Note |
|------------|-----|------|
| `github.com/siderolabs/talos/pkg/machinery` v1.14.x | client, COSI resources, config generate/patch/diff, secrets, compatibility, `stdpatches` | **Requires go 1.26.5** (local toolchain is 1.25.6, so `GOTOOLCHAIN=auto` will fetch it). Its go.mod has a `replace` for `mdlayher/ethtool`, which is ignored downstream; copy it if anything we import pulls ethtool. |
| `github.com/siderolabs/go-kubernetes` | `upgrade.Path` / `upgrade.NewChecks`, `nodedrain`, `manifests`, `ssa` | Lightweight. |
| `sigs.k8s.io/controller-runtime` | operator framework | Must line up with `k8s.io/*` v0.37 (machinery's). |
| **Not** `github.com/siderolabs/talos` (main module) or anything under `cmd/talosctl` | — | Heavy dependency tree (provisioning, CNI). talosctl helpers stopped being exported in 1.14. |

**Version skew rule:** the operator's machinery minor must be **≥** the newest Talos minor it manages. `configloader` rejects unknown document kinds. Each operator release declares a supported Talos range, and automatic upgrades never target a Talos minor outside it.

Scaffolding follows the `vault-operator` conventions: operator-sdk layout, Makefile targets, Helm chart, envtest, e2e under `test/e2e`.

---

## 5. API (CRDs)

API group: `talos.jarensaa.github.io/v1alpha1` (§12.5). There are four kinds, all namespaced (§12.10):

| Kind | Holds | One place for |
|---|---|---|
| `TalosCluster` | identity, generation inputs, versions, management, cluster-wide config | anything that is the same on every node |
| `TalosMachinePool` | a role, an installer override, config shared by its members | everything the members of a pool share |
| `TalosMachine` | one node: hardware identity, address, pool membership, node-specific config | what only this node has |
| `TalosMachineConfig` | reusable config documents, referenced by any of the above | config shared across pools, machines or clusters |

Talos nodes are immutable and, within a role, identical apart from a few facts. So a large cluster is a handful of pools and shared configs, not one config per node. Talos's own defaults are never written down anywhere: they are generated from the cluster's inputs at render time (§6.1).

**Config sources.** The TalosCluster, every pool and every machine carry an ordered list `config` (at most 32 entries). Each entry is exactly one of:

```yaml
config:
  - configRef: { name: bonded-network }        # a TalosMachineConfig in the same namespace
  - inline: |                                  # config documents, multi-doc YAML
      apiVersion: v1alpha1
      kind: KubeNodeConfig
      labels: { topology.kubernetes.io/zone: rack-1 }
  - secretKeyRef: { name: registry-auth, key: config.yaml }   # documents with sensitive values; their diffs are withheld (§9)
```

Documents of the multi-document kinds (`BondConfig`, `HostnameConfig`, `UnattendedInstallConfig`, …) are declared whole. Only the legacy `v1alpha1` document is a partial overlay. A generated document is removed with `$patch: delete`. Entries are strategic merges onto the generated config, applied in order with later entries winning (§6.1). JSON6902 is refused.

### 5.1 `TalosCluster`

```yaml
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosCluster
metadata:
  name: my-cluster
spec:
  # Management is enabled per capability, so trust grows gradually (§7.1 stage 4).
  # config: Manage is refused at admission unless status says Adopted=True (§5.2); upgrades and
  # membership need config: Manage.
  management:
    config: Observe                # Observe | Manage
    upgrades: Disabled             # Disabled | Enabled   (Talos + Kubernetes rollouts)
    membership: Disabled           # Disabled | Enabled   (add / remove / replace machines)
  credentials: { source: TalosServiceAccount }       # default: Talos issues the operator's certificate (§9)
  secretsRef: { name: my-cluster-talos-secrets }    # secrets bundle; written by the operator during adoption if absent

  # Everything below is optional while observing; it is copied from the adoption proposal (§7.1).
  clusterName: my-cluster          # immutable once set
  controlPlaneEndpoint: https://cluster.example.com:6443   # immutable once set

  versions:
    talos: v1.14.0
    kubernetes: 1.37.0
  configGeneration:
    versionContract: v1.14           # pinned explicitly; bump = deliberate, previewed change
    installer:
      factoryURL: https://factory.talos.dev
      platform: metal
      schematicID: 376567988ad3...   # default for all pools; a pool may override it
    options:                          # mirrors generate.Input options that matter
      dnsDomain: cluster.local
      podSubnets: [10.244.0.0/16]
      serviceSubnets: [10.96.0.0/12]
      kubePrismPort: 7445
  config:                             # cluster-wide layer, applied to every node first (§5 config sources)
    - configRef: { name: bonded-network }
    - inline: |
        apiVersion: v1alpha1
        kind: KubeProxyConfig
        enabled: false

  rollout:
    driftPolicy: Report               # Report | Enforce
    tryTimeout: 5m                    # how long Talos keeps a tried config before reverting it (§8.5.1)
    soak: 2m                          # how long a node must stay healthy before its config is confirmed
    stepTimeout: 10m                  # every other wait of a node step; a timeout is a failure
    maintenanceWindows:               # empty = always allowed
      - { schedule: "0 2 * * *", duration: 3h, timeZone: Europe/Oslo }
    drainTimeout: 10m
    gates:                            # all must be true before AND after each node step
      - name: ceph-healthy
        apiVersion: ceph.rook.io/v1
        kind: CephCluster
        namespace: rook-ceph
        name: rook-ceph
        expression: object.status.ceph.health == "HEALTH_OK"
    hooks:                            # optional Jobs; e.g. ceph osd set/unset noout
      preRollout: []
      postRollout: []
      preNode: []
      postNode: []
    kubernetes:
      manifestSync: Disabled          # Disabled | Apply | ApplyAndPrune  (bootstrap manifests)

  paused: false                       # stops all mutating actions immediately

status:
  phase: Observing | Adopting | Adopted | Managed | Degraded
  conditions:                         # Ready, Adopted, ConfigHandover, ConfigInSync, Healthy, RolloutBlocked, Locked, ...
  observed:
    talosVersions: { v1.14.0: 3 }
    kubernetesVersion: 1.37.0
    etcdMembers: [ { id, name, isLearner } ]
  bootstrapped: true
  adoption:
    preconditions: [ … ]              # each with status + message
    proposalRef: { name: my-cluster-adoption-proposal }   # ConfigMap with proposed CR YAML
    unmatchedMembers: []              # live nodes with no TalosMachine yet
    liveConfigHashes: { node-a: sha256:… }
    provenAt: …
  operation:                          # THE disruption lock; at most one at a time
    id: 3f1c…
    type: TalosUpgrade                # ConfigRollout | TalosUpgrade | KubernetesUpgrade | AddControlPlane | RemoveMachine | Bootstrap
    target: v1.14.2                   # for a ConfigRollout: the hash of the change (§6.3)
    plan: [talos-cp-2, talos-cp-3, talos-cp-1]
    targets: { talos-cp-2: sha256:… } # ConfigRollout: the rendered config each machine is to reach,
    bases: { talos-cp-2: sha256:… }   #   and the live config it had when the rollout was planned
    current: { machine: talos-cp-3, step: Rebooting, preRebootBootID: …, since: … }
                                      # ConfigRollout: step Trying | Verifying | Confirming | RollingBack,
                                      #   with class, previousHash, desiredHash, deadline, healthySince, attempt
    completed: [talos-cp-2]
    skipped: []
    failure: null                     # set => halted until a human acts (§8.3): machine, step, reason,
                                      #   message, remediation, token
  approval: { hash: 9c1e…, machines: [ … ] }   # a change waiting for a human (§6.3)
  abortedTargets: {}                  # what an aborted rollout was applying; not started again (§8.3)
  history: []                         # the last 10 operations: outcome Succeeded | Aborted | Superseded
```

### 5.2 `TalosMachinePool`

A set of machines of one role that share config. Every machine belongs to exactly one pool.

```yaml
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachinePool
metadata:
  name: control-plane
spec:
  clusterRef: { name: my-cluster }     # immutable
  role: ControlPlane                   # immutable; ControlPlane | Worker; selects the generation template
  installer: { schematicID: null }     # override the cluster default, e.g. for a pool needing other extensions
  config:                              # pool layer, applied after the cluster's (§5 config sources)
    - configRef: { name: nvme-install-disk }
  # M3+: rollout: { maxUnavailable: 1 }  (control-plane pools are always serial, §8.1)

status:
  members: 3
  matched: 3
  configMatchesLive: 3
  conditions: […]                      # InUse: lists the members
```

### 5.3 `TalosMachineConfig`

Reusable config documents. Any TalosCluster, pool or machine in the namespace can reference it, so one definition serves every node that needs it.

```yaml
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachineConfig
metadata:
  name: bonded-network
spec:
  documents: |                         # config documents, as in an `inline` source
    apiVersion: v1alpha1
    kind: BondConfig
    name: bond0
    links: [eth0, eth1]
    bondMode: 802.3ad
    ---
    apiVersion: v1alpha1
    kind: DHCPv4Config
    name: bond0

status:
  conditions: […]                      # Valid: parses as strategic-merge config documents; InUse
  documentKeys: [v1alpha1/BondConfig/bond0, v1alpha1/DHCPv4Config/bond0]
  usedBy: { clusters: [my-cluster], pools: [control-plane], machines: 3 }   # the blast radius of an edit
```

- A TalosMachineConfig cannot reference others. Resolution stays one level deep, with no cycles.
- Sensitive values do not belong here. A `secretKeyRef` source at the point of use keeps them in a Secret.
- Editing a TalosMachineConfig changes every machine that uses it, directly or through its pool or cluster. `status.usedBy` and each machine's `desiredHash` show that impact before anything rolls out, and the rollout plans it pool by pool (§6.3).

### 5.4 `TalosMachine`

There is one per node, whether existing, adopted or to-be-added. Everything it shares with other nodes lives in its pool or cluster. With pool-level disk selectors, a new node needs nothing but `poolRef`, `identity` and `address`.

```yaml
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachine
metadata:
  name: talos-cp-1
spec:
  poolRef: { name: control-plane }    # the pool decides the cluster and the role
  address: 10.0.0.11              # Talos API address (maintenance or configured)
  identity:                           # REQUIRED to provision a maintenance-mode machine (§7.6)
    systemUUID: aaaaaaaa-…            # from `talosctl get systeminformation --insecure`
  config:                             # node-specific layer, applied last (e.g. a disk WWID, a static address)
    - inline: |
        apiVersion: v1alpha1
        kind: UnattendedInstallConfig
        provisioning: { diskSelector: { match: 'disk.wwid == "eui.…"' } }
  desiredState: Present               # Present | Removed  (deletion of the CR alone never wipes a node; §7.7)
  hold: false                         # exclude from rollouts (Omni's "locked")

status:
  phase: Pending | AwaitingIdentity | Provisioning | Joining | Ready | Updating | Draining | Rebooting | Removing | Failed
  identity: { systemUUID, nodeID, hostname, kubernetesNodeName }
  talos: { version, schematicID, bootID, stage, ready }
  kubelet: { version }
  config:                             # hashes of the normalized config: desiredHash == liveHash iff ConfigMatchesLive
    desiredHash: sha256:…             # what the spec renders
    liveHash: sha256:…                # what the node runs
    appliedHash: sha256:…             # what the operator last confirmed on the node, or took over (§6.2)
    skippedHash: sha256:…             # a rendered config a human skipped for this machine (§8.3)
    changeClass: LiveSafe             # class of the change from live to rendered, and what put it there (§6.3)
    changeReasons: [v1alpha1/KubeNodeConfig]
    diff: |                           # redacted, per document; this is what an approval is reviewed against
      …
  etcd: { memberID, isLearner }
  conditions: […]                     # Matched, ConfigMatchesLive, NodeObserved
```

`NodeObserved` says whether the node's state was read in the last pass, and names what was not. Status never shows a moment that has passed as the present: `talos.stage`, `talos.ready`, `talos.bootID` and `config.liveHash` are empty unless they were read in the last pass, and a machine is `Ready` only on what its node said in that pass. A boot ID that survived a failed read would say that no reboot happened. Facts that rarely change (version, schematic, identity, etcd member) keep their last value, which is more use than nothing while a node is away.

`poolRef` may change between pools of the same role; that is a config change like any other. A pool of the other role cannot match the live node, which keeps reporting its own machine type. The machine then stays `Matched=False` (`RoleMismatch`) and is never acted on.

**Validation.** There is no webhook. Rules on one object are CEL in the CRDs. Rules that need the stored object or a DELETE are **ValidatingAdmissionPolicies** (`config/admission/policies.yaml`, shipped by the chart). The API server evaluates them itself, so they hold while the operator is down; a self-hosted operator's webhook would be unavailable during the very node drains it runs (§12.8).
- Immutable fields stay immutable: a pool's `clusterRef` and `role` cannot change (CRD CEL).
- `upgrades` and `membership` can be `Enabled` only with `config: Manage` (CRD CEL).
- `config` may become `Manage` only while the **stored** status shows `Adopted=True` (policy `…-management-gate`). A spec update cannot carry its own status, so the gate reads `oldObject.status`. `Adopted` is kept current by the proof (§7.1 stage 3): any live or spec change since the proof sets it False. Once managed, a later invalidated proof does not lock the spec.
- A TalosMachine cannot be **deleted while still `Present`** unless annotated `talos.jarensaa.github.io/orphan: "true"` (policy `…-deletion-guard`). This guards against a GitOps prune or a namespace deletion silently dropping a machine. `desiredState: Removed` is how a node leaves (§7.7).
- **In-use objects are protected by the finalizer `talos.jarensaa.github.io/in-use`**, the way Kubernetes protects bound PVCs. Their `InUse` condition says what holds them. A TalosMachineConfig stays while anything references it, and a pool while it has members. Deletion completes once they are unreferenced. If the operator is down, the deletion waits, which is the safe direction.
- **A missing reference fails closed.** A `configRef` or `secretKeyRef` that does not resolve sets `ConfigMatchesLive=False` (`ConfigUnresolved`) on every affected machine, and no rollout touches them.
- **No two machines act on one node.** This is enforced by the controller, failing closed, because admission cannot see other objects. Machines claiming the same systemUUID are both `Matched=False` (`Conflict`) and never acted on. A duplicate address with a different UUID fails the hardware check at that address.

---

## 6. Config model

### 6.1 Rendering pipeline (`internal/render`)

```
desired(node) =
  generate.NewInput(clusterName, endpoint, k8sBaseVersion,
                    WithSecretsBundle(bundle), WithVersionContract(contract),
                    WithInstallImage(pool.installer ?? cluster.installer), …options).Config(pool.role)
  ⊕ cluster.config ⊕ pool.config ⊕ machine.config   (each list in order; a configRef expands in place)
  ⊕ operator overlays   (in-flight rollout state: k8s component images per node, §7.4)
  → configloader validate (ValidateAtRuntime-equivalent)
  → normalise (drop comments, stable doc order) → hash
```

- **`k8sBaseVersion` is the version the rollout started from**, not `spec.versions.kubernetes`. Changing the spec version does not change any node's rendered config until the Kubernetes upgrade orchestrator advances it node by node via overlays (built with machinery `stdpatches.With*Image`). When the rollout completes, the base moves to the target and the overlays disappear with no change to the hash. Omni uses the same idea with per-machine upgrade patches.
- **The operator owns the full config.** Anything a human wants is expressed as config documents in one of the three layers. This is what makes the state truly declarative, and it is the opposite of "apply patches on top of whatever is live".

### 6.2 Drift detection
- Every reconcile reads the live `MachineConfigs.config.talos.dev/v1alpha1` resource (this needs `os:admin`; the resource is Sensitive).
- It compares the live config with the desired config **per document** (keyed by `kind`+`name`), with comments removed. The text diff from the server is order-sensitive, so it is not used for this.
- It also checks the `persistent` config ID. Talos only creates it on an apply in the current boot; a node booted from disk has none (`acquire.go:224`). If it exists and differs from `active`, a staged apply is pending, and the operator does not apply anything. A TRY apply changes only `active`, so this check cannot see one in progress. The operator tracks its own TRY applies through `status.operation` (§8.5.1).
- Each TalosMachine records `status.config.appliedHash`: the config the operator last confirmed on the node, or the live config it took over at the handover. That separates the two reasons a node can differ from its rendered config:
  - the spec renders something else than was applied: a **change**, which is rolled out whatever the drift policy;
  - the spec still renders what was applied, but the node runs something else: **drift**. `driftPolicy: Report` sets `ConfigInSync=False` (`Drift`) and leaves the node alone; the redacted diff is in the machine's status. `Enforce` rolls the node back to its rendered config, like any other change.
- A spec edited to say what a drifted node already runs makes that config the new baseline, with nothing applied.

### 6.3 Applying a config change (ConfigRollout)

Implemented in `internal/classify` (what kind of change), `internal/plan` (what may happen next) and `internal/controller/rollout.go` (doing it).

**Classification.** The change from a node's live config to its rendered config is classified per document, and per field for the legacy `v1alpha1` document. A change spanning several classes takes the most severe one. The class and what put the change there are on the machine (`status.config.changeClass`, `changeReasons`).

| Class | Examples | What happens |
|-------|----------|--------------|
| Protected | `machine.ca`, `cluster.etcd.ca`, K8s/aggregator/SA CA docs, `KubeEtcdEncryptionConfig`, `DiscoveryIdentityConfig`, tokens, `KubeClusterConfig`, `HostnameConfig`, `machine.type` | **Blocked** (`RolloutBlocked=ProtectedChange`). Only an explicit, future rotation workflow may change these. |
| Boot-time | installer image / schematic (`UnattendedInstallConfig`), kernel args, system disk encryption | **Blocked** (`RequiresReinstall`) until M4: it becomes a reinstall (upgrade to the same version with the new installer image, §7.3). |
| Unknown | any document kind or field the classifier has no rule for | Requires approval, so a kind Talos adds later is never treated as safe by default. |
| Storage | `VolumeConfig`, `UserVolumeConfig`, `RawVolumeConfig`, LVM, RAID | Requires approval. |
| Connectivity | `BondConfig`, `LinkConfig`, `VLANConfig`, `DHCPv*Config`, resolvers, routes, firewall rules, `KubePrismConfig`, `DiscoveryServiceConfig` | Applied. |
| Control-plane restart | etcd `extraArgs`, control-plane component documents | Applied. |
| Live-safe | kubelet config, labels/taints, sysctls, registries, time | Applied. |

One blocked machine blocks the rollout for every machine: a rollout either applies the whole change or none of it.

**Every applied change is tried first.** Whatever its class, a config is applied with Talos's `TRY` mode and made persistent only after the node was verified from outside and stayed healthy for the soak (§8.5.1). The class decides whether a change is applied, not how. The reason is that the classifier cannot know what breaks a node: bad kubelet arguments stop its heartbeat as surely as a wrong bond does, and a config applied with `NO_REBOOT` that leaves a node unhealthy stays there, with the cluster-health gate then refusing the rollout that would take it back. With a try, a node that does not come out healthy returns to its previous config by itself.

**Approval flow.** For changes that require approval, the operator sets `RolloutBlocked=AwaitingApproval` and `status.approval`: the machines concerned, with the reasons, and the hash of the change. The hash covers every machine of the rollout with the config it runs and the config it is to get. The diff to review is each machine's `status.config.diff`, the operator's own redacted per-document diff. (Talos's dry-run diff is not used for this: it is an unredacted text diff of the whole config.) A human approves by annotating the cluster with `talos.jarensaa.github.io/approve-diff=<hash>`. The approval matches only that exact change: a node that drifts, or a spec that renders anything else, changes the hash.

**Planning.** A rollout is due when a machine's rendered config differs from its live config and from what was last applied (or the drift policy is `Enforce`). The planner (`plan.PlanRollout`) answers with the machines in order, workers before control planes and by name within each group, or with the reason nothing starts, most fundamental first: a protected or boot-time change, `paused`, a TalosMachine whose status has not caught up, an earlier abort (§8.3), a missing approval, the cluster gate. The cluster gate is open when every member was read, every live member has a TalosMachine, all members run, etcd has exactly the control planes as healthy voters, no node has a staged config pending, and Talos's own health checks pass. Anything that could not be determined closes it.

Starting a rollout only takes the lock: `status.operation` records the plan, and for each machine the config it is to leave (`bases`) and the one it is to reach (`targets`). **An operation applies exactly those pairs**, which are what was classified and approved. When a machine's turn comes and it renders, or runs, something else, the operation ends as `Superseded` and the rest is planned afresh.

**Per-node sequence** (§8.5.1 for the protocol and its failure handling):
1. Persist the step: `Trying`, with the previous and desired hashes and the deadline.
2. Wait for the cluster gate; while it is closed nothing is sent. A gate that stays closed for `stepTimeout` is a failure.
3. Render again. Only the planned config is ever sent; a dry run validates it on the node first.
4. Apply with `TRY`.
5. Verify from outside and soak.
6. Confirm, and read back that the config is active and persistent.

A node that already runs its target config when its turn comes is not applied to, but it is verified and soaked like the others: the rollout only moves past a node it has seen healthy.

`hold: true` on a TalosMachine leaves it out. `paused: true` on the cluster starts no rollout and no further node; a node whose config is being tried is still verified and confirmed, because stopping there would itself be a change (Talos would revert it). Setting `config` back to `Observe` ends the rollout at once and writes nothing more: a config that is being tried is neither confirmed nor withdrawn, and Talos reverts it when its window ends.

---

## 7. Workflows

### 7.1 Adoption (R3)

Adoption is a **staged trust ladder**. The operator is installed first and stays reactive: it only reports, while the user adds resources one at a time. Management is enabled per capability once each stage has been proven safe. The design draws on Omni's `cluster import`, including the lessons from its 1.14 bug #3424.

**Ground rules**
- The live cluster is the only source of truth; the files it was built from are assumed stale.
- **The operator never creates or edits the user's `TalosCluster`/`TalosMachine` resources.** They come from the user (often from Git, through a GitOps tool that would fight operator-created objects). The operator only writes `status`, Events, a proposal ConfigMap, and the secrets/backup Secrets.
- Until `management.config: Manage`, the controllers only receive a **read-only Talos client interface** that has no mutating methods. This is a guarantee in the type system, not a runtime check.
- At every stage, a mismatch produces a report and nothing else: no node is ever written to in order to "fix" adoption.

**Stage 0: install**

The CRDs and the operator are installed (Helm chart). With no `TalosCluster` the operator does nothing and holds no Talos credentials.

**Stage 1: observe**

The user enables Talos API access for the operator's namespace on every control plane, with the existing config tooling (one-off, before the operator manages config; adoption then folds it into the control-plane patch layer):

```yaml
apiVersion: v1alpha1
kind: KubeTalosAPIAccessConfig
allowedRoles: [os:admin]
allowedKubernetesNamespaces: [talos-operator]
```

Then the user adds a minimal `TalosCluster`. No credentials are supplied: the operator requests them from Talos (§9).

```yaml
spec:
  management: { config: Observe, upgrades: Disabled, membership: Disabled }
```

The operator then does the following:
1. **Discovery.** Enumerate `Members` and `EtcdMemberList`. For every node, read Version, MachineType, `MachineStatus`, `SystemInformation`, `ImageFactorySchematic`, `Nodename`, and the active and persistent MachineConfig.
2. **Preconditions.** Each one is reported as a condition, and all must hold before stage 3:
   - every member reachable, `running` and ready;
   - etcd healthy, with its member set equal to the CP set;
   - no pending staged config (no `persistent` config, or one equal to `active`);
   - Talos version ≥ 1.13 and ≤ the operator's machinery version;
   - every node has a detectable schematic.
3. **Secrets.**
   - Run `secrets.NewBundleFromConfig(<a CP config>)`.
   - Verify the result against **every** CP config, and check the CA certificates on any workers. Compare public keys, not PEM text.
   - Refuse to continue on AESCBC encryption secrets (generator bug).
   - If `secretsRef` points to an existing Secret, it is **verified, not trusted**: a mismatch is reported.
   - Otherwise the operator writes the bundle to `secretsRef` (no ownerRef; defaults to `<cluster>-talos-secrets`, key `secrets.yaml`) and sets `SecretsBackedUp=False` until the user annotates that they have exported it off-cluster.
   - The annotation is `talos.jarensaa.github.io/secrets-backed-up: <bundle hash>`; the condition message carries the hash. Tying it to the hash means a changed bundle needs a fresh confirmation.
   - `SecretsBackedUp` is informational and never gates management: recovery from cluster loss uses fresh PKI (§12.7), so an off-cluster copy is only break-glass for a running cluster whose every client certificate is lost.
   - `acceptedCAs` and extra etcd encryption keys cannot be represented in the bundle, so they become part of the proposed cluster-wide config.
4. **Proposal.**
   - Infer the generation inputs from live config:
     - contract = the current Talos minor;
     - name and endpoint from `KubeClusterConfig`;
     - Kubernetes version from the kubelet image tag;
     - network options from `KubeNetworkConfig`;
     - installer/schematic from `UnattendedInstallConfig` / `ImageFactorySchematic`;
     - KubePrism, discovery and CNI settings.
   - Regenerate the base config per role and derive `configdiff.Patch(base, live)` for each node.
   - Factor the differences into layers. What is identical across every node goes on the TalosCluster. What is identical across a role goes on that role's pool (one per role, named `<cluster>-control-plane` and `<cluster>-workers`). The rest stays on the machine. Everything is proposed inline; extracting shared parts into TalosMachineConfigs is a refactoring the user can do afterwards, and `ConfigMatchesLive` confirms it renders the same.
   - Identity fields must never appear in derived config. If one does, the reconstruction is wrong: stop and report.
   - Before publishing, render every node from the proposed specs through the same path the controllers use; the result must equal its live config.
   - Write the proposed `TalosCluster` spec plus one `TalosMachine` per live node to the **proposal ConfigMap** `<cluster>-adoption-proposal` (owned by the TalosCluster; keys `taloscluster.yaml`, `talosmachinepools.yaml`, `talosmachines.yaml`, `notes.md`), as YAML ready to commit, with Renovate annotations on the versions. Status points at it through `status.adoption.proposalRef` and `AdoptionProposed`.
   - The ConfigMap is annotated with a fingerprint of its inputs (per-node identity, version and config hash, the bundle hash and `secretsRef`) and is rebuilt only when that changes.
   - Back up all live configs to a Secret (never a ConfigMap; they contain secrets). It is created once, before anything else is written to Talos, and never updated.

**Stage 2: reactive loop**

The user applies the proposed resources, editing them as desired; with GitOps that means committing them to Git, but any tool works (§12.9). For each `TalosMachine`, the operator:
1. Matches it to a live node by `identity.systemUUID`. A machine that matches no live member gets `Matched=False` and is never acted on. Live members with no `TalosMachine` are listed in `status.adoption.unmatchedMembers`.
2. Renders the desired config and compares it with the live config **per document**. Config applies in layer order (cluster, pool, machine), from inline documents, TalosMachineConfigs or Secrets. A missing source is reported as `ConfigUnresolved`. Changes to a referenced TalosMachineConfig, pool or Secret, or to the bundle, re-evaluate the affected machines at once; live changes are picked up by the observe interval.
3. Reports `ConfigMatchesLive=True`, or `False` naming the differing documents, with a redacted per-document diff (live → desired, truncated) in `status.config.diff`. Documents set by a patch from a Secret are named but their diff is withheld, because rule-based redaction only knows credential-named fields.

The user adjusts config until every machine is green.

**Stage 3: proof**

When every live member is matched and the client-side diff is empty for every node, the operator runs the server-side `ApplyConfiguration{DryRun:true}` against each node. Every node must answer "No changes".
- **Gate:** `AdoptionPreconditionsMet` and `SecretsMatchLive` hold, no live member is unmatched, and every TalosMachine is `Matched` and `ConfigMatchesLive`, with status that is current. Current means observed at its generation, and compared against the live config the cluster sees now (`liveHash` and `desiredHash` both equal that config's normalized hash). Stale machine status blocks the proof rather than being trusted.
- **Mode TRY, always.** Talos cancels a pending TRY rollback for every other mode *before* it checks `DryRun` (`v1alpha1_server.go:203-205` in Talos v1.14), so a dry run in `AUTO` or `NO_REBOOT` would disarm the rollback protecting an in-flight change (§8.5). The read-only interceptor refuses any dry run that is not in mode TRY.
- **The server's verdict only.** The server's diff is an unredacted text diff of the whole config (`generateDiff`, `v1alpha1_server.go:323-334`), so the Reader returns only "no changes" or "changes". Anything to show the user comes from the operator's redacted per-document diff. A disagreement (`ServerReportsChanges`) names the nodes, never the diff.
- **Same document order as live.** That text diff is order-sensitive, so the rendered documents are sent in the live config's order. Order carries no meaning to Talos.

The operator then sets `Adopted=True` and records, per TalosMachine, the proven hash (`liveConfigHashes`, where live equals desired) and `provenAt`. While nothing moves, the proof is not re-run.

Any later change to live state (for example a manual `talosctl` edit) or to what the spec renders invalidates the proof (`Adopted=False`, `ProofInvalidated`), and the cluster returns to stage 2. The proof re-runs by itself once the gate holds again. The proof runs only while `config` is `Observe`.

**Stage 4: enable management, one capability at a time**

Admission refuses `config: Manage` unless the stored status shows `Adopted=True`, and refuses `upgrades`/`membership: Enabled` without `config: Manage` (§5.2).

1. **`config: Manage`**
   - **The first reconcile must be a no-op**, because desired equals live; this is asserted as the **handover**. Once the cluster is managed, the adoption proof stops being re-evaluated. So the handover repeats it: the proof's gate, then a fresh Talos dry run on every node (mode TRY, verdict only).
   - **Verified:** `ConfigHandover=True` and phase `Managed`.
   - **Not verified:** `ConfigHandover=False` (or `Unknown` if a node could not be asked), `RolloutBlocked=True` (`HandoverUnverified`), phase `Degraded`, and a Warning Event. No rollout starts in that state. The check repeats every reconcile, so a spec that renders the live config again lifts the block. Returning `config` to `Observe` forgets the handover, so the next `Manage` is verified afresh.
   - **After the handover the spec is the truth.** Every node's live config is recorded as its baseline (`appliedHash`). A spec that renders something else from then on is rolled out (§6.3). A node that changes while the spec does not is drift: reported, and left alone unless `driftPolicy` is `Enforce` (§6.2). `ConfigInSync` says which of the two is the case, and names the machines. `Adopted` keeps its value from the moment management began.
   - Credentials do not change: the operator keeps its Talos-issued certificate (§9). The user keeps their own admin talosconfig off-cluster as break-glass.
   - Validate the apply path with a harmless change first, such as a node label.
2. **`upgrades: Enabled`.** The first real exercise should be a Talos patch release (1.14.0 → 1.14.2).
3. **`membership: Enabled`.** Enable this last, when a node actually needs to be added or replaced.

> Like Omni, the cluster's PKI remains known to whoever held the old talosconfig and secrets. PKI rotation is a later workflow (§10, M9).

### 7.2 Config change (R1)

The flow is: the user edits config (on the cluster, a pool, a machine or a shared TalosMachineConfig) → the render hash changes on the affected nodes → ConfigRollout (§6.3) under the disruption lock, one node at a time, each tried and confirmed (§8.5.1). `ConfigInSync` carries the progress, `status.operation` the step in flight, and `status.history` the outcome.

> **§7.3–7.8 describe milestones that are not built.** They record what was learned from talosctl and from prior art before there was any code, and they are where a milestone's design starts. What each milestone has to deliver is in §10; how is decided when it starts, and these sections are rewritten then.

### 7.3 Talos OS upgrade (R2)

The trigger is a change to `spec.versions.talos`, or to the effective schematic of any machine (a reinstall).

**Pre-rollout checks**
1. `compatibility.ParseTalosVersion(target).UpgradeableFrom(current)` for every node. Talos allows skipping minors when its table permits, e.g. 1.12→1.14.
2. `KubernetesVersion(current k8s).SupportedWith(target Talos)`. If this fails, the Kubernetes upgrade must happen first, so block with a clear message.
3. The target must be within the operator's supported Talos range.
4. Resolve each node's installer as `<factory>/<platform>-installer/<schematic>:<target>`. Verify the schematic exists for that version. **Never** fall back to a vanilla image; that would silently drop extensions.

**Plan.** Workers first (configurable parallelism, default 1), then CPs strictly one at a time. The CP hosting the operator leader and the etcd leader go last.

**Per-node state machine**
(each step is persisted first and is idempotent on resume)

```
Gate        cluster health (ClusterService.HealthCheck, excluding "schedulable" check) + custom gates;
            for CP: EtcdMemberList: not exactly 2 members, all started, no learners; EtcdStatus per member OK;
            all other CPs Ready.  (Re-implements the guard LifecycleService dropped.)
PreHook     optional Jobs (e.g. ceph osd set noout)
Pull        ImageService.Pull(installer)  — before cordon, so drain time is not spent pulling
Install     LifecycleService.Upgrade(installer) → require exit_code == 0
            (failure here is harmless: node still runs old version; mark failed, halt)
Drain       cordon + go-kubernetes nodedrain (PDB-respecting) with drainTimeout; on timeout → halt, leave cordoned
Leadership  CP only: EtcdForfeitLeadership if leader
Reboot      record bootID; MachineService.Reboot(DEFAULT)
WaitBoot    BootID changed AND MachineStatus Running+Ready AND Version == target AND schematic == expected
            (version/schematic mismatch = A/B auto-rollback happened → halt, do NOT retry)
Uncordon    wait for Node Ready, then uncordon
PostGate    same gates as Gate; PostHook
```

The operator never sets `force`. It never writes Talos-owned META keys, which 1.14 rejects (see Omni #3355). It never drains through talosctl as well as itself; tuppr #575 was a double-drain bug.

### 7.4 Kubernetes upgrade (R2)

The trigger is `spec.versions.kubernetes` differing from the observed version. A path that skips minors is decomposed into sequential single-minor hops (`upgrade.NewPath`).

1. **Prechecks**
   - Every node's Talos version supports the target (`SupportedWith`). Prior-art bug: tuppr #544.
   - `go-kubernetes upgrade.NewChecks(path, …).Run()`: removed flags, feature gates, admission plugins, and stored objects at removed API versions.
2. **Pre-pull** the control-plane images on CPs and the kubelet image on every node via `ImageService.Pull`.
3. **Static pods, per component and per CP node:** kube-apiserver → kube-controller-manager → kube-scheduler.
   - Add the image overlay for this node and component, then apply with `NO_REBOOT`.
   - Wait for the matching COSI `k8s.*Config` resource version.
   - Wait for the pod whose `talos.dev/static-pod-config-version` annotation matches the new version to be Ready (5m timeout).
4. **kube-proxy**, only if it is enabled.
5. **kubelet:** control planes, then workers. Overlay the kubelet image, then wait for the `kubelet` service to restart healthy and the Node to report `kubeletVersion == target` and Ready.
6. **Bootstrap manifests:**
   - `Disabled` (default for adopted clusters) does nothing.
   - `Apply` / `ApplyAndPrune` use go-kubernetes `ssa` with Talos's inventory (Talos ≥1.13).
7. **Finish.** Move `k8sBaseVersion` to the target and drop the overlays. The rendered hash is unchanged, so nodes see no change.

### 7.5 Automatic upgrades (R2)

Decision (§12): layer 1 only for v1. Layer 2 is M9 stretch.

1. **Declarative auto-rollout (core, always on).** Any version change in the spec is rolled out automatically, within maintenance windows and behind the gates. Combined with **Renovate on the Flux repo** bumping `versions.talos` and `versions.kubernetes`, this gives automatic upgrades while Git stays the source of truth. This is the recommended route.
2. **Operator-driven version tracking (optional).** With `autoUpgrade.<x>.policy: Patch|Minor`, the operator polls the Image Factory `/versions` endpoint (Talos) and the dl.k8s.io stable channels (Kubernetes). It picks the highest allowed version that passes the compatibility matrix and the operator's supported range, and writes it into **`status.autoUpgrade.target`**, not the spec. The effective target is `max(spec, status.autoUpgrade.target)`. Status is reported. Downside: the cluster diverges from Git.

Both layers share:
- maintenance windows;
- a Talos upgrade and a Kubernetes upgrade never run concurrently (the single disruption lock);
- Talos goes first when the new Kubernetes version needs it, and Kubernetes goes first when the new Talos version drops support for the current one;
- fail-closed: after a failed rollout, nothing automatic happens until a human acks (§8.3).

### 7.6 Expanding the cluster (R4)

The user creates a `TalosMachine` with `poolRef`, `address` and `identity.systemUUID`, for a machine booted into maintenance mode. The pool supplies the role and the shared config, so a node that differs from its pool in nothing needs no config of its own.

1. **Probe** with an insecure maintenance client (`client.WithMaintenanceMode`). Require `MachineStatus.stage == maintenance`.
2. **Verify identity.** Read `SystemInformation` over the same TLS session. If the UUID does not equal `spec.identity.systemUUID`, refuse and set `AwaitingIdentity`. Also record the disks (`storage.Disks`) and links (`LinkStatus`). The disk selector must match exactly one disk, which catches Talos's "first match wins" behaviour. Every link named in the rendered config must exist.
   - If `identity` is empty, the operator only reports what it found (UUID, serial, disks, links) in status. It **never applies config to an unpinned machine**: the config carries every cluster secret, and maintenance mode is TOFU.
3. **Gates.**
   - Workers: cluster healthy.
   - Control planes: take the disruption lock; etcd healthy with all members started and no learners; **the rendered hostname must not collide with an existing etcd member** (Talos would evict that member: `etcd.go:315`).
4. **Apply** the rendered config with `ApplyConfiguration`. Expect `Unavailable`/`Canceled` afterwards while apid restarts with mTLS.
5. **Wait for install.** Switch to the mTLS client. Wait for `UnattendedInstallStatus == installed`, the reboot if the config requests one, and then `MachineStatus` Running+Ready.
6. **Join.**
   - Control planes: the etcd member appears, then the learner is promoted (`IsLearner=false`). Then the etcd consistency check and CP static pods Ready.
   - Workers: kubelet healthy.
7. **Done.** The Node is Ready, then gates pass. Phase becomes `Ready`.

**Concurrency:** CP additions go one at a time (etcd allows one learner). Worker additions may run in parallel and are non-disruptive, so they do not take the lock. The design note "3→5 voters in two serialized steps" is enforced by the planner.

**Stretch alternatives** that remove TOFU:
- serving configs via `talos.config=` URLs keyed by UUID (pull model);
- SideroLink.

### 7.7 Removal and replacement

`desiredState: Removed` triggers removal. Deleting the CR without that is refused unless it is explicitly orphaned.

1. **Gates.** For a CP: the remaining voters must number at least 3, unless `…/allow-reduced-quorum` is set. Remaining members must be healthy. For a **replacement**, the planner requires the new CP to be `Ready` before the old one is removed.
2. **Drain.** Cordon, then drain with a bounded timeout. Ceph gates and hooks run first; the OSD out/purge is a hook, not built in.
3. **Leave.** For a CP: forfeit leadership, then graceful `Reset` (which leaves etcd itself). Wipe only `STATE` and `EPHEMERAL`, **never user disks unless they are listed explicitly**.
   - If the node is dead, use `EtcdRemoveMemberByID` from a healthy peer, gated by `…/node-is-dead` confirmation.
4. **Clean up.** Delete the Kubernetes Node object and remove the finalizer.

### 7.8 Greenfield bootstrap (lower priority, M9)

1. Machines are pinned as in §7.6. The bundle is generated once with `secrets.NewBundle(clock, contract)` and stored with no ownerRef.
2. Apply configs to all CPs.
3. Bootstrap **one** CP, chosen deterministically. Persist `status.bootstrapped=true` **before** the call, and treat `AlreadyExists` as success.
4. Wait for etcd and the API server, then hand over to normal reconciliation.

This only makes sense from a management cluster (or a CLI), since the cluster does not exist yet.

---

## 8. Safety model

### 8.1 Invariants
These are enforced in `internal/plan` and exhaustively table-tested.

1. At most **one disruptive operation** per cluster: `status.operation` is the lock.
2. At most **one CP** is not Ready or not a healthy etcd member at any time.
3. etcd membership never goes 3→2 without an override. There is never more than one learner, and never two membership changes at once.
4. No disruptive step starts outside a maintenance window. An in-flight node step started inside a window finishes, but the next node waits.
5. No disruptive step starts while `paused`, while `operation.failure` is set, or while any gate is false.
6. No mutation happens for a capability that is not enabled in `spec.management`. Before `config: Manage`, controllers only hold a read-only Talos client interface, so writes are impossible by construction.
7. Protected config fields never change.
8. The operator never applies config to a machine whose identity is not pinned.

### 8.2 Bounded everything
Every wait has a timeout: drain, pull, install, reboot, static pod, kubelet, etcd promotion. A timeout is a failure, never a skip.

Observing is bounded too, because a rollout's deadlines are only as good as the loop that watches them:
- every read of a node while observing has 10s, not the Talos client's 30s for calls that do work;
- a node that does not answer is not asked again in that pass. Every further read would wait for its own timeout;
- all nodes of a cluster are read within one budget (1m). Nodes not reached within it count as not read.

A node that was not read is unknown to every gate, so a slow or partial pass closes gates and never opens one.

### 8.3 Fail-closed, with escape hatches
This avoids the CACPPT #262 permablock.

- On failure, `status.operation.failure` is set with the machine, the step, a reason, a message, a suggested remediation and a **token**. The operation keeps the lock and does nothing more: no later node is touched. `RolloutBlocked=OperationFailed` and a `RolloutHalted` Event are emitted, and the phase is `Degraded`.
- Human actions, all via annotations on the TalosCluster whose value is the failure's token. The token names one failure, so an annotation left behind from an earlier one never acts on the next. The remediation text spells the three annotations out.
  - `talos.jarensaa.github.io/retry=<token>`: begin the failed machine's step again, from what the node runs now.
  - `talos.jarensaa.github.io/skip-machine=<token>`: leave the failed machine out and continue. Its rendered config is recorded as skipped and is not rolled out to it again; a spec that renders something else is.
  - `talos.jarensaa.github.io/abort=<token>`: end the operation and leave nodes as they are. What was left of it is recorded in `status.abortedTargets` and is **not started again**; otherwise the same rollout would begin anew a moment later. A spec that renders anything else for a machine, a revert included, is a new change: it rolls out and clears the record.
- Gates that can never become true are reported along with what is false. They are never silently bypassed.

**Known gap: repairing an unhealthy node.** The cluster gate requires a healthy cluster before any node is touched, the node about to be changed included. A config that passes its soak and breaks the node later therefore cannot be taken back by a rollout: the revert is planned, and waits for the health the revert would restore. Until the gate learns to leave the target node's own health out of the question (bead `talos-rlw.9`, with the built-in gates of M4), that node has to be repaired by hand.

### 8.4 Self-eviction
Covered in §4.2: 2 replicas, steps persisted before side effects, resume keyed on observed facts.

### 8.5 Rollback strategy

**Principle:** a self-managed cluster can only be safe if **losing contact with a node results in a rollback**, not in silence. Rollback should be performed by Talos itself, which needs no network, kubelet or pods. A change is only kept after an observer outside the node has positively confirmed it.

#### 8.5.1 Config changes: TRY-then-confirm

How Talos 1.14 implements `TRY` (`T/internal/app/machined/internal/server/v1alpha1/v1alpha1_server.go:181-311`, `T/internal/app/machined/pkg/runtime/v1alpha1/v1alpha1_runtime.go:92-108`):
- The new config becomes **active only**. The persistent config keeps the previous version.
- An in-memory timer in machined (`try_mode_timeout`, default `ConfigTryTimeout` = 1m) restores the previous config when it fires.
- Any subsequent non-TRY apply cancels the timer, **before** that apply is parsed or validated, and even if it is a dry run. Dry runs are therefore always sent in mode `TRY`.
- A reboot or machined crash inside the window boots from the persisted (old) config, so that also reverts.
- **"Previous" means whatever is active when the try arrives.** `RollbackToConfigAfter` captures the active config and replaces any earlier timer. A second `TRY` of the same config would therefore make the tried config its own rollback target, and the first timer would be gone.
- `TRY` and `NO_REBOOT` accept the same configs; an invalid one is answered with `InvalidArgument`, and a concurrent apply with `FailedPrecondition`.

The operator applies **every** config change this way (§6.3). The protocol for one node:

1. **Persist** `operation.current = {machine, step: Trying, previousHash, desiredHash, deadline}`. The deadline is `now + tryTimeout` (default 5m). Nothing is sent in the reconcile that persists a step: every side effect happens in a later one, after reading the TalosCluster back from the API server and finding it unchanged. A replica that takes over, or a controller working from a stale cache, therefore never acts on a step it has not seen persisted.
2. **Try.** Only while the node is reachable and its active config is still `previousHash`. That is the one observation under which no try can be pending, and it is what rules out the double try above. The config is validated with a dry run first, then applied with `TRY` and a timeout of *deadline minus now*, so that Talos's own timer always ends at the persisted deadline or later.
3. **Observe** the config active (`activeHash == desiredHash`), then persist `Verifying`. A try whose answer was lost is recognised here and never sent again.
4. **Verify from outside.** All of these must hold:
   - The node's Talos API answers when reached through the endpoint of a **different** control plane (`WithNode`), and its hardware identity is the pinned one. The node being changed is never an endpoint. (A cluster with a single control plane has no other way in for that node.)
   - Every member runs, etcd has exactly the control planes as healthy voters, and Talos's own health checks pass.
   - Kubernetes reports the Node `Ready`, with a network.
   - The node's kubelet has renewed its Lease in `kube-node-lease` since verification began, and within the last 40s. `Ready` alone is the control plane's last word on a node and stays True for most of a minute after it went dark.
   - Custom gates (M4).
5. **Soak.** The checks must hold without interruption for `soak` (default 2m); a failing check starts the soak over.
6. **Confirm** by applying the node's active config again with `NO_REBOOT`, which makes it persistent and stops the timer. A confirm is only sent while more than 30s of the window are left, and the call is bounded to half of that: a confirm that arrived after Talos had already reverted would apply the config with no way back.
7. **Read back.** The active *and* persistent config hashes must equal `desiredHash`. Only then is the machine done and its `appliedHash` updated.

If the node is not verified in time, the operator does **not** confirm.
- It stops hoping 30s before the deadline (`RollingBack`), waits until 30s after it, then requires the node to be reachable on `previousHash`.
- It then marks the operation failed with reason `RolledBack` and halts (§8.3) before any other node is touched.
- A standby replica that takes over mid-protocol resumes from the persisted step and follows the same rules: what to do next is decided from the step and what the node reports, never from memory.
- A confirm that did land, but whose answer was lost, is recognised by the read-back even after the deadline.
- If the node does not come back on `previousHash` within `stepTimeout`, the failure is `NodeUnreachableAfterRollback`. This needs a human, because Talos's revert is only logged if it fails (`config rollback failed`).

Other failures of a node step: `Timeout` (the cluster gate stayed closed, or a node that needed no change was not verified, for `stepTimeout`), `ConfigChangedUnderneath` (the node runs neither the previous nor the desired config), `Rejected` (Talos refused the config; nothing was applied if the dry run caught it). What Talos says about a config is withheld when a config source is a Secret, because validation errors can quote values.

Waiting for a closed gate never lets a persisted deadline lapse into a rollback: while the node is still on its previous config before the deadline, the deadline is moved (`Rearm`). Past the deadline the same observation could be a try that was reverted, so it is treated as one.

What this does not cover:
- A node booted from disk has no `persistent` config resource, so a try that someone else left pending on it looks like a settled config. The operator's own tries are tracked in `status.operation`; a foreign one reverts within its window and is then reported as drift, or as `ConfigChangedUnderneath` if a step was in flight.
- The guarantee that Talos's timer outlasts the persisted deadline assumes that a `TRY` call which timed out on the operator's side is not carried out by the node much later.

#### 8.5.2 Talos OS upgrades: A/B boot slots

- `LifecycleService.Upgrade` writes the **inactive** slot without rebooting. An install failure is harmless: the node keeps running the old OS, and drain/reboot only happen after a successful install.
- If machined's boot sequence fails fatally on the new slot, it reverts the bootloader to the previous slot and reboots (`T/internal/app/machined/main.go:101-115`, `revert.go`).
- After the upgrade reboot, the operator requires **boot ID changed, version == target, and schematic == expected**. A version or schematic mismatch means Talos already fell back on its own. That is a failure: halt and do not retry.
- `MachineService.Rollback` switches back to the previous slot manually. It works until the next upgrade overwrites that slot. It is exposed as an explicit, human-triggered action (annotation), never automatic.
- Limits:
  - A hang before machined starts (kernel, early boot) triggers no automatic fallback.
  - Once the node is Running and Ready, Talos drops its fallback marker (`DropUpgradeFallbackController`). The operator must not write Talos-owned META keys to change this; 1.14 rejects such writes (see Omni #3355).

#### 8.5.3 Kubernetes upgrades: fail forward, contain the blast radius

- `talosctl upgrade-k8s` has no rollback, and Kubernetes does not support downgrading across a minor version (etcd storage versions).
- Safety therefore comes from strict prechecks (§7.4) and moving one component on one node at a time.
- If a step fails, the operator reverts **only that node's** image overlay to the previous patch version of the same minor, then halts. Nodes already upgraded stay on the new version, which is within the supported version skew.

#### 8.5.4 Last resort: etcd snapshot

Before every control-plane operation, take an etcd snapshot and ship it off-cluster (M11, §14.6; manual until then). This is the only recovery from quorum loss. Recovery itself is out-of-band (`talosctl bootstrap --recover-from`).

#### 8.5.5 Rejected alternative: node-local rollback agent (DaemonSet)

We considered running a DaemonSet that watches local connectivity and reverts the node's config itself. It was rejected for v1:
- **Credential blast radius.** Reverting needs `os:admin` on every node. A single compromised node would then hold credentials able to rewrite the whole cluster.
- **It depends on what just broke.** In-cluster Talos API access goes through the `talos.default` Service, i.e. over the CNI. A host-network pod calling localhost avoids that, but still needs kubelet and containerd to be healthy. machined's TRY timer needs none of this.
- **Partition ambiguity.** From inside, a node cannot tell "my network is broken" from "the rest of the cluster is down". Acting on a local view risks reverting good changes during a partition, and flapping. Reachability has to be judged from outside, with "no answer" defaulting to rollback, which is what TRY provides.

**Residual gap:** a failure that appears *after* confirmation, e.g. a DHCP renewal failing hours later. At that point the bad config is persisted, and recovery needs a console or a maintenance-mode reboot. A longer soak narrows this gap but cannot close it. A narrowly scoped last-known-good watchdog is listed under M9 for re-evaluation, not built.

---

## 9. Secrets and credentials

| Object | Contents | Owner/lifecycle |
|--------|----------|-----------------|
| `<cluster>-talos-secrets` | secrets bundle (`secrets.yaml` format) | Operator-created during adoption (or user-supplied, then verified). Needed to render configs. **No ownerRef**, so deleting the TalosCluster never deletes the PKI. An off-cluster copy is optional break-glass (§12.7). |
| Operator Talos client cert | Requested through a `talos.dev/v1alpha1` `ServiceAccount` `<cluster>-talos-operator` (roles `[os:admin]`, owned by the TalosCluster) in the TalosCluster's namespace. Talos answers with a Secret of the same name, key `config`. | Issued by Talos from the cluster's current CA: 6 h TTL, renewed 1 h before expiry (`kubeaccess/serviceaccount/crd_controller.go:49-50,90` in Talos v1.14). Nothing is stored outside the cluster, so it survives a rebuild with fresh PKI. Needs a `KubeTalosAPIAccessConfig` on the control planes allowing `os:admin` for the namespace; Talos then installs the CRD (`controllers/k8s/manifest.go:264`). The operator reads the CRD unstructured and never watches it, so a missing CRD is reported (`TalosAPIAccessDisabled`) instead of breaking startup; Talos's `status.failureReason` is reported as `TalosAPIAccessDenied`. The issued talosconfig names the in-cluster Service `talos.default`, which the operator never dials: endpoints are the control-plane Nodes' InternalIPs (§4.2). |
| Supplied talosconfig (`credentials.source: Secret`) | An admin talosconfig whose endpoints are node IPs | Alternative for clusters where workload API access cannot be enabled. Expires with its certificate (talosctl's default is 1 year); `CredentialsValid` reports the expiry. |
| `<cluster>-talosconfig` / `-kubeconfig` (optional) | Short-lived admin credentials for humans | Opt-in, refreshed on a timer. |
| `<cluster>-adoption-backup` | Live configs captured during adoption | Created once and kept. |
| `<cluster>-state` (considered) | Copy of critical `status` (bootstrapped, operation) with no ownerRef, so a CR re-create does not re-bootstrap | Prior art: alperencelik. M9. |

Other rules:
- Rendered configs, dry-run diffs and Events are **redacted**: secret-bearing fields are replaced by a hash. Diffs of documents that take values from a Secret-sourced patch are withheld entirely, and so are render errors when such a patch is involved.
- Configs are never logged.
- RBAC: the operator reads Secrets and writes `talos.dev` ServiceAccounts only in its own namespace. Anyone who can create a `talos.dev` ServiceAccount there obtains the allowed Talos roles, so that namespace is dedicated to the operator.
- An off-cluster copy of the bundle is optional. `SecretsBackedUp` reports whether one is confirmed, and never blocks.

---

## 10. Milestones

**How to read this section.** A milestone says what someone can do once it is done, what it leaves out, and how we will know it is done. It does not say how. The design of a milestone is written when the milestone starts, against the Talos source and this codebase as they are then, and it lands in this document together with the code, the way §6.3 and §8.5.1 did for M3.

§7.3–7.8, §14 and the package list in §4.1 were written before there was any code. For everything not built yet they are notes to start from, not commitments. M3 already departed from its notes where reading Talos's source showed a better way (§12.11, §12.12). Where those sections disagree with this one or with the code, this section and the code are right.

Task beads under a milestone are cut when the milestone starts, for the same reason.

### Delivered

| M | What it gave | Release |
|---|--------------|---------|
| **M0 Foundations** | Project, CRDs, the Talos access layer, a fake Talos, the QEMU e2e harness | v0.1.0 |
| **M1 Observe** | Read-only status of a cluster and its machines, with writes impossible by construction | v0.1.0 |
| **M2 Adoption** | A proposal that reproduces a running cluster, proven by Talos's own dry run to change nothing, then a gated switch to `config: Manage`; pools and shared configs; Talos-issued credentials | v0.2.0, v0.3.0 |
| **M3 Config management** | Classified config changes rolled out one node at a time, tried and confirmed only after verification from outside; drift reporting and enforcement; fail-closed with retry, skip and abort | v0.4.0 |

### What every remaining milestone builds on

- **One safety model.** A new kind of operation is a new planner and executor on the machinery M3 built: the one lock, a step persisted before its effect, decisions from what is observed, bounded waits, halt on failure with token-bound escape hatches, and "not read" never counting as "fine". No milestone brings its own.
- **One switch per capability.** Each new kind of write is off until its `spec.management` switch is on.
- **Four levels of proof.** Planner invariants as unit tests; controller behaviour against the fake; the real thing on QEMU; and, last, a stated check on a real cluster, run by its owner. A milestone is not done before the fourth.

### The real cluster

Not a milestone, and not blocked on one: adopting a real cluster, switching it to `config: Manage`, and rolling out one harmless config change. It needs the adoption runbook. It is the only place the operator meets a real CNI, storage on the nodes and real hardware, so what it turns up feeds every milestone below, and M4 should not be called done on QEMU alone.

### M4 Talos upgrades

Three slices, in this order.

**M4a: run as deployed.** So far the e2e runs the controllers inside the test process. After this slice the chart's image runs in the test cluster with two replicas, and a rollout that takes down the node its leader runs on is finished by the other replica. That is the clause of M3's acceptance not yet shown. It also closes the known gap of §8.3: a rollout can change a node that is already unhealthy, so a bad config that was confirmed can be taken back.

**M4b: upgrades.** Changing the Talos version in the spec, or the installer a machine is built from, takes every node there one at a time, draining it first. A node that does not come back as expected halts the rollout, and Talos's own fallback to the previous boot is recognised for what it is. Config changes that only take effect through a reinstall, refused today (`RequiresReinstall`), roll out the same way.
- In: refusing up front what cannot work (a jump Talos does not allow, a version this operator does not support, an installer that does not exist); workers before control planes, control planes one at a time with etcd never at risk; drains that respect disruption budgets and time out; retry, skip and abort as in M3.
- Out: Kubernetes versions (M5); learning that a new version exists (M6).

**M4c: rollout policy.** Maintenance windows, gates the user defines on objects in the cluster (Ceph health, for one), and hooks before and after a rollout or a node. These apply to config rollouts as well. On a cluster with storage on its nodes this slice is what makes M4b safe to use, so a real cluster gets M4b and M4c together.

- **Done when:** a multi-node QEMU cluster goes through a patch upgrade with its workloads up; the operator upgrades the node it runs on and carries on; an injected failure halts with the node still cordoned, and a retry resumes; an installer change reinstalls; a real cluster takes a patch release inside a window, behind a storage-health gate.
- **To decide:** where three-control-plane scenarios run (see below); whether downgrades are refused outright; whether workers may go more than one at a time yet.

### M5 Kubernetes upgrades

Changing the Kubernetes version in the spec upgrades the control-plane components and then the kubelets, node by node.
- In: one minor version at a time, with a larger jump taken as a sequence; refusing up front an upgrade that would strand stored objects or use removed flags, and one the running Talos version does not support, in either direction, with a message that says which to upgrade first; halting on a component that does not come up.
- Out: anything Talos does not run itself (CNI, CSI, workloads); rolling a Kubernetes upgrade back. A failed upgrade halts and is fixed forward.
- **Done when:** a minor upgrade passes on QEMU; an object stored at a removed API version blocks the upgrade before any node is touched; so does a Talos version too old for the target; a real cluster takes a minor release.
- **To decide:** who owns the manifests Talos applies at bootstrap (kube-proxy, CoreDNS) on an adopted cluster. Today's answer is nobody, and that may be the right one.
- Needs M4a and M4b.

### M6 Version awareness and automatic upgrades

Status says which versions exist and which sequence of upgrades leads to them. A version bump that cannot work is refused before it reaches the cluster, by a check that runs in CI, and again by the operator. With a bot proposing bumps to the repository that holds the spec, upgrades happen without anyone picking versions.
- In: working out the order of Talos and Kubernetes steps; the check as a command; reporting what is available; an example bot configuration.
- Out: the operator changing the desired version by itself (M9). Requiring Git or Flux: the check and the bot configuration are optional tooling around an operator that still only acts on its objects (§12.9).
- **Done when:** a bump whose prerequisite step has not landed is rejected with the reason; a valid one rolls out inside the window and not outside it; the check fails a pull request that names an installer which does not exist.
- Needs M4 and M5.

### M7 Adding machines

Declaring a machine that waits in maintenance mode makes it join the cluster, as a worker or a control plane, once its hardware is the hardware that was declared.
- In: refusing a machine whose identity does not match, or is not declared, without sending it anything; checking that the rendered config fits the machine before sending it; control planes joining one at a time, with etcd whole in between; the `membership` switch.
- Out: getting the machine to boot; finding machines by itself (M9).
- **Done when:** a worker is added on QEMU; the control plane grows; a machine with the wrong identity is refused and received nothing.
- **To decide:** whether the identity pin is enough. The config carries every cluster secret, and a machine in maintenance mode is whoever it says it is. §12.3 says the pin suffices for v1; this is the milestone to revisit that in.
- Needs M3 only. It can move ahead of M4–M6 if a node is needed sooner.

### M8 Removing and replacing machines

Marking a machine as removed takes it out of the cluster cleanly. A control plane is replaced without the cluster ever having fewer healthy voters than it started with. A machine that is already dead can be removed after an explicit confirmation.
- In: draining; keeping etcd whole and refusing a removal that would not; cleaning up what Kubernetes still holds of the node; a stated rule for what is wiped on the machine.
- Out: taking storage out of service. What runs on the node (Ceph OSDs) stays with the user and the hooks of M4c. Disks other than Talos's own are never wiped unless named.
- **Done when:** a control plane is replaced on QEMU with etcd healthy throughout; a removal below three voters is refused; a dead worker is removed after confirmation.
- Needs M7 for replacement, M4 for draining, and the snapshots of M11a.

### M10 Observability

**M10a: the operator's own state.** Metrics for what the operator already knows: a rollout in flight or halted, drift, nodes it cannot read, etcd and node state. With alerts and a dashboard in the chart. It depends on nothing after M3 and should come before upgrades are trusted on the real cluster: a halted rollout should reach someone.

**M10b: the cluster's own metrics and logs.** Exposing etcd and control-plane metrics, and shipping Talos's logs, are config changes that a user can declare since M3.
- **To decide:** whether this is an operator feature at all (the earlier plan was a `spec.observability` profile) or documented config that the e2e proves. The second is less to own.

- **Done when:** a halted rollout raises an alert on QEMU; the dashboard shows a real cluster.

### M11 Maintenance

**M11a: etcd snapshots.** Taken on a schedule, stored off the cluster, and taken before any operation that touches a control plane. This is the safety net under M4, M5 and M8, so it comes before those run on the real cluster. It is the first thing that needs a secret from outside the cluster (§12.6).

**M11b: the rest.** Defragmenting etcd and handling its alarms; a support bundle attached to a failed rollout; keeping human credentials fresh.

- **Done when:** a snapshot restores a QEMU cluster; a fragmented etcd is defragmented with the cluster healthy between members; a failed rollout has a bundle.

### M9 Later, unscheduled

Bootstrapping a cluster from nothing; rotating the cluster's PKI; a management-cluster mode; discovering machines instead of declaring them; the operator choosing versions; a command-line tool that runs the same planners; a watchdog for failures that appear after a change was confirmed.

Two of these have a reason to be promoted: PKI rotation, because adoption leaves the old PKI with whoever held it before, and the command-line tool, because an operator cannot repair the cluster it lives in.

### Owned by no milestone, and needing a decision

- **Where multi-control-plane tests run.** CI has one control plane and one worker, which is what a standard runner fits. Serialising control planes (M4b), growing them (M7) and replacing one (M8) cannot be shown there. A larger runner, a self-hosted one, or a nightly run on other hardware: this has to be settled before M4b.
- **Upgrading the operator itself.** The API is `v1alpha1` and may change between releases, but nothing says how an installed operator and its stored objects get from one release to the next, and Helm does not upgrade the CRDs a chart ships. This needs a rule before a real cluster depends on it.
- **Scale.** Nodes are read one after the other and workers roll one at a time. Both are right for a handful of nodes and wrong for a hundred.

### Suggested order

1. The real cluster, alongside everything else, at its owner's pace.
2. M4a, because every later milestone is tested through it.
3. M10a and M11a: small, independent, and wanted before upgrades touch a real control plane.
4. M4b with M4c, then M5, then M6.
5. M7, then M8.
6. M10b, M11b, and whatever of M9 has earned it.

R1 is covered by M1–M3, R3 by M2, R2 by M4–M6, and R4 by M7.

### Testing strategy
- **Pure unit tests** for planners (invariant tables), render/diff/patch derivation (golden files from real-shaped multi-doc configs with throwaway secrets), and compatibility ordering.
- **envtest** controller tests against the fake Talos client: step persistence, resume after restart, fail-closed behaviour.
- **e2e on QEMU** (`make e2e-cluster-up`, see `docs/dev/e2e.md`). The Docker provisioner is unusable for Talos 1.14 on Docker Desktop: container mode needs a host kernel ≥ 6.15 and Docker Desktop ships 6.12. It also cannot exercise upgrades, because `LifecycleService` refuses container mode. QEMU on macOS needs root, so the harness runs only `talosctl cluster create/destroy` under sudo. The `maintenance` preset lets the harness apply configs built with `gen config` and patches, and leaves spare nodes for expansion tests.
- **Staged rollout to a real cluster:** each milestone's last check runs there (§10), with a fresh off-cluster etcd snapshot beforehand, taken by hand until M11a takes it.

---

## 11. Risks

| Risk | Mitigation |
|------|------------|
| Talos API churn: 1.13 and 1.14 both reshaped upgrade and config; legacy Upgrade is removed in 1.18 | Access layer isolates machinery. Supported-range gate per operator release. Track Talos minors. |
| Rendering parity with talosctl defaults; Omni's import broke on exactly this with 1.14 (#3424) | The adoption exactness proof is mandatory. Golden tests against `talosctl gen config` output per contract. |
| Quorum loss on a 3-CP cluster during an operation | Strict CP serialisation, etcd gates, never `force`. Off-cluster etcd snapshot before CP ops (M11; manual until then). DR stays out-of-band. |
| Operator evicting itself mid-step | Persist before side effect, resume by observation, 2 replicas. |
| TOFU secret disclosure on node add | Mandatory UUID pin. Pull-config / SideroLink later. |
| Ceph data loss or unavailability | Ceph CEL gate, noout hooks, never wipe user disks, one node at a time. |
| A PDB-blocked drain hangs | Bounded `drainTimeout` leads to fail-closed. |
| Bootstrap manifest sync pruning Flux-owned objects | `manifestSync: Disabled` by default for adopted clusters. |
| PKI known to previous credential holders after adoption | Documented. Rotation workflow in M9. |
| machinery requires go 1.26.5 plus `replace` directives | Pin the toolchain in go.mod. Verify the build in M0. |
| Network change that breaks a node is confirmed anyway | Verification goes through a different node's apid and needs a fresh kubelet heartbeat, plus a soak period inside the TRY window. Read back active and persistent hashes after confirming (§8.5.1). |
| A change of a "safe" class breaks a node | Every change is tried, whatever its class, so the node reverts by itself (§6.3). |
| A confirmed change breaks a node later, and the health gate refuses the rollout that would revert it | Known gap (§8.3); the node is repaired by hand until the gate leaves the target's own health out. |
| Failure that appears after a confirmed change (e.g. DHCP renewal hours later) | Not covered by TRY; needs console or maintenance-mode recovery. Longer soak; last-known-good watchdog re-evaluated in M9 (§8.5.5). |
| Early-boot hang after an OS upgrade (before machined starts) | No automatic A/B fallback in that case. One node at a time keeps quorum; manual recovery via console (§8.5.2). |

---

## 12. Decisions

Decided 2026-10-08:

1. **Topology: self-hosted first.** The operator runs inside the cluster it manages. The core is written against a target-cluster abstraction so that management-cluster mode can be added later (M9).
2. **Automatic upgrades = Renovate bumps Git.** The operator rolls out spec version changes automatically, within maintenance windows and behind gates. Operator-side version tracking (§7.5 layer 2) moves to M9 stretch, and `autoUpgrade` is dropped from the v1alpha1 API.
3. **New-node trust: mandatory UUID pin** in `TalosMachine.spec.identity`.
4. **Config ownership: full render.** The operator owns the complete config, and everything human-authored is config documents layered onto the generated defaults (§6.1).

5. **API group and module path** (decided 2026-10-09):
   - Go module `github.com/jarensaa/talos-operator`;
   - API group `talos.jarensaa.github.io`, version `v1alpha1`;
   - `TalosCluster` and `TalosMachine` are **namespaced**: `credentials.secretRef` and `secretsRef` are same-namespace Secret references.
   - Annotation keys use the same prefix, e.g. `talos.jarensaa.github.io/orphan`.
6. **Secrets from outside the cluster are the owner's to deliver** (decided 2026-10-09). The operator reads them from Secrets and does not care how they got there. Since 7, no Talos credential or PKI is among them; the first one needed is the storage credential for etcd snapshots (M11).
7. **Talos credentials are issued in-cluster** (decided 2026-10-10). Recovery from cluster loss is a rebuild with fresh PKI, so any talosconfig or bundle kept outside the cluster would be stale. The operator requests its certificate from Talos through a `talos.dev` ServiceAccount (§9). This replaces the user-supplied, SOPS-delivered talosconfig and the operator-minted certificate.
8. **Admission policies, not a webhook** (decided 2026-10-10). The gates that need the stored object or a DELETE are ValidatingAdmissionPolicies (GA since Kubernetes 1.30). A webhook served by the operator, with `failurePolicy: Fail`, would block every TalosCluster/TalosMachine write whenever the operator's pods are down, which in a self-hosted cluster includes the drains it orchestrates. It would also need cert-manager for its serving certificate. Uniqueness across machines, which admission cannot see, stays a fail-closed controller check.
9. **Delivery-agnostic** (decided 2026-10-10). The operator acts on the TalosCluster and TalosMachine objects as they are, however they were created or edited: Flux, kubectl, or anything else. It never inspects field managers, GitOps labels or the source of an object, and its gates (preconditions, proof, admission) depend only on the objects and the live cluster. Keeping the config in Git is the user's workflow, not something the operator enforces.
10. **Pools and reusable configs** (decided 2026-10-10). This supersedes v1's "two kinds". Talos nodes are immutable and identical within a role apart from a few facts, so scaling to 100 nodes means sharing config, not repeating it.
   - `TalosMachinePool` holds a role and the config its members share. Every machine belongs to exactly one pool, by `poolRef`.
   - `TalosMachineConfig` holds documents that any cluster, pool or machine can reference.
   - Membership is an explicit reference, not a label selector, so a machine is never in two pools and the layer order is never ambiguous.
   - Config is declared as documents layered onto the generated defaults, not as a complete config. Talos's defaults are never maintained by hand, secrets are generated from the bundle, and new defaults arrive with the version contract.
11. **Every config change is tried** (decided 2026-10-10, with M3). The first design applied only connectivity changes with `TRY` and everything else with `NO_REBOOT`. That makes safety depend on the classifier knowing what can break a node, and leaves a node broken by a "safe" change with no way back that the operator would take (§6.3). The class now decides only whether a change is applied and whether it needs approval. The cost is that every node takes at least the soak, also for a label.
12. **Approvals and escape hatches name what they act on** (decided 2026-10-10, with M3). An approval carries the hash of the change, and `retry`, `skip-machine` and `abort` carry the token of the failure. An annotation is a standing instruction, and Git keeps it around; bound to a hash or token it can only ever mean the thing a human looked at.

## 13. Prior art summary

| Project | License | Take | Avoid |
|---------|---------|------|-------|
| Omni (incl. `cluster import`) | BSL 1.1: ideas only | Import flow (backup → diff into patches → locked until unlock → tainted until PKI rotation), per-machine upgrade patches, cluster lock | Its import breaks on 1.14 multi-doc configs (#3424) |
| CABPT / CACPPT (community-maintained since 2026-08) | MPL-2.0 | Annotate before etcd-leave, orphan-member audit, contract pinning | Immutable replacement for bare metal; gates with no escape (#262); unbounded drain (#258) |
| tuppr | AGPL-3.0: ideas only | CEL gates, pre/post hooks, maintenance windows, image pre-pull, schematic-preserving image resolution | Double drain (#575); not enforcing compatibility (#544) |
| RTLDeutschland/talos-operator | MIT | Fail-closed automation, `appliedConfigHash`, PDB-respecting drain | Imperative `spec.operation` |
| alperencelik/talos-operator | Apache-2.0 | Periodic dry-run drift detection, state Secret with no ownerRef | — |
| cozystack/talm | Apache-2.0 | Pre-apply link/disk existence checks, post-upgrade version check for silent A/B rollback | Vendoring `cmd/talosctl` |

---

## 14. Day-2 operations

> **Not built.** This section is the earlier plan for M6, M10 and M11. §10 says what those milestones have to deliver, and leaves open some of what is decided here (whether cluster metrics are an operator feature, for one).

Beyond R1–R4, a self-managed cluster needs four things:
- the ability to see what Talos is doing;
- knowing which upgrades are safe to take;
- a way for new versions to arrive without hand-editing;
- routine upkeep, so that problems are prevented rather than reacted to.

This section scopes those. Findings are verified against the Talos 1.14.0 source and the live Image Factory API (2026-10-08).

### 14.1 Assumptions about the cluster's surroundings

| Area | Assumed | Consequence |
|------|---------|-------------|
| Metrics | A Prometheus Operator stack with Grafana may be present | The chart can ship a `ServiceMonitor`, a `PrometheusRule` and a dashboard ConfigMap, all optional. The operator needs no metrics stack of its own. |
| Logs | Often no log backend at all | Talos service logs (etcd, kubelet, apid) and kernel logs are then only reachable through `talosctl logs`. See §14.3. |
| Version bumps | Versions are edited by hand, or duplicated across scripts | The `TalosCluster` becomes the one place a version is written; tooling proposes and checks changes to it (§14.5). |
| Node isolation | `SecurityProfileConfig.workloadIsolation` may be on | Pods then run in a sandboxed PID and mount namespace. A node exporter's `hostPID` and host mounts may not see the Talos root. Verify filesystem metrics. |

### 14.2 Metrics

**What Talos exposes natively (1.14):**
- **etcd.** `/metrics` and `/health` moved to **:2383**; :2379 is now gRPC only (`M/constants/constants.go:266`, `T/internal/app/machined/pkg/system/services/etcd.go:458,543`). Both still require client mTLS. Plaintext metrics need `cluster.etcd.extraArgs.listen-metrics-urls` (not deny-listed), which is the community pattern. Etcd config is still v1alpha1-only; there is no multi-doc etcd document in 1.14.
- **kube-controller-manager / kube-scheduler** bind to `127.0.0.1` (`T/internal/app/machined/pkg/controllers/k8s/control_plane_final.go:274,395`). Override with `bind-address` in `KubeControllerManagerConfig.extraArgs` / `KubeSchedulerConfig.extraArgs`; the key is not in the merge-deny list.
- **kubelet** :10250 and **kube-apiserver**: standard.
- **machined, apid, trustd and containerd have no Prometheus endpoint** (verified: no `promhttp` handler anywhere). Talos state is only available through the API, which is why the operator exports it (below).

**Metrics profile.** A feature flag rendered by the operator, rather than a patch for users to copy:

```yaml
spec:
  observability:
    metrics:
      etcd: { listenAddress: node }     # renders cluster.etcd.extraArgs.listen-metrics-urls=http://<node-ip>:2381
      controlPlane: true                # renders bind-address=0.0.0.0 for controller-manager + scheduler
```

- It expands into an operator-managed config layer. It goes through the normal ConfigRollout and shows up in the dry-run diff.
- The etcd argument change restarts etcd. A new change class, **control-plane-restart**, rolls this out one CP at a time behind the etcd gates (§6.3, §8.1).
- Binding to the node IP rather than `0.0.0.0` keeps it off other interfaces. Cilium host policies can restrict access further.
- The matching kube-prometheus-stack values (`kubeEtcd` port 2381, controller-manager/scheduler endpoints, `kubeProxy` disabled) stay in Flux. The operator docs give the values snippet.

**Operator exporter.** These are metrics only the Talos API can provide, built from state the operator already observes (COSI watches, not polling where possible):

| Metric family | Source | Why |
|---------------|--------|-----|
| `talos_machine_stage`, `talos_machine_ready`, `talos_machine_unmet_conditions` | `MachineStatus` | Top-level node health |
| `talos_service_healthy{service}` | `Services.v1alpha1.talos.dev` | etcd, kubelet, apid, trustd, containerd, … |
| `talos_diagnostic{id}` | `Diagnostics.runtime.talos.dev` (`address-overlap`, `kubelet-csr`) | Talos's own misconfiguration warnings |
| `talos_time_synced`, `talos_time_offset_seconds`, `talos_ntp_spikes` | `TimeStatus`, `AdjtimeStatus`, `NTPStatus` | Clock drift breaks etcd and certificates |
| `talos_etcd_db_size_bytes`, `…_in_use_bytes`, `…_is_leader`, `…_alarm{type}`, `…_raft_index` | `EtcdStatus`, `EtcdAlarmList` API | Fragmentation, NOSPACE, leader churn |
| `talos_link_up`, `talos_link_speed_mbits`, `talos_bond_member_up` | `LinkStatus` | **with an LACP bond over two links, a single link failing is silent** |
| `talos_volume_phase`, `talos_volume_error`, `talos_fs_scrub_last_success` | `VolumeStatus`, `FSScrubStatus` | EPHEMERAL/STATE health |
| `talos_oom_action_total` | `OOMActions` | Talos userspace OOM handler fired |
| `talos_info{version,schematic,boot_entry,secure_boot,…}` | `Version`, `ImageFactorySchematic`, `BootedEntry`, `SecurityState` | Inventory; detects silent A/B fallback |
| `talos_operator_operation{type,phase}`, `…_rollout_blocked`, `…_config_drift`, `…_upgrade_available{component,type}`, `…_spec_path_valid`, `…_client_cert_expiry_seconds` | Operator state | Rollouts, drift and upgrade intelligence |

CPU, memory, disk and network counters are **not** duplicated; they are node-exporter's job. The closest prior art is `yuriy-kovalchuk/talos-monitoring` (MIT, ideas only).

**Shipped in the Helm chart:**
- a `ServiceMonitor`;
- a Grafana dashboard ConfigMap labelled for the kube-prometheus-stack sidecar, with panels for cluster overview, per-node Talos health, etcd, time, links and bonds, rollouts and upgrades;
- a `PrometheusRule` with these alerts:
  - `TalosMachineNotReady`, `TalosServiceUnhealthy`, `TalosDiagnosticActive`;
  - `TalosEtcdAlarm` (critical for CORRUPT), `TalosEtcdFragmented`, `TalosEtcdLeaderChurn`;
  - `TalosClockUnsynced`, `TalosBondDegraded`, `TalosVolumeError`;
  - `TalosConfigDrift`, `TalosRolloutBlocked`, `TalosOperatorCredentialsExpiring`;
  - `TalosUpgradeAvailable` (info).

### 14.3 Logs (optional, needs a log backend)

Talos can push its logs to a collector:
- **Service logs:** `machine.logging.destinations`, which is v1alpha1 only. JSON lines over `tcp://` or `udp://` (`T/internal/app/machined/pkg/runtime/logging/sender_jsonlines.go:53-77`), with fields `msg`, `talos-time`, `talos-level`, `talos-service`.
- **Kernel logs:** the `KmsgLogConfig` document.
- **Machine events:** `EventSinkConfig` (gRPC), or pulled via `MachineService.Events`.

These cover etcd, kubelet, apid and containerd logs, which are not pod logs and are invisible to normal Kubernetes log collection. They are exactly what is needed to debug a rollout that went wrong.

Scope:
- The owner deploys a log backend plus a host-network collector DaemonSet (Vector `socket` source or Alloy) listening on a node-local port.
- The operator renders `spec.observability.logs.endpoint: udp://127.0.0.1:<port>` into the logging config.
- Without a backend, nothing is rendered.

### 14.4 Upgrade-path awareness

**Rules.** One planner package (`internal/plan/versions`) is used by the operator **and** by the `validate` CLI (§14.5). It encodes:
1. `compatibility.TalosVersion.UpgradeableFrom` (embedded machinery tables).
2. `KubernetesVersion.SupportedWith(talos)` for every intermediate state.
3. go-kubernetes `upgrade.Path.IsSupported` (Kubernetes: same minor, or +1 minor only).
4. **Talos minors step +1 and land on that minor's latest patch.** This is the documented recommendation and is stricter than the tables, which allow 1.12→1.14.

Current tables (machinery v1.14.0 and v1.15.0-alpha.0):

| Target Talos | Upgrade from host ≥ | Downgrade from host < | Kubernetes |
|--------------|---------------------|-----------------------|------------|
| 1.13 | 1.11.0 | 1.15.0 | 1.31–1.36 |
| 1.14 | 1.12.0 | 1.16.0 | 1.32–1.37 |
| 1.15 (alpha, may change) | 1.13.0 | 1.17.0 | 1.33–1.38 |

**Planning.** The planner turns (observed state, desired spec) into an ordered list of `(talos, kubernetes)` hops in which every adjacent pair satisfies all four rules. That one mechanism solves ordering:
- Talos goes first when the new Kubernetes version needs it.
- Kubernetes goes first when the new Talos version raises the Kubernetes floor.
- A spec that jumps several minors is executed through the intermediate latest patches.

The full plan is published in status before anything starts.

**Availability.** Every 6–12h, with caching (no auth, no documented rate limit), the operator polls:
- factory `GET /versions`;
- the stable Kubernetes releases (kubelet tags on `ghcr.io/siderolabs/kubelet`).

It then publishes:

```yaml
status:
  versions:
    observed: { talos: v1.14.0, kubernetes: 1.37.0 }
    available:
      talos:      [ { version: v1.14.2, type: patch, supported: true } ]
      kubernetes: [ { version: 1.37.1, type: patch, supported: true },
                    { version: 1.38.0, type: minor, supported: false,
                      reason: "Talos 1.14 supports Kubernetes < 1.38; requires Talos >= 1.15" } ]
    recommendedPath: [ { talos: v1.14.2 }, { kubernetes: 1.37.1 } ]
    specPlan: [ … ]                    # concrete hops for the current spec
    lastChecked: …
  conditions: [ UpgradeAvailable, SpecPathValid, InstallerAvailable, ExtensionsAvailable ]
```

**Pre-rollout checks against Image Factory:**
- `HEAD /v2/<platform>-installer/<schematic>/manifests/<version>` must return 200; manifests are built lazily, so use a generous timeout.
- The schematic's extensions must be a subset of `GET /version/<v>/extensions/official`. Extensions do appear and disappear between versions, e.g. `intel-hd-audio` exists in 1.14.2 but not in 1.14.0.

**Schematic as data.** `POST /schematics` is content-addressed and idempotent: the ID is the SHA-256 of the canonical schematic. A later option is to declare extensions and kernel args in the CR and let the operator derive the schematic ID, instead of hand-copying a 64-character hash.

**Machinery skew.** A Talos minor newer than the operator's embedded tables is reported as `supported: unknown` and is never rolled out. Upgrading the operator (with newer machinery) is therefore a prerequisite of every Talos minor bump. Renovate tracks the machinery Go module in this repo, and the operator image wherever the operator is deployed from.

**Admission.** Spec versions without a valid path are **not** rejected at admission: that would wedge the Flux Kustomization. Instead they set `SpecPathValid=False` and no rollout starts.

### 14.5 Version bump automation (Renovate + CI)

**Renovate** runs on the repository that holds the spec. No Talos preset exists upstream; the config follows the home-operations pattern.
- The CR carries annotations, in block style so each value can have a comment:
  ```yaml
  versions:
    # renovate: datasource=github-releases depName=siderolabs/talos
    talos: v1.14.0
    # renovate: datasource=github-releases depName=kubernetes/kubernetes
    kubernetes: 1.37.0
  ```
- `github-releases` is the datasource for both, because it has release timestamps. Since Renovate 42, `minimumReleaseAge` requires timestamps, which the factory and ghcr.io datasources lack. Existence of the installer and kubelet image is checked by CI instead.
- Docker lookups for `factory.talos.dev/*` are disabled: the factory does not support `tags/list`.
- Pre-releases are ignored. Patch and minor bumps are separated, with one PR per minor (`separateMultipleMinor`).
- **Patches:** 3-day soak, automerge **only** through the required `validate` check.
- **Minors:** 14-day soak, Dependency Dashboard approval, never automerged, release-notes reminder in the PR body.
- `rebaseWhen: behind-base-branch`, so a Kubernetes PR blocked on Talos re-runs CI once the Talos PR merges.
- The full proposed `renovate.json5` is in the M6 beads issue.

**`talos-operator validate`.** A CLI in the operator image runs the §14.4 planner as a required GitHub Actions check on PRs that touch the CR. It checks that:
- the versions parse and are not pre-releases;
- a path exists from the base-branch spec to the PR spec;
- the target is in the factory `/versions` list;
- the schematic exists and its extensions are available;
- the installer manifest exists;
- the kubelet image exists.

It writes the plan as markdown to `$GITHUB_STEP_SUMMARY`.

*Limitation:* the base branch is not what is running. A previous bump may be waiting for a maintenance window, and CI cannot reach the cluster. CI is a pre-merge filter; the operator re-plans from live state and is the final authority.

**`talos-operator render`.** It renders machine configs from the CRs plus a secrets bundle, using the same code as the operator. It replaces hand-written generation scripts for disaster recovery, so the rebuild runbook and the operator can never disagree. Such scripts can be retired once the cluster is adopted.

**Conflict to watch for:** automation that commits directly to `main` (image update bots, for one). A branch-protection rule that requires `validate` blocks those pushes unless the rule is scoped to the CR paths (§14.7).

### 14.6 Maintenance automation

These are recurring tasks the operator runs through the Talos API. Disruptive ones take the disruption lock and respect maintenance windows.

| Task | Mechanism | Policy |
|------|-----------|--------|
| **Scheduled etcd snapshots** | `EtcdSnapshot` streams from one healthy member, using a dedicated `os:etcd:backup` certificate from a second Talos ServiceAccount (least privilege; the access config must allow that role). | Cron schedule plus retention, shipped **off-cluster** (S3-compatible; not rook RGW on the same cluster). Also taken before every CP operation (§8.5.4). Moved out of M9. |
| **etcd defragmentation** | `EtcdStatus` ratio `db_size_in_use/db_size`; `EtcdDefragment` defragments the **local** member only (`v1alpha1_server.go:2172`), so it is targeted per node | When ratio < 0.5 and the DB is above a size floor. One member at a time, followers first, then `EtcdForfeitLeadership` and the old leader last. Health check between members. |
| **etcd alarms** | `EtcdAlarmList` / `EtcdAlarmDisarm` | NOSPACE: defrag, then disarm. **CORRUPT: alert only, never auto-disarm.** Optional `quota-backend-bytes` via the metrics/etcd profile (default 2 GiB). |
| **Support bundle on failure** | `github.com/siderolabs/go-talos-support` (MPL-2.0, depends only on machinery): service and kernel logs, kube-system logs, non-secret COSI resources, controller graph | Captured automatically when a rollout fails. Stored on a PVC or object store and referenced from `status.operation.failure`. No Sidero age recipients. |
| **Credential hygiene** | Talos re-issues the operator's certificates (§9); the operator alerts if they are not renewed. Optional human talosconfig/kubeconfig Secrets refreshed. | Alert on the operator's own cert expiry and on CA age; CAs last 10 years. Talos already rotates apid/trustd (24h) and control-plane certs (1y) itself. |
| **Hardware watchdog** | `WatchdogTimerConfig` (`device`, `timeout` ≥ 10s; machined feeds it every timeout/3) | Recommended patch for bare metal. It recovers a hung kernel or machined after boot. It does **not** cover network breakage (TRY does) or an early-boot hang before machined (§8.5.2). |
| **Contract-bump hygiene** | Documents that new clusters get by default but upgraded ones lack (`FilesystemTrimConfig`, `SecurityProfileConfig`, scrub, …) | Handled by the version-contract bump workflow: preview diff, then approval. Not silent. |

**Not in v1:**
- CA rotation (M9);
- `ImageVerificationConfig` signature policy (low value for a small cluster; CRI enforcement unverified);
- automated disk wipes for Rook OSD recycling;
- containerd metrics via `CRICustomizationConfig`.

Image GC needs nothing: Talos already runs it every 15 minutes (`T/internal/app/machined/pkg/controllers/cri/image_gc.go:32`).

### 14.7 Decisions

Decided 2026-10-09:
1. **Log shipping goes to a collector on the node.** Which backend is behind it is the owner's choice. The operator renders Talos service and kernel log shipping to the collector (§14.3).
2. **Snapshot target: S3-compatible object storage**, off-site. Its credentials are a Secret in the operator's namespace. Snapshots are encrypted client-side before upload, because they contain every Kubernetes Secret.
3. **How the bot that proposes bumps is run is the owner's choice.** The repository ships an example configuration, nothing more.
4. **The `validate` check is meant to be required only for changes touching the Talos CRs,** so that it does not stand in the way of unrelated automation.
