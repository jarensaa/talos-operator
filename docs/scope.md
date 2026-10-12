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

Removing and replacing nodes (§7.7) falls out of R1/R4 and is scoped too. Bootstrapping a cluster from nothing is not (§12.26): the operator manages a cluster that exists. Day-2 operations — observability, upgrade-path awareness, version-bump automation and maintenance — are scoped in §14.

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
- **Backups.** Snapshots of etcd, and backups of what runs on the cluster, are its owner's to take, store, and above all to restore, with tools made for that. The operator takes none, on a schedule or before an operation (§8.5.4, §12.23).
- **Bootstrapping a cluster.** The operator runs in the cluster it manages, so there is none to run in before there is a cluster. A cluster is built with `talosctl`, or whatever its owner builds clusters with, and then adopted (§7.1).
- **Managing workloads or CNI.** A CNI, a storage system and so on stay with whatever installs them.
- **Replacing Omni.** No SideroLink, no management of many clusters from one place, no multi-tenant SaaS features.
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
internal/gates              the user's gates: a bounded CEL expression over one object (§6.4)
```

**Key structural decision:** planners in `internal/plan` are pure functions with no I/O. Controllers do observe → plan → execute one step → persist → requeue. Every safety invariant lives in one place and can be table-tested exhaustively. Examples:
- never more than one CP not-Ready;
- never fewer than 3 etcd voters without an override;
- never two disruptive operations at once.

What is in `internal/plan` today: the cluster preconditions (`Preconditions`), the gates of a rollout (`TouchGate` before a node is touched, `VerifyGate` while a changed node is verified), whether an operation is due and may start (`PlanRollout`, `PlanUpgrade`, `PlanKubernetes`), what a node step does next (`NextStep`, `NextUpgradeStep`, `NextKubernetesStep`), when a maintenance window is open (`Windows`), and what an operation does between its turns, hooks included (`NextTurn`, `NextHook`). The controllers hand them plain facts: what a pass read from each node, with everything that was not read marked as unknown, never as fine. The adoption proof's gate (`proofGate`) is the one exception: it is pure and table-tested, but it judges TalosMachine objects directly and so sits with the controller that reads them.

**A failure is reported as what it is.** A node that does not answer makes its own facts unknown. A failure of the Kubernetes API the operator runs against is not a fact about Talos at all: the pass fails and is retried, and no condition claims the Talos cluster is unreachable. Every read that fails is logged with the node and what was asked; conditions carry the names of what was not read, which do not change from pass to pass.

### 4.2 Topology

**Recommendation:** run self-hosted first, but code against a target-cluster abstraction:
- endpoints + credentials, where "self" is just one target;
- a management-cluster mode would be configuration, not a rewrite. It is not planned (§12.26).

Self-hosted on a 3-CP-only cluster means the operator will drain and reboot its own node. That is handled by design:
- **2 replicas**, leader election, required pod anti-affinity, PDB `maxUnavailable: 1`.
- **Every step is persisted before its side effect** (CACPPT's "annotate before etcd-leave" pattern). Example: `status.operation.step = Draining(node-2)` is written before the drain starts. If the leader is evicted, the standby resumes at that step.
- **Steps are keyed on observed facts, not memory.** A reboot step stores the pre-reboot boot ID. On resume, a changed boot ID means "reboot happened".
- **Talos calls are never routed through the target node.** `WithEndpoints` uses the IPs of healthy non-target CPs, and `client.WithNode(target)` reaches the target through apid proxying. Endpoints are re-resolved after every reboot.
- **The node hosting the current leader is upgraded last** in its group. This minimises failovers; it is not required for correctness.

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
  # config: Manage is refused at admission unless status says ConfigProven=True (§5.2); upgrades,
  # membership and maintenance need config: Manage.
  management:
    config: Observe                # Observe | Manage
    upgrades: Disabled             # Disabled | Enabled   (Talos + Kubernetes rollouts)
    membership: Disabled           # Disabled | Enabled   (add / remove / replace machines)
    maintenance: Disabled          # Disabled | Enabled   (defragmenting etcd, §14.6)
  credentials: { source: TalosServiceAccount }       # default: Talos issues the operator's certificate (§9)
  secretsRef: { name: my-cluster-talos-secrets }    # secrets bundle; written by the operator during adoption if absent

  # Everything below may be left empty: adoption fills it in from the live cluster (§7.1).
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
    drainTimeout: 10m                 # how long an upgrade waits for a node's pods to be evicted (§7.3)
    maintenanceWindows:               # when an operation may start, and begin another node (§6.4); none = any time
      - { schedule: "0 2 * * 6", duration: 3h, timeZone: Europe/Oslo }
    gates:                            # must be true before a node is touched and while a changed node is verified
      - name: storage-healthy
        object: { apiVersion: ceph.rook.io/v1, kind: CephCluster, namespace: storage, name: storage }
        expression: object.status.ceph.health == "HEALTH_OK"
    hooks:                            # Jobs; each a name, a batch/v1 JobSpec and an optional timeout
      preRollout: []
      postRollout: []
      preNode:
        - name: noout
          job: { template: { spec: { containers: [ { name: ceph, image: …, command: [ … ] } ] } } }
      postNode: []

  paused: false                       # stops all mutating actions immediately

  maintenance:                        # when the upkeep that management.maintenance enables is due (§14.6)
    etcd: { freePercent: 50, minimumSize: 100Mi, interval: 24h }
  observability:
    controlPlaneMetrics: Disabled     # Enabled: the controller manager and the scheduler listen beyond localhost (§14.2)

status:
  phase: Adopting | Adopted | Managed | Degraded   # only moves forward (§12.14)
  conditions:                         # ResourcesDerived, ConfigProven, ConfigHandover, ConfigInSync, TalosUpToDate, KubernetesUpToDate, VersionsReachable, Healthy, RolloutBlocked, EtcdMaintained, ...
  observed:
    talosVersions: { v1.14.0: 3 }
    kubernetesVersion: 1.37.0
    etcdMembers: [ { id, name, isLearner } ]
  bootstrapped: true
  adoption:
    preconditions: [ … ]              # each with status + message
    adoptedAt: …                      # first proof; from then on the operator writes no spec (§12.14)
    source: 5c1f…                     # fingerprint of the live state the resources were derived from
    notes: []                         # facts worth knowing when taking the resources over
    unmatchedMembers: []              # live nodes with no TalosMachine yet
    liveConfigHashes: { node-a: sha256:… }
    provenAt: …
  operation:                          # THE disruption lock; at most one at a time
    id: 3f1c…
    type: TalosUpgrade                # ConfigRollout | TalosUpgrade | KubernetesUpgrade | AddMachine | RemoveMachine | EtcdDefragment
    target: 9c1e…                     # the hash of the change: which machines go from what to what (§6.3, §7.3)
    plan: [talos-cp-2, talos-cp-3, talos-cp-1]   # KubernetesUpgrade: one component on one machine each, "kube-apiserver/talos-cp-1"
    targets: { talos-cp-2: sha256:… } # ConfigRollout: the rendered config each machine is to reach,
    bases: { talos-cp-2: sha256:… }   #   and the live config it had when the rollout was planned
                                      # TalosUpgrade: targets holds the installer image for each machine
    current: { machine: talos-cp-3, step: Rebooting, image: …, cordoned: true, preRebootBootID: …, since: … }
                                      # TalosUpgrade: step Pulling | Installing | Draining | Rebooting | Booting |
                                      #   Uncordoning | Verifying (§7.3)
                                      # ConfigRollout: step Trying | Verifying | Confirming | RollingBack,
                                      #   with class, previousHash, desiredHash, deadline, healthySince, attempt
                                      # KubernetesUpgrade: step Pulling, then those of a config change, with component and image
    hook: null                        # the hook that is running: stage, name, machine, job, attempt (§6.4)
    announced: true                   # the preRollout hooks ran; prepared / finishing: the turn whose node hooks ran / are due
    ending: null                      # set when only the postRollout hooks remain: outcome, message
    completed: [talos-cp-2]
    skipped: []
    failure: null                     # set => halted until a human acts (§8.3): machine, step, reason,
                                      #   message, remediation, token, and bundle: the Secret with what the
                                      #   machine said when it failed (§14.6)
  approval: { hash: 9c1e…, machines: [ … ] }   # a change waiting for a human (§6.3)
  holds:                              # what a human took out of the operations of each kind, by type (§8.3):
    ConfigRollout: { skipped: {}, aborted: {} }   # machine -> what it was being taken to (a config hash, an
    TalosUpgrade: { skipped: {}, aborted: {} }    #   installer image); not started again as it is
    KubernetesUpgrade: { aborted: { "*": "1.37.1" } }   # the version whose upgrade was aborted (§7.4)
  versions:                           # what runs, what exists upstream, and the way to what the spec asks for (§7.5)
    talos: v1.14.1
    kubernetes: 1.36.5
    plan: [ { kind: Talos, from: v1.14.1, to: v1.14.2 }, { kind: Kubernetes, from: 1.36.5, to: 1.37.1 } ]
    available: { talos: [v1.14.2], kubernetes: [1.36.5, 1.37.1], problem: "" }
  certificates:                       # from when until when each is valid (§14.6): the operator's client
    - { name: operator, issuedAt: …, expiresAt: … }   # certificate, and the authorities os, kubernetes,
                                      #   kubernetes-aggregator and etcd
  maintenance: { etcdDefragmentedAt: … }
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
  phase: Pending | AwaitingIdentity | Provisioning | Joining | Ready | Draining | Removing | Removed
  joinedAt: …                         # when it was first found to be a member; never unset (§7.6)
  removedAt: …                        # when the operator finished taking it out; unset when it is added again (§7.7)
  identity: { systemUUID, nodeID, hostname, kubernetesNodeName }
  talos: { version, schematicID, bootID, stage, ready }
  kubelet: { version }
  config:                             # hashes of the normalized config: desiredHash == liveHash iff ConfigMatchesLive
    desiredHash: sha256:…             # what the spec renders
    liveHash: sha256:…                # what the node runs
    appliedHash: sha256:…             # what the operator last confirmed on the node, or took over (§6.2)
    changeClass: LiveSafe             # class of the change from live to rendered, and what put it there (§6.3)
    changeReasons: [v1alpha1/KubeNodeConfig]
    diff: |                           # redacted, per document; this is what an approval is reviewed against
      …
  etcd: { memberID, isLearner, leader, dbSize, dbSizeInUse }
  health:                             # what Talos itself says about the node (§14.2)
    services: [ { name, running, healthy } ]
    diagnostics: [ { id, message } ]
    timeSynced: true
    links: [ { name, up, speedMbps, bond } ]
    volumes: [ { name, phase, error } ]
  conditions: […]                     # Matched, ConfigMatchesLive, NodeObserved, Provisionable
```

A machine is a **member** from `joinedAt` until `removedAt`. Only members are rolled out to, upgraded, and counted by the adoption proof. A machine that is declared and has not joined, or has left, holds nothing up: for it the conditions above say `NotAMember`, and `Provisionable` says whether it could be added and, if not, what it waits for (§7.6).

`NodeObserved` says whether the node's state was read in the last pass, and names what was not. Status never shows a moment that has passed as the present: `talos.stage`, `talos.ready`, `talos.bootID`, `config.liveHash`, `health` and what `etcd` says besides which member it is are empty unless they were read in the last pass, and a machine is `Ready` only on what its node said in that pass. A boot ID that survived a failed read would say that no reboot happened. Facts that rarely change (version, schematic, identity, etcd member) keep their last value, which is more use than nothing while a node is away.

`poolRef` may change between pools of the same role; that is a config change like any other. A pool of the other role cannot match the live node, which keeps reporting its own machine type. The machine then stays `Matched=False` (`RoleMismatch`) and is never acted on.

**Validation.** There is no webhook. Rules on one object are CEL in the CRDs. Rules that need the stored object or a DELETE are **ValidatingAdmissionPolicies** (`config/admission/policies.yaml`, shipped by the chart). The API server evaluates them itself, so they hold while the operator is down; a self-hosted operator's webhook would be unavailable during the very node drains it runs (§12.8).
- Immutable fields stay immutable: a pool's `clusterRef` and `role` cannot change (CRD CEL).
- `upgrades`, `membership` and `maintenance` can be `Enabled` only with `config: Manage` (CRD CEL).
- `config` may become `Manage` only while the **stored** status shows `ConfigProven=True` (policy `…-management-gate`). A spec update cannot carry its own status, so the gate reads `oldObject.status`. `Adopted` is kept current by the proof (§7.1 stage 3): any live or spec change since the proof sets it False. Once managed, a later invalidated proof does not lock the spec.
- A TalosMachine cannot be **deleted while its node is in the cluster** unless annotated `talos.jarensaa.github.io/orphan: "true"` (policy `…-deletion-guard`). That is while it is `Present`, and while it is `Removed` and a member still: `status.joinedAt` without `status.removedAt`. This guards against a GitOps prune or a namespace deletion silently dropping a machine, and against a commit that asks for a removal and deletes the resource at once, which would leave a node nobody describes. `desiredState: Removed` is how a node leaves (§7.7). There is no finalizer: the API server keeps the resource by itself, the operator running or not.
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
| Installer | the installer image alone: another Talos version, or another schematic | **Blocked** (`AwaitingUpgrade`) while the node runs something else: an upgrade brings it about (§7.3). Applied like any other change once the node runs what the image installs. |
| Kubernetes | the image of `kube-apiserver`, `kube-controller-manager`, `kube-scheduler`, kube-proxy or the kubelet | Never part of a config rollout: a Kubernetes upgrade changes these, in its own order (§7.4). A rollout compares and applies a node's rendered config with these images as the node has them, so everything else of a change goes through. The class only shows, and then blocks (`AwaitingKubernetesUpgrade`), where that cannot be done. |
| Boot-time | everything else only an install can change: the install disk, kernel args, system disk encryption | **Blocked** (`RequiresReinstall`). The operator does not reinstall a node for these. |
| Unknown | any document kind or field the classifier has no rule for | Requires approval, so a kind Talos adds later is never treated as safe by default. |
| Storage | `VolumeConfig`, `UserVolumeConfig`, `RawVolumeConfig`, LVM, RAID | Requires approval. |
| Connectivity | `BondConfig`, `LinkConfig`, `VLANConfig`, `DHCPv*Config`, resolvers, routes, firewall rules, `KubePrismConfig`, `DiscoveryServiceConfig` | Applied. |
| Control-plane restart | etcd `extraArgs`, control-plane component documents | Applied. A component's change takes effect at once: Talos renders its static pod again. **etcd's does not:** Talos hands etcd its arguments when it starts it (`T/internal/app/machined/pkg/system/services/etcd.go:111`) and restarts it for no config change, so the config is applied, verified and in sync while etcd runs with what it started with until its node reboots. Nothing restarts it yet, and nothing says so (§12.27). |
| Live-safe | kubelet config, labels/taints, sysctls, registries, time, what the config says about kube-proxy besides its image (§12.20) | Applied. |

One blocked machine blocks the rollout for every machine: a rollout either applies the whole change or none of it.

**Every applied change is tried first.** Whatever its class, a config is applied with Talos's `TRY` mode and made persistent only after the node was verified from outside and stayed healthy for the soak (§8.5.1). The class decides whether a change is applied, not how. The reason is that the classifier cannot know what breaks a node: bad kubelet arguments stop its heartbeat as surely as a wrong bond does, and a config applied with `NO_REBOOT` that leaves a node unhealthy stays there, with the cluster-health gate then refusing the rollout that would take it back. With a try, a node that does not come out healthy returns to its previous config by itself.

**Approval flow.** For changes that require approval, the operator sets `RolloutBlocked=AwaitingApproval` and `status.approval`: the machines concerned, with the reasons, and the hash of the change. The hash covers every machine of the rollout with the config it runs and the config it is to get. The diff to review is each machine's `status.config.diff`, the operator's own redacted per-document diff. (Talos's dry-run diff is not used for this: it is an unredacted text diff of the whole config.) A human approves by annotating the cluster with `talos.jarensaa.github.io/approve-diff=<hash>`. The approval matches only that exact change: a node that drifts, or a spec that renders anything else, changes the hash.

**Planning.** A rollout is due when a machine's rendered config differs from its live config and from what was last applied (or the drift policy is `Enforce`). The planner (`plan.PlanRollout`) answers with the machines in order, or with the reason nothing starts, most fundamental first: a protected or boot-time change, `paused`, a TalosMachine whose status has not caught up, an earlier abort (§8.3), a missing approval, the gate for the first node. The order is a machine whose node is unhealthy first, then workers before control planes, by name within each group.

**The gate before a node is touched** (`plan.TouchGate`) is asked for one node. It is open when all of this is known to be so, and anything that could not be determined closes it:
- every member was read, and every live member has a TalosMachine;
- etcd has exactly the control planes as started, voting members;
- no node has a config pending, staged or tried, the node itself included;
- every *other* member is healthy by its own facts: it runs and reports itself ready, Kubernetes reports its Node Ready with a network, and its etcd member reports no errors;
- Talos's own health checks pass.

The node's own health is not asked about. Touching a node that is already unhealthy takes nothing from the cluster that it still has, and a rollout is how a config that broke a node is taken back (§8.3). When the node is known to be unhealthy, Talos's health checks are not asked either: they judge the cluster as a whole, cannot leave a node out (`T/pkg/cluster/check/kubernetes.go:32-85` fails on a Node that is not in the list it was given), and so cannot pass. The other members then stand on their own facts alone. Not knowing whether the node is healthy excuses nothing: only a node that was read and found unhealthy sets Talos's checks aside. So an unhealthy node is only ever touched while it is the only one, and a healthy node is never touched while any other is unhealthy.

Starting a rollout only takes the lock: `status.operation` records the plan, and for each machine the config it is to leave (`bases`) and the one it is to reach (`targets`). **An operation applies exactly those pairs**, which are what was classified and approved. When a machine's turn comes and it renders, or runs, something else, the operation ends as `Superseded` and the rest is planned afresh.

**Per-node sequence** (§8.5.1 for the protocol and its failure handling):
1. Persist the step: `Trying`, with the previous and desired hashes and the deadline.
2. Wait for the node's gate (above); while it is closed nothing is sent. A gate that stays closed for `stepTimeout` is a failure.
3. Render again. Only the planned config is ever sent; a dry run validates it on the node first.
4. Apply with `TRY`.
5. Verify from outside and soak.
6. Confirm, and read back that the config is active and persistent.

A node that already runs its target config when its turn comes is not applied to, but it is verified and soaked like the others: the rollout only moves past a node it has seen healthy.

`hold: true` on a TalosMachine leaves it out. `paused: true` on the cluster starts no rollout and no further node; a node whose config is being tried is still verified and confirmed, because stopping there would itself be a change (Talos would revert it). Setting `config` back to `Observe` ends the rollout at once and writes nothing more: a config that is being tried is neither confirmed nor withdrawn, and Talos reverts it when its window ends.

### 6.4 Rollout policy: windows, gates and hooks

Three things a user declares in `spec.rollout`, which hold for every kind of operation: a config rollout, a Talos upgrade (§7.3) and a Kubernetes upgrade (§7.4). Implemented in `internal/plan/policy.go` (when a window is open, which hook or node comes next), `internal/gates` (what a gate says), and `internal/controller/policy.go`, `hooks.go` and `operation.go` (reading the objects, running the Jobs, driving every kind of operation through the same turns).

**Maintenance windows** (`maintenanceWindows`). Each is a five-field cron `schedule` for when it opens, a `duration`, and an optional `timeZone` (UTC if empty). With none, any time is allowed. Outside every window:
- no operation starts (`RolloutBlocked=OutsideWindow`, with the time the next window opens);
- an operation in flight begins no new node. A node that is being changed is finished: a config that is being tried is verified and confirmed, and a node that is cordoned is upgraded and uncordoned. This is what `paused` does, and a window is nothing but a pause that the clock sets.

A schedule that does not parse, or a time zone that does not exist, closes the window for good and is reported (`InvalidRollout`).

**Gates** (`gates`). Each names one object (`apiVersion`, `kind`, `namespace`, `name`) and a CEL `expression` over it (`object`), which must evaluate to `true`. Gates are asked wherever the cluster's own health is asked (§6.3): before a node is touched, and while a changed node is verified. A node is not done until every gate has been true for the soak, so a gate on storage health keeps the next node from starting while the last one's volumes are still recovering. Everything that is not a plain `true` closes the gate: an object that does not exist or may not be read, a field the expression reads that is not there, an expression that does not compile or does not return a boolean, one that costs more than its budget. A closed gate is bounded like every other wait (`stepTimeout`), and what it said is in the condition.

The operator reads a gate's object with its own ServiceAccount, and has no permission for arbitrary kinds. The chart grants what `gates.rules` lists.

**Hooks** (`hooks`). Four lists of Jobs: `preRollout` and `postRollout` around an operation, `preNode` and `postNode` around each node's turn. A hook is a name, a `batch/v1` Job spec and a timeout (`stepTimeout` if not set). The operator creates the Job in the TalosCluster's namespace, owned by the TalosCluster, with a name derived from the operation, the node and the attempt, so a replica that takes over finds the Job that was already started. Every container gets `TALOS_CLUSTER`, `TALOS_OPERATION` (the type), `TALOS_OPERATION_ID`, `TALOS_HOOK` and, for a node's hooks, `TALOS_MACHINE` and `TALOS_NODE`. The hooks of a list run one after the other.

| Hook | Runs | If it fails |
|---|---|---|
| `preRollout` | after the operation took the lock, before the first node | the operation halts with nothing changed |
| `preNode` | when a node's turn comes, after the window and `paused` allow it, before anything is sent to the node | the operation halts with the node untouched |
| `postNode` | after a node was verified and completed | the operation halts; the node is done |
| `postRollout` | when an operation that ran its `preRollout` hooks ends, however it ends: succeeded, aborted by a human, or superseded | the operation halts and keeps the lock |

A hook fails when its Job fails or does not finish within its timeout (`HookFailed`). The failure is handled like any other (§8.3): `retry` runs the hook again as a new Job, `skip-machine` leaves the node out (for a node's hooks), `abort` ends the operation. What does not run a hook: a node that failed gets no `postNode` (clean up in `postRollout`), and when management is switched off in the middle of an operation nothing more is run at all. A hook may run more than once for the same node, after a retry or a takeover, so it has to be safe to repeat.

A hook Job runs in the operator's namespace with the pod spec its author wrote. Whoever may edit a TalosCluster may therefore run pods there. They could already put any config on every node, so this grants nothing new, but it is a reason to keep that permission narrow.

Decided against, for now (§12.19): more than one worker at a time, and an annotation that rolls a node back to its previous boot slot.

---

## 7. Workflows

### 7.1 Adoption (R3)

Adoption is a **staged trust ladder**. The operator is installed first and writes nothing to a node: it describes the cluster in resources, proves that they change nothing, and hands them to the user. Management is enabled per capability once each stage has been proven safe. The design draws on Omni's `cluster import`, including the lessons from its 1.14 bug #3424.

**Ground rules**
- The live cluster is the only source of truth; the files it was built from are assumed stale.
- **While a cluster is being adopted, the operator writes the resources that describe it** (§12.14): the pools, the machines, and what the TalosCluster's spec leaves empty. Once the cluster is adopted they are the user's, and the operator never writes a spec again. Besides those it only writes `status`, Events, and the secrets/backup Secrets.
- **The operator never overwrites what someone else set.** It writes pools and machines by server-side apply without force, under a field manager of its own (`talos-operator-adoption`). A field another manager set to something else is a conflict: the object is left as it is and named in `ResourcesDerived`. On the TalosCluster it only fills fields that are empty.
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
4. **Resources.**
   - Infer the generation inputs from live config:
     - contract = the current Talos minor;
     - name and endpoint from `KubeClusterConfig`;
     - Kubernetes version from the kubelet image tag;
     - network options from `KubeNetworkConfig`;
     - installer/schematic from `UnattendedInstallConfig` / `ImageFactorySchematic`;
     - KubePrism, discovery and CNI settings.
   - Regenerate the base config per role and derive `configdiff.Patch(base, live)` for each node.
   - Factor the differences into layers. What is identical across every node goes on the TalosCluster. What is identical across a role goes on that role's pool (one per role, named `<cluster>-control-plane` and `<cluster>-workers`). The rest stays on the machine. Everything is written inline; extracting shared parts into TalosMachineConfigs is a refactoring the user can do afterwards, and `ConfigMatchesLive` confirms it renders the same.
   - Identity fields must never appear in derived config. If one does, the reconstruction is wrong: stop and report.
   - Before writing anything, render every node from the derived specs through the same path the controllers use; the result must equal its live config.
   - **Fill in the TalosCluster.** Name, endpoint, versions, generation inputs and the cluster-wide config are set where the spec leaves them empty, and never where it does not. The pass ends there; everything else is decided from the spec as stored.
   - **Write the pools and machines**, by server-side apply without force. A node that already has a TalosMachine under another name is not given a second one, a machine of that name in another cluster's pool is left alone, and a pool is only written for machines written here. The result is `ResourcesDerived`: true with what was written, or false naming what was left alone because someone else changed it.
   - **Keep them in line with the cluster** for as long as it is being adopted. The resources are derived again when a fingerprint of their inputs changes (per-node identity, version and config hash, the bundle hash and `secretsRef`; `status.adoption.source`), or when a node has no machine. Fields nobody else touched follow the cluster; fields someone else set stay theirs.
   - What is worth knowing when taking the resources over, such as a node whose Talos version differs from the one its config installs, goes to `status.adoption.notes`.
   - Back up all live configs to a Secret (never a ConfigMap; they contain secrets). It is created once, before anything else is written to Talos, and never updated.

**Stage 2: reactive loop**

The resources may also come from the user, before the operator looks or in place of what it wrote; any tool works (§12.9). For each `TalosMachine`, whoever wrote it, the operator:
1. Matches it to a live node by `identity.systemUUID`. A machine that matches no live member gets `Matched=False` and is never acted on. Live members with no `TalosMachine` are listed in `status.adoption.unmatchedMembers`.
2. Renders the desired config and compares it with the live config **per document**. Config applies in layer order (cluster, pool, machine), from inline documents, TalosMachineConfigs or Secrets. A missing source is reported as `ConfigUnresolved`. Changes to a referenced TalosMachineConfig, pool or Secret, or to the bundle, re-evaluate the affected machines at once; live changes are picked up by the observe interval.
3. Reports `ConfigMatchesLive=True`, or `False` naming the differing documents, with a redacted per-document diff (live → desired, truncated) in `status.config.diff`. Documents set by a patch from a Secret are named but their diff is withheld, because rule-based redaction only knows credential-named fields.

Resources the operator derived are green by construction. Where the user wrote or changed them, the user adjusts config until every machine is green.

**Stage 3: proof**

When every live member is matched and the client-side diff is empty for every node, the operator runs the server-side `ApplyConfiguration{DryRun:true}` against each node. Every node must answer "No changes".
- **Gate:** `AdoptionPreconditionsMet` and `SecretsMatchLive` hold, no live member is unmatched, and every TalosMachine is `Matched` and `ConfigMatchesLive`, with status that is current. Current means observed at its generation, and compared against the live config the cluster sees now (`liveHash` and `desiredHash` both equal that config's normalized hash). Stale machine status blocks the proof rather than being trusted.
- **Mode TRY, always.** Talos cancels a pending TRY rollback for every other mode *before* it checks `DryRun` (`v1alpha1_server.go:203-205` in Talos v1.14), so a dry run in `AUTO` or `NO_REBOOT` would disarm the rollback protecting an in-flight change (§8.5). The read-only interceptor refuses any dry run that is not in mode TRY.
- **The server's verdict only.** The server's diff is an unredacted text diff of the whole config (`generateDiff`, `v1alpha1_server.go:323-334`), so the Reader returns only "no changes" or "changes". Anything to show the user comes from the operator's redacted per-document diff. A disagreement (`ServerReportsChanges`) names the nodes, never the diff.
- **Same document order as live.** That text diff is order-sensitive, so the rendered documents are sent in the live config's order. Order carries no meaning to Talos.

The operator then sets `ConfigProven=True` and records, per TalosMachine, the proven hash (`liveConfigHashes`, where live equals desired) and `provenAt`. While nothing moves, the proof is not re-run.

**The first proof adopts the cluster.** `status.adoption.adoptedAt` is set, the phase becomes `Adopted`, and nothing takes either back (§12.14). From here on:
- The operator writes no spec. A resource that is edited or deleted stays edited or deleted, and a cluster that changes is reported, not followed.
- The operator gives up what it is recorded as owning. On the TalosCluster its entry for the fields it filled in goes. On each pool and machine its entry shrinks to the one label it set (`talos.jarensaa.github.io/cluster`). It is not removed altogether: an object with no managed fields at all gets a `before-first-apply` owner of everything from the API server at the next apply (`managedfields/internal/skipnonapplied.go`), which would keep a field alive after its new owner dropped it, exactly as the operator's own entry would have.
- If the status is lost, as when a TalosCluster is restored from a backup of its spec, the operator is adopting again. Applying without force is what makes that harmless: what someone else manages is a conflict and stays as it is.

Any later change to live state (for example a manual `talosctl` edit) or to what the spec renders invalidates the proof (`ConfigProven=False`, `ProofInvalidated`). The phase stays `Adopted`; the proof re-runs by itself once the gate holds again. The proof runs only while `config` is `Observe`.

**Stage 4: enable management, one capability at a time**

Admission refuses `config: Manage` unless the stored status shows `ConfigProven=True`, and refuses `upgrades`/`membership: Enabled` without `config: Manage` (§5.2).

1. **`config: Manage`**
   - **The first reconcile must be a no-op**, because desired equals live; this is asserted as the **handover**. Once the cluster is managed, the adoption proof stops being re-evaluated. So the handover repeats it: the proof's gate, then a fresh Talos dry run on every node (mode TRY, verdict only).
   - **Verified:** `ConfigHandover=True` and phase `Managed`.
   - **Not verified:** `ConfigHandover=False` (or `Unknown` if a node could not be asked), `RolloutBlocked=True` (`HandoverUnverified`), phase `Degraded`, and a Warning Event. No rollout starts in that state. The check repeats every reconcile, so a spec that renders the live config again lifts the block. Returning `config` to `Observe` forgets the handover, so the next `Manage` is verified afresh.
   - **After the handover the spec is the truth.** Every node's live config is recorded as its baseline (`appliedHash`). A spec that renders something else from then on is rolled out (§6.3). A node that changes while the spec does not is drift: reported, and left alone unless `driftPolicy` is `Enforce` (§6.2). `ConfigInSync` says which of the two is the case, and names the machines. `Adopted` keeps its value from the moment management began.
   - Credentials do not change: the operator keeps its Talos-issued certificate (§9). The user keeps their own admin talosconfig off-cluster as break-glass.
   - Validate the apply path with a harmless change first, such as a node label.
2. **`upgrades: Enabled`.** The first real exercise should be a Talos patch release (1.14.0 → 1.14.2).
3. **`membership: Enabled`.** Enable this last, when a node actually needs to be added or replaced.

> Like Omni, the cluster's PKI remains known to whoever held the old talosconfig and secrets. Rotating the PKI is the one workflow left for later (§10).

### 7.2 Config change (R1)

The flow is: the user edits config (on the cluster, a pool, a machine or a shared TalosMachineConfig) → the render hash changes on the affected nodes → ConfigRollout (§6.3) under the disruption lock, one node at a time, each tried and confirmed (§8.5.1). `ConfigInSync` carries the progress, `status.operation` the step in flight, and `status.history` the outcome.

> **§7.3–7.8 describe milestones that are not built.** They record what was learned from talosctl and from prior art before there was any code, and they are where a milestone's design starts. What each milestone has to deliver is in §10; how is decided when it starts, and these sections are rewritten then.

### 7.3 Talos OS upgrade (R2)

An upgrade is due when a node does not run the Talos version in `spec.versions.talos`, or was not installed from the schematic its spec asks for (the cluster's, or its pool's own). It needs `management.upgrades: Enabled`. Without it the operator only reports what is behind (`TalosUpToDate=False`, `UpgradesDisabled`).

**The system first, its config second.** An upgrade changes what a node runs, not its config. The config also names the installer, and so differs from what the node has once the spec changes; that difference is a class of its own (`Installer`, §6.3). A config rollout refuses it while the node runs something else (`AwaitingUpgrade`) and applies it like any other change once the node runs what it names. So a version change is two operations, one after the other: the upgrade of every node, then a config rollout that brings every node's config in line. The second reboots nothing.

**Refused up front** (`talos.UpgradeProblem`; `TalosUpToDate=False`, `Incompatible`), before any node is touched:
1. A target outside the operator's supported Talos range.
2. A downgrade. Talos allows some, but a downgraded node may not understand the config it has, and nothing here can prove that it does.
3. A jump Talos does not allow: `compatibility.ParseTalosVersion(target).UpgradeableFrom(current)`. Talos allows skipping minors when its table permits, for instance 1.12 to 1.14.
4. A Kubernetes version the target does not run: `KubernetesVersion(current).SupportedWith(target)`. The Kubernetes upgrade then has to come first.

An installer image that does not exist is found at the first node's pull, which is before anything on any node changed. The node is where the answer is: it pulls through its own registry configuration, and the operator may not be able to reach the registry at all.

**Plan** (`plan.PlanUpgrade`). One node at a time. A machine whose node is unhealthy first, for the reason a config rollout has (§6.3). Then workers before control planes. Within a group the etcd leader and the node the operator itself runs on go last: each is a handover that upgrading that node costs, and neither is needed more than once.

**The gate before a node is taken down** (`plan.RebootGate`) is the gate of a config rollout (§6.3), and for a control plane also the guard Talos's own upgrade path dropped (§3.2): etcd must not have exactly two members. With two, losing either loses quorum. With one there is no quorum to protect beyond the node itself, and a cluster with a single control plane is down for the length of that node's reboot. With three or more the others hold quorum, which the gate has already established: every other member is a started voter that reports no errors. A node in the middle of its own upgrade is cordoned, which Talos's cluster-wide checks fail on; like a node that is unhealthy, it is the one node those checks are set aside for, and a cordoned node that is *not* the one being upgraded closes the gate for every other.

**Per-node steps** (`plan.NextUpgradeStep`). Each is persisted before it is acted on, and can be repeated or resumed from what the node says. The order is Talos's own since 1.13 (§3.1): everything before the cordon leaves the node exactly as useful as it was.

| Step | What happens | If it does not work |
|---|---|---|
| `Pulling` | Wait for the gate, then `ImageService.Pull` of the installer to the node's system image store. | `ImagePullFailed`. Nothing changed. |
| `Installing` | `LifecycleService.Upgrade` writes the new system to the inactive boot slot. The installer's exit code arrives as a message on the stream, not as an error of the call. | `InstallFailed`. The node runs what it ran. |
| `Draining` | Ask the gate again. Record that the node is about to be cordoned, cordon it, then evict its pods through the eviction API, so PodDisruptionBudgets count. Static pods, DaemonSet pods and finished pods stay. | `DrainTimeout` after `drainTimeout` (10m). The node stays cordoned and is not rebooted. |
| `Rebooting` | Record the boot ID, ask the gate once more, hand off etcd leadership if the node is a control plane, `MachineService.Reboot` in the default, graceful mode. | `Timeout`. |
| `Booting` | Wait for another boot ID, for the node to be running and ready, and for its version and schematic to be the ones installed. | A node that is back and ready on anything else is `FellBack`: Talos reverts to the previous boot slot by itself when the new one does not come up (§8.5.2). A node that does not come back is `Timeout`. |
| `Uncordoning` | Once Kubernetes reports the node Ready, uncordon it, if the operator cordoned it. | `Timeout`. The node stays cordoned. |
| `Verifying` | The whole cluster, this node included, healthy for the soak, as after a config change (§8.5.1). | `Timeout`. |

Rules the planner never breaks, checked over every combination of step, observation, gate and time:
- a node is not cordoned, and not rebooted, while the gate is closed;
- a reboot is only asked of a node that still has the recorded boot ID, so a node that rebooted is never rebooted again, whatever became of the pass that asked;
- a node that came back on anything but the installed system is a failure, never a success;
- only a cordon the operator set is undone by the operator. A node someone else cordoned is drained as it is and left cordoned.

**Draining is done once, by the operator.** Talos leaves it to the client since 1.13, so nothing drains a second time (tuppr #575 was a double drain). Evictions go out without the Kubernetes client's own retries: a budget that has nothing to give answers 429 with `Retry-After`, and the client would sleep through ten of those, well over a minute per pod, inside a reconcile. The operator's own loop is what asks again.

**Failures** halt the operation as a config rollout's do (§8.3), with the same three annotations. A skip or an abort covers one installer image for the machines concerned (`status.holds.TalosUpgrade`) and is spent when the spec asks for another. A retry starts the machine's turn over from what the node runs now, and a cordon the operator set stays its own to undo.

**Pausing** leaves alone a node nothing was taken from yet. A node that is already cordoned is finished: stopping there would leave it out of the cluster.

**The Talos access** is a third kind of connection (`talos.Upgrader`), next to reading and applying config: pull, install to the inactive slot, hand off etcd leadership, reboot, and nothing else. It cannot apply config, install from scratch, reset, roll back, or change etcd membership, and the deprecated `MachineService.Upgrade`, which takes `force`, is not on its list. It is handed out only while `upgrades` is `Enabled`.

Maintenance windows, the user's gates and hooks apply to an upgrade as to every operation (§6.4). A node's `preNode` hooks run before its installer is pulled, its `postNode` hooks after it is verified.

Not built: more than one worker at a time (§12.19), and reinstalling a node for a change that only an install can make (kernel arguments, the install disk, disk encryption). Those stay refused as `RequiresReinstall`.

### 7.4 Kubernetes upgrade (R2)

An upgrade is due when a node's config names another image of a Kubernetes component than its rendered config does. The components are the ones Talos runs itself, `kube-apiserver`, `kube-controller-manager` and `kube-scheduler` on control planes and the kubelet on every node, and kube-proxy, whose image a control plane's config names without anything running from it (§12.20). It needs `management.upgrades: Enabled`; without it the operator only reports (`KubernetesUpToDate=False`, `UpgradesDisabled`).

**The upgrade is a config change with an order.** Unlike a Talos upgrade, nothing but the config changes: the images it names are what runs. But the order matters, and a config rollout has none: every API server must run the new version before any controller manager, scheduler or kubelet does (the Kubernetes version skew policy). So the two kinds of operation divide the config between them. This one changes the image fields, one component on one node at a time, and each change is the node's live config with one image replaced (`stdpatches`, the patches `talosctl upgrade-k8s` uses, `T/pkg/cluster/kubernetes/talos_managed.go`). A config rollout changes everything else: what it compares and applies is a node's rendered config with these images as the node has them (`withLiveKubernetes`). Neither waits for the other. That matters when an upgrade is refused: what stands in its way is often a flag in the config, and taking it out is a config change that must go through while the upgrade may not.

**One minor at a time.** Kubernetes does not skip minors. A spec that is more than one minor ahead is reached in hops, each an operation of its own: first `.0` of every minor in between, then the version in the spec. Each hop is checked before it starts.

**Refused up front** (`KubernetesUpToDate=False`, `Incompatible`), before any node is touched:
1. A downgrade, and a version outside what go-kubernetes knows a path for.
2. A Talos version on any node that does not support the hop's version (`compatibility.KubernetesVersion.SupportedWith`). The message says to upgrade Talos first. The other direction, a Talos upgrade to a version that does not support the Kubernetes version the cluster runs, is refused in §7.3.
3. A component flag, feature gate or admission plugin on a node that the new version removed (go-kubernetes `upgrade.Checks`, read from each node's static pods and kubelet spec).
4. An object of an API that the new version no longer serves (the same checks, asked of the Kubernetes API).
5. A control plane on hold. A Kubernetes upgrade cannot leave a control plane out: the next component would be newer than its API server. A worker on hold is left out, and reported.

What cannot be read is not fine: a check that fails to run blocks the upgrade (`AwaitingStatus`).

**Plan** (`plan.PlanKubernetes`). `kube-apiserver` on every control plane, then `kube-controller-manager`, then `kube-scheduler`, then the kube-proxy image of each control plane's config, then the kubelet on control planes and on workers. It is talosctl's order. A component that already runs the hop's version is left out. It starts only when the whole cluster is healthy.

**Per-step** (`plan.NextKubernetesStep`), for one component on one node:

| Step | What happens | If it does not work |
|---|---|---|
| `Pulling` | Wait for the gate (§6.3, §6.4), then pull the image to the node, so the component is down only for its restart. Nothing is pulled for kube-proxy. | `ImagePullFailed`. Nothing changed. |
| `Trying` | Apply the config with the new image in `TRY` mode (§8.5.1). | Talos reverts it. |
| `Verifying` | The component runs the new version and is ready (the static pod's mirror pod; the Node's `kubeletVersion`; nothing for kube-proxy), and the whole cluster and every gate are healthy for the soak. | `RolledBack`: Talos puts the previous image back when the try window ends. |
| `Confirming` | Make the config persistent. | |

**The API server's turn gets a longer try** (`plan.KubernetesTryTimeout`): `stepTimeout` to be healthy again, then the soak and the margin to confirm in, instead of `tryTimeout`. Its restart takes the Kubernetes API away from everything on its node, and on a cluster with one control plane from everything: the controller manager and the scheduler lose their leases and restart, the operator loses its own, and nothing can be verified until the API answers. On QEMU, with one control plane, the API was back after 80 seconds and the cluster was not whole again within the three minutes a four-minute try left, so Talos took back a change that was fine. A cluster in that state is settling, not unhealthy, and gets what a node gets to come back after a reboot. The price is on the other side: an API server that really does not come up is reverted after that longer time.

A failure halts the operation (§8.3). There is no `skip-machine` here, for the reason a control plane cannot be on hold; `retry` and `abort` remain.

Windows and gates hold a Kubernetes upgrade as they hold every operation (§6.4), and its `preRollout` and `postRollout` hooks run. A node's hooks do not: no node is taken out of the cluster. An aborted upgrade is not started again until the spec asks for another version (`status.holds.KubernetesUpgrade`).

**The Talos access** is the two that exist: the image is pulled through the upgrade connection, into the namespace the component runs from, and the config is applied through the config connection.

**Not done, by decision (§12.20):** the manifests Talos applies when a cluster is bootstrapped (kube-proxy, CoreDNS, their RBAC). Talos applies them once and never again, and on an adopted cluster they have often been changed or replaced since. The upgrade does bring the kube-proxy image of each control plane's config in line, as talosctl does, so that the manifest Talos would apply names the cluster's version; that changes nothing that runs. A kube-proxy DaemonSet that is left behind the cluster's version is named in `KubernetesUpToDate`.

### 7.5 Version awareness (R2)

A version change in the spec is rolled out by the upgrades of §7.3 and §7.4, inside the maintenance windows and behind the gates of §6.4. With a bot that proposes version bumps to the repository holding the spec, upgrades happen without anyone picking versions, and Git stays what says which versions a cluster runs. This section is what makes that safe to leave alone: knowing which versions exist, working out the way to the ones asked for, and refusing a bump that cannot work, before it reaches the cluster and again when it does.

Implemented in `internal/plan/versions.go` (the way), `internal/catalog` (what exists upstream), `internal/controller/versions.go` (status) and `internal/validate` with `cmd/validate.go` (the check).

**The way** (`plan.PlanVersions`). From the versions a cluster runs to the versions its spec asks for, as a sequence of upgrades, or the reason there is none. Talos and Kubernetes take turns as their versions allow: a Talos version is only installed while it runs the Kubernetes version the cluster has then, and a Kubernetes version only reached, one minor at a time, while the nodes' Talos runs it. Where both could go next, Talos does. The planner starts nothing. Each upgrade still decides for itself whether it may start, by the same rules, which are Talos's own (`compatibility`); the plan is that answer worked out ahead.

**Status** (`status.versions`, condition `VersionsReachable`). What the cluster runs, the plan, and what exists upstream. `VersionsReachable` is false, with the reason, when the spec asks for versions there is no way to: a Talos release that does not run the Kubernetes version asked for, a downgrade, a jump Talos does not make.

**What goes together is compiled in.** Which Kubernetes a Talos release runs, which steps each allows, and what a Kubernetes release removed (flags, feature gates, admission plugins, APIs) are the rules of `machinery`'s `compatibility` package and of go-kubernetes's upgrade checks, the ones `talosctl` uses. They are Sidero's to keep and reach the operator by a dependency bump; the operator keeps no matrix of its own beside them, which would only ever be the same rules, later. An operator therefore knows nothing of a Talos minor newer than the one it was built for, and says so.

**What exists upstream** (`catalog`) is the one thing no build can know: which releases are out, and whether Image Factory builds an installer for this cluster's schematic. Only where `versionSources.enabled` switches it on (§12.22), the operator asks, over HTTPS and nothing else, and only asks:
- the Image Factory a TalosCluster's installer names: which Talos releases it builds, whether it has the schematic, whether the release has every extension the schematic names (extensions come and go between releases), and whether it builds the installer;
- the Kubernetes release site: the newest patch release of a minor;
- the registries of the images a Kubernetes upgrade is about to pull: whether each exists, asked the way a node asks when it pulls, without credentials.

These are other people's services, run for everyone, so they are asked as rarely as is of use: an answer is kept for six hours, a "no" and a failure for ten minutes, and every request says who is asking (`catalog.UserAgent`). A request that fails on the way, or that a server answers by saying it is unwell, is sent again, three times at most and with a pause: one connection that was not made is not what is known about upstream, and the check would fail on it. An answer about what was asked for is final, and so is one that says to ask less often. All of it is the documented public API of Image Factory and of a registry. None of the services publishes terms that forbid it, and two publish none at all; §12.22 has what was looked for. Upstream is never asked while an operation is in flight, and the planners take what is known from status, not from the network: every replica plans with the same releases, and an operation is not replanned because a release came out.

What is found out is used for exactly three things:
1. `status.versions.available` lists the newest release of each Talos minor this operator supports, and of each Kubernetes minor those run. It is information. Nothing is upgraded because of it; the operator changing the version it is asked for is not planned (§12.2, §12.26).
2. A Kubernetes upgrade over several minors goes through the newest release known of each minor in between; where none is known it goes through `.0` (§12.21). A hop that was begun is finished at the release it was begun with.
3. An upgrade to something that certainly does not exist is refused before a node is asked to pull it (`Incompatible`): an installer Image Factory does not build, an image of a Kubernetes component that its registry does not have. The images are asked about, not the release: the kubelet image Talos runs is published a while after the Kubernetes release it is of.

Every answer has three values, and "not known" is neither yes nor no. A source that is down, a registry that wants credentials, or an operator that was not told to ask, refuses nothing and reports nothing: what a node can pull is for the node to find out, through its own registry configuration (§7.3).

**The check** (`manager validate`). The same binary, run on the manifests in a repository, with no cluster and no secret:
- the versions go together, are ones this operator upgrades to, and, given the manifests as they were before the change (`--base`), can be reached from there: no downgrade, no jump Talos does not make;
- the installer of the cluster and of every pool with a schematic of its own exists, and so do the images of the Kubernetes components that the rendered configs name. Here "not known" is a failure: a check that passes because a site was down is not one to rely on;
- maintenance windows and gate expressions can be understood, which the API server does not check;
- every machine's config renders, with secrets made up for the purpose. A machine whose config comes partly from a Secret is not rendered, and is named;
- a field the API does not have is an error, where the API server would drop it without a word.

It exits 1 when something is wrong and prints what; on GitHub Actions each problem is also an annotation, and the run's summary lists the upgrades the change means. It checks the change against the base branch, which is not what the cluster runs: an earlier bump may still be waiting for its window. It is a filter before a merge. The operator plans again from what the nodes run, and has the last word. `docs/examples` has a workflow that runs it on pull requests and a Renovate configuration that proposes the bumps.

**What decides the order of things** stays as it was: one operation at a time (§8.1), a failed operation halts until a human acts (§8.3), and nothing starts outside a window.

### 7.6 Adding machines (R4)

> **Built** (M7). `plan.PlanJoin`, `plan.NextJoinStep`, `internal/controller/membership.go`.

The user creates a `TalosMachine` with `poolRef`, `address` and `identity.systemUUID`, for a machine booted into maintenance mode. The pool supplies the role and the shared config, so a node that differs from its pool in nothing needs no config of its own. Nothing happens while `spec.management.membership` is not `Enabled`: the machine is not even asked who it is.

**Who is talked to.** A machine in maintenance mode has no config, so no certificate anyone could check, and it takes a config from whoever hands it one. It is reached over a connection of its own (`talos.Maintenance`): to its address, without the cluster's credentials, good for reading its hardware and for handing it a config and nothing else. The connection remembers the key of whoever answered first and talks to nobody else afterwards, so the machine that said who it is and the machine that is sent the config are the same one, as far as an unauthenticated connection can tell.

**Whether it can be added** is the machine's own controller's to say, in the condition `Provisionable`:
- it waits in maintenance mode at the address (`MachineStatus.stage == maintenance`);
- the UUID it reports is the pinned one. If not, the phase is `AwaitingIdentity` and the machine is sent nothing: the config carries every secret of the cluster;
- the config rendered for it fits it. Its install disk selector must match exactly one of the machine's disks, by Talos's own matching (`blockhelpers.MatchDisks`): Talos installs to the first match without a word. Every physical link the config configures by name must exist.

**The operation** (`AddMachine`) takes the lock like every other, and adds one machine at a time, workers before control planes. It only starts while:
- the cluster is at rest: `ConfigInSync`, `TalosUpToDate` and `KubernetesUpToDate` say there is nothing to roll out. A machine joins on what its spec renders, and that must be what its peers run. A rollout or upgrade that is due goes first;
- the whole cluster is healthy, nobody left out, no config pending anywhere, etcd with no learner (`plan.JoinGate`);
- the hostname its config gives it is nobody's. A control plane that joins under the name of an etcd member has that member removed by Talos itself (`etcd.go:310-318`). A config that leaves the machine to name itself cannot collide this way.

A machine's turn:
1. **Provisioning.** The machine is asked again, in this pass, over one connection: in maintenance mode, the pinned identity, the config fits. Then it is handed exactly the config that was planned, on that connection (`ApplyConfiguration`, mode `NO_REBOOT`, the one mode that makes a machine without a config take one). A machine that says it is another one fails the operation at once.
2. **Joining.** The machine installs itself and reboots; nothing answers for a while. It is a member once a node answers with the cluster's credentials and the pinned identity, has finished booting, has a Node that is Ready, and, for a control plane, is a voting member of etcd. It has three step timeouts for that.
3. **Verifying.** The whole cluster, the new node included, healthy for the soak (`plan.VerifyGate`).

A machine that was handed its config is never handed one again: a resumed step sees a member, and moves on. A failed turn halts the operation (§8.3). `abort` and `skip-machine` record the config the machine was to get (`status.holds.AddMachine`), and it is not added with that config again.

**Not built:** adding several workers at once (the earlier plan; one at a time is the rule for every operation, §12.19); finding machines instead of declaring them; reporting what was found at an address whose identity is not pinned (`identity` is required).

Two ways to do without trust on first use were considered and are not planned (§12.26): serving configs from `talos.config=` URLs keyed by UUID, and SideroLink.

### 7.7 Removing and replacing machines

> **Built** (M8). `plan.PlanRemoval`, `plan.NextRemovalStep`, `plan.RemovalGate`.

`desiredState: Removed` asks for a machine to leave. Deleting the resource does not: the API server refuses that while the node is in the cluster (§5.4).

**The operation** (`RemoveMachine`) takes the lock, and takes out one machine at a time: workers first, the node the operator itself runs on last. Removals are considered before anything else: a node that is to go is not upgraded first, and one that ails can be taken out without waiting for the rollout it holds up. It only starts while every *other* node is healthy (`plan.TouchGate`), and for a control plane while etcd can spare it (`plan.RemovalGate`):
- the only voting member is never taken out;
- a member is only taken out if at least three voting members remain. With two left, losing one more loses the cluster;
- there is no override. A cluster of three control planes is not shrunk by this operator.

**A replacement** is those two rules and the order of things: the removal of a control plane that would leave too few is refused, the new machine is added (§7.6), and the old one then goes. One commit can ask for both.

A machine's turn:
1. **Draining.** Cordon, then evict through the eviction API, within the drain timeout; `preNode` hooks ran before (taking a storage daemon out of service is the user's hook, §6.4). A machine whose Talos API does not answer is not drained: what cannot be reset afterwards is left as it is.
2. **Resetting.** A control plane hands off etcd leadership. Then one `Reset`: graceful (the node leaves etcd itself), wiping the `STATE` and `EPHEMERAL` partitions of the system disk, with a reboot. The connection that sends it (`talos.Remover`) lets no other reset through: not of the whole system disk, not of any other disk, whoever builds the request.
3. **Leaving.** The machine is gone when it is seen to be: its member is out of etcd, and the machine answers in maintenance mode again at its address, with its identity. That is the proof that it forgot the cluster, and why the reset reboots instead of powering off.
4. **Deleting.** The machine's Node is deleted, which Talos does not do.
5. **Verifying.** What is left of the cluster, healthy for the soak. `status.removedAt` is set, and the TalosMachine may be deleted.

**A machine that does not answer** is not judged. Its removal begins, takes nothing from the cluster, and halts when the machine has not answered for a step timeout (`NodeUnreachable`); a machine that was reset and does not come back halts it too (`NotWiped`). A human then says which it is. `talos.jarensaa.github.io/confirm-dead=<token>` on the TalosCluster says the machine is gone for good: it is asked nothing more, its etcd member is removed through another control plane (`EtcdRemoveMemberByID`), and its Node is deleted. Nothing on the machine is wiped, because nothing can be, so it must not be switched on again as it is. A dead member holds no vote that could be lost, so the three-voter rule does not stand in the way of taking it out, which is what makes room for its replacement.

The cluster's discovery goes on listing a machine for a while after it left. One that was taken out and does not answer is not counted as a member that could not be read; one that answers again is a member again, and is taken out again.

**What is wiped:** Talos's own `STATE` and `EPHEMERAL`, on the system disk, and nothing else. Naming other disks to wipe is not built.

**Not built:** shrinking the control plane below three; a reset that is not graceful, for a control plane that answers and cannot leave etcd by itself; wiping user disks.

### 7.8 Bootstrapping a cluster

Out of scope (§12.26). The operator manages a cluster that exists: one is built with `talosctl` or its owner's own tooling, and adopted (§7.1).

---

## 8. Safety model

### 8.1 Invariants
These are enforced in `internal/plan` and exhaustively table-tested.

1. At most **one disruptive operation** per cluster: `status.operation` is the lock.
2. At most **one CP** is not Ready or not a healthy etcd member at any time. The gate of a config rollout holds every node to this, workers included: a node is touched only while every other node is healthy (§6.3).
3. A control plane only leaves etcd while three voting members remain, unless a human confirmed it dead; there is no other override. There is never more than one learner, and never two membership changes at once: adding and removing each hold the lock, for one machine at a time (`plan.RemovalGate`, `plan.JoinGate`).
4. No disruptive step starts outside a maintenance window. An in-flight node step started inside a window finishes, but the next node waits.
5. No disruptive step starts while `paused`, while `operation.failure` is set, or while any gate is false.
6. No mutation happens for a capability that is not enabled in `spec.management`. Before `config: Manage`, controllers only hold a read-only Talos client interface, so writes are impossible by construction.
7. Protected config fields never change.
8. The operator never applies config to a machine whose identity is not pinned, and hands a machine in maintenance mode its config only when the machine said, in that pass and on that connection, that it is the one declared (`plan.NextJoinStep`).
9. A machine is only reset the way a removal resets it, and its Node is only deleted once the machine is seen to have forgotten the cluster or a human confirmed it dead.

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
  - `talos.jarensaa.github.io/abort=<token>`: end the operation and leave nodes as they are. What was left of it is recorded and is **not started again**; otherwise the same rollout would begin anew a moment later. A spec that renders anything else for a machine, a revert included, is a new change: it rolls out and clears the record.

  Both are recorded in one place and one shape for every kind of operation: `status.holds`, by the type of the operation, as `skipped` and `aborted`, each a map from the machine to what the operation was taking it to (the hash of a config, an installer image). A removal has nothing a machine could be taken to instead, so its entries are empty and stand until the spec asks for the removal anew. A Kubernetes upgrade is not aborted machine by machine: its one entry, under `*`, is the version (§7.4). An etcd defragmentation records nothing: its interval keeps the next one away.
- Gates that can never become true are reported along with what is false. They are never silently bypassed.
- A hook that fails halts the operation the same way (`HookFailed`, §6.4): `retry` runs the hook again as a new Job, `skip-machine` leaves out the node whose `preNode` hook failed, `abort` ends the operation.

**Repairing an unhealthy node.** A config can pass its soak, be confirmed, and break the node later. Reverting the spec takes it back: the gate does not ask about the health of the node it is asked for (§6.3), and the planner puts that node first. The repair is a rollout like any other. The config is tried, the whole cluster, that node included, must then be healthy for the soak, and a config that does not make the node healthy is reverted by Talos and halts the rollout.

What this does not reach:
- **Two unhealthy nodes.** Neither is touched while the other is unhealthy, and verification would not pass anyway. A config that breaks every node it was confirmed on, some time later, is beyond a rollout; the soak is what stands against it.
- **A node whose Talos API does not answer.** Nothing can be sent to it.
- **A fault no node reports as its own.** If every node calls itself ready and Kubernetes agrees, but Talos's cluster-wide checks fail, the gate stays closed for every node: nothing says that the node to be touched is the cause.

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
   - Every member runs and is Ready to Kubernetes, etcd has exactly the control planes as healthy voters, and Talos's own health checks pass (`plan.VerifyGate`). Nobody is left out here: the node that was changed has to be healthy too.
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

Other failures of a node step: `Timeout` (the node's gate stayed closed, or a node that needed no change was not verified, for `stepTimeout`), `ConfigChangedUnderneath` (the node runs neither the previous nor the desired config), `Rejected` (Talos refused the config; nothing was applied if the dry run caught it). What Talos says about a config is withheld when a config source is a Secret, because validation errors can quote values.

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

#### 8.5.4 Last resort: an etcd snapshot, which is the owner's

A snapshot of etcd is the only recovery from quorum loss, and recovery is out-of-band (`talosctl bootstrap --recover-from`). The operator takes no snapshot (§12.23): what it does about quorum is to never be the cause of losing it. An owner who wants one taken before every operation runs their own snapshot Job as a `preRollout` hook (§6.4): no node is touched until the Job has succeeded, and the operation halts with nothing changed if it fails.

#### 8.5.5 Rejected alternative: node-local rollback agent (DaemonSet)

We considered running a DaemonSet that watches local connectivity and reverts the node's config itself. It was rejected for v1:
- **Credential blast radius.** Reverting needs `os:admin` on every node. A single compromised node would then hold credentials able to rewrite the whole cluster.
- **It depends on what just broke.** In-cluster Talos API access goes through the `talos.default` Service, i.e. over the CNI. A host-network pod calling localhost avoids that, but still needs kubelet and containerd to be healthy. machined's TRY timer needs none of this.
- **Partition ambiguity.** From inside, a node cannot tell "my network is broken" from "the rest of the cluster is down". Acting on a local view risks reverting good changes during a partition, and flapping. Reachability has to be judged from outside, with "no answer" defaulting to rollback, which is what TRY provides.

**Residual gap:** a failure that appears *after* confirmation, e.g. a DHCP renewal failing hours later. At that point the bad config is persisted, and recovery needs a console or a maintenance-mode reboot. A longer soak narrows this gap but cannot close it. A watchdog that reverts to a last known good config was considered for this gap and is not planned (§12.26): it would be a second thing on every node that changes its config, and the alert on a node that stops answering (§14.2) is what tells its owner.

---

## 9. Secrets and credentials

| Object | Contents | Owner/lifecycle |
|--------|----------|-----------------|
| `<cluster>-talos-secrets` | secrets bundle (`secrets.yaml` format) | Operator-created during adoption (or user-supplied, then verified). Needed to render configs. **No ownerRef**, so deleting the TalosCluster never deletes the PKI. An off-cluster copy is optional break-glass (§12.7). |
| Operator Talos client cert | Requested through a `talos.dev/v1alpha1` `ServiceAccount` `<cluster>-talos-operator` (roles `[os:admin]`, owned by the TalosCluster) in the TalosCluster's namespace. Talos answers with a Secret of the same name, key `config`. | Issued by Talos from the cluster's current CA: 6 h TTL, renewed 1 h before expiry (`kubeaccess/serviceaccount/crd_controller.go:49-50,90` in Talos v1.14). Nothing is stored outside the cluster, so it survives a rebuild with fresh PKI. Needs a `KubeTalosAPIAccessConfig` on the control planes allowing `os:admin` for the namespace; Talos then installs the CRD (`controllers/k8s/manifest.go:264`). The operator reads the CRD unstructured and never watches it, so a missing CRD is reported (`TalosAPIAccessDisabled`) instead of breaking startup; Talos's `status.failureReason` is reported as `TalosAPIAccessDenied`. The issued talosconfig names the in-cluster Service `talos.default`, which the operator never dials: endpoints are the control-plane Nodes' InternalIPs (§4.2). |
| Supplied talosconfig (`credentials.source: Secret`) | An admin talosconfig whose endpoints are node IPs | Alternative for clusters where workload API access cannot be enabled. Expires with its certificate (talosctl's default is 1 year); `CredentialsValid` reports the expiry. |
| `<cluster>-support-bundle` | What the machine of a failed operation said: the state of its services and the end of their logs and of the kernel's (§14.6) | Written by the operator when an operation halts, overwritten by the next failure, owned by the TalosCluster. A Secret because a log can quote a config. |
| `<cluster>-adoption-backup` | Live configs captured during adoption | Created once and kept. |
| `<cluster>-state` (considered) | Copy of critical `status` (bootstrapped, operation) with no ownerRef, so a CR re-create does not re-bootstrap | Prior art: alperencelik. M9. |

Other rules:
- Rendered configs, dry-run diffs and Events are **redacted**: secret-bearing fields are replaced by a hash. Diffs of documents that take values from a Secret-sourced patch are withheld entirely, and so are render errors when such a patch is involved.
- Configs are never logged.
- The operator keeps no credentials for people: no talosconfig and no kubeconfig for an administrator (§12.27).
- RBAC: the operator reads Secrets, creates them and updates them (the support bundle is the one it updates), and writes `talos.dev` ServiceAccounts, only in its own namespace. Anyone who can create a `talos.dev` ServiceAccount there obtains the allowed Talos roles, so that namespace is dedicated to the operator.
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
| **M2 Adoption** | Resources that reproduce a running cluster, written by the operator (§12.14), proven by Talos's own dry run to change nothing, then a gated switch to `config: Manage`; pools and shared configs; Talos-issued credentials | v0.2.0, v0.3.0 |
| **M3 Config management** | Classified config changes rolled out one node at a time, tried and confirmed only after verification from outside; drift reporting and enforcement; fail-closed with retry, skip and abort | v0.4.0 |
| **M4a Run as deployed** | The e2e tests the chart's image as installed: two replicas, the chart's CRDs, admission policies and permissions; a leader killed in the middle of a rollout is replaced and the rollout finishes | v0.7.0 |
| **M4b Talos upgrades** | A version or installer change takes every node there, one at a time: installed, drained, rebooted, verified; the operator upgrades the nodes it runs on; the config that names the installer follows (§7.3) | v0.7.0 |
| **M4c Rollout policy** | Maintenance windows, gates on objects in the cluster, and hook Jobs around an operation and around each node, for every kind of operation (§6.4) | v0.8.0 |
| **M5 Kubernetes upgrades** | A version change takes the components Talos runs there, one component on one node at a time, every API server first, a minor at a time; refused up front when the nodes' Talos does not run the version or when it would strand a flag or an object (§7.4) | v0.8.0 |
| **M6 Version awareness** | Status plans the upgrades to what the spec asks for and, where the operator is told to ask upstream, says which Talos and Kubernetes releases exist; an upgrade to an installer or image that does not exist is then refused before a node pulls; a `validate` command checks a repository's manifests before they are merged; an example bot configuration (§7.5) | v0.9.0 |
| **M10a The operator's state as metrics** | What status says, exported at every scrape by every replica: an operation in flight or halted, what holds a change back, drift, nodes that cannot be read, etcd membership. The chart installs a ServiceMonitor, alerts and a Grafana dashboard where the cluster has the Prometheus Operator's API (§14.2) | v0.10.0 |
| **M7 Adding machines** | A declared machine that waits in maintenance mode joins as a worker or a control plane, once it says it is the hardware that was declared and its config fits it; one at a time, on a cluster at rest (§7.6) | v0.11.0 |
| **M8 Removing and replacing machines** | A machine marked `Removed` is drained, reset, seen to have forgotten the cluster, and its Node deleted; a control plane only while three voters remain, which makes a replacement an addition followed by a removal; a dead machine after a human confirms it (§7.7) | v0.11.0 |
| **M10b The cluster's own health** | What Talos says about each node, in status, as metrics, with alerts and a second dashboard: services, Talos's warnings, clocks, links and bonds, volumes, the size of each etcd database, and how long the cluster's certificates last. A switch that makes the controller manager and the scheduler scrapable (§14.2) | v0.12.0 |
| **M11 Maintenance** | etcd defragmented before a database reaches its quota, as an operation behind the same gates as every other; the machine of a failed operation asked for its logs, kept in a Secret the failure names (§14.6) | v0.12.0 |

### What every remaining milestone builds on

- **One safety model.** A new kind of operation is a new planner and executor on the machinery M3 built: the one lock, a step persisted before its effect, decisions from what is observed, bounded waits, halt on failure with token-bound escape hatches, and "not read" never counting as "fine". No milestone brings its own.
- **One switch per capability.** Each new kind of write is off until its `spec.management` switch is on.
- **Four levels of proof.** Planner invariants as unit tests; controller behaviour against the fake; the real thing on QEMU; and, last, a stated check on a real cluster, run by its owner. A milestone is not done before the fourth.

### The real cluster

Not a milestone, and not blocked on one: adopting a real cluster, switching it to `config: Manage`, and rolling out one harmless config change. It needs the adoption runbook. It is the only place the operator meets a real CNI, storage on the nodes and real hardware, so what it turns up feeds every milestone below.

It is postponed on purpose (decided 2026-10-10). While no cluster depends on the operator, the API and the behaviour can change without anyone having to migrate, and M4 and M5 used that freedom. The price is that everything delivered so far stops at the third level of proof. What the fourth still owes:

| M | To be shown on a real cluster |
|---|---|
| M2, M3 | Adoption, `config: Manage`, and one harmless config change. |
| M4 | A Talos patch release, inside a maintenance window and behind a gate on the cluster's storage, with a hook around each node. |
| M5 | A Kubernetes minor release. |
| M6 | A version bump proposed by a bot, checked by `validate` in the repository that holds the spec, merged, and rolled out inside a window. |

### M4 to M6, as delivered

§6.4, §7.3, §7.4 and §7.5 describe the rollout policy, Talos upgrades, Kubernetes upgrades and version awareness as built.

- **Shown on QEMU:** a Talos patch upgrade of every node with a workload kept up behind a disruption budget; the operator upgrading the nodes it runs on and carrying on; the leader killed in the middle of a config rollout; a rollout held back outside its window and behind a gate, with hooks run as Jobs around it and around each node; a Kubernetes minor upgrade, API server restarts included, refused first while a kubelet was started with a flag the new version removed, with the config change that took the flag out going through meanwhile.
- **Shown against the fake only:** a node that falls back to its previous boot, a drain a budget never allows, an image that cannot be pulled, a Kubernetes component that does not come up, a hook that fails or does not finish; each halting with the node as the failure left it and resuming on retry. Also a Kubernetes version the nodes' Talos does not run, one more than a minor away, and objects of an API the new version stops serving: an API server does not serve a stand-in for such an API, and the real ones are alpha APIs a cluster has to be started with.
- **Shown for M6:** on QEMU, status listing what really exists upstream, the resources of an adopted cluster passing `validate` against the real Image Factory and registries and failing it for a schematic that does not exist, and a Talos bump held to its maintenance window. Against a fake upstream: an installer and an image that do not exist refused before a node is touched, a hop through the newest release of a minor, and a source that is down refusing nothing. The example Renovate configuration has not been run.
- **Out of M5:** anything Talos does not run itself (CNI, CSI, workloads), the manifests it applies at bootstrap (§12.20), and rolling a Kubernetes upgrade back. A failed upgrade halts and is fixed forward; the one component that was being changed goes back by itself.
- **Not built in M4:** more than one worker at a time, a rollback to the previous boot slot on request (§12.19), and reinstalling a node for a change only an install can make, which stays refused (`RequiresReinstall`) and belongs with replacing machines (M8).

### M7 and M8, as delivered

- **Shown:** the planners' rules as tables and as sweeps over every combination of what can be observed (a config is only handed to a machine that just said it is the one; nothing is taken while a gate is closed; a node is reset once, and only while it answers; an etcd member is removed from the outside only for a machine called dead). Against the fake: a worker added, a control plane added and not done before it votes, a wrong identity sent nothing, a control plane replaced, a removal out of three refused, a dead worker and a dead control plane taken out after confirmation. On QEMU: a machine in maintenance mode added as a worker after being refused under a wrong identity, taken out again and found waiting in maintenance mode, the removal of a control plane the cluster cannot spare refused, a machine that was cut off for good taken out after confirmation; and on three control planes, one replaced with three voters throughout.
- **The identity pin stays the trust** (§12.3, §12.25). What was added to it: the config goes out on the connection, and to the key, that the identity was read from.
- **Out of both:** getting a machine to boot; finding machines; shrinking the control plane; wiping other disks; taking storage out of service (the user's hooks).

### M10 and M11, as delivered

- **M10a** (§14.2). **Shown:** the alert rules are tested by promtool on the series the exporter really exports, a halted rollout among them, each rule broken once to see its test fail; on QEMU the halted rollout is found in the metrics of both replicas, with the labels the alert carries, and is gone from them once it is aborted. The QEMU cluster has no Prometheus, so no alert was seen to fire there. Off any cluster, a Prometheus loaded with the chart's rules and scraping the exporter's output for a halted rollout fired `TalosOperationHalted`, and a Grafana showed the dashboard filled from it; neither has been seen on a cluster yet.
- **M10b** (§14.2). **Shown:** every new alert has a scenario in the rule test, a clock that is off and a bond that lost a member among them, and scenarios for what must stay quiet (a port nothing is plugged into, an etcd that waits for its interval, a node that can no longer be read); each rule was broken once to see its test fail. Both dashboards are held against what the exporter describes, so a panel cannot ask for a series or a label that nobody exports. On QEMU every node reports every service healthy, its clock synchronised and its volumes ready, exactly one control plane leads etcd, and both replicas export it; the controller manager and the scheduler refuse a connection from outside their node before the switch and answer after it, with no reboot and no worker touched. **Not shown:** the dashboard in a Grafana on a cluster.
- **M11** (§14.6). **Shown:** the planner's rules as tables (who is defragmented, in what order, what blocks it); against the fake, every member defragmented with the leader last and not again within the interval, nothing started while the cluster is unhealthy or paused, the next member not begun before the last was verified, a halt at a member that cannot be defragmented with the node's logs kept, and nothing written over a Secret that is not the operator's. On QEMU, a defragmentation of every member with the leader last, and a bundle for the rollout that Talos reverted.
- **Out of both**, each for a reason (§12.27): handling an alarm of etcd, keeping credentials for people, a switch for etcd's metrics, and a switch for shipping Talos's logs.

### Later, unscheduled

**Rotating the cluster's PKI.** Adoption leaves the old PKI with whoever held it before: the talosconfig and the secrets the cluster was built with still open it. Until the operator can take a cluster through a rotation, its owner does it with `talosctl`, and the operator then has a secrets bundle that no longer matches. That it says so (`SecretsMatchLive`) is all it does about it today.

### Descoped

What the earlier plan kept under "M9, later" and is no longer planned (§12.26):
- **Bootstrapping a cluster from nothing** (§7.8).
- **A management-cluster mode.** Without bootstrapping there is little left for one to do that the operator inside the cluster does not.
- **Discovering machines instead of declaring them** (SideroLink, configs pulled by UUID).
- **The operator choosing versions.** A bot proposes them in the repository that holds the spec, and `validate` checks them (§7.5, §12.2).
- **A command-line tool that runs the same planners.** What an operator cannot do for the cluster it lives in is a recovery, and recovery is `talosctl` and the owner's runbook (§1); the `render` command went the same way (§14.5).
- **A watchdog for failures that appear after a change was confirmed** (§8.5).

### Owned by no milestone, and needing a decision

- **A changed etcd argument takes effect only when etcd restarts** (§6.3, §12.27). Restarting etcd one control plane at a time, behind the etcd gates, is an operation the operator does not have. Until it has, a change under `cluster.etcd` waits for the node's next reboot without anything saying so, and the metrics switch leaves etcd out.

- **Upgrading the operator itself.** The API is `v1alpha1` and may change between releases, but nothing says what a release may change in it, or how stored objects get from one shape to the next. How the CRDs themselves are upgraded is settled (§12.15). The rest needs a rule before a real cluster depends on it.
- **Scale.** Nodes are read one after the other and workers roll one at a time. Both are right for a handful of nodes and wrong for a hundred.

### Suggested order

1. The real cluster, when its owner decides the API has settled enough to depend on.

R1 is covered by M1–M3, R3 by M2, R2 by M4–M6, and R4 by M7 and M8. No milestone is open: what is left is under "Later" and under "Owned by no milestone".

### Testing strategy
- **Pure unit tests** for planners (invariant tables), render/diff/patch derivation (golden files from real-shaped multi-doc configs with throwaway secrets), and compatibility ordering.
- **envtest** controller tests against the fake Talos client: step persistence, resume after restart, fail-closed behaviour.
- **e2e on QEMU** (`make e2e-cluster-up`, see `docs/dev/e2e.md`). The Docker provisioner is unusable for Talos 1.14 on Docker Desktop: container mode needs a host kernel ≥ 6.15 and Docker Desktop ships 6.12. It also cannot exercise upgrades, because `LifecycleService` refuses container mode. QEMU on macOS needs root, so the harness runs only `talosctl cluster create/destroy` under sudo. The `maintenance` preset lets the harness apply configs built with `gen config` and patches, and leaves spare nodes for expansion tests.
- **Staged rollout to a real cluster:** each milestone's last check runs there (§10), with a fresh off-cluster etcd snapshot beforehand, which is its owner's to take (§8.5.4).

---

## 11. Risks

| Risk | Mitigation |
|------|------------|
| Talos API churn: 1.13 and 1.14 both reshaped upgrade and config; legacy Upgrade is removed in 1.18 | Access layer isolates machinery. Supported-range gate per operator release. Track Talos minors. |
| Rendering parity with talosctl defaults; Omni's import broke on exactly this with 1.14 (#3424) | The adoption exactness proof is mandatory. Golden tests against `talosctl gen config` output per contract. |
| Quorum loss on a 3-CP cluster during an operation | Strict CP serialisation, etcd gates, never `force`. A snapshot before control-plane operations is the owner's to take (§8.5.4). DR stays out-of-band. |
| Operator evicting itself mid-step | Persist before side effect, resume by observation, 2 replicas. |
| TOFU secret disclosure on node add | Mandatory UUID pin. Keep the network machines boot on one that is trusted. |
| Ceph data loss or unavailability | Ceph CEL gate, noout hooks, never wipe user disks, one node at a time. |
| A PDB-blocked drain hangs | Bounded `drainTimeout` leads to fail-closed. |
| Bootstrap manifests applied over objects someone else owns | The operator applies none of them (§12.20). |
| PKI known to previous credential holders after adoption | Documented. Rotating it is the one thing left for later (§10). |
| machinery requires go 1.26.5 plus `replace` directives | Pin the toolchain in go.mod. Verify the build in M0. |
| Network change that breaks a node is confirmed anyway | Verification goes through a different node's apid and needs a fresh kubelet heartbeat, plus a soak period inside the TRY window. Read back active and persistent hashes after confirming (§8.5.1). |
| A change of a "safe" class breaks a node | Every change is tried, whatever its class, so the node reverts by itself (§6.3). |
| A confirmed change breaks a node later, and the health gate refuses the rollout that would revert it | The gate leaves out the health of the node it is asked for, so reverting the spec repairs the node, as long as it is the only unhealthy one (§6.3, §8.3). |
| Failure that appears after a confirmed change (e.g. DHCP renewal hours later) | Not covered by TRY; needs console or maintenance-mode recovery. A longer soak narrows it; the alerts of §14.2 say when it happened. |
| Early-boot hang after an OS upgrade (before machined starts) | No automatic A/B fallback in that case. One node at a time keeps quorum; manual recovery via console (§8.5.2). |

---

## 12. Decisions

Decided 2026-10-08:

1. **Topology: self-hosted first.** The operator runs inside the cluster it manages. The core is written against a target-cluster abstraction so that a management-cluster mode could be added. None is planned (26).
2. **Automatic upgrades = Renovate bumps Git.** The operator rolls out spec version changes automatically, within maintenance windows and behind gates. The operator choosing versions by itself is not planned (26), and `autoUpgrade` is dropped from the v1alpha1 API.
3. **New-node trust: mandatory UUID pin** in `TalosMachine.spec.identity`.
4. **Config ownership: full render.** The operator owns the complete config, and everything human-authored is config documents layered onto the generated defaults (§6.1).

5. **API group and module path** (decided 2026-10-09):
   - Go module `github.com/jarensaa/talos-operator`;
   - API group `talos.jarensaa.github.io`, version `v1alpha1`;
   - `TalosCluster` and `TalosMachine` are **namespaced**: `credentials.secretRef` and `secretsRef` are same-namespace Secret references.
   - Annotation keys use the same prefix, e.g. `talos.jarensaa.github.io/orphan`.
6. **Secrets from outside the cluster are the owner's to deliver** (decided 2026-10-09). The operator reads them from Secrets and does not care how they got there. Since 7, no Talos credential or PKI is among them, and since 23 no storage credential either: so far the operator needs none.
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
13. **Multi-control-plane tests run on the standard hosted runner, weekly and on request** (decided 2026-10-10). Serialising control planes (M4b), growing them (M7) and replacing one (M8) cannot be shown on the one control plane a pull request's check runs.
   - **Where:** GitHub's standard hosted runner. For a public repository it has 4 vCPUs and 16 GB, and three control planes with a worker have passed on it in about the time a pull request's run takes. A larger runner needs a paid organisation, and a self-hosted runner attached to a public repository is a risk of its own; neither is needed.
   - **When:** the weekly run uses three control planes, so the shape is exercised without anyone remembering it. A manual run takes the shape as inputs and is how a branch is shown before it is merged. Pull requests keep one control plane and one worker as their required check, because that is quick and every change needs it.
   - **What depends on people:** a change to how control planes are handled is run on three control planes before it is merged, and its pull request links the run (docs/dev/e2e.md). If that proves easy to forget, the next step is to run the larger shape on every pull request, which costs a few minutes each and nothing else.
   - **What would change this:** a scenario that does not fit 16 GB, for instance several upgrades with image pulls in flight, or a hosted runner that turns out too slow for the timeouts a real cluster would use.
14. **Adoption creates the resources** (decided 2026-10-10). A GitOps tool fights an operator that keeps writing objects, which is the usual reason for an operator never to create the user's resources. A one-way handover removes that reason.
   - **While a cluster is being adopted, the cluster describes the resources.** The operator creates the pools and machines of the live cluster and fills in what the TalosCluster's spec leaves empty, without asking. Adding the TalosCluster is the consent, and none of this writes to a node.
   - **Once adopted, the resources describe the cluster, and there is no way back.** The phase only moves forward: Adopting, Adopted, Managed. From Adopted on the operator never writes a spec again, and it gives up its recorded ownership of every spec field it set, so that whoever manages the resources next is their only owner. A field later removed there is then really removed.
   - **Whether the resources match the nodes right now is a separate question**, answered by a condition that can turn false again and that still gates `config: Manage`. An edit made between adoption and management is checked like any other.
   - **The operator never overwrites what someone else set**, also while adopting: it applies without force, so a field another manager changed is a conflict and the object is left alone (§7.1). The phase lives in status, and status can be lost; that must never make the operator overwrite what someone else manages.
   - **Delivery stays the owner's business** (§12.9). With Flux, a Kustomization leaves objects alone that are not in Git, and takes over the fields that are once they are committed. There is no separate proposal to review: the resources themselves are what the operator found, and a second copy would only go stale once someone edits them. Whoever wants them in Git exports them from the cluster. What is worth knowing before taking them over, such as a node whose Talos version differs from the one its config installs, is reported on the TalosCluster. Helm only takes over objects that carry its ownership labels, which is the user's to add if they deliver that way.
15. **The chart installs and upgrades the CRDs; the operator never does** (decided 2026-10-10). An operator that registers its own CRDs needs cluster-wide rights to do it, has two replicas of different versions writing them during an upgrade, and makes a first GitOps sync fail until it has started. Helm's `crds/` directory avoids that but installs once and never upgrades. So the CRDs become chart templates: on by default, so that `helm upgrade` upgrades them, switchable off for those who manage CRDs separately, and kept on uninstall, because deleting a CRD deletes every object of its kind.
16. **An upgrade changes the system first, and its config second** (decided 2026-10-10, with M4b). The config names the installer, so a version change is also a config change. Applying the config first would put a config on a node that says what the node does not run; folding it into the upgrade would put a tried config under a reboot, which reverts it. So the upgrade leaves config alone, and the installer image is a change class of its own that a config rollout applies once the node runs what it names (§6.3, §7.3). The cost is a second pass over the nodes, with a soak each and no reboot.
17. **Downgrades are refused** (decided 2026-10-10, with M4b). Talos allows some. A downgraded node may not understand the config it has, and the operator has no way to prove beforehand that it does.
18. **The e2e tests the operator as deployed** (decided 2026-10-10, with M4a). The suite starts no controllers of its own: the image built from the checkout runs in the test cluster, installed by the chart with two replicas. The image reaches the nodes through Talos's image import, with the harness's admin access, so no registry is involved and no registry configuration ends up in the nodes' config. What an in-process manager could not show is what this is for: the chart's permissions, leader election, and a rollout that outlives its leader.
19. **One node at a time, and no rollback on request** (decided 2026-10-10, with M4c). Several workers at once would make an upgrade of a large cluster faster, and would need a budget for how many, an order among those in flight, and an answer for one of them failing while the others are half way. Nothing the operator is used for yet has enough workers to pay for that. Rolling a node back to its previous boot slot by annotation (`MachineService.Rollback`) would be a fourth kind of Talos access for a case Talos already handles where it matters: a system that does not come up falls back by itself (§8.5.2). A system that comes up and is wrong is taken back by asking for the previous version, which is a downgrade and refused (§12.17); that refusal is the thing to revisit if the need shows up, not a side door around it.
20. **The manifests Talos applies at bootstrap are nobody's** (decided 2026-10-10, with M5). Talos applies kube-proxy, CoreDNS and their RBAC when a cluster is bootstrapped and never again (`T/internal/app/machined/pkg/controllers/k8s/manifest_apply.go`); `talosctl upgrade-k8s` re-applies them, with force as an option. On an adopted cluster they have often been edited, replaced by a CNI's own proxy, or put under GitOps since, and applying them would take cluster-admin rights and make the operator a second owner of objects in `kube-system`. So a Kubernetes upgrade changes a node's config and nothing else. That includes the kube-proxy image a control plane's config names: it belongs to the version, and a config that named another would have Talos apply the wrong kube-proxy wherever it does apply that manifest. Nothing runs from it. A kube-proxy DaemonSet that a Kubernetes upgrade left behind is named in `KubernetesUpToDate`: kube-proxy may be older than the API server, not newer, so being left behind is safe for a while and never silent.
21. **A Kubernetes upgrade applies its own configs, and a larger jump goes through `.0`** (decided 2026-10-10, with M5). `talosctl upgrade-k8s` patches each node's config component by component; an operator that owns the config has to do the same itself, or the two would fight (§3.8). The patch is taken from the node's live config, not from the rendered one, so that the upgrade changes one image and leaves every other difference to the config rollout that owns it. The config rollout in turn leaves the images alone: it applies the rendered config with the images the node has. An earlier cut blocked a rollout whenever an image differed, and so a refused upgrade blocked the removal of the very flag it was refused for. Each patched config is tried and confirmed like any other change, which upstream does not do: a component that does not come up goes back to its previous image without anyone reaching the node. Kubernetes does not skip minors, so a spec that is further away is reached through the first release of each minor in between. The operator knows no newer release of those minors without asking the network, which is M6's business; until then, `.0` is the one version of a minor that is sure to exist.
22. **The operator may ask upstream what exists, only that, and only when told to** (decided 2026-10-11, with M6). Knowing which releases exist is what turns "the image could not be pulled" on the first node into a refusal before any node, and what lets a hop over several Kubernetes minors use a release with its fixes. The price is that the operator talks to something other than its cluster, and to services that are not this project's. How others do it: Omni lists the releases of Image Factory and scans a registry for Kubernetes tags every fifteen minutes, by default, and filters them with the compiled-in compatibility rules; OpenShift's version operator and the k3s upgrade controller ask an update service or a channel server; kubeadm asks the Kubernetes release site. Each of them asks a service its own project runs. tuppr, the one that like this operator is a third party to Talos, asks nothing: the version is what the resource says, a bot proposes it, and a release that does not exist fails at the pull. So asking is **off by default** (`versionSources.enabled`), and switching it on is the cluster owner's decision to use those services. Where it is on it is kept small: reads only, HTTPS only, a handful of requests every six hours, identified by a User-Agent, never during an operation. What it learns can refuse an upgrade and can choose a release on the way to the version asked for; it can never choose the version. The services asked are Image Factory, the Kubernetes release site and the registries the cluster's nodes pull from anyway. Image Factory's API reference documents the routes used as public; neither it nor the Kubernetes release site publishes terms for its use that were found, `registry.k8s.io` is a volunteer-run service that asks heavy users to mirror, and GitHub's acceptable use policy forbids burdening its servers by automation without naming a limit. Nothing was found that forbids these requests, and nothing that grants them either. The `validate` check asks the same services by default: it runs once for a pull request, beside the bot that proposed the change by asking them too. It is a subcommand of the operator's own binary, so that the image a cluster runs is the image that checks its manifests. **Not kept: a compatibility matrix of this project's own.** The rules (what runs what, which steps are allowed, what a release removed) are machinery's and go-kubernetes's, compiled in, and the removed kubelet flag the e2e uses is found by them. A table of the newest release of each minor, built in and refreshed by a bot, would let an operator that asks nothing go through a better release than `.0`; it is not built.
23. **Backups are the owner's** (decided 2026-10-11). The earlier plan had the operator take etcd snapshots on a schedule and before every control-plane operation, encrypt them and ship them to object storage. That is a backup product inside an upgrade operator: storage, credentials from outside the cluster, encryption keys that must outlive the cluster, retention, and a restore that has to be rehearsed to be worth anything, which the operator cannot perform on the cluster it runs in. Whoever runs a cluster already answers for backing up what is on it, and has tools for it; a second, partial answer in the operator would be the one nobody tests. So the operator takes no snapshot and no other backup. What it owes the owner is not to cause the loss (§8), and a place to hang their own snapshot before an operation: a `preRollout` hook (§6.4).
24. **Metrics are the status, read when scraped, and monitoring is installed where there is a Prometheus** (decided 2026-10-11, with M10a). The operator keeps no metric of the cluster's state beside the status it writes: a collector reads the resources at every scrape. Gauges set by the controllers would be the leader's alone, would outlive the resource they describe, and could drift from the status. The price is that nothing is exported that is not in status. The chart cannot ask whether a Prometheus is running, but it can see whether the cluster has the Prometheus Operator's API, and installs its ServiceMonitor, rules and dashboard exactly then (`auto`); a cluster without it gets none of the three, and a value forces either way. A chart finds this out when it is installed or upgraded, so a Prometheus that arrives later is picked up by the next upgrade. Grafana has no API to be told by, so the dashboard goes by the same sign: it is a ConfigMap, which does no harm unread. A Prometheus only reads what its selectors match, which the chart cannot know: `monitoring.labels` is where the owner says it.
25. **Machines are added and removed like everything else is done, and a machine that left has to be seen to have left** (decided 2026-10-11, with M7 and M8). The earlier plan let workers join in parallel, outside the lock; one machine at a time through the one driver is the rule (§12.19), and a second way of doing things would have been a second set of invariants. The identity pin remains what a new machine is trusted by (3): a machine in maintenance mode can prove nothing, so what can be had is that the pinned identity is read in the very pass, and over the very connection and key, that the config then goes out on. A removal resets with a reboot and is done when the machine answers in maintenance mode again: a machine that was only asked to wipe itself may not have, and it holds the cluster's CA keys. For the same reason a machine that does not answer is never guessed about; a human says it is dead, with the token of that one failure. A control plane leaves only while three voters remain and there is no override: shrinking a control plane is rare, and an annotation that makes a quorum loss a typo away is not worth having for it. A TalosMachine is kept from deletion by the API server until `removedAt`, where the plan had a finalizer: a finalizer needs the operator to be there to let go. Only Talos's own two partitions are wiped.
26. **The operator manages a cluster that exists, and what was parked as "later" is mostly not coming** (decided 2026-10-11). With R1 to R4 delivered, the list of things for some later milestone was looked at again. Bootstrapping is out: an operator that runs inside its cluster has nowhere to run before there is one, and building a cluster is a solved problem that is not this project's. A management-cluster mode, machine discovery, the operator choosing versions, a planner CLI and a last-known-good watchdog go with it; each either served bootstrapping, duplicates what a tool beside the operator already does, or would be a second mechanism on every node. Monitoring stays and grows (M10b), because the operator already speaks to a Prometheus and a Grafana, and a cluster's own health belongs on the same page. Rotating the PKI is the one thing kept for later: adoption leaves the old keys with whoever built the cluster, and nothing else in the list answers that.

27. **Upkeep prevents what an operator inside the cluster cannot repair, and four things the earlier plan had are not built** (decided 2026-10-12, with M10b and M11).
   - **An alarm of etcd is not handled, and not even read.** The plan was to defragment and disarm on `NOSPACE` and to alert on `CORRUPT`. With either alarm raised etcd refuses writes, and with them every way an operator inside the cluster has of saying or doing anything: status, Events, the step it must persist before a side effect. A metric of the alarm would have to be a gauge beside the status (§12.24), exported by a process that can do nothing else. So the upkeep is to never get there: etcd is defragmented while it accepts writes, and an alert fires on a database at four fifths of the default quota. Getting out of an alarm is `talosctl`, and no connection of the operator can list or disarm one (`internal/talos/health_test.go`).
   - **Defragmentation is an operation, not a background task.** A member answers nobody while it is defragmented, which is a disruption like any other: it takes the lock, goes through the driver (§6.4), and each member waits for the gates and is verified before the next. Every member is defragmented when one is over the threshold, because what fragments one fragments them all, and the leader goes last. The time of the last one is recorded when it starts, so one that fails or is aborted is not tried again before the interval has passed.
   - **Status follows a number that always changes at a distance.** The size of an etcd database changes with every write to the cluster, and a status that followed it would be written in every pass, which breaks the rule that a cluster at rest is not written to. Status follows it once it has moved by a twentieth of the file. What decides a defragmentation is read where it is decided, not from status.
   - **The support bundle is what the node says, in a Secret, and nothing more.** The plan named `go-talos-support` and a PVC or an object store. That library collects for a person at a terminal: hundreds of resources, a controller graph, logs of every pod, as an archive of tens of megabytes. What explains a node that did not come up is the state of its services and the end of their logs and of the kernel's, a few hundred kilobytes compressed, and a Secret needs no storage that could be missing. One per cluster, overwritten: it is for the failure somebody is looking at.
   - **No credentials for people.** The plan had optional talosconfig and kubeconfig Secrets for administrators, refreshed on a timer. The operator holds the cluster's authorities and could mint them, and that is the reason not to: an administrator's credential lying in a Secret is readable by whatever can read Secrets in that namespace, renewed forever, and revoked by nothing. Whoever holds the cluster's secrets makes one with `talosctl` when needed. What the operator does is say when the certificates it knows end, by how much of each is left: Talos issues the operator's own for six hours and renews it, an authority lasts ten years, and one rule has to cover both.
   - **The metrics switch covers the controller manager and the scheduler, not etcd.** Reading Talos's source for this showed that etcd is handed its arguments only when it is started and is restarted for no config change. A switch that named a metrics address for etcd would be rolled out, verified and in sync, and change nothing until the node reboots. The same is true of any etcd argument a user declares, which §6.3 had wrong. The fix is an operation that restarts etcd member by member; it is not built (§10).
   - **Shipping Talos's logs is config the user declares.** It needs a collector listening on every node, which is the owner's to run, and with that it is three lines of config that roll out like any other. A field of the spec that renders those three lines adds a name to learn and nothing else. The guide has the lines.

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

> Built with M6, M10 and M11. The section began as the plan for them; it now says what was built, and where that departs from the plan it says why (§12.27).

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

**The switch for the control-plane components (M10b, built).** `spec.observability.controlPlaneMetrics: Enabled` makes the controller manager and the scheduler of every control plane listen on every interface (`bind-address: 0.0.0.0`). Both authenticate and authorize whoever asks, and their probes ask on localhost, so the node's own address alone would not do.
- It is rendered before the user's layers (`render.Input.ControlPlaneMetrics`), in the form of config the version contract has: the components' own documents from 1.14 on, the v1alpha1 fields before. A config must not have both. A layer of the spec that sets the same argument wins.
- It goes through a config rollout like any other change, classified as a control-plane restart, and shows up in the diff.
- **etcd is not part of it** (§12.27): Talos hands etcd its arguments only when it starts it (`T/internal/app/machined/pkg/system/services/etcd.go:111`), and restarts it for no config change. `cluster.etcd.extraArgs.listen-metrics-urls` in a user's layer is applied and takes effect at the node's next reboot.
- The matching values of kube-prometheus-stack (its controller-manager and scheduler endpoints, `kubeProxy` where there is none) stay with whoever deploys it.

**What is exported (M10a and M10b, built).** `internal/exporter` exports what the operator has written into the status of its resources, and that includes what it read from Talos about each node (below). It is a collector that reads TalosClusters, TalosMachinePools and TalosMachines from the manager's cache when it is scraped, and keeps nothing itself:
- every replica exports the same, the leader or not, so a rule takes the maximum over them and an alert does not depend on which replica is scraped;
- a resource that is deleted takes its series with it;
- no metric can say anything else than the status a person reads, and no node is asked anything for a scrape;
- a scrape that cannot read everything exports nothing about the resources and counts it (`talos_operator_state_read_errors_total`): half of a state would look like a state.

Every series of a cluster carries `namespace` and `taloscluster`, and of a machine also `machine`. The label is not called `cluster`, which a Prometheus that watches several clusters uses for itself.

| Metric (`talos_operator_…`) | Says |
|---|---|
| `info{version,revision}` | An operator is running. No series means none is scraped. |
| `cluster_phase{phase}`, `cluster_condition{condition,status,reason}` | The phase, and every condition as it is now. Messages are not exported: they are prose, and can quote a node. |
| `cluster_observed_timestamp_seconds` | When the last observation finished: how old everything else is. |
| `cluster_operation_in_progress`, `cluster_operation_halted`, `cluster_approval_pending` | 0 or 1, always there. |
| `operation_info{type,target}`, `operation_start_timestamp_seconds`, `operation_turns{state}` | The operation that holds the cluster, and how far it is. |
| `operation_failure_info{type,machine,step,reason,hook}`, `operation_failure_timestamp_seconds` | Where a halted operation waits. There only while it is halted. |
| `cluster_version_info{component,desired,running}`, `cluster_version_plan_steps` | Versions asked for and run, and the upgrades between them (§7.5). |
| `cluster_etcd_members{role}`, `machine_etcd_member{role}` | Voters and learners, as last observed; for each control plane, whether it is one. |
| `machine_info{pool,role,node,talos_version,kubelet_version,schematic}`, `machine_phase{phase}`, `machine_condition{…}` | A machine, what its node runs, and its conditions: `NodeObserved` is whether it can be read. |
| `machine_ready`, `machine_hold`, `machine_config_change_info{class}` | Ready when last read; on hold; a config change that is pending, by class. |
| `machine_service_healthy{service}`, `machine_diagnostic_info{id}`, `machine_time_synced` | What Talos says about a node: each of its services, each warning it raises about the node's configuration, the clock. |
| `machine_link_up{link,bond}`, `machine_link_speed_mbps{link}`, `machine_volume_ready{volume,phase}` | The physical links with the bond they are in, and Talos's volumes. |
| `machine_etcd_leader`, `machine_etcd_db_size_bytes`, `machine_etcd_db_size_in_use_bytes` | For a control plane: whether it leads etcd, and its database. |
| `cluster_certificate_issued_timestamp_seconds{certificate}`, `cluster_certificate_expiry_timestamp_seconds{certificate}`, `cluster_etcd_defragmented_timestamp_seconds` | The validity of the certificates in status; when etcd was last defragmented. |

**What Talos says about a node (M10b).** machined, apid, trustd and containerd have no metrics endpoint, so this state exists nowhere a Prometheus could read it. The TalosMachine controller reads it in the pass it already makes over its node, through `nodeReads`, into `status.health` and `status.etcd`:
- **It describes a moment.** Each part is in status only when it was read in the last pass, and a node that does not answer has none of it: nothing of it is exported then, and nothing says the node is fine. `TalosMachineNotObserved` is the alert for that node.
- **It costs five reads of a node per pass**, and no read at a scrape.
- **A quiet cluster is still not written to.** Services, links, volumes and the clock change when something happens. The size of an etcd database changes all the time, so status follows it only once it moved by a twentieth of the file (`followedSize`).
- **It is bounded:** 64 entries of each list, and 512 bytes of what Talos says in prose, which is in status and never a label.
- Links are the physical ones. Volumes are Talos's own, user volumes included.

Beside them are `talos_operator_rejected_rpcs_total`, the Talos calls the operator's own allowlist refused, and what controller-runtime exports of itself.

**What the chart installs** (`monitoring` in its values): a `ServiceMonitor`, the alerts as a `PrometheusRule`, and two Grafana dashboards in a ConfigMap labelled for Grafana's sidecar: what the operator does (`files/dashboard.json`), and what Talos says about the nodes (`files/dashboard-nodes.json`). A unit test holds every expression of both against the series and labels the exporter describes. Each is `auto`, `true` or `false`, and `auto` installs it where the cluster has the Prometheus Operator's API, which is how a chart can tell that there is a Prometheus to read it (§12.24).

**Alerts** (`charts/talos-operator/files/alerts.yaml`):

| Alert | Severity | When |
|---|---|---|
| `TalosOperatorDown` | critical | No operator has been scraped for 10 minutes. |
| `TalosOperatorCannotReadState` | warning | The operator cannot read its own resources. |
| `TalosClusterNotObserved` | warning | The last finished observation is more than 10 minutes old. |
| `TalosOperationHalted` | critical | An operation failed and waits for a person. |
| `TalosRolloutBlocked` | warning | A change has waited 30 minutes for anything but a pause or its maintenance window: a closed gate, an approval, a protected change. |
| `TalosConfigDrift`, `TalosNodeWithoutMachine` | warning | A node does not run what the operator applied and is left so; a node has no TalosMachine. |
| `TalosVersionsUnreachable` | warning | The spec asks for versions there is no way to. |
| `TalosAPIUnreachable` | critical | No working connection to the Talos API, or no credentials, for 10 minutes. |
| `TalosClusterUnhealthy` | warning | Talos's own health check has failed for 15 minutes. |
| `TalosMachineNotObserved`, `TalosMachineNotReady` | warning | A node cannot be read; a node answers and is not ready. |
| `TalosControlPlaneNotEtcdVoter` | warning | A control plane is no voting member of etcd. |
| `TalosServiceUnhealthy`, `TalosNodeDiagnostic`, `TalosVolumeNotReady` | warning | For 15 minutes: a Talos service does not run or fails its health check; Talos raises a warning about a node; a volume of Talos is not ready. |
| `TalosNodeClockNotSynced` | warning | A node's clock has not been synchronised for 10 minutes. |
| `TalosBondMemberDown` | warning | A link that is a member of a bond has been down for 5 minutes. A link in no bond raises nothing: it may be a port nothing is plugged into, and a node whose only link is down cannot be read at all. |
| `TalosEtcdDatabaseLarge` | critical | An etcd database is over four fifths of etcd's default quota of 2 GiB. A quota set through `extraArgs` is not known to the rule. |
| `TalosEtcdFragmented` | warning | A database has been due for defragmenting for an hour, and what holds it back is not a pause, a window or the interval. |
| `TalosCertificateExpiring`, `TalosCertificateExpired` | warning, critical | Less than a tenth of a certificate's validity is left; none is. A tenth is half an hour of the six hours Talos issues the operator's certificate for, and a year of an authority's ten. |

A rule that names a series or a label wrongly is silent for good, so the rules are tested on what the exporter exports: `internal/exporter` holds scenarios, each a state of the resources and the alerts it has to raise, and writes from them a test for promtool whose input is the collector's own output (`make alerts`, `make alerts-test`). Every alert is asked about in every scenario, the ones that must stay quiet included, and an alert that no scenario raises fails the unit tests.

**Not built:** the OOM handler's events, which the earlier plan listed with the rest. CPU, memory, disk and network are a node exporter's.

### 14.3 Logs

Talos can push its logs to a collector:
- **Service logs:** `machine.logging.destinations`, which is v1alpha1 only. JSON lines over `tcp://` or `udp://` (`T/internal/app/machined/pkg/runtime/logging/sender_jsonlines.go:53-77`), with fields `msg`, `talos-time`, `talos-level`, `talos-service`.
- **Kernel logs:** the `KmsgLogConfig` document.
- **Machine events:** `EventSinkConfig` (gRPC), or pulled via `MachineService.Events`.

These cover etcd, kubelet, apid and containerd logs, which are not pod logs and are invisible to normal Kubernetes log collection.

**The operator has no feature for it** (§12.27). Shipping them needs a collector that listens on every node, and with one it is a few lines of config in a layer of the spec, which the guide gives. What the operator does for the case that matters most, a rollout that went wrong on a cluster with no log backend at all, is the support bundle (§14.6).

### 14.4 Upgrade-path awareness

Built with M6: §7.5 describes it. What the earlier plan had here and M6 left out:
- **Talos minors stepping one at a time, onto each minor's newest patch.** Talos's own tables allow more, and the operator follows the tables. Within the two minors this operator supports there is only one step to take.
- **A richer `status.versions`**, with every release marked as supported or not and a recommended path that the spec does not ask for. Status lists what exists and plans what the spec asks for.
- **The schematic as data**: declaring extensions and kernel arguments in the spec and letting the operator derive the schematic ID.

### 14.5 Version bump automation

Built with M6 as far as it is the operator's: the `validate` command (§7.5) and, in `docs/examples`, a Renovate configuration and a workflow that runs the check.

Out of scope: a `render` command that prints machine configs from the manifests and a secrets bundle, for rebuilding a cluster by hand. A cluster that is lost is rebuilt with fresh PKI from its owner's own runbook (§1, §12.7), so configs rendered with the old bundle are not what a rebuild needs, and their output would be a secret to handle outside the cluster.

One conflict to watch for where the check is a required one: automation that commits straight to the main branch, such as an image update bot, is blocked by it unless the rule is scoped to the paths that hold the cluster's manifests.

### 14.6 Maintenance

**Defragmenting etcd** (`plan.PlanDefragment`, `internal/controller/upkeep.go`). A member's database file only grows: what compaction and deletion free stays allocated until the member is defragmented, and etcd counts the file against its quota. At the quota etcd raises `NOSPACE` and refuses every write.
- **When.** While `spec.management.maintenance` is `Enabled`, and a member's file is at least `minimumSize` (100Mi) with at least `freePercent` (50) of it free. Not within `interval` (24h) of the last one, counted from when that started. The numbers are read from every control plane in the pass that decides.
- **What.** An operation of its own kind, `EtcdDefragment`, through the driver (§6.4): the lock, the hooks around it, no member begun while paused or outside a window. Every control plane is in the plan, followers by name and the member that leads last.
- **Each member** is defragmented only while every node of the cluster is healthy, the member's own included (`plan.TouchGate` with nobody left out, and the user's gates), and the next begins after the cluster was verified healthy for the soak (`plan.VerifyGate`). The member that leads is asked to hand the lead over first, as a courtesy. `EtcdDefragment` reaches the local member only (`T/internal/app/machined/internal/server/v1alpha1/v1alpha1_server.go:2172`), so it is addressed to that node, through another one.
- **Failure.** A member that cannot be defragmented within the step timeout halts the operation, like any step. Retry defragments it again, which does no harm; skip leaves it out.
- **The connection** is a kind of its own (`talos.Keeper`): reading, `EtcdDefragment` and `EtcdForfeitLeadership`, made only while maintenance is enabled.
- **With fewer than three members**, etcd has no quorum while one is defragmented, and the Kubernetes API stalls for that long. It is allowed all the same: the alternative is the quota.
- `EtcdMaintained` says where things stand; a defragmentation that is due and does not start has the reason of what it waits for. With maintenance disabled the operator still says that a database is fragmented.

**Alarms of etcd are not handled** (§12.27).

**The support bundle** (`internal/controller/bundle.go`). When an operation halts at a machine, the operator reads from that machine, with the connection every observation uses, the state of its services, the last 500 lines of the logs of `machined`, `apid`, `kubelet` and `cri` (and `etcd` on a control plane), and the kernel log, at most 128 KiB of each. They go, compressed, into the Secret `<cluster>-support-bundle`, and `status.operation.failure.bundle` names it.
- Once per failure, in the pass that records it. A node that does not answer then is tried for five minutes; after that `bundle` says why there is none.
- One Secret per cluster, overwritten by the next failure, owned by the TalosCluster. A Secret of that name that is not the cluster's is left alone.
- Never logged. It needs `update` on Secrets in the operator's namespace, which is the one permission this added.

**Certificates.** `status.certificates` has the validity of the operator's own client certificate and of the authorities in the secrets bundle (only the certificates are parsed, never their keys). A certificate that cannot be read keeps the dates it had: the one that matters most is the one that just stopped working. The alerts are in §14.2. Talos rotates what it issues itself (apid and trustd certificates daily, control-plane certificates yearly); rotating an authority is not built (§10).

**Recommended, not rendered:** a `WatchdogTimerConfig` on bare metal, which recovers a hung kernel or machined after boot. It does not cover a config that cuts a node off (TRY does) or a hang before machined (§8.5.2). The guide has the document.

**Not built:**
- etcd snapshots, scheduled or before an operation: out of scope altogether (§12.23);
- credentials for people (§12.27);
- CA rotation (later, §10);
- `ImageVerificationConfig` signature policy (low value for a small cluster; CRI enforcement unverified);
- automated disk wipes for Rook OSD recycling;
- containerd metrics via `CRICustomizationConfig`.

Image GC needs nothing: Talos already runs it every 15 minutes (`T/internal/app/machined/pkg/controllers/cri/image_gc.go:32`).

### 14.7 Decisions

Decided 2026-10-09:
1. **Superseded 2026-10-12:** log shipping goes to a collector on the node, which is the owner's to run, and the config for it is the owner's to declare (§12.27).
2. **Superseded 2026-10-11:** there is no snapshot target, because the operator takes no snapshots (§12.23).
3. **How the bot that proposes bumps is run is the owner's choice.** The repository ships an example configuration, nothing more.
4. **The `validate` check is meant to be required only for changes touching the Talos CRs,** so that it does not stand in the way of unrelated automation.
