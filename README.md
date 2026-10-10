# talos-operator

talos-operator is a Kubernetes operator that manages the [Talos Linux](https://www.siderolabs.com/talos-linux) cluster it runs on. Machines and their configuration are declared as custom resources. The operator renders each node's machine config from those resources, compares it with what the node is running, and rolls out the difference one node at a time.

It is built for clusters that already exist. The operator derives its resources from the live cluster and takes ownership only after Talos's own dry-run apply reports no changes on every node.

**Project status: alpha.** Observing a cluster, adopting it and applying configuration changes are implemented and tested end to end. Upgrading Talos and Kubernetes, and adding or removing machines, are planned and not implemented yet. See [Project status](#project-status).

## Features

- **Adoption without changes.** The operator reads every node and proposes the `TalosCluster`, `TalosMachinePool` and `TalosMachine` resources that reproduce the cluster as it runs. The proposal contains only what differs from Talos's generated defaults. Adoption completes when a dry-run apply on every node reports no changes. No node is rebooted or reconfigured.
- **Read-only observation.** Until management is enabled, the operator uses a Talos client that rejects every API call that would modify a node.
- **Config changes that revert by themselves.** Every change is applied in Talos's try mode, one node at a time. The operator confirms it only after the node answered through another node, sent kubelet heartbeats and stayed healthy for a soak period. Otherwise Talos restores the previous config and the rollout halts before the next node.
- **Classified changes.** Changes to CAs, tokens and other cluster identity are refused. Changes to storage, and anything the operator has no rule for, wait for approval of that exact change.
- **Machine pools and shared config.** Config is layered: cluster, then pool, then machine. Reusable `TalosMachineConfig` resources can be referenced from any layer, and report how many machines use them.
- **Config comparison and drift.** Each machine reports whether its rendered config equals the node's live config, per document, with a diff in which secrets are redacted. A node changed by hand is reported, and put back only if the drift policy says so.
- **No stored Talos credentials.** The operator requests a certificate through Talos's [Kubernetes API access](https://docs.siderolabs.com/kubernetes-guides/advanced-guides/talos-api-access-from-k8s). Talos issues it for six hours and renews it.
- **Hardware identity.** Every machine is pinned to an SMBIOS system UUID. A node whose UUID does not match is never acted on.
- **Admission rules in the API server.** `ValidatingAdmissionPolicy` resources prevent enabling management before adoption, and prevent deleting a machine that is still part of the cluster. They do not depend on the operator being available.

## Example

```yaml
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachineConfig
metadata:
  name: bonded-network
spec:
  documents: |
    apiVersion: v1alpha1
    kind: BondConfig
    name: bond0
    links: [eth0, eth1]
    bondMode: 802.3ad
---
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachinePool
metadata:
  name: control-plane
spec:
  clusterRef: { name: my-cluster }
  role: ControlPlane
  config:
    - configRef: { name: bonded-network }
---
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachine
metadata:
  name: cp-1
spec:
  poolRef: { name: control-plane }
  address: 10.0.0.11
  identity:
    systemUUID: aaaaaaaa-bbbb-4ccc-8ddd-000000000001
```

```console
$ kubectl get taloscluster
NAME         PHASE       TALOS     KUBERNETES   ADOPTED   AGE
my-cluster   Observing   v1.14.0   1.37.0       True      2d
```

## How adoption works

1. A `TalosCluster` with an empty spec puts the operator in observe mode.
2. The operator checks that the cluster is safe to adopt, recovers the secrets bundle from a control plane, backs up every node's config, and writes a proposal to a ConfigMap.
3. The proposed resources are applied, by any means. Each `TalosMachine` then reports `ConfigMatchesLive`.
4. When every node has a matching machine, the operator sends each node a dry-run apply. `Adopted` becomes true only if every node reports no changes.
5. `config: Manage` is accepted by the API server only while `Adopted` is true. On entering it, the dry-run check runs once more.

[Getting started](docs/getting-started.md) walks through these steps.

## Project status

| Area | State |
|---|---|
| Observing clusters and machines | Implemented |
| Adoption: proposal, config comparison, dry-run proof, gated switch to `Manage` | Implemented |
| Machine pools and reusable config | Implemented |
| Drift reporting and enforcement | Implemented |
| Applying config changes, with Talos try-mode rollback | Implemented |
| Config changes that need a reinstall (installer image, kernel arguments) | Planned |
| Talos and Kubernetes upgrades | Planned next |
| Adding and removing machines | Planned |
| Metrics, alerts and dashboards | Planned |

The API is `v1alpha1` and may change incompatibly between releases.

Adoption and config rollouts are tested in CI against Talos clusters running in QEMU. The tests assert that adoption leaves boot IDs and config hashes unchanged, that a label reaches a node without a reboot, and that a config which cuts a node off is reverted and never confirmed.

### Requirements

- Talos 1.13 or 1.14
- Kubernetes 1.30 or newer
- Installer images from [Image Factory](https://factory.talos.dev)

## Non-goals

- **Hardware provisioning.** Machines are expected to exist and to boot Talos.
- **Workloads and CNI.** The operator manages Talos machine config, not what runs on the cluster.
- **Disaster recovery.** The operator runs in the cluster it manages, so it cannot recover that cluster from total quorum loss.
- **Multi-cluster management.** It manages the cluster it runs on. It is not a replacement for [Omni](https://www.siderolabs.com/omni).

## Documentation

- [Getting started](docs/getting-started.md): install the operator, adopt a cluster and change its config.
- [Design](docs/scope.md): the API, the adoption process, the safety model and the roadmap.
- [End-to-end tests](docs/dev/e2e.md): how the QEMU test cluster works.

## Development

```bash
make test    # unit and API-server tests
make lint
```

Releases are created automatically from [Conventional Commits](https://www.conventionalcommits.org) on `main`.

## License

Apache 2.0. See [LICENSE](LICENSE).
