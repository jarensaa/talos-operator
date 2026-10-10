# talos-operator

talos-operator is a Kubernetes operator that manages the [Talos Linux](https://www.siderolabs.com/talos-linux) cluster it runs on. Machines and their configuration are declared as custom resources. The operator renders each node's machine config from those resources, compares it with what the node is running, and rolls out the difference one node at a time.

It is built for clusters that already exist. The operator derives its resources from the live cluster and takes ownership only after Talos's own dry-run apply reports no changes on every node.

**Project status: alpha.** Observing a cluster, adopting it and applying configuration changes are implemented and tested end to end. Upgrading Talos and Kubernetes, and adding or removing machines, are planned and not implemented yet. See [Project status](#project-status).

## Features

- **Adoption without changes.** The operator reads every node and creates the `TalosMachinePool` and `TalosMachine` resources that reproduce the cluster as it runs. They contain only what differs from Talos's generated defaults. Adoption completes when a dry-run apply on every node reports no changes. No node is rebooted or reconfigured, and from then on the resources are yours to manage.
- **Read-only observation.** Until management is enabled, the operator uses a Talos client that rejects every API call that would modify a node.
- **Config changes that revert by themselves.** Every change is applied in Talos's try mode, one node at a time. The operator confirms it only after the node answered through another node, sent kubelet heartbeats and stayed healthy for a soak period. Otherwise Talos restores the previous config and the rollout halts before the next node.
- **Classified changes.** Changes to CAs, tokens and other cluster identity are refused. Changes to storage, and anything the operator has no rule for, wait for approval of that exact change.
- **Machine pools and shared config.** Config is layered: cluster, then pool, then machine. Reusable `TalosMachineConfig` resources can be referenced from any layer, and report how many machines use them.
- **Config comparison and drift.** Each machine reports whether its rendered config equals the node's live config, per document, with a diff in which secrets are redacted. A node changed by hand is reported, and put back only if the drift policy says so.
- **No stored Talos credentials.** The operator requests a certificate through Talos's [Kubernetes API access](https://docs.siderolabs.com/kubernetes-guides/advanced-guides/talos-api-access-from-k8s). Talos issues it for six hours and renews it.
- **Hardware identity.** Every machine is pinned to an SMBIOS system UUID. A node whose UUID does not match is never acted on.
- **Admission rules in the API server.** `ValidatingAdmissionPolicy` resources prevent enabling management before adoption, and prevent deleting a machine that is still part of the cluster. They do not depend on the operator being available.

## Getting started

You need:

- Talos 1.13 or 1.14;
- Kubernetes 1.30 or newer;
- installer images from [Image Factory](https://factory.talos.dev).

Nothing on a node changes before step 6. The [guide](docs/guide.md) has what these steps leave out: what the conditions mean while a cluster is adopted, and what to do when a rollout halts, asks for approval, or finds a node changed by hand.

### 1. Allow the operator to request Talos credentials

The operator takes no talosconfig. Talos issues it a short-lived certificate once you allow that for the operator's namespace. Add this document to the machine config of every control plane:

```yaml
apiVersion: v1alpha1
kind: KubeTalosAPIAccessConfig
allowedRoles: [os:admin]
allowedKubernetesNamespaces: [talos-operator]
```

```bash
talosctl -n <node-ip> patch machineconfig --patch @talos-api-access.yaml
```

### 2. Install the chart

```bash
helm install talos-operator oci://ghcr.io/jarensaa/charts/talos-operator \
  --version <version> --namespace talos-operator --create-namespace
```

The chart includes the CRDs, and upgrading the chart upgrades them. Take the version from the [latest release](https://github.com/jarensaa/talos-operator/releases/latest).

### 3. Add a TalosCluster

An empty spec is enough. It tells the operator to adopt the cluster it runs on, and nothing more:

```bash
kubectl apply -f - <<'EOF'
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosCluster
metadata:
  name: my-cluster
  namespace: talos-operator
EOF
```

### 4. Wait for adoption

The operator reads every node and writes down what it finds:

- two `TalosMachinePool`s, `my-cluster-control-plane` and `my-cluster-workers`, each with the config its nodes share (a cluster without workers gets only the first);
- one `TalosMachine` per node, named after the node and pinned to its hardware UUID;
- the versions, the endpoint and the config every node shares, in the `TalosCluster`'s own spec, where you left them empty;
- two Secrets: the cluster's secrets bundle, and a backup of every node's config as it was.

It then sends each node a dry-run apply of the config these resources render. When every node answers "no changes", the cluster is adopted:

```console
$ kubectl -n talos-operator get taloscluster
NAME         PHASE     TALOS     KUBERNETES   ADOPTED   AGE
my-cluster   Adopted   v1.14.0   1.37.0       True      4m

$ kubectl -n talos-operator get talosmachinepools
NAME                       CLUSTER      ROLE           MEMBERS   MATCHED   AGE
my-cluster-control-plane   my-cluster   ControlPlane   3         3         3m
my-cluster-workers         my-cluster   Worker         2         2         3m
```

If the phase stays `Adopting`, the conditions say what is in the way:

```bash
kubectl -n talos-operator describe taloscluster my-cluster
```

### 5. Take over the resources

From `Adopted` on the operator never writes to these resources again. They are yours, and there is no way back to `Adopting`.

You can leave them in the cluster and edit them with `kubectl`. To manage them from Git, export them and strip what the API server added (this uses [yq](https://github.com/mikefarah/yq)):

```bash
kubectl -n talos-operator get talosclusters,talosmachinepools,talosmachines -o yaml \
  | yq '.items[] | del(.status) | .metadata |= pick(["name", "namespace", "labels", "annotations"]) | split_doc' \
  > my-cluster.yaml
```

Commit the file and point your GitOps tool at it. Any tool works; a Flux Kustomization, for instance, takes the existing objects over as they are.

Leave the two Secrets where they are. They hold the cluster's keys and do not belong in Git unencrypted, and the operator does not need them delivered from outside.

### 6. Switch management on

Set `management.config` to `Manage` in the `TalosCluster`. It is the one line that changes:

```yaml
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosCluster
metadata:
  name: my-cluster
  namespace: talos-operator
spec:
  management:
    config: Manage   # was Observe
  # ...the rest stays as adoption wrote it
```

Commit it, or without Git:

```bash
kubectl -n talos-operator patch taloscluster my-cluster --type merge \
  -p '{"spec":{"management":{"config":"Manage"}}}'
```

The API server accepts this only while the resources still render exactly what the nodes run. The operator repeats the dry run on every node, and the phase becomes `Managed`. From here on the resources are the truth, and the operator makes the nodes follow them.

### 7. Change something

Config is layered: the cluster's applies to every node, then the pool's, then the machine's. A node label on the workers is a good first change. Add it to the pool's `config`:

```yaml
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachinePool
metadata:
  name: my-cluster-workers
  namespace: talos-operator
spec:
  clusterRef: { name: my-cluster }
  role: Worker
  config:
    - inline: |
        apiVersion: v1alpha1
        kind: KubeNodeConfig
        labels:
          example.com/pool: general
```

The operator applies it to one node at a time in Talos's try mode, and confirms it only after the node stayed healthy. A node that does not come out healthy gets its previous config back from Talos, and the rollout stops there.

Config that several layers share can live in a `TalosMachineConfig` and be referenced from any of them:

```yaml
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachineConfig
metadata:
  name: bonded-network
  namespace: talos-operator
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
  name: my-cluster-control-plane
  namespace: talos-operator
spec:
  clusterRef: { name: my-cluster }
  role: ControlPlane
  config:
    - configRef: { name: bonded-network }
```

## Project status

| Area | State |
|---|---|
| Observing clusters and machines | Implemented |
| Adoption: resources created from the live cluster, dry-run proof, gated switch to `Manage` | Implemented |
| Machine pools and reusable config | Implemented |
| Drift reporting and enforcement | Implemented |
| Applying config changes, with Talos try-mode rollback | Implemented |
| Config changes that need a reinstall (installer image, kernel arguments) | Planned |
| Talos and Kubernetes upgrades | Planned next |
| Adding and removing machines | Planned |
| Metrics, alerts and dashboards | Planned |

The API is `v1alpha1` and may change incompatibly between releases.

Adoption and config rollouts are tested in CI against Talos clusters running in QEMU. The tests assert that adoption leaves boot IDs and config hashes unchanged, that a label reaches a node without a reboot, and that a config which cuts a node off is reverted and never confirmed.

## Non-goals

- **Hardware provisioning.** Machines are expected to exist and to boot Talos.
- **Workloads and CNI.** The operator manages Talos machine config, not what runs on the cluster.
- **Disaster recovery.** The operator runs in the cluster it manages, so it cannot recover that cluster from total quorum loss.
- **Multi-cluster management.** It manages the cluster it runs on. It is not a replacement for [Omni](https://www.siderolabs.com/omni).

## Documentation

- [Guide](docs/guide.md): what the steps above leave out, from reading adoption's conditions to halted rollouts, approvals, drift and backing out.
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
