# talos-operator

talos-operator is a Kubernetes operator that manages the [Talos Linux](https://www.siderolabs.com/talos-linux) cluster it runs on. Machines and their configuration are declared as custom resources. The operator renders each node's machine config from those resources, compares it with what the node is running, and rolls out the difference one node at a time.

It is built for clusters that already exist. The operator derives its resources from the live cluster and takes ownership only after Talos's own dry-run apply reports no changes on every node.

**Project status: alpha.** Observing a cluster, adopting it, applying configuration changes and upgrading Talos and Kubernetes, and adding, removing and replacing machines are implemented and tested end to end. See [Project status](#project-status).

## Features

- **Adoption without changes.** The operator reads every node and writes the `TalosMachinePool` and `TalosMachine` resources that reproduce the cluster as it runs. They contain only what differs from Talos's generated defaults. Adoption completes when a dry-run apply on every node reports no changes. No node is rebooted or reconfigured, and from then on the resources are yours to manage.
- **Read-only observation.** Until management is enabled, the operator uses a Talos client that rejects every API call that would modify a node.
- **Config changes that revert by themselves.** Every change is applied in Talos's try mode, one node at a time. The operator confirms it only after the node answered through another node, sent kubelet heartbeats and stayed healthy for a soak period. Otherwise Talos restores the previous config and the rollout halts before the next node.
- **Talos upgrades, one node at a time.** A new version or installer is installed to a node's inactive boot slot while it keeps working. Only then is the node drained, with disruption budgets respected, and rebooted. No node is touched unless every node would boot the new system: a machine whose Secure Boot firmware does not trust the image's signer refuses the upgrade before it starts. A node that does not come back on the new system, or does not get healthy, halts the upgrade with that node still cordoned. The operator runs two replicas and upgrades the nodes it runs on.
- **Kubernetes upgrades, one component at a time.** A new version reaches the API servers first, then the other control-plane components, then the kubelets, one minor at a time. Each change is tried and kept only once the component runs the new version. An upgrade is refused up front when a node's Talos does not run the version, or when it would strand a flag or an object of a removed API.
- **Windows, gates and hooks.** Changes can be held to maintenance windows, made to wait on a CEL expression over any object in the cluster, such as a storage cluster's health, and wrapped in Jobs that run around an operation and around each node.
- **Version awareness.** Status lists the Talos and Kubernetes releases that exist and the upgrades that lead to what the spec asks for. An upgrade to an installer or image that does not exist is refused before a node is asked to pull it. A `validate` command checks the manifests in a repository before they are merged, and a `bump` command writes the next version into them.
- **Adding, removing and replacing machines.** A machine that waits in Talos's maintenance mode joins the cluster once you declare it, if it says it is the hardware you declared and its config fits its disks. A machine you mark as removed is drained, reset, and only counted as gone when it waits in maintenance mode again. A control plane leaves only while three voting members of etcd remain, so replacing one is declaring the new and removing the old.
- **Metrics, alerts and dashboards.** What the operator knows is exported as metrics: an operation that is halted, what holds a change back, drift, nodes that cannot be read, etcd membership. So is what Talos says about each node and has no metrics endpoint for: its services, its warnings, the clock, links and bonds, volumes, the size of each etcd database. Where the cluster has the Prometheus Operator, the chart installs a ServiceMonitor, alerts and two Grafana dashboards; where it has none, nothing.
- **Upkeep.** etcd is defragmented before a database reaches its quota, one member at a time and the leader last, behind the same gates as a rollout. When an operation fails at a machine, the end of that machine's logs is kept in a Secret. Status says how long the cluster's certificates last.
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
spec: {}
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
NAME         PHASE     TALOS     KUBERNETES   PROVEN   AGE
my-cluster   Adopted   v1.14.0   1.37.0       True     4m

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

From `Adopted` on the operator never writes to these resources again. They are yours, and there is no way back to `Adopting`. `PROVEN` keeps saying whether they still render exactly what the nodes run.

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

### 8. Upgrade Talos

Switch upgrades on and name the version:

```yaml
spec:
  management:
    config: Manage
    upgrades: Enabled
  versions:
    talos: v1.14.2
```

The operator takes the nodes there one at a time, workers first: it installs the new system next to the running one, drains the node, reboots it, and waits until it is back and healthy. Then it brings each node's config in line, which reboots nothing. `TalosUpToDate` carries the progress, and the [guide](docs/guide.md#upgrading-talos) says what happens when a node does not come back as expected.

### 9. Upgrade Kubernetes

Name the version, with upgrades on:

```yaml
spec:
  versions:
    kubernetes: 1.37.1
```

The operator changes one component on one node at a time: the API server on every control plane, then the controller manager, the scheduler, and the kubelet on every node. Each is tried first, and kept only once it runs the new version and the cluster stayed healthy. A version more than one minor ahead is reached one minor at a time. `KubernetesUpToDate` carries the progress, and says what stands in the way when the upgrade is refused. The [guide](docs/guide.md#upgrading-kubernetes) has the details, including what the operator leaves alone.

### 10. Say when, and behind what

Three things in `spec.rollout` hold for every kind of change, a config change included:

```yaml
spec:
  rollout:
    maintenanceWindows:
      - schedule: "0 2 * * 6"       # Saturdays at 02:00
        duration: 3h
        timeZone: Europe/Oslo
    gates:
      - name: storage-healthy
        object:
          apiVersion: ceph.rook.io/v1
          kind: CephCluster
          namespace: storage
          name: storage
        expression: object.status.ceph.health == "HEALTH_OK"
    hooks:
      preNode:
        - name: announce
          job:
            template:
              spec:
                containers:
                  - name: announce
                    image: registry.example.com/tools:1
                    command: ["announce", "$(TALOS_NODE)"]
```

No node is begun outside a window; a node that is being changed when the window closes is finished. A gate must be true before a node is touched and stay true while the node is verified afterwards. A hook is a Job the operator runs and waits for, around the whole operation or around each node. The operator needs permission to read a gate's object, which the chart grants through `gates.rules`. The [guide](docs/guide.md#windows-gates-and-hooks) has the rest.

### 11. Check a change before it merges

If the resources live in Git, the operator's own image checks them, with no cluster and no secret:

```bash
docker run --rm -v "$PWD:/work:ro" -w /work ghcr.io/jarensaa/talos-operator:v0.9.0 \
  validate --base ../main/clusters/my-cluster clusters/my-cluster
```

It fails when the Talos and Kubernetes versions do not go together, when the installer or a Kubernetes image does not exist, when the step from `--base` is a downgrade, when a machine's config does not render, or when a maintenance window or a gate cannot be understood. [`docs/examples`](docs/examples) has a GitHub Actions workflow that runs it on pull requests, and a Renovate configuration that proposes version bumps. With both, a new release becomes a pull request, the check decides whether it can work, and the operator rolls out what is merged inside your windows.

Without a bot, the same image bumps the versions itself. Run on a schedule, it writes the next Talos or Kubernetes release into the TalosCluster it finds, one step for every run, and prints what it wrote for your script to commit:

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work ghcr.io/jarensaa/talos-operator:v0.14.0 \
  bump clusters/my-cluster
```

It takes the newest release of the minor the file names, then the newest of the next minor, Talos before Kubernetes wherever the versions allow it. It writes only a version this operator upgrades to and that passes the check above, and changes nothing else in the file. `--talos` and `--kubernetes` bump one of the two only, `--patch` stays on the minor, and `--dry-run` writes nothing. The [guide](docs/guide.md#bumping-versions-on-a-schedule) has the rest, and [`docs/examples`](docs/examples) a workflow that runs it every night.

`status.versions` on the TalosCluster says the same from the cluster's side: what it runs, which releases exist, and the upgrades to what the spec asks for. Which releases exist is only listed with `versionSources.enabled: true` in the chart: the operator then asks Image Factory, the Kubernetes release site and the image registries a few times a day. Without it, it talks to nothing but its cluster, except for one question before a Talos upgrade of a machine with Secure Boot on: which certificate Image Factory signs boot images with.

## Project status

| Area | State |
|---|---|
| Observing clusters and machines | Implemented |
| Adoption: resources written from the live cluster, dry-run proof, gated switch to `Manage` | Implemented |
| CRDs installed and upgraded with the chart | Implemented |
| Machine pools and reusable config | Implemented |
| Drift reporting and enforcement | Implemented |
| Applying config changes, with Talos try-mode rollback | Implemented |
| Talos upgrades: version or installer, drained and rebooted node by node | Implemented |
| Kubernetes upgrades: the components Talos runs, a minor at a time | Implemented |
| Maintenance windows, gates on objects in the cluster, hook Jobs | Implemented |
| Version awareness: releases that exist, the planned upgrades, a `validate` check for CI, a `bump` command for a schedule | Implemented |
| The cluster's own health in Prometheus and Grafana: Talos's view of each node, a Talos dashboard | Implemented |
| Defragmenting etcd; a support bundle for a failed operation; certificate expiry | Implemented |
| Handling an alarm of etcd, restarting etcd for a changed argument, credentials for people | Not built |
| Rotating the cluster's PKI | Later |
| Bootstrapping a new cluster, managing clusters from a management cluster, discovering machines | Not planned |
| The operator choosing versions by itself | Not planned |
| The manifests Talos applies at bootstrap (kube-proxy, CoreDNS) | Not planned |
| Config changes that only an install can make (kernel arguments, install disk) | Not planned |
| Adding, removing and replacing machines | Implemented |
| Shrinking the control plane below three, wiping disks other than Talos's own | Not planned |
| Metrics, alerts and a dashboard for what the operator knows | Implemented |
| Backups, etcd snapshots included | Not planned: they are the cluster owner's |

The API is `v1alpha1` and may change incompatibly between releases.

Adoption, rollouts and upgrades are tested in CI against Talos clusters running in QEMU. The operator under test is the chart's image, installed with two replicas. The tests assert that adoption needs nothing applied by hand and leaves boot IDs and config hashes unchanged, that every node is upgraded to the next Talos release while a workload stays up, that a rollout finishes when the leader is killed in the middle of it, that a label reaches a node without a reboot, that a config which cuts a node off is reverted and never confirmed, that the adopted resources pass the `validate` check against the real Image Factory and fail it for an installer that does not exist, that a rollout waits for its window and its gate and runs its hooks, that Kubernetes is upgraded by a minor after being refused while a kubelet was started with a flag the new version removed, that every node reports its Talos services healthy, and that etcd is defragmented one member at a time with the leader last.

No cluster other than those depends on the operator yet. That is on purpose: until one does, the API changes without migrations.

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
make hooks   # git hooks: quick checks before a commit, lint and unit tests before a push
```

Releases are created automatically from [Conventional Commits](https://www.conventionalcommits.org) on `main`.

## License

Apache 2.0. See [LICENSE](LICENSE).
