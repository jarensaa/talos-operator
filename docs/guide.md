# Guide

The [README](../README.md#getting-started) takes you from installing the operator to a first config change. This guide has what those steps leave out.

## Allowing Talos API access

The `KubeTalosAPIAccessConfig` document of step 1 is the one change you make to your nodes yourself. Patch one control plane at a time, with a dry run first to see what it changes and whether Talos wants a reboot for it:

```bash
talosctl -n <node-ip> -e <another-node-ip> patch machineconfig --patch @talos-api-access.yaml --dry-run
```

Once it's applied, Talos installs a `serviceaccounts.talos.dev` CRD:

```bash
kubectl get crd serviceaccounts.talos.dev
```

The operator needs Kubernetes 1.30 or newer for the admission policies the chart installs, and installer images from Image Factory: adoption doesn't understand other installers yet.

## While a cluster is being adopted

```bash
kubectl -n talos-operator describe taloscluster my-cluster
```

The conditions tell you where it is:

| Condition | Means |
|---|---|
| `CredentialsValid` | Talos issued the certificate. `TalosAPIAccessDisabled` here means step 1 didn't take effect. |
| `TalosAPIReachable`, `Healthy` | The operator can read every node, and Talos's own health checks pass. |
| `AdoptionPreconditionsMet` | The cluster is in a state that's safe to adopt. The status lists each check. |
| `SecretsMatchLive` | The secrets bundle, recovered or supplied, is the one every node's config was made with. |
| `ResourcesDerived` | The operator wrote the pools and machines. False with reason `ChangedElsewhere` names resources it left alone because someone else set a value the cluster contradicts. |
| `ConfigProven` | The resources render exactly what every node runs, and Talos's dry run agrees. The first time this is true, the cluster is adopted. |

`status.adoption.notes` lists what is worth knowing before you take the resources over, such as a node whose Talos version differs from the one its config installs.

Each machine reports on its own node:

```bash
kubectl -n talos-operator get talosmachines
kubectl -n talos-operator describe talosmachine <name>
```

- `ConfigMatchesLive` says whether the machine renders exactly what its node runs. While it is false, `status.config.diff` shows what differs, with secrets replaced by a hash.
- `NodeObserved` is false when the operator couldn't read everything from the node in its last pass, and names what it couldn't. The stage, readiness and boot ID in status are then empty rather than out of date.

Two Secrets appear next to the resources, and the operator never deletes them:

- `my-cluster-talos-secrets`: the cluster's secrets bundle, recovered from a control plane.
- `my-cluster-adoption-backup`: a copy of every node's config as it was before adoption.

### Bringing your own resources

The operator writes a machine only for a node that has none. If you create the pools and machines yourself before the `TalosCluster`, under whatever names you like, it writes nothing for those nodes and proves yours instead.

It also never overwrites what you set. A field of the `TalosCluster` that is not empty stays as it is, and a pool or machine you changed is left alone. If what you set differs from what the nodes run, the machines say so in `ConfigMatchesLive`, and the cluster stays `Adopting` until they match.

One Talos cluster has one `TalosCluster`. A second one finds every node described already and leaves the machines alone.

## Between adoption and management

Once the cluster is adopted, the resources are yours and the operator only compares. If you change one so that it renders something else, or edit a node by hand, `ConfigProven` goes false and the machine concerned shows the difference. The phase stays `Adopted`, and the API server refuses `config: Manage` until `ConfigProven` is true again.

The operator no longer follows the cluster either: a node changed by hand is reported, not copied into its machine.

When you switch management on, the operator repeats the dry run on every node. If it passes, `ConfigHandover` is true and the phase becomes `Managed`. What each node runs at that moment is recorded as its baseline, and `ConfigInSync` reports whether every node still runs its rendered config.

## Changing config

Edit the config of the cluster, a pool, a machine, or a `TalosMachineConfig` they reference, as in step 7 of the README.

Each affected `TalosMachine` shows what would change before anything does: `status.config.diff`, and `status.config.changeClass`.

The operator then rolls the change out one node at a time, workers first. No node is touched while another node is unhealthy. On each node it:

1. applies the config in Talos's try mode, which Talos reverts by itself after `tryTimeout` (5m) unless it is confirmed;
2. checks the node from outside: its Talos API answers through another control plane, Kubernetes sees it Ready, its kubelet is sending heartbeats, etcd and the rest of the cluster are healthy;
3. waits until that has held for `soak` (2m);
4. confirms the config, which makes it persistent.

Watch it:

```bash
kubectl -n talos-operator get taloscluster my-cluster -o jsonpath='{.status.operation}' | jq
kubectl -n talos-operator describe taloscluster my-cluster   # ConfigInSync carries the progress
```

The timings are in `spec.rollout`. A node takes at least `soak`, whatever the change.

### When a node doesn't come out healthy

The operator doesn't confirm, Talos puts the previous config back, and the rollout halts. No other node is touched. `RolloutBlocked` is true with reason `OperationFailed`, and `status.operation.failure` says what happened and what you can do. Each option is an annotation that carries the failure's token:

```bash
TOKEN=$(kubectl -n talos-operator get taloscluster my-cluster -o jsonpath='{.status.operation.failure.token}')

# Run the step again, for instance after fixing what kept the node unhealthy:
kubectl -n talos-operator annotate taloscluster my-cluster talos.jarensaa.github.io/retry=$TOKEN

# Leave this machine out and go on with the others:
kubectl -n talos-operator annotate taloscluster my-cluster talos.jarensaa.github.io/skip-machine=$TOKEN

# End the rollout and leave the nodes as they are:
kubectl -n talos-operator annotate taloscluster my-cluster talos.jarensaa.github.io/abort=$TOKEN
```

After an abort the same change isn't started again. Change the spec, or revert it, and the operator rolls out whatever it renders then.

### Changes the operator won't make, or asks about first

| `RolloutBlocked` reason | Means |
|---|---|
| `ProtectedChange` | The change touches cluster identity: a CA, a token, the cluster name, a hostname. The operator never applies these. |
| `RequiresReinstall` | The change only takes effect through a reinstall, such as a different installer image. Not supported yet. |
| `AwaitingApproval` | The change touches storage, or something the operator has no rule for. Review each machine's `status.config.diff`, then approve with the hash from `status.approval`: `kubectl annotate taloscluster my-cluster talos.jarensaa.github.io/approve-diff=<hash>`. The approval is for exactly that change. |
| `GateClosed` | A node other than the one to be changed isn't healthy, or the cluster as a whole isn't, so no node is touched. The message says what is wrong. |
| `Paused` | `spec.paused` is set. |

One blocked machine blocks the whole rollout.

### Nodes changed by hand

If someone changes a node's config outside the operator, `ConfigInSync` goes false with reason `Drift` and names the machine. By default that's all: `spec.rollout.driftPolicy` is `Report`. Set it to `Enforce` and the operator rolls the node back to its rendered config. Or put the change into the spec, and the node's config becomes the new baseline.

### Limits to know about

- Once a config is confirmed, it stays until the spec changes. If it breaks a node hours later, revert the spec: the operator starts with the unhealthy node and puts its previous config back. That works while it is the only unhealthy node and its Talos API still answers. If the config broke several nodes, the operator touches none of them.
- A cluster with one control plane has no other node to check that control plane through.

## Upgrading Talos

Set `management.upgrades` to `Enabled` and change `versions.talos`, or the schematic of the cluster's or a pool's installer. The API server refuses `upgrades: Enabled` unless `config` is `Manage`.

Without `upgrades: Enabled` the operator only says what is behind: `TalosUpToDate` is false with reason `UpgradesDisabled`, and the config rollout waits with `AwaitingUpgrade`, because the config now names an installer the nodes don't run.

### What happens to a node

One node at a time, workers first; the etcd leader and the node the operator itself runs on go last in their group.

1. The installer image is pulled to the node and the new system is written to its inactive boot slot. The node keeps running what it runs. Nothing has been taken from the cluster yet.
2. The node is cordoned and its pods are evicted. Evictions respect PodDisruptionBudgets. Static pods and DaemonSet pods stay.
3. A control plane hands off etcd leadership. The node reboots.
4. The operator waits for it to come back on the new system, running and ready.
5. Once Kubernetes reports the node Ready it is uncordoned, and the whole cluster has to stay healthy for `soak` before the next node begins.

When every node is done, a config rollout follows on its own and brings each node's config in line with the installer it now runs. That rollout reboots nothing.

```bash
kubectl -n talos-operator get taloscluster my-cluster -o jsonpath='{.status.operation}' | jq
kubectl -n talos-operator describe taloscluster my-cluster   # TalosUpToDate carries the progress
```

### What is refused before any node is touched

`TalosUpToDate` is false with reason `Incompatible`, and says which of these it is:

- a version this operator doesn't support: older than 1.13, or newer than the Talos it was built for;
- a downgrade;
- a jump Talos doesn't allow in one step;
- a Kubernetes version the new Talos release doesn't run. Upgrade Kubernetes first then.

### When a node doesn't come through

The upgrade halts, no other node is touched, and `status.operation.failure` says what happened. The annotations are the same three as for a config change (`retry`, `skip-machine`, `abort`), each with the failure's token.

| Reason | What it means | State of the node |
|---|---|---|
| `ImagePullFailed` | The installer image couldn't be pulled. Check that it exists: the schematic, and the version. | Unchanged. |
| `InstallFailed` | The installer ran and failed. | Unchanged; it runs what it ran. |
| `DrainTimeout` | Pods were still on the node after `drainTimeout` (10m). A disruption budget that never lets a pod go ends here. | Cordoned, not rebooted. |
| `FellBack` | The node rebooted and is running, but on the system it had before. Talos does that by itself when the new one doesn't boot. Its console says why. | Cordoned, on the old system. |
| `Timeout` | A wait ran out: the cluster didn't get healthy, the node didn't come back, or Kubernetes never called it Ready. | Depends on the step; the message says. |

A node the operator cordoned stays cordoned through a failure, and through a skip or an abort: it didn't come back as expected, so it isn't handed back to the scheduler without someone looking. A retry picks it up where the node is now and uncordons it at the end.

After a skip or an abort the operator doesn't come back with the same installer for those machines. Ask for another version, or the one the nodes run, and it starts afresh.

### Limits to know about

- **One control plane means downtime.** While that node reboots there is no Kubernetes API. The upgrade carries on when it is back.
- **Two control planes can't be upgraded.** With two etcd members, taking either down loses quorum, so the operator refuses to reboot one. Run one or three.
- **Workers go one at a time.**
- **Only the installer image is upgraded.** A change that only a fresh install can make (kernel arguments, the install disk, disk encryption) stays refused, with reason `RequiresReinstall`.
- **Pausing** (`spec.paused`) leaves alone a node nothing was taken from. A node that is already cordoned is finished.

## Backing out

Set `config` back to `Observe` and the operator stops writing to your nodes at once. A rollout in flight ends there; a config that was being tried is reverted by Talos when its window ends.

To remove the operator, uninstall the chart and remove the `KubeTalosAPIAccessConfig` from the control planes.

A `TalosMachine` can't be deleted while it's `Present`, so a stray prune can't drop a node. To release one, annotate it first:

```bash
kubectl -n talos-operator annotate talosmachine <name> talos.jarensaa.github.io/orphan=true
```

The two Secrets adoption wrote stay behind, because the operator never deletes them itself. Delete them if you don't want them.
