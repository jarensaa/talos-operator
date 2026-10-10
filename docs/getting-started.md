# Getting started

This installs the operator in observe mode, walks through adopting an existing cluster, and then through changing its config. Nothing on your nodes changes until the cluster is adopted and you choose to switch management on.

## Before you start

You need:

- Talos 1.13 or 1.14;
- Kubernetes 1.30 or newer, for the admission policies the chart installs;
- installer images from [Image Factory](https://factory.talos.dev). Adoption doesn't understand other installers yet.

## 1. Let the operator request Talos credentials

The operator doesn't take a talosconfig. Talos issues it a short-lived certificate, once you allow that for the operator's namespace. Add this document to each control plane's machine config:

```yaml
apiVersion: v1alpha1
kind: KubeTalosAPIAccessConfig
allowedRoles: [os:admin]
allowedKubernetesNamespaces: [talos-operator]
```

Patch one node at a time, with a dry run first to see what it changes and whether Talos wants a reboot for it:

```bash
talosctl -n <node-ip> -e <another-node-ip> patch machineconfig --patch @talos-api-access.yaml --dry-run
```

Once it's applied, Talos installs a `serviceaccounts.talos.dev` CRD:

```bash
kubectl get crd serviceaccounts.talos.dev
```

## 2. Install the chart

Take the version from the [latest release](https://github.com/jarensaa/talos-operator/releases/latest):

```bash
helm install talos-operator oci://ghcr.io/jarensaa/charts/talos-operator \
  --version <version> --namespace talos-operator --create-namespace
```

## 3. Create a cluster

An empty spec means observe only:

```bash
kubectl apply -f - <<'EOF'
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosCluster
metadata:
  name: my-cluster
  namespace: talos-operator
EOF
```

Watch it look around:

```bash
kubectl -n talos-operator describe taloscluster my-cluster
```

The conditions tell you where it is:

| Condition | Means |
|---|---|
| `CredentialsValid` | Talos issued the certificate. `TalosAPIAccessDisabled` here means step 1 didn't take effect. |
| `TalosAPIReachable`, `Healthy` | The operator can read every node, and Talos's own health checks pass. |
| `AdoptionPreconditionsMet` | The cluster is in a state that's safe to adopt. The status lists each check. |
| `AdoptionProposed` | The proposal is ready. |

## 4. Read the proposal

Once `AdoptionProposed` is true:

```bash
kubectl -n talos-operator get configmap my-cluster-adoption-proposal -o yaml
```

It holds three documents: the `TalosCluster` spec, one `TalosMachinePool` per role, and one `TalosMachine` per node. Together they reproduce your cluster as it runs now. Talos's defaults aren't in there; they're generated when the config is rendered.

Two Secrets appear alongside it:

- `my-cluster-talos-secrets`: the cluster's secrets bundle, recovered from a control plane.
- `my-cluster-adoption-backup`: a copy of every node's config as it was before adoption.

## 5. Apply it

Apply the three documents however you manage your cluster: commit them to Git, or `kubectl apply` them. The operator doesn't care how they arrive.

Each machine then reports whether it still matches its node:

```bash
kubectl -n talos-operator get talosmachines
kubectl -n talos-operator describe talosmachine <name>
```

If `ConfigMatchesLive` is false, `status.config.diff` shows what differs, with secrets replaced by a hash. Adjust the config until every machine matches.

If `NodeObserved` is false, the operator couldn't read everything from that node in its last pass, and the condition names what it couldn't. The stage, readiness and boot ID in status are then empty rather than out of date.

## 6. Adopted

When every node has a matching `TalosMachine`, the operator sends each node a dry-run apply of the config it would own. If every node answers "no changes", the cluster is adopted:

```console
$ kubectl -n talos-operator get taloscluster
NAME         PHASE       TALOS     KUBERNETES   ADOPTED   AGE
my-cluster   Observing   v1.14.0   1.37.0       True      2d
```

If you edit a node by hand afterwards, or change the spec so it renders differently, `Adopted` goes back to false until things match again.

## 7. Switching management on

Until `Adopted` is true, the API server refuses `config: Manage`. After that:

```bash
kubectl -n talos-operator patch taloscluster my-cluster --type merge \
  -p '{"spec":{"management":{"config":"Manage"}}}'
```

The operator runs the dry-run check once more. If it passes, `ConfigHandover` is true and the phase becomes `Managed`.

From here on the spec is the truth. What each node runs at this moment is recorded as its baseline, and `ConfigInSync` reports whether every node still runs its rendered config.

## 8. Changing config

Edit the config of the cluster, a pool, a machine, or a `TalosMachineConfig` they reference. A good first change is a node label on one pool:

```yaml
apiVersion: talos.jarensaa.github.io/v1alpha1
kind: TalosMachinePool
metadata:
  name: my-cluster-workers
spec:
  config:
    - inline: |
        apiVersion: v1alpha1
        kind: KubeNodeConfig
        labels:
          example.com/pool: general
```

Each affected `TalosMachine` shows what would change before anything does: `status.config.diff`, and `status.config.changeClass`.

The operator then rolls the change out one node at a time, workers first. On each node it:

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
| `GateClosed` | The cluster isn't healthy, so no node is touched. The message says what is wrong. |
| `Paused` | `spec.paused` is set. |

One blocked machine blocks the whole rollout.

### Nodes changed by hand

If someone changes a node's config outside the operator, `ConfigInSync` goes false with reason `Drift` and names the machine. By default that's all: `spec.rollout.driftPolicy` is `Report`. Set it to `Enforce` and the operator rolls the node back to its rendered config. Or put the change into the spec, and the node's config becomes the new baseline.

### Limits to know about

- Once a config is confirmed, it stays. If it breaks the node hours later, the operator won't touch an unhealthy cluster, so that node needs fixing by hand.
- A cluster with one control plane has no other node to check that control plane through.

## Backing out

Set `config` back to `Observe` and the operator stops writing to your nodes at once. A rollout in flight ends there; a config that was being tried is reverted by Talos when its window ends.

To remove the operator, uninstall the chart and remove the `KubeTalosAPIAccessConfig` from the control planes.

A `TalosMachine` can't be deleted while it's `Present`, so a stray prune can't drop a node. To release one, annotate it first:

```bash
kubectl -n talos-operator annotate talosmachine <name> talos.jarensaa.github.io/orphan=true
```

The two Secrets from step 4 stay behind, because the operator never deletes them itself. Delete them if you don't want them.
