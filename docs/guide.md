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
- a Kubernetes version the new Talos release doesn't run. Upgrade Kubernetes first then;
- an installer Image Factory doesn't build: a release or a schematic it doesn't have, or an extension the schematic names that the release lacks.

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
- **There is no rollback on request.** A system that does not boot falls back by itself. One that boots and is wrong is not taken back by the operator: asking for the previous version is a downgrade, which it refuses.
- **Only the installer image is upgraded.** A change that only a fresh install can make (kernel arguments, the install disk, disk encryption) stays refused, with reason `RequiresReinstall`.
- **Pausing** (`spec.paused`) leaves alone a node nothing was taken from. A node that is already cordoned is finished.

## Upgrading Kubernetes

Set `versions.kubernetes`, with `management.upgrades` set to `Enabled`. What is upgraded is what Talos runs from a node's machine config: `kube-apiserver`, `kube-controller-manager` and `kube-scheduler` on control planes, and the kubelet on every node.

Without `upgrades: Enabled` the operator only says what is behind: `KubernetesUpToDate` is false with reason `UpgradesDisabled`, and nothing changes.

A config rollout never changes the images of these components: no other component may be newer than the API servers, and a rollout has no such order. It applies everything else of a change and leaves the images as the node has them, also while an upgrade is due, running or refused. A machine whose config differs from its node's only in these images says so in `ConfigMatchesLive`.

### What happens

One component on one node at a time, in this order: the API server on every control plane, then the controller manager on each, then the scheduler, then the kube-proxy image that each control plane's config names (see below), then the kubelet on control planes and on workers. `status.operation.plan` lists the turns, as `kube-apiserver/<machine>`.

| Step | What happens |
|---|---|
| `Pulling` | The new image is pulled to the node, so the component is only down for its restart. |
| `Trying` | The node's config is applied with that one image changed, in try mode, like every config change. |
| `Verifying` | The component must run the new version and be ready, and the whole cluster must stay healthy for the soak. For a control-plane component that is its pod in `kube-system`; for the kubelet, the version its Node reports. |
| `Confirming` | The config is made persistent. |

A version more than one minor ahead of what the cluster runs is reached one minor at a time, each as an operation of its own. The steps in between go to the newest release the operator knows of each minor, or to its first release (`1.36.0` on the way from 1.35 to 1.37) where it does not know.

With one control plane, the Kubernetes API is down while its API server restarts, for a minute or two, and the rest of the control plane needs a few more to find its feet again. The operator loses its leader lease then and restarts; whichever replica comes back carries on from what is in status. Because of this the API server's turn is given `stepTimeout` (10m) to be healthy again before its soak, not the shorter `tryTimeout`. If an API server really does not come up, that is also how long it takes until Talos puts the previous one back.

### What is refused before any node is touched

`KubernetesUpToDate` is false with reason `Incompatible`, and says which of these it is:

- a downgrade;
- a version the Talos release on some node does not run. The message names the nodes and says to upgrade Talos first;
- a flag, feature gate or admission plugin that a component is started with and the new version removed;
- objects of an API the new version no longer serves. Migrate or delete them first;
- an image the upgrade would pull that its registry does not have;
- a control plane on hold (`spec.hold`). A worker on hold is left out, and its kubelet stays behind.

If one of those checks cannot be run, the upgrade waits (`AwaitingStatus`) and says which.

A removed flag is usually in your own config. Take it out of the pool's or machine's config: that change rolls out although the upgrade is refused, and the upgrade starts once the last node is rid of the flag.

### When a component doesn't come up

Talos puts the previous image back when the try ends, the upgrade halts, and nothing after it is touched. `retry` runs that turn again and `abort` ends the upgrade; there is no `skip-machine`, because what comes after would be newer than the component that was skipped. After an abort the operator does not start again for the same version.

| Reason | What it means | State of the node |
|---|---|---|
| `ImagePullFailed` | The image could not be pulled. | Unchanged. |
| `RolledBack` | The component did not run the new version, or the cluster did not stay healthy, within `tryTimeout`. | Back on the previous image. |
| `Timeout` | The cluster did not get healthy before the turn began. | Unchanged. |

### What the operator leaves alone

- **kube-proxy, CoreDNS and the other manifests Talos applies when a cluster is bootstrapped.** Talos applies them once and never again, and the operator does not either. The upgrade does update the kube-proxy image that each control plane's config names, so that the config is right; nothing runs from it. If your cluster still runs the kube-proxy Talos deployed, update its DaemonSet yourself; `KubernetesUpToDate` says when it is behind. kube-proxy may be older than the API server, so there is no hurry, but it does not follow by itself.
- **Everything Talos does not run:** the CNI, CSI drivers, workloads.

## Versions: what exists, and what is planned

`status.versions` on a TalosCluster says what the cluster runs, the upgrades that lead to what its spec asks for, and which releases exist:

```yaml
status:
  versions:
    talos: v1.14.1
    kubernetes: 1.36.5
    plan:
      - { kind: Talos, from: v1.14.1, to: v1.14.2 }
      - { kind: Kubernetes, from: 1.36.5, to: 1.37.1 }
    available:
      talos: [v1.14.2]
      kubernetes: [1.36.5, 1.37.1]
```

`plan` is in the order the operator takes the steps. Talos and Kubernetes take turns as their versions allow: a Talos release is only installed while it runs the Kubernetes version the cluster has then, and the other way round. `VersionsReachable` is false, with the reason, when there is no way to the versions the spec asks for.

`available` is information. Nothing is upgraded because a release is listed: the version a cluster runs is the one its spec names. Talos lists the newest release of each minor this operator supports; a newer minor needs a newer operator first.

### What the operator asks, and whom

Out of the box the operator talks to nothing but the cluster it manages, and `status.versions` has no `available`. With `versionSources.enabled: true` in the chart's values it asks services outside the cluster what exists, a few times a day and never during an upgrade:

| Whom | What |
|---|---|
| The Image Factory your installer names | Which Talos releases it builds; before an upgrade, whether it has your schematic, the extensions it names for that release, and the installer. |
| The Kubernetes release site (`dl.k8s.io`) | The newest patch release of a minor. |
| The registries of the Kubernetes images | Before a Kubernetes upgrade, whether each image it is about to pull exists. |

It only reads, only over HTTPS, and says who it is in every request. What it learns is used to refuse an upgrade to something that does not exist before a node tries to pull it, and to take a Kubernetes upgrade over several minors through the newest release of each. If a service does not answer, nothing is refused: the nodes find out when they pull.

It is off unless you switch it on because these are other people's services, and using them from your cluster is yours to decide. Nothing is less safe without it. Which versions go together, which steps Talos and Kubernetes allow, and what a Kubernetes release removed are rules the operator is built with, not something it asks for. A release that does not exist is then found by the first node that fails to pull it, before anything on that node is changed, and the upgrade halts there. And a Kubernetes upgrade over several minors goes through the first release of each minor in between, so take one minor at a time.

The check below asks the same services, once for every pull request it runs on, whether the operator does or not.

### Checking a change before it merges

The operator's image has a second command for repositories that hold the resources:

```bash
docker run --rm -v "$PWD:/work:ro" -w /work ghcr.io/jarensaa/talos-operator:v0.9.0 \
  validate --base ../main/clusters/my-cluster clusters/my-cluster
```

It reads the TalosCluster, TalosMachinePool, TalosMachine and TalosMachineConfig documents in the files and directories given, and ignores everything else. It needs no cluster and no secret, and checks that:

- the Talos and Kubernetes versions go together and are ones this operator upgrades to;
- with `--base`, the manifests as they were before the change, the step is one that can be taken: no downgrade, no jump Talos does not make;
- the installer exists, for the cluster and for every pool with a schematic of its own, extensions included, and so do the Kubernetes images;
- maintenance windows and gate expressions can be understood;
- every machine's config renders, with secrets made up for the purpose. A machine whose config comes partly from a Secret is not rendered, and is named;
- no document has a field the API does not know, which the API server would drop without a word.

It exits with 1 when something is wrong, and with 2 when the files cannot be read. If a service it asks does not answer, that is a failure too; `--offline` skips the questions. On GitHub Actions every problem is an annotation, and the run's summary lists the upgrades the change means.

The check compares a change with the base branch, not with the cluster: an earlier bump may still be waiting for its window. The operator plans again from what the nodes run when the change arrives.

[`docs/examples`](examples) has a workflow that runs the check on pull requests and a Renovate configuration that proposes version bumps, with patch releases merged by themselves once the check passes and minors left for a human.

## Windows, gates and hooks

`spec.rollout` holds three things that apply to every operation: a config rollout, a Talos upgrade and a Kubernetes upgrade.

### Maintenance windows

```yaml
spec:
  rollout:
    maintenanceWindows:
      - schedule: "0 2 * * 6"
        duration: 3h
        timeZone: Europe/Oslo
```

`schedule` is a cron expression of five fields (minute, hour, day of month, month, day of week) for when the window opens; `timeZone` is an IANA name and defaults to UTC. With no windows, any time is allowed.

Outside every window nothing starts (`RolloutBlocked` with reason `OutsideWindow`, and the time the next window opens), and an operation in flight begins no new node. A node that is being changed is finished: a config that is being tried is verified and confirmed, a node that is cordoned is upgraded and uncordoned. So a window should be long enough for one node, not for the whole cluster; the rest waits for the next one.

A schedule or time zone that cannot be understood closes the window until it is fixed (`InvalidRollout`).

### Gates

```yaml
spec:
  rollout:
    gates:
      - name: storage-healthy
        object: { apiVersion: ceph.rook.io/v1, kind: CephCluster, namespace: storage, name: storage }
        expression: object.status.ceph.health == "HEALTH_OK"
```

A gate is a [CEL](https://kubernetes.io/docs/reference/using-api/cel/) expression over one object, available as `object`. It is asked before a node is touched and while a changed node is verified: a node is not done, and the next not begun, until every gate has been true for the soak.

Only a plain `true` opens a gate. An object that does not exist, a field that is not there, a result that is not a boolean: all closed, with the reason in `RolloutBlocked` or in the progress of the operation. Use `has(object.status.x)` where a field may be missing.

The operator may only read what it is granted. Give it `get` on each kind a gate names, in the chart's values:

```yaml
gates:
  rules:
    - apiGroups: ["ceph.rook.io"]
      resources: ["cephclusters"]
      verbs: ["get"]
```

A gate that stays closed is bounded like every other wait. Before a node it ends in `Timeout` after `stepTimeout`; after a config change, Talos takes the change back when `tryTimeout` ends. If what a gate watches needs longer than that to settle after a node, such as storage that rebalances after a reboot, raise `stepTimeout`.

### Hooks

```yaml
spec:
  rollout:
    hooks:
      preNode:
        - name: noout
          timeout: 5m
          job:
            template:
              spec:
                serviceAccountName: storage-tools
                containers:
                  - name: ceph
                    image: registry.example.com/ceph-tools:1
                    command: ["ceph", "osd", "set-group", "noout", "$(TALOS_NODE)"]
```

A hook is a `batch/v1` Job spec. The operator creates the Job in the TalosCluster's namespace and waits for it.

| Stage | Runs |
|---|---|
| `preRollout` | Once, after the operation started and before its first node. |
| `preNode` | Before anything is sent to a node. |
| `postNode` | After a node was verified. |
| `postRollout` | When an operation that ran its `preRollout` hooks ends: finished, aborted, or replaced by a newer change. |

A Kubernetes upgrade takes no node out of the cluster and runs only `preRollout` and `postRollout`.

Every container gets these environment variables:

| Variable | Value |
|---|---|
| `TALOS_CLUSTER` | The TalosCluster's name. |
| `TALOS_OPERATION` | `ConfigRollout`, `TalosUpgrade` or `KubernetesUpgrade`. |
| `TALOS_OPERATION_ID` | The operation's ID, as in `status.operation.id`. |
| `TALOS_HOOK` | The stage. |
| `TALOS_MACHINE`, `TALOS_NODE` | For a node's hooks: the TalosMachine, and its Kubernetes Node. |

Where the spec leaves them open, the operator sets `restartPolicy: Never`, `backoffLimit: 0`, `activeDeadlineSeconds` to the hook's timeout (`stepTimeout` if the hook has none), and keeps a finished Job for a day.

**When a hook fails** (its Job fails, or does not finish in time), the operation halts with reason `HookFailed`, and the remediation names the Job to read the log of. `retry` runs the hook again as a new Job. `skip-machine` leaves out the node whose `preNode` hook failed, or goes on without the rest of a node's `postNode` hooks. `abort` ends the operation.

Three things to know when writing hooks:

- **A hook may run more than once for the same node**, after a retry. Write hooks that are safe to repeat.
- **A node that failed gets no `postNode` hook.** What `preNode` set up for it is still set up while the operation is halted, and after a skip. Clean up in `postRollout`, which runs however the operation ends.
- **A hook runs in the operator's namespace**, with the pod spec you wrote. Whoever may edit a TalosCluster can therefore run pods there, next to the cluster's secrets. They could already put any config on every node, so this grants nothing new, but it is one more reason to keep that permission narrow.

## Monitoring

The operator exports what it knows as metrics, and the chart brings alerts on them and a Grafana dashboard. An operation that has failed and waits for you is the reason to set this up: without it, a halted rollout is only visible to someone who looks at the TalosCluster.

### What is installed, and when

The chart installs three things, under `monitoring` in its values:

| Part | What it is |
|---|---|
| `serviceMonitor` | A `ServiceMonitor` that has your Prometheus scrape every replica. |
| `alerts` | The alerts below, as a `PrometheusRule`. |
| `dashboard` | A dashboard, as a ConfigMap with the label Grafana's sidecar loads (`grafana_dashboard: "1"`). |

Each is `auto` unless you say `true` or `false`. `auto` installs it if the cluster has the Prometheus Operator's API (`monitoring.coreos.com/v1`), which kube-prometheus-stack brings, and leaves it out if not. So a cluster without a Prometheus gets none of it, and nothing fails.

Three things to know:

- **Your Prometheus has to select them.** A Prometheus only reads ServiceMonitors and rules that match its selectors. kube-prometheus-stack, as it comes, wants the label `release: <its release name>`. Put it in `monitoring.labels`:

  ```yaml
  monitoring:
    labels:
      release: kube-prometheus-stack
  ```

- **The chart looks when it is installed or upgraded.** If the Prometheus Operator arrives after the operator, the next upgrade of the chart picks it up; or set the three to `true`. Rendering the chart outside a cluster (`helm template`) does not see the API unless you pass `--api-versions monitoring.coreos.com/v1/ServiceMonitor`.
- **Grafana cannot be detected**, so the dashboard goes by the same sign as the rest. A Grafana that does not run the sidecar needs the dashboard imported by hand: it is `files/dashboard.json` in the chart.

The metrics endpoint is HTTPS and asks the Kubernetes API whether the scraper may read `/metrics`. kube-prometheus-stack's Prometheus may. For another scraper, bind its ServiceAccount to the ClusterRole the chart creates for that: `talos-operator-metrics-reader`, for a release named `talos-operator`.

### Alerts

| Alert | Severity | When |
|---|---|---|
| `TalosOperationHalted` | critical | A rollout or an upgrade failed and waits for you: retry, skip the machine, or abort ([When a node doesn't come out healthy](#when-a-node-doesnt-come-out-healthy)). |
| `TalosOperatorDown` | critical | No operator has been scraped for 10 minutes. Nothing else here can fire while that lasts. |
| `TalosAPIUnreachable` | critical | The operator has had no working connection to the Talos API, or no credentials, for 10 minutes. |
| `TalosRolloutBlocked` | warning | A change has waited 30 minutes for something other than a pause or its maintenance window: a closed gate, your approval, a change the operator will not make. |
| `TalosClusterNotObserved` | warning | The operator's last finished observation is more than 10 minutes old. |
| `TalosOperatorCannotReadState` | warning | The operator cannot read its own resources, and exports nothing about them. |
| `TalosConfigDrift` | warning | A node no longer runs the config the operator applied, and `driftPolicy: Report` leaves it so. |
| `TalosNodeWithoutMachine` | warning | The cluster has a node that no TalosMachine describes. |
| `TalosVersionsUnreachable` | warning | The spec asks for versions there is no way to. |
| `TalosClusterUnhealthy` | warning | Talos's own health check has failed for 15 minutes. |
| `TalosMachineNotObserved` | warning | The operator has not been able to read a node for 10 minutes. |
| `TalosMachineNotReady` | warning | A node answers and has not been ready for 15 minutes. |
| `TalosControlPlaneNotEtcdVoter` | warning | A control plane has not been a voting member of etcd for 15 minutes. |

Waiting as you told it to raises nothing: a paused cluster, or a change outside its maintenance window.

Every alert names the cluster in the labels `namespace` and `taloscluster`, and where it is about one, the `machine`. Its description says where in the TalosCluster's status to read on.

### Metrics

All of them start with `talos_operator_`, and all of them are what the status of your resources says, read when the operator is scraped. Both replicas export the same, so take the maximum, as the alerts do:

```promql
max by (taloscluster) (talos_operator_cluster_operation_halted)
```

| Metric | Says |
|---|---|
| `info` | An operator is running, and which build. |
| `cluster_phase`, `cluster_condition` | The cluster's phase, and each of its conditions with the status and reason it has now. |
| `cluster_observed_timestamp_seconds` | When the operator last finished observing the cluster. |
| `cluster_operation_in_progress`, `cluster_operation_halted`, `cluster_approval_pending` | 0 or 1. |
| `operation_info`, `operation_start_timestamp_seconds`, `operation_turns` | The rollout or upgrade in flight, and how far it is. |
| `operation_failure_info`, `operation_failure_timestamp_seconds` | Where a halted operation waits: the machine, the step, the reason. |
| `cluster_version_info`, `cluster_version_plan_steps` | The versions asked for and run, and how many upgrades lie between them. |
| `cluster_etcd_members`, `machine_etcd_member` | Voters and learners; for each control plane, whether it is one. |
| `machine_info`, `machine_phase`, `machine_condition` | Each machine, what its node runs, and its conditions. |
| `machine_ready`, `machine_hold`, `machine_config_change_info` | Whether a node was ready when last read; whether it is on hold; a config change that is pending. |

What the operator does not observe, it does not export: CPU, memory and disks are a node exporter's, and the cluster's own etcd and control-plane metrics are Kubernetes's.

## Backing out

Set `config` back to `Observe` and the operator stops writing to your nodes at once. A rollout in flight ends there; a config that was being tried is reverted by Talos when its window ends.

To remove the operator, uninstall the chart and remove the `KubeTalosAPIAccessConfig` from the control planes.

A `TalosMachine` can't be deleted while it's `Present`, so a stray prune can't drop a node. To release one, annotate it first:

```bash
kubectl -n talos-operator annotate talosmachine <name> talos.jarensaa.github.io/orphan=true
```

The two Secrets adoption wrote stay behind, because the operator never deletes them itself. Delete them if you don't want them.
