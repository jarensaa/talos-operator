# End-to-end testing

e2e tests run against a disposable Talos cluster built the way a hand-rolled bare-metal cluster usually is:
- 3 control planes and 1 worker by default;
- configs generated with `talosctl gen config --with-secrets` plus one all-nodes patch and one per-node patch;
- a control-plane patch enabling Talos API access for the `talos-operator-e2e` namespace, so the operator runs on Talos-issued credentials as it does in production.

```bash
make e2e-cluster-up     # prompts for sudo; reuses a running cluster
make e2e-deploy         # builds the operator image, loads it into the nodes, installs the chart
make test-e2e           # e2e-tagged tests in internal/talos and test/e2e
make e2e-cluster-down   # prompts for sudo; removes .e2e/ entirely
```

**The operator under test is the one a user runs** (docs/scope.md §12.18): the image built from the checkout, installed by the chart into the `talos-operator-e2e` namespace with two replicas on different nodes, with the chart's CRDs, admission policies and permissions. The suite starts no controllers itself. `hack/e2e/deploy.sh` builds the image, saves it as a tarball and loads it into every node through Talos's image import (`hack/e2e/import-image`), so no registry is involved and the nodes' config carries nothing that is only there for the tests. It needs `docker` and `helm`.

The suite reads the operator's metrics from each pod's own endpoint, over HTTPS and as a reader the Kubernetes API authorizes, to check that no Talos call was refused by the operator's allowlist.

`.e2e/` is gitignored. It holds throwaway secrets, generated configs, `talosconfig`, `kubeconfig` and an `env` file that `make test-e2e` sources.

QEMU state (VM disks, console logs, sockets) lives in `~/.talos/clusters/talos-operator-e2e`, talosctl's default, not under the checkout. QEMU's monitor socket path must fit the Unix socket limit (104 bytes on macOS, 108 on Linux), and checkout paths are often too long. The harness checks the length before starting; override the location with `E2E_STATE_DIR`.

Knobs (environment variables for `hack/e2e/cluster-up.sh`):

| Variable | Default | Purpose |
|---|---|---|
| `TALOS_VERSION` | `v1.14.1` | Talos version booted and installed: one patch release below `UPGRADE_TO` |
| `UPGRADE_TO` | `v1.14.2` | The version the suite upgrades every node to; the one the operator is built for |
| `KUBERNETES_VERSION` | `1.36.0` | Kubernetes version the cluster is created with: one minor below `KUBERNETES_UPGRADE_TO` |
| `KUBERNETES_UPGRADE_TO` | `1.37.0` | The Kubernetes version the suite upgrades the cluster to |
| `CONTROLPLANES` / `WORKERS` | `3` / `1` | Configured nodes |
| `MAINTENANCE_NODES` | `0` (CI: `1`) | Extra machines left in maintenance mode, without a config: what the suite has the operator add, remove and replace a control plane with |
| `SCHEMATIC_ID` | vanilla schematic | Installer schematic |
| `VM_CPUS` / `VM_MEMORY` | `2.0` / `2GiB` | Per-VM resources (CI uses less) |
| `WORKER_MEMORY` | `VM_MEMORY` | Memory per worker VM, when workers can be smaller than control planes |

## Why QEMU, and why sudo

Spike `talos-w3y.11`, 2026-10-09, on macOS arm64 with Docker Desktop 29.4.3:

- **The Docker provisioner cannot run Talos 1.14 here.** Container mode needs a host kernel ≥ 6.15. Talos 1.14 builds its writable `/etc` overlay from file-descriptor lower layers (`fsconfig(FSCONFIG_SET_FD, "lowerdir+")`), which older kernels reject with `EBADF`. Docker Desktop runs kernel 6.12.76, so machined fails with `failed to compose writable /etc overlay` and apid never starts. Upstream tracks this in [siderolabs/talos#14476](https://github.com/siderolabs/talos/issues/14476). Container mode would also refuse `LifecycleService` upgrades, which M4 needs.
- **The QEMU provisioner works but needs root** on macOS, for CNI networking and the HVF accelerator: `talosctl` exits with "please run as root user". The harness therefore runs only `talosctl cluster create/destroy` under `sudo -E`. Config generation, applies and bootstrap run as the invoking user.
- **Pass `--talosconfig-destination`.** Without it, `talosctl cluster create` merges the new context into `~/.talos/config`, next to the contexts of real clusters.
- QEMU from Homebrew (`brew install qemu`) provides `qemu-system-aarch64`. The Talos ISO comes from Image Factory and is cached in `~/.talos/cache`.

## How the harness builds the cluster

1. `talosctl cluster create qemu --presets iso,maintenance` boots every VM from the Image Factory ISO into maintenance mode. The provisioner's own configs are ignored.
2. Throwaway secrets, then `gen config --with-secrets --install-disk /dev/vda --install-image factory.talos.dev/metal-installer/<schematic>:<version>`. With the 1.14 contract this produces an `UnattendedInstallConfig` with a disk selector, as in production.
3. Per node: `machineconfig patch` with `hack/e2e/patches/all.patch.yaml`, on control planes also `controlplane.patch.yaml` (`KubeTalosAPIAccessConfig`), then a per-node `HostnameConfig` patch. Adoption has to factor these back into the `all`, `controlPlane` and per-machine patch layers.
4. Dry-run every config, then apply each with `apply-config --insecure`. Nodes install to disk and reboot. The dry run's output is discarded: it is a diff of the whole config, keys included, and the harness log is printed in CI.
5. Bootstrap the first control plane (retried until install and reboot finish; `AlreadyExists` counts as done), wait for `talosctl health`, then write the kubeconfig.

Addresses: the provisioner assigns `10.5.0.1` to the gateway, then control planes from `.2` and workers after them.

## CI is the e2e path

Local runs are optional. The `E2E` workflow (`.github/workflows/e2e.yml`) runs the same harness on a GitHub-hosted Linux runner, where sudo is passwordless and `/dev/kvm` is available. It runs:
- on every pull request. A first job skips the cluster when nothing it exercises changed, so the check can be required;
- weekly (Mondays), with three control planes, to catch drift such as new Image Factory builds or Talos releases;
- on demand (`workflow_dispatch`), with the cluster shape you ask for.

The job starts cluster creation in the background as soon as QEMU and `talosctl` are installed. Go setup, `make test-e2e-build` and the build of the operator image run while the VMs boot, so only importing the image, installing the chart and the tests themselves wait for the cluster. `make test-e2e` runs only tests named `TestE2E*`; unit tests in the same packages run in the Tests workflow.

Every pull request runs one control plane and one worker (`CONTROLPLANES=1 WORKERS=1 VM_CPUS=1.0`), which is quick and is the required check.

**Three control planes run weekly, and on request** (docs/scope.md §12.13). A manual run of the workflow takes the number of control planes and workers, and the memory per worker, as inputs, and `--ref` picks the branch:

```bash
gh workflow run e2e.yml --ref my-branch -f controlplanes=3 -f workers=1
```

This is where scenarios that need several control planes are shown: serialising them in an upgrade, growing them, replacing one. The rules that keep that honest:

- **A change to how control planes are handled is run this way before it is merged**, and the pull request links the run. That covers the order and gates of a rollout, upgrades, etcd membership, and adding or removing a control plane. The pull request's own check cannot show any of it.
- **A spec that needs several control planes says so and skips on a smaller cluster**, with a message that names what it needs. It must never pass without having run.
- **It is not a required check.** A failure of the weekly run is a failure of `main` and is looked at like one.

The runner is GitHub's standard hosted one, which for a public repository has 4 vCPUs and 16 GB. Three control planes and a worker at 2 GiB each use half of that memory, and the whole run takes about as long as a pull request's. No larger or self-hosted runner is involved.

What the suite covers on that cluster, in order and on one TalosCluster: adoption from an empty TalosCluster, with the operator writing the pools and machines, nothing applied by hand and no node changed; an edit made before management is on, which is caught and makes the API server refuse `config: Manage`; the operator listing in status what really exists upstream, and the resources adoption wrote passing the `validate` check against the real Image Factory and registries and failing it for a schematic that does not exist; a Talos upgrade of every node to the next patch release, held back first while its maintenance window is closed, the nodes the operator's own replicas run on included, with a workload behind a disruption budget kept up, followed by the config rollout that names the new installer; a rollout whose leader is killed in the middle of a node's try, finished by the other replica; a label rolled out to the worker without a reboot; a worker whose kubelet is broken by a config applied behind the operator's back, which the operator repairs once the drift policy is `Enforce`, although the node is unhealthy; a protected field refused; and a config that makes the worker drop everything it sends to the control planes, which Talos must revert while the operator confirms nothing and leaves the control plane's own pending change unapplied, with the halted rollout found in the metrics of both replicas as the chart's alert reads them, and gone from them once it is aborted; a rollout that waits outside a maintenance window, then behind a gate on a ConfigMap that does not exist and then says the wrong thing, and once the gate is true runs a hook Job at every stage; and last a Kubernetes upgrade by a minor, refused while the kubelets of one pool are started with a flag the new version removed, and then, once a config rollout took the flag out again, taken through every component on every node; and last the machines themselves: the spare machine added as a worker after it was refused under another machine's identity, the removal of a control plane the cluster cannot spare refused, the worker taken out again and found waiting in maintenance mode with its Node deleted, on three control planes one replaced by the spare with three voting members of etcd throughout, and whatever machine waits at the end added once more, cut off for good, and taken out once the suite says it is dead. The cut-off scenario waits out a four-minute try window, during which the worker is offline; `make test-e2e` therefore runs its packages one after the other.

The machines the suite adds carry a taint that keeps the operator's own pods off them: the operator's image is in no registry and was loaded into the nodes the harness built, not into a machine that joined later. A machine is "dead" for the suite when it drops everything it would send to the control planes; the VM goes on running, and the cluster cannot tell the difference.

The hook Jobs run the operator's own image (`/manager --help`, which exits with 0): it is the one image every node is known to have. The gate needs the operator to read ConfigMaps, which `hack/e2e/deploy.sh` grants through the chart's `gates.rules`. The same script sets `versionSources.enabled`, which is off in the chart as it comes, so that the operator under test lists what exists upstream; the run therefore needs Image Factory, the Kubernetes release site and the image registries, which it needs anyway to build the cluster. The removed flag is one of the cAdvisor flags kubelet 1.37 dropped, set to its default, so it changes nothing while it is there. Objects of a removed API are not part of the scenario: an API server does not serve a CustomResourceDefinition inside a group of its own, and the real APIs in question are alpha APIs a cluster has to be started with.

The Talos upgrade reboots every node and the Kubernetes upgrade restarts every component on each, so a run takes most of an hour on the small shape and longer on the larger ones. On a cluster with one control plane the Kubernetes API is down while that node reboots, and again while its API server restarts; the replica on the worker loses its lease, restarts, and carries on from the persisted step once the API is back.

On failure the workflow uploads `e2e-diagnostics`: VM console logs, the operator's pods, logs and events, plus `dmesg` and service status from each node it can reach. The cluster is always destroyed at the end.
