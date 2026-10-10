#!/usr/bin/env bash
# Creates a disposable Talos cluster of the shape docs/scope.md §2 describes: machines boot
# into maintenance mode, then receive configs produced the way such clusters usually are,
# `talosctl gen config --with-secrets` plus an all-nodes patch and a per-node patch.
#
# The QEMU provisioner needs root on macOS (CNI and the HVF accelerator), so it runs under
# `sudo -E`. Everything else runs as the invoking user. Idempotent: an existing cluster is reused.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)

E2E_DIR=${E2E_DIR:-"$REPO_ROOT/.e2e"}
CLUSTER_NAME=${CLUSTER_NAME:-talos-operator-e2e}
# The cluster boots one patch release below UPGRADE_TO, the version the operator is built for, so
# that the suite has a Talos upgrade to perform (test/e2e/adoption_test.go).
TALOS_VERSION=${TALOS_VERSION:-v1.14.1}
UPGRADE_TO=${UPGRADE_TO:-v1.14.2}
# Likewise for Kubernetes: one minor below KUBERNETES_UPGRADE_TO, for the suite to upgrade.
KUBERNETES_VERSION=${KUBERNETES_VERSION:-1.36.0}
KUBERNETES_UPGRADE_TO=${KUBERNETES_UPGRADE_TO:-1.37.0}
CONTROLPLANES=${CONTROLPLANES:-3}
WORKERS=${WORKERS:-1}
# Extra workers left in maintenance mode, for expansion tests (M7).
MAINTENANCE_NODES=${MAINTENANCE_NODES:-0}
CIDR_PREFIX=${CIDR_PREFIX:-10.5.0}
# Per-VM resources; CI runners are small, so these are tunable.
VM_CPUS=${VM_CPUS:-2.0}
VM_MEMORY=${VM_MEMORY:-2GiB}
WORKER_MEMORY=${WORKER_MEMORY:-$VM_MEMORY}
# The vanilla Image Factory schematic (no extensions).
SCHEMATIC_ID=${SCHEMATIC_ID:-376567988ad370138ad8b2698212367b8edcb69b5fd68c80be1f2ec7d603b4ba}
INSTALL_IMAGE="factory.talos.dev/metal-installer/${SCHEMATIC_ID}:${TALOS_VERSION}"
IMAGE_FACTORY_URL=${IMAGE_FACTORY_URL:-https://factory.talos.dev/}

# QEMU puts its monitor socket in the state directory, and Unix socket paths are limited to 104
# bytes on macOS and 108 on Linux. The repo checkout path is usually too long (it broke the first
# CI runs), so state lives under talosctl's own default, ~/.talos/clusters.
STATE_DIR=${E2E_STATE_DIR:-"$HOME/.talos/clusters"}
GENERATED_DIR="$E2E_DIR/generated"
TALOSCONFIG="$E2E_DIR/talosconfig"
KUBECONFIG_PATH="$E2E_DIR/kubeconfig"

log() { printf '==> %s\n' "$*" >&2; }

# The QEMU provisioner assigns addresses in order: gateway .1, control planes from .2, then workers.
node_ip() { printf '%s.%d' "$CIDR_PREFIX" "$(( $1 + 2 ))"; }

controlplane_ips=()
for ((i = 0; i < CONTROLPLANES; i++)); do controlplane_ips+=("$(node_ip "$i")"); done
worker_ips=()
for ((i = 0; i < WORKERS; i++)); do worker_ips+=("$(node_ip $((CONTROLPLANES + i)))"); done
maintenance_ips=()
for ((i = 0; i < MAINTENANCE_NODES; i++)); do maintenance_ips+=("$(node_ip $((CONTROLPLANES + WORKERS + i)))"); done
cp1="${controlplane_ips[0]}"

longest_socket="$STATE_DIR/$CLUSTER_NAME/$CLUSTER_NAME-controlplane-$((CONTROLPLANES + WORKERS + MAINTENANCE_NODES)).monitor"
if (( ${#longest_socket} > 103 )); then
  printf 'QEMU socket path would be %d bytes (limit 103): %s\nSet E2E_STATE_DIR to a shorter directory.\n' \
    "${#longest_socket}" "$longest_socket" >&2
  exit 1
fi

if [[ -s "$TALOSCONFIG" ]] && talosctl --talosconfig "$TALOSCONFIG" -n "$cp1" version --short >/dev/null 2>&1; then
  log "cluster $CLUSTER_NAME is already running; reusing it"
  exit 0
fi

mkdir -p "$E2E_DIR" "$GENERATED_DIR"
umask 077

# Written before anything can fail, so diagnostics can find the nodes even if creation breaks.
printf '%s\n' "${controlplane_ips[@]}" "${worker_ips[@]}" >"$E2E_DIR/nodes"

# Pre-fetch the boot ISO with retries. talosctl downloads it once with no retry, so a single
# network blip would fail the run. talosctl skips its download when the file already sits in
# ~/.talos/cache under its URL with '/' and ':' replaced by '-'
# (cmd/talosctl/cmd/mgmt/cluster/create/create.go:74-91 in Talos v1.14).
prefetch_iso() {
  local arch iso_url cache_dir cached tmp
  case "$(uname -m)" in
    x86_64 | amd64) arch=amd64 ;;
    arm64 | aarch64) arch=arm64 ;;
    *) printf 'unsupported architecture %s\n' "$(uname -m)" >&2; return 1 ;;
  esac

  iso_url="${IMAGE_FACTORY_URL}image/${SCHEMATIC_ID}/${TALOS_VERSION}/metal-${arch}.iso"
  cache_dir="$HOME/.talos/cache"
  cached="$cache_dir/$(printf '%s' "$iso_url" | tr '/:' '--')"

  if [[ -s "$cached" ]]; then
    return 0
  fi

  log "pre-fetching $iso_url"
  tmp="$E2E_DIR/boot.iso.partial"
  curl -fsSL --retry 5 --retry-all-errors --retry-delay 10 --connect-timeout 30 -o "$tmp" "$iso_url"
  mkdir -p "$cache_dir" 2>/dev/null || sudo mkdir -p "$cache_dir"
  mv "$tmp" "$cached" 2>/dev/null || sudo mv "$tmp" "$cached"
}

if [[ ! -d "$STATE_DIR/$CLUSTER_NAME" ]]; then
  prefetch_iso

  log "booting $CONTROLPLANES control plane(s), $((WORKERS + MAINTENANCE_NODES)) worker(s) into maintenance mode (sudo required)"
  # The maintenance preset writes its own (unused) machine configs to the working directory.
  mkdir -p "$E2E_DIR/provisioner"
  (cd "$E2E_DIR/provisioner" && sudo -E talosctl cluster create qemu \
    --name "$CLUSTER_NAME" \
    --state "$STATE_DIR" \
    --talos-version "$TALOS_VERSION" \
    --kubernetes-version "$KUBERNETES_VERSION" \
    --controlplanes "$CONTROLPLANES" \
    --workers "$((WORKERS + MAINTENANCE_NODES))" \
    --cidr "${CIDR_PREFIX}.0/24" \
    --cpus-controlplanes "$VM_CPUS" \
    --cpus-workers "$VM_CPUS" \
    --memory-controlplanes "$VM_MEMORY" \
    --memory-workers "$WORKER_MEMORY" \
    --presets iso,maintenance \
    --talosconfig-destination "$E2E_DIR/provisioner/talosconfig")
  # Files written under sudo belong to root; the state directory stays root-owned for QEMU.
  sudo chown -R "$(id -u):$(id -g)" "$E2E_DIR/provisioner"
fi

if [[ ! -s "$E2E_DIR/secrets.yaml" ]]; then
  log "generating throwaway cluster secrets"
  talosctl gen secrets --output-file "$E2E_DIR/secrets.yaml"
fi

log "generating machine configs (gen config --with-secrets + patches)"
talosctl gen config "$CLUSTER_NAME" "https://${cp1}:6443" \
  --with-secrets "$E2E_DIR/secrets.yaml" \
  --install-image "$INSTALL_IMAGE" \
  --install-disk /dev/vda \
  --kubernetes-version "$KUBERNETES_VERSION" \
  --output-types controlplane,worker,talosconfig \
  --with-docs=false \
  --with-examples=false \
  --force \
  --output "$GENERATED_DIR/base" >/dev/null

render_node() { # role index ip
  local role=$1 ip=$3 hostname="e2e-${1}-${2}"
  cat >"$GENERATED_DIR/node-${hostname}.patch.yaml" <<PATCH
apiVersion: v1alpha1
kind: HostnameConfig
auto: "off"
hostname: ${hostname}
PATCH
  local role_patch=()
  if [[ -f "$SCRIPT_DIR/patches/${role}.patch.yaml" ]]; then
    role_patch=(--patch @"$SCRIPT_DIR/patches/${role}.patch.yaml")
  fi
  talosctl machineconfig patch "$GENERATED_DIR/base/${role}.yaml" \
    --patch @"$SCRIPT_DIR/patches/all.patch.yaml" \
    ${role_patch[@]+"${role_patch[@]}"} \
    --patch @"$GENERATED_DIR/node-${hostname}.patch.yaml" \
    --output "$GENERATED_DIR/${hostname}.yaml"
  printf '%s %s\n' "$ip" "$GENERATED_DIR/${hostname}.yaml"
}

targets=()
for i in "${!controlplane_ips[@]}"; do targets+=("$(render_node controlplane "$((i + 1))" "${controlplane_ips[$i]}")"); done
for i in "${!worker_ips[@]}"; do targets+=("$(render_node worker "$((i + 1))" "${worker_ips[$i]}")"); done

# `cluster create` returns once the VMs are launched, not once they have booted the ISO.
wait_for_maintenance() { # ip
  local ip=$1 deadline=$((SECONDS + 600))
  until talosctl --nodes "$ip" version --insecure >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      printf 'node %s did not reach maintenance mode within 10 minutes\n' "$ip" >&2
      return 1
    fi
    sleep 5
  done
}

for t in "${targets[@]}"; do
  read -r ip _ <<<"$t"
  log "waiting for $ip to answer in maintenance mode"
  wait_for_maintenance "$ip"
done

log "dry-running every config before applying any "
for t in "${targets[@]}"; do
  read -r ip file <<<"$t"
  # A dry run answers with a diff of the whole machine config, on stderr: every CA key and token of
  # the cluster. This output ends up in CI logs, so none of it is printed, not even on failure.
  if ! talosctl apply-config --insecure --nodes "$ip" --file "$file" --dry-run >/dev/null 2>&1; then
    printf 'dry run of %s on %s failed; run it by hand to see why (its output shows the config)\n' "$(basename "$file")" "$ip" >&2
    exit 1
  fi
done

for t in "${targets[@]}"; do
  read -r ip file <<<"$t"
  log "applying $(basename "$file") to $ip"
  talosctl apply-config --insecure --nodes "$ip" --file "$file"
done

cp "$GENERATED_DIR/base/talosconfig" "$TALOSCONFIG"
talosctl --talosconfig "$TALOSCONFIG" config endpoint "${controlplane_ips[@]}"

log "bootstrapping etcd on $cp1 (retrying while nodes install and reboot)"
for attempt in $(seq 1 60); do
  if out=$(talosctl --talosconfig "$TALOSCONFIG" -n "$cp1" bootstrap 2>&1); then
    break
  fi
  if grep -q "AlreadyExists" <<<"$out"; then
    break
  fi
  if (( attempt == 60 )); then
    printf '%s\n' "$out" >&2
    exit 1
  fi
  sleep 10
done

log "waiting for the cluster to become healthy"
health_args=(--control-plane-nodes "$(IFS=,; echo "${controlplane_ips[*]}")" --wait-timeout 20m)
if (( ${#worker_ips[@]} > 0 )); then
  health_args+=(--worker-nodes "$(IFS=,; echo "${worker_ips[*]}")")
fi
talosctl --talosconfig "$TALOSCONFIG" -n "$cp1" health "${health_args[@]}"

talosctl --talosconfig "$TALOSCONFIG" -n "$cp1" kubeconfig "$KUBECONFIG_PATH" --force

cat >"$E2E_DIR/env" <<ENV
export E2E_TALOSCONFIG="$TALOSCONFIG"
export E2E_KUBECONFIG="$KUBECONFIG_PATH"
export E2E_CONTROLPLANES="${controlplane_ips[*]}"
export E2E_WORKERS="${worker_ips[*]}"
export E2E_MAINTENANCE_NODES="${maintenance_ips[*]}"
export E2E_TALOS_VERSION="$TALOS_VERSION"
export E2E_UPGRADE_TO="$UPGRADE_TO"
export E2E_KUBERNETES_VERSION="$KUBERNETES_VERSION"
export E2E_KUBERNETES_UPGRADE_TO="$KUBERNETES_UPGRADE_TO"
ENV

log "cluster $CLUSTER_NAME is ready; source $E2E_DIR/env"
