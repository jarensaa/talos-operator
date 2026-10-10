#!/usr/bin/env bash
# Destroys the cluster created by cluster-up.sh and removes all generated state.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
E2E_DIR=${E2E_DIR:-"$REPO_ROOT/.e2e"}
CLUSTER_NAME=${CLUSTER_NAME:-talos-operator-e2e}
STATE_DIR=${E2E_STATE_DIR:-"$HOME/.talos/clusters"}

if [[ -d "$STATE_DIR/$CLUSTER_NAME" ]]; then
  sudo -E talosctl cluster destroy --name "$CLUSTER_NAME" --state "$STATE_DIR" --provisioner qemu
fi

sudo rm -rf "$E2E_DIR"
