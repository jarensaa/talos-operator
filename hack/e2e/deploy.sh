#!/usr/bin/env bash
# Runs the operator in the e2e cluster the way a user would have it: the image built from this
# checkout, installed by the chart with two replicas. The image goes to the nodes through Talos's
# image import, so no registry is involved.
#
#   deploy.sh build    build the image and save it as a tarball (needs no cluster)
#   deploy.sh install  import the tarball into every node and install the chart
#   deploy.sh          both
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)

E2E_DIR=${E2E_DIR:-"$REPO_ROOT/.e2e"}
NAMESPACE=${E2E_NAMESPACE:-talos-operator-e2e}
CONTAINER_TOOL=${CONTAINER_TOOL:-docker}
# containerd names an image imported from a `docker save` tarball like this.
IMAGE_REPOSITORY=docker.io/library/talos-operator
IMAGE_TAG=e2e
TARBALL="$E2E_DIR/operator-image.tar"

log() { printf '==> %s\n' "$*" >&2; }

build() {
  local arch
  case "$(uname -m)" in
    x86_64 | amd64) arch=amd64 ;;
    arm64 | aarch64) arch=arm64 ;;
    *) printf 'unsupported architecture %s\n' "$(uname -m)" >&2; return 1 ;;
  esac

  mkdir -p "$E2E_DIR"

  log "building the operator image for linux/$arch"
  make -C "$REPO_ROOT" build-image-binaries IMAGE_PLATFORMS="linux/$arch"
  "$CONTAINER_TOOL" build --platform "linux/$arch" -t "talos-operator:$IMAGE_TAG" "$REPO_ROOT"
  "$CONTAINER_TOOL" save "talos-operator:$IMAGE_TAG" -o "$TARBALL"
}

install() {
  test -s "$E2E_DIR/env" || { echo "No e2e cluster; run 'make e2e-cluster-up' first." >&2; exit 1; }
  test -s "$TARBALL" || { echo "No image tarball; run '$0 build' first." >&2; exit 1; }
  # shellcheck disable=SC1091
  . "$E2E_DIR/env"

  log "importing the image into every node"
  (cd "$REPO_ROOT" && go run ./hack/e2e/import-image "$TARBALL")

  log "installing the chart into $NAMESPACE"
  # Short intervals keep the specs short, the suite's gate asks about a ConfigMap, and one spec
  # reads which releases exist upstream from status; everything else is the chart's default, two
  # replicas that never share a node included.
  helm upgrade --install talos-operator "$REPO_ROOT/charts/talos-operator" \
    --kubeconfig "$E2E_KUBECONFIG" \
    --namespace "$NAMESPACE" --create-namespace \
    --set image.repository="$IMAGE_REPOSITORY" \
    --set image.tag="$IMAGE_TAG" \
    --set versionSources.enabled=true \
    --set-json 'extraArgs=["--observe-interval=10s"]' \
    --set-json 'gates.rules=[{"apiGroups":[""],"resources":["configmaps"],"verbs":["get"]}]' \
    --wait --timeout 5m
}

case "${1:-all}" in
  build) build ;;
  install) install ;;
  all) build; install ;;
  *) echo "usage: $0 [build|install]" >&2; exit 2 ;;
esac
