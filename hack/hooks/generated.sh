# shellcheck shell=bash
# Sourced by the hooks. What CI's first step refuses (`make committed`): a go.mod that is not
# tidy, and generated files that are not what the tree generates.

# Everything `go mod tidy` and `make manifests generate` write.
# shellcheck disable=SC2034 # used by the hooks that source this file
generated_paths=(go.mod go.sum config/crd config/rbac charts/talos-operator/files api/v1alpha1/zz_generated.deepcopy.go)

# regenerate runs the generators. They write to the working tree, which is the point: what they
# write is what has to be committed.
regenerate() {
  go mod tidy
  make --no-print-directory manifests generate >/dev/null
}
