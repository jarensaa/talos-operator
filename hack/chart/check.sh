#!/usr/bin/env bash
# Checks what the chart renders that `helm lint` does not look at: the CRDs are part of the
# release, identical to the generated ones, kept on uninstall, and can be left out.
set -euo pipefail

chart=${1:-charts/talos-operator}
generated=config/crd/bases

fail() { echo "chart check: $*" >&2; exit 1; }

# crds prints the names of the CRDs a rendering contains, with a marker for those Helm keeps.
crds() {
  helm template talos-operator "$chart" --namespace talos-operator "$@" |
    yq -N 'select(.kind == "CustomResourceDefinition") |
      .metadata.name + " " + (.metadata.annotations."helm.sh/resource-policy" // "-")'
}

want=$(ls "$generated"/*.yaml | wc -l | tr -d ' ')
[ "$want" -gt 0 ] || fail "no generated CRDs in $generated"

[ ! -d "$chart/crds" ] || fail "$chart/crds exists: Helm never upgrades CRDs it finds there"

rendered=$(crds)
[ "$(echo "$rendered" | wc -l | tr -d ' ')" = "$want" ] || fail "expected $want CRDs, rendered: $rendered"
! echo "$rendered" | grep -qv ' keep$' || fail "every CRD must be kept on uninstall by default: $rendered"

[ -z "$(crds --set crds.enabled=false)" ] || fail "crds.enabled=false must leave the CRDs out"
! crds --set crds.keep=false | grep -q ' keep$' || fail "crds.keep=false must drop the keep policy"

# The rendered CRDs are the generated ones, apart from the annotation the chart adds.
for file in "$generated"/*.yaml; do
  name=$(yq '.metadata.name' "$file")
  diff <(yq -o json -I0 'sort_keys(..)' "$file") \
    <(helm template talos-operator "$chart" --set crds.keep=false |
      yq -o json -I0 "select(.kind == \"CustomResourceDefinition\" and .metadata.name == \"$name\") | sort_keys(..)") \
    >/dev/null || fail "the rendered CRD $name differs from $file"
done

echo "chart check: $want CRDs rendered, kept on uninstall, identical to $generated"
