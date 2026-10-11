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

# Permissions that come from files and values: both must reach the ServiceAccount, and a gate's
# kind is granted only when someone asks for it.
gate='gates.rules[0].apiGroups[0]=ceph.rook.io,gates.rules[0].resources[0]=cephclusters,gates.rules[0].verbs[0]=get'

bound() {
  helm template talos-operator "$chart" --namespace talos-operator "$@" |
    yq -N "select(.kind == \"ClusterRoleBinding\") | .metadata.name"
}

rules() { # the rules of a ClusterRole, as JSON
  local name=$1; shift
  helm template talos-operator "$chart" --namespace talos-operator "$@" |
    yq -o json -I0 "select(.kind == \"ClusterRole\" and .metadata.name == \"$name\") | .rules"
}

# Captured first: grep -q ends a pipe early, which pipefail takes for a failure of what feeds it.
default=$(bound)
gated=$(bound --set "$gate")

grep -qx talos-operator-upgrade-checks <<<"$default" || fail "the upgrade checks' ClusterRole is not bound"
diff <(rules talos-operator-upgrade-checks) <(yq -o json -I0 '.rules' "$chart/files/upgrade-check-rules.yaml") >/dev/null ||
  fail "the upgrade checks' ClusterRole does not carry files/upgrade-check-rules.yaml"

! grep -q gates <<<"$default" || fail "no gate is granted anything by default"
grep -qx talos-operator-gates <<<"$gated" || fail "gates.rules must be bound to the operator"
[ "$(rules talos-operator-gates --set "$gate")" = '[{"apiGroups":["ceph.rook.io"],"resources":["cephclusters"],"verbs":["get"]}]' ] ||
  fail "gates.rules must become the rules of the gates ClusterRole"

# The chart as it comes asks nothing outside the cluster: asking upstream is switched on, never off.
args() {
  helm template talos-operator "$chart" --namespace talos-operator "$@" |
    yq -r 'select(.kind == "Deployment") | .spec.template.spec.containers[0].args[]'
}

quiet=$(args)
asking=$(args --set versionSources.enabled=true --set versionSources.kubernetesReleases=https://releases.example.com)
grep -qx -- '--version-sources=false' <<<"$quiet" || fail "the operator must not ask upstream by default"
grep -qx -- '--version-sources=true' <<<"$asking" || fail "versionSources.enabled must switch asking upstream on"
grep -qx -- '--kubernetes-releases=https://releases.example.com' <<<"$asking" ||
  fail "versionSources.kubernetesReleases must reach the operator"

echo "chart check: $want CRDs rendered, kept on uninstall, identical to $generated; permissions bound; nothing asked upstream by default"
