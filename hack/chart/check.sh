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

# The generated role is bound in the release's namespace, where its rules about Nodes and pods
# mean nothing: what the operator does to them cluster-wide has to be granted here, by hand.
for role in node-reader node-drainer node-remover; do
  grep -qx "talos-operator-$role" <<<"$default" || fail "the ClusterRole $role is not bound"
done
[ "$(rules talos-operator-node-remover)" = '[{"apiGroups":[""],"resources":["nodes"],"verbs":["delete"]}]' ] ||
  fail "removing a machine deletes its Node, and the operator deletes nothing else"

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

# Monitoring is installed where there is something to read it: where the cluster has the
# Prometheus Operator's API, unless a value says otherwise.
monitoring() {
  helm template talos-operator "$chart" --namespace talos-operator "$@" |
    yq -N 'select(.kind == "ServiceMonitor" or .kind == "PrometheusRule" or .metadata.name == "talos-operator-dashboard") | .kind' |
    sort
}

prometheus=(--api-versions monitoring.coreos.com/v1/ServiceMonitor)
all=$'ConfigMap\nPrometheusRule\nServiceMonitor'

[ -z "$(monitoring)" ] || fail "no monitoring is installed on a cluster without the Prometheus Operator's API"
[ "$(monitoring "${prometheus[@]}")" = "$all" ] || fail "monitoring must be installed where the Prometheus Operator's API is"
[ "$(monitoring --set monitoring.serviceMonitor.enabled=true --set monitoring.alerts.enabled=true \
  --set monitoring.dashboard.enabled=true)" = "$all" ] || fail "monitoring.*.enabled=true must install it whatever the cluster has"
[ -z "$(monitoring "${prometheus[@]}" --set monitoring.serviceMonitor.enabled=false --set monitoring.alerts.enabled=false \
  --set monitoring.dashboard.enabled=false)" ] || fail "monitoring.*.enabled=false must leave it out"
[ "$(monitoring "${prometheus[@]}" --set monitoring.alerts.enabled=false)" = $'ConfigMap\nServiceMonitor' ] ||
  fail "each part of the monitoring has its own switch"
[ -z "$(monitoring "${prometheus[@]}" --set metrics.enabled=false)" ] || fail "without metrics there is nothing to monitor"
! helm template talos-operator "$chart" --set monitoring.alerts.enabled=sometimes >/dev/null 2>&1 ||
  fail "monitoring.alerts.enabled takes auto, true or false"
! helm template talos-operator "$chart" --set monitoring.alerts.enabled=true --set metrics.enabled=false >/dev/null 2>&1 ||
  fail "monitoring that is asked for without metrics must be refused"

monitored=$(helm template talos-operator "$chart" --namespace talos-operator "${prometheus[@]}" --set monitoring.labels.release=prometheus)

# The rules that are installed are the file that `make alerts-test` tests.
diff <(yq -o json -I0 'sort_keys(..)' "$chart/files/alerts.yaml") \
  <(yq -o json -I0 'select(.kind == "PrometheusRule") | .spec | sort_keys(..)' <<<"$monitored") >/dev/null ||
  fail "the PrometheusRule does not carry files/alerts.yaml"
[ "$(yq -N 'select(.kind == "ServiceMonitor" or .kind == "PrometheusRule") | .metadata.labels.release' <<<"$monitored")" = $'prometheus\nprometheus' ] ||
  fail "monitoring.labels must reach the ServiceMonitor and the PrometheusRule"
[ "$(yq -N 'select(.kind == "ServiceMonitor") | .spec.endpoints[0] | .port + "," + .scheme' <<<"$monitored")" = "metrics,https" ] ||
  fail "the ServiceMonitor must scrape the metrics port over HTTPS"
# The ServiceMonitor finds the Service by the labels the Service has.
[ "$(yq -o json -I0 'select(.kind == "ServiceMonitor") | .spec.selector.matchLabels' <<<"$monitored")" = \
  "$(yq -o json -I0 'select(.kind == "Service") | .spec.selector' <<<"$monitored")" ] ||
  fail "the ServiceMonitor does not select the metrics Service"
for dashboard in talos-operator talos-nodes; do
  yq -N 'select(.metadata.name == "talos-operator-dashboard") | .data["'"$dashboard"'.json"]' <<<"$monitored" |
    yq -p json -e '.panels | length > 0 and .uid == "'"$dashboard"'"' >/dev/null || fail "the dashboard $dashboard is not JSON with panels"
done
[ "$(yq -N 'select(.metadata.name == "talos-operator-dashboard") | .metadata.labels.grafana_dashboard' <<<"$monitored")" = "1" ] ||
  fail "the dashboard must carry the label Grafana's sidecar looks for"

echo "chart check: $want CRDs rendered, kept on uninstall, identical to $generated; permissions bound; nothing asked upstream by default; monitoring where there is a Prometheus"
