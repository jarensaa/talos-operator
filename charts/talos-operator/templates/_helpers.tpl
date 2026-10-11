{{- define "talos-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "talos-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "talos-operator.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "talos-operator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "talos-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "talos-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "talos-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "talos-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Whether a part of the monitoring is installed. Its setting is true, false or auto, and auto means:
where the cluster has the API of the Prometheus Operator, which is how a chart can tell that
there is a Prometheus to read what it installs. "true" for yes, empty for no.
Takes a dict: ctx (the root context), name (the value's path, for the error) and setting.
*/}}
{{- define "talos-operator.monitoring" -}}
{{- $setting := toString .setting -}}
{{- if not (has $setting (list "auto" "true" "false")) -}}
{{- fail (printf "%s must be auto, true or false, and is %q" .name $setting) -}}
{{- end -}}
{{- if .ctx.Values.metrics.enabled -}}
{{- if eq $setting "true" -}}
true
{{- else if and (eq $setting "auto") (.ctx.Capabilities.APIVersions.Has "monitoring.coreos.com/v1/ServiceMonitor") -}}
true
{{- end -}}
{{- else if eq $setting "true" -}}
{{- fail (printf "%s needs metrics.enabled" .name) -}}
{{- end -}}
{{- end }}
