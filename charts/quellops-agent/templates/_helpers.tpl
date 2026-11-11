{{- define "quellops-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "quellops-agent.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "quellops-agent.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "quellops-agent.labels" -}}
app.kubernetes.io/name: {{ include "quellops-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "quellops-agent.secretName" -}}
{{- default (include "quellops-agent.fullname" .) .Values.credentials.existingSecret }}
{{- end }}

