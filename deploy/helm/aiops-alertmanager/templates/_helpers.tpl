{{- define "aiops-am.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "aiops-am.labels" -}}
app.kubernetes.io/name: aiops-alertmanager
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "aiops-am.selector" -}}
app.kubernetes.io/name: aiops-alertmanager
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
