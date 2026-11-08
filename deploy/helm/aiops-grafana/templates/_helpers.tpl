{{- define "grafana.labels" -}}
app.kubernetes.io/name: grafana
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
