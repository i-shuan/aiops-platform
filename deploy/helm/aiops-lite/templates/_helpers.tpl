{{/* 由 values 的常用設定組出 config.yaml 的基本內容；進階的選填設定由 extraConfig 合併進來（見 secret.yaml）。 */}}
{{- define "aiops-lite.baseConfig" -}}
grafana:
  url: {{ required "grafana.url 必填" .Values.grafana.url | quote }}
  token: {{ required "grafana.token 必填" .Values.grafana.token | quote }}
webhook:
  token: {{ required "webhook.token 必填" .Values.webhook.token | quote }}
  port: 8080
{{- if .Values.admin.token }}
admin:
  token: {{ .Values.admin.token | quote }}
{{- end }}
database:
  url: {{ default (printf "postgresql+psycopg://aiops:%s@%s-postgres:5432/aiops" .Values.postgres.password .Release.Name) .Values.database.url | quote }}
llm:
  url: {{ .Values.llm.url | quote }}
  api_key: {{ .Values.llm.apiKey | quote }}
  model: {{ .Values.llm.model | quote }}
  {{- if ne .Values.llm.temperature nil }}
  temperature: {{ .Values.llm.temperature }}
  {{- end }}
  retry_initial_seconds: {{ int .Values.llm.retryInitialSeconds }}
  retry_max_seconds: {{ int .Values.llm.retryMaxSeconds }}
es:
  datasource_uid: {{ .Values.es.datasourceUid | quote }}
  log_index: {{ .Values.es.logIndex | quote }}
  trace_index: {{ .Values.es.traceIndex | quote }}
  id_fields: {{ toJson .Values.es.idFields }}
  log_fields: {{ toJson .Values.es.logFields }}
  trace_fields: {{ toJson .Values.es.traceFields }}
tempo:
  datasource_uid: {{ .Values.tempo.datasourceUid | quote }}
timezone: {{ .Values.timezone | quote }}
log_level: {{ .Values.logLevel | quote }}
debounce_seconds: {{ int .Values.timing.debounceSeconds }}
followup_seconds: {{ int .Values.timing.followupSeconds }}
close_after_seconds: {{ int .Values.timing.closeAfterSeconds }}
edges:
{{- range .Values.edges }}
  - [{{ index . 0 | quote }}, {{ index . 1 | quote }}]
{{- end }}
{{- end -}}
