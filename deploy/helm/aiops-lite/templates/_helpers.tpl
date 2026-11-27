{{/* 由 values 的常用設定組出 config.yaml 的基本內容；進階的選填設定由 extraConfig 合併進來（見 secret.yaml）。 */}}
{{- define "aiops-lite.baseConfig" -}}
grafana:
  url: {{ required "grafana.url 必填" .Values.grafana.url | quote }}
  token: {{ required "grafana.token 必填" .Values.grafana.token | quote }}
webhook:
  token: {{ required "webhook.token 必填" .Values.webhook.token | quote }}
  port: 8080
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
logs:
  type: {{ .Values.logs.type | quote }}
  datasource_uid: {{ .Values.logs.datasourceUid | quote }}
  {{- if eq .Values.logs.type "elasticsearch" }}
  index: {{ default "*" .Values.logs.index | quote }}
  {{- else }}
  extra_filter: {{ .Values.logs.extraFilter | quote }}
  {{- end }}
  id_fields: {{ toJson .Values.logs.idFields }}
  fields: {{ toJson .Values.logs.fields }}
traces:
  type: {{ .Values.traces.type | quote }}
  datasource_uid: {{ .Values.traces.datasourceUid | quote }}
  service_graph_datasource_uid: {{ .Values.traces.serviceGraphDatasourceUid | quote }}
outputs:
  incident_api:
    url: {{ .Values.outputs.incidentApi.url | quote }}
    token: {{ .Values.outputs.incidentApi.token | quote }}
  remediation:
    url: {{ .Values.outputs.remediation.url | quote }}
    token: {{ .Values.outputs.remediation.token | quote }}
    enabled: {{ .Values.outputs.remediation.enabled }}
metrics:
  datasource_uid: {{ .Values.metrics.datasourceUid | quote }}
  queries: {{ toJson (default (list) .Values.metrics.queries) }}
timezone: {{ .Values.timezone | quote }}
log_level: {{ .Values.logLevel | quote }}
debounce_seconds: {{ int .Values.timing.debounceSeconds }}
followup_seconds: {{ int .Values.timing.followupSeconds }}
close_after_seconds: {{ int .Values.timing.closeAfterSeconds }}
{{- end -}}
