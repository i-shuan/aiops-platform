{{- define "aiops.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "aiops.labels" -}}
app.kubernetes.io/name: aiops-platform
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "aiops.secretName" -}}
{{- if .Values.existingSecret -}}
{{ .Values.existingSecret }}
{{- else if .Values.devSecretEnv -}}
{{ include "aiops.fullname" . }}-dev-env
{{- else -}}
{{- fail "請設定 existingSecret（正式）或 devSecretEnv（只限開發）；至少要有 AIOPS_API_TOKEN" -}}
{{- end -}}
{{- end -}}

{{/* API 與排程共用的環境變數與安全設定 */}}
{{- define "aiops.container" -}}
image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
imagePullPolicy: {{ .Values.image.pullPolicy }}
envFrom:
  - secretRef:
      name: {{ include "aiops.secretName" . }}
env:
  {{- range $k, $v := .Values.env }}
  - name: {{ $k }}
    value: {{ $v | quote }}
  {{- end }}
  {{- if and .Values.devPostgres.enabled (not (hasKey .Values.devSecretEnv "AIOPS_DATABASE_URL")) }}
  - name: AIOPS_DATABASE_URL
    value: "postgresql+psycopg://aiops:{{ .Values.devPostgres.password }}@{{ include "aiops.fullname" . }}-dev-postgres:5432/aiops"
  {{- end }}
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
{{- end -}}
