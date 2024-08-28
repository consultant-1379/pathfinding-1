{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "eric-oss-eniq-feature-installer.name" }}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart version as used by the chart label.
*/}}
{{- define "eric-oss-eniq-feature-installer.version" }}
{{- printf "%s" .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "eric-oss-eniq-feature-installer.fullname" }}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- $name | trunc 63 | trimSuffix "-" }}
{{/* Ericsson mandates the name defined in metadata should start with chart name. */}}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "eric-oss-eniq-feature-installer.chart" }}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{/*
Create image repo path
*/}}
{{- define "eric-oss-eniq-feature-installer.repoPath" }}
{{- if .Values.imageCredentials.repoPath }}
{{- print .Values.imageCredentials.repoPath "/" }}
{{- end }}
{{- end }}

{{/*
Create image registry url
*/}}
{{- define "eric-oss-eniq-feature-installer.registryUrl" }}
    {{- $registryURL := "armdocker.rnd.ericsson.se" }}
    {{-  if .Values.global }}
        {{- if .Values.global.registry }}
            {{- if .Values.global.registry.url }}
                {{- $registryURL = .Values.global.registry.url }}
            {{- end }}
        {{- end }}
    {{- end }}
    {{- if .Values.imageCredentials.registry }}
        {{- if .Values.imageCredentials.registry.url }}
            {{- $registryURL = .Values.imageCredentials.registry.url }}
        {{- end }}
    {{- end }}
    {{- print $registryURL }}
{{- end -}}

{{/*
Create image pull secrets
*/}}
{{- define "eric-oss-eniq-feature-installer.pullSecrets" }}
{{- $pullSecret := "" }}
{{- if .Values.global }}
    {{- if .Values.global.pullSecret }}
        {{- $pullSecret = .Values.global.pullSecret }}
    {{- end }}
{{- end }}
{{- if .Values.imageCredentials }}
    {{- if .Values.imageCredentials.pullSecret }}
        {{- $pullSecret = .Values.imageCredentials.pullSecret }}
    {{- end }}
{{- end }}
{{- print $pullSecret }}
{{- end }}
{{/*
Timezone variable
*/}}
{{- define "eric-oss-eniq-feature-installer.timezone" }}
{{- $timezone := "UTC" }}
{{- if .Values.global }}
    {{- if .Values.global.timezone }}
        {{- $timezone = .Values.global.timezone }}
    {{- end }}
{{- end }}
{{- print $timezone | quote }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "eric-oss-eniq-feature-installer.labels" }}
app.kubernetes.io/name: {{ include "eric-oss-eniq-feature-installer.name" . }}
helm.sh/chart: {{ include "eric-oss-eniq-feature-installer.chart" . }}
{{ include "eric-oss-eniq-feature-installer.selectorLabels" . }}
app.kubernetes.io/version: {{ include "eric-oss-eniq-feature-installer.version" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "eric-oss-eniq-feature-installer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "eric-oss-eniq-feature-installer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "eric-oss-eniq-feature-installer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "eric-oss-eniq-feature-installer.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
TODO: Please change this product number to a valid one, once it is available.
*/}}
{{- define "eric-oss-eniq-feature-installer.product-info" }}
ericsson.com/product-name: "Microservice Chassis"
ericsson.com/product-number: "CXC90001"
ericsson.com/product-revision: {{regexReplaceAll "(.*)[+|-].*" .Chart.Version "${1}" | quote }}
{{- end }}

{{/*
Return the fsgroup set via global parameter if it's set, otherwise 10000
*/}}
{{- define "eric-oss-eniq-feature-installer.fsGroup.coordinated" -}}
  {{- if .Values.global -}}
    {{- if .Values.global.fsGroup -}}
      {{- if .Values.global.fsGroup.manual -}}
        {{ .Values.global.fsGroup.manual }}
      {{- else -}}
        {{- if eq .Values.global.fsGroup.namespace true -}}
          # The 'default' defined in the Security Policy will be used.
        {{- else -}}
          10000
      {{- end -}}
    {{- end -}}
  {{- else -}}
    10000
  {{- end -}}
  {{- else -}}
    10000
  {{- end -}}
{{- end -}}


{{/*
Create a user defined annotation
*/}}
{{- define "eric-oss-eniq-feature-installer.config-annotations" }}
  {{- if .Values.annotations -}}
    {{- range $name, $config := .Values.annotations }}
      {{ $name }}: {{ tpl $config $ }}
    {{- end }}
  {{- end }}
{{- end}}


{{/*
Define the role reference for security policy
*/}}
{{- define "eric-oss-eniq-feature-installer.securityPolicy.reference" -}}
  {{- if .Values.global -}}
    {{- if .Values.global.security -}}
      {{- if .Values.global.security.policyReferenceMap -}}
        {{ $mapped := index .Values "global" "security" "policyReferenceMap" "default-restricted-security-policy" }}
        {{- if $mapped -}}
          {{ $mapped }}
        {{- else -}}
          default-restricted-security-policy
        {{- end -}}
      {{- else -}}
        default-restricted-security-policy
      {{- end -}}
    {{- else -}}
      default-restricted-security-policy
    {{- end -}}
  {{- else -}}
    default-restricted-security-policy
  {{- end -}}
{{- end -}}


{{/*
Define the annotations for security policy
*/}}
{{- define "eric-oss-eniq-feature-installer.securityPolicy.annotations" -}}
# Automatically generated annotations for documentation purposes.
{{- end -}}

Define Pod Disruption Budget value taking into account its type (int or string)
*/}}
{{- define "eric-oss-eniq-feature-installer.pod-disruption-budget" -}}
  {{- if kindIs "string" .Values.podDisruptionBudget.minAvailable -}}
    {{- print .Values.podDisruptionBudget.minAvailable | quote -}}
  {{- else -}}
    {{- print .Values.podDisruptionBudget.minAvailable | atoi -}}
  {{- end -}}
{{- end -}}
