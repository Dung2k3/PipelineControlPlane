param(
    [Parameter(Mandatory = $true)][string]$PipelineId,
    [string]$NodeSelector = ""
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Template = Join-Path $ScriptDir "..\..\api\src\main\resources\k8s\pipeline-deployment.template.yaml"

function EnvOrDefault([string]$name, [string]$default) {
    $value = [Environment]::GetEnvironmentVariable($name)
    if ([string]::IsNullOrWhiteSpace($value)) { return $default }
    return $value
}

$Image = EnvOrDefault "IMAGE" "streamflow/pipeline-control-plane:latest"
$CouchbaseConnectionString = EnvOrDefault "COUCHBASE_CONNECTION_STRING" "couchbase://couchbase"
$CouchbaseUsername = EnvOrDefault "COUCHBASE_USERNAME" "Administrator"
$CouchbasePassword = EnvOrDefault "COUCHBASE_PASSWORD" "password"
$CouchbaseBucket = EnvOrDefault "COUCHBASE_BUCKET" "streamflow"

$content = Get-Content $Template -Raw
$content = $content.Replace('${PIPELINE_ID}', $PipelineId)
$content = $content.Replace('${IMAGE}', $Image)
$content = $content.Replace('${COUCHBASE_CONNECTION_STRING}', $CouchbaseConnectionString)
$content = $content.Replace('${COUCHBASE_USERNAME}', $CouchbaseUsername)
$content = $content.Replace('${COUCHBASE_PASSWORD}', $CouchbasePassword)
$content = $content.Replace('${COUCHBASE_BUCKET}', $CouchbaseBucket)

if ($NodeSelector) {
    $parts = $NodeSelector.Split('=', 2)
    $key = $parts[0]
    $value = $parts[1]
    $nodeSelectorBlock = "      nodeSelector:`n        ${key}: `"$value`""
    $content = $content -replace '(?m)^(\s*terminationGracePeriodSeconds:.*)$', "`$1`n$nodeSelectorBlock"
}

Write-Output $content
