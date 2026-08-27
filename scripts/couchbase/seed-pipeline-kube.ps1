# Nap 1 pipeline config vao Couchbase (doc id "pipeline::<pipelineId>") de test
# CouchbasePipelineConfigStore (PipelineControlPlane) that su, khong chi qua unit test.
# Doc noi dung tu scripts/couchbase/pipelines/<pipelineId>.json - sua file JSON do de doi
# noi dung nap, khong sua script nay.
#
# Doi pipeline muon nap qua -PipelineId (mac dinh: customer-orders-demo).
# Yeu cau: scripts/couchbase/init-cluster.ps1 da chay xong (co bucket "streamflow" +
# primary index).
param(
    [string]$PipelineId = "customer-orders-demo"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Container = "couchbase"
$AdminUser = "Administrator"
$AdminPass = "password"
$Bucket = "streamflow"
$SourceJson = Join-Path $ScriptDir "pipelines\$PipelineId.json"

if (-not (Test-Path $SourceJson)) {
    Write-Error "Khong tim thay file: $SourceJson"
    Write-Host "Cac pipeline JSON co san:"
    Get-ChildItem (Join-Path $ScriptDir "pipelines\*.json") | ForEach-Object { $_.Name }
    exit 1
}

$DocId = "pipeline::$PipelineId"
$JsonContent = Get-Content $SourceJson -Raw
$CbPod = kubectl get pod -l app=couchbase -o jsonpath='{.items[0].metadata.name}'

Write-Host "Nap '$SourceJson' vao doc '$DocId' (bucket '$Bucket')..."
$UpsertQuery = "UPSERT INTO ``$Bucket`` (KEY, VALUE) VALUES ('$DocId', $JsonContent);"
$UpsertQuery | kubectl exec -i $CbPod -- cbq -e "http://localhost:8093" -u Administrator -p password

Write-Host "Xong. Doc trong Couchbase:"
kubectl exec $CbPod -- cbq -e "http://localhost:8093" -u Administrator -p password -s "SELECT * FROM ``streamflow`` USE KEYS '$DocId';"
