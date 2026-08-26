# Khoi tao cluster Couchbase 1-node cho dev local: cluster-init + bucket "streamflow" +
# primary index (can cho N1QL UPSERT o seed-pipeline.ps1). Day la nguon config that cho
# PipelineControlPlane.CouchbaseConnection - xem docs/plan.md.
#
# Idempotent: bo qua tung buoc neu da co roi, an toan chay lai nhieu lan.
# Yeu cau: `docker compose up -d couchbase` da chay. Lan dau container co the mat 15-30s
# de web console san sang - script tu cho o buoc dau.
$ErrorActionPreference = "Stop"

$Container = "couchbase"
$HostUrl = "http://localhost:8091"
$AdminUser = "Administrator"
$AdminPass = "password"
$Bucket = "streamflow"

Write-Host "Cho Couchbase web console san sang tren $HostUrl..."
while ($true) {
    try {
        Invoke-WebRequest -Uri "$HostUrl/pools" -UseBasicParsing -TimeoutSec 3 | Out-Null
        break
    } catch {
        Start-Sleep -Seconds 2
    }
}

$cred = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${AdminUser}:${AdminPass}"))
$headers = @{ Authorization = "Basic $cred" }

$alreadyInit = $false
try {
    $pools = Invoke-RestMethod -Uri "$HostUrl/pools/default" -Headers $headers -TimeoutSec 5
    if ($pools.clusterName) { $alreadyInit = $true }
} catch {
    $alreadyInit = $false
}

if ($alreadyInit) {
    Write-Host "Cluster da duoc init truoc do - bo qua buoc cluster-init."
} else {
    Write-Host "Init cluster (admin=$AdminUser, services data+index+query)..."
    docker exec $Container couchbase-cli cluster-init `
        --cluster-username $AdminUser `
        --cluster-password $AdminPass `
        --cluster-ramsize 512 `
        --cluster-index-ramsize 256 `
        --services data,index,query
}

$bucketList = docker exec $Container couchbase-cli bucket-list -c localhost -u $AdminUser -p $AdminPass
if ($bucketList -match "^$Bucket$") {
    Write-Host "Bucket '$Bucket' da ton tai - bo qua."
} else {
    Write-Host "Tao bucket '$Bucket'..."
    docker exec $Container couchbase-cli bucket-create `
        -c localhost -u $AdminUser -p $AdminPass `
        --bucket $Bucket --bucket-type couchbase --bucket-ramsize 256
}

Write-Host "Tao primary index tren bucket '$Bucket'..."
docker exec $Container cbq -e "http://localhost:8093" -u $AdminUser -p $AdminPass `
    -s "CREATE PRIMARY INDEX IF NOT EXISTS ON ``$Bucket``;"

Write-Host ""
Write-Host "Xong. Web console: $HostUrl (user=$AdminUser, pass=$AdminPass)"
Write-Host "Bien env cho PipelineControlPlane (gia tri mac dinh cua CouchbaseConnection da khop, khong bat buoc set):"
Write-Host "  COUCHBASE_CONNECTION_STRING=couchbase://localhost"
Write-Host "  COUCHBASE_USERNAME=$AdminUser"
Write-Host "  COUCHBASE_PASSWORD=$AdminPass"
Write-Host "  COUCHBASE_BUCKET=$Bucket"
