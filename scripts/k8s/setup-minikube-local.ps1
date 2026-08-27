$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ComposeNetwork = "pipelinecontrolplane_default"

Write-Host "==> 1. Kafka broker + Couchbase (docker-compose, ngoai cluster)"
Push-Location $RepoRoot
try {
    docker compose up -d
} finally {
    Pop-Location
}

Write-Host "Cho Couchbase Web UI san sang..."
$deadline = (Get-Date).AddSeconds(120)
while ((Get-Date) -lt $deadline) {
    try {
        Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8091/ui/index.html" -TimeoutSec 3 | Out-Null
        break
    } catch {
        Start-Sleep -Seconds 3
    }
}

Write-Host "==> 2. Noi network Docker cua minikube voi network cua docker-compose (de Pod resolve duoc ten broker/couchbase)"
$connected = docker network inspect $ComposeNetwork --format '{{range .Containers}}{{.Name}} {{end}}' 2>$null
if ($connected -notmatch "minikube") {
    docker network connect $ComposeNetwork minikube
} else {
    Write-Host "  (da noi tu truoc, bo qua)"
}

Write-Host "==> 3. Init Couchbase (cluster-init + bucket + primary index)"
docker exec couchbase couchbase-cli cluster-init `
    --cluster-username Administrator --cluster-password password `
    --cluster-ramsize 512 --cluster-index-ramsize 256 --services data,index,query
docker exec couchbase couchbase-cli bucket-create `
    -c localhost -u Administrator -p password `
    --bucket streamflow --bucket-type couchbase --bucket-ramsize 256
docker exec couchbase cbq -e "http://localhost:8093" -u Administrator -p password `
    -s "CREATE PRIMARY INDEX IF NOT EXISTS ON ``streamflow``;"

Write-Host "==> 4. Nap pipeline config vao Couchbase (doc tu scripts/couchbase/pipelines/*.json)"
$pipelineJson = Get-Content (Join-Path $RepoRoot "scripts\couchbase\pipelines\customer-orders-demo.json") -Raw
$docId = "pipeline::customer-orders-demo"
$upsert = "UPSERT INTO ``streamflow`` (KEY, VALUE) VALUES ('$docId', $pipelineJson);"
$upsert | docker exec -i couchbase cbq -e "http://localhost:8093" -u Administrator -p password

Write-Host "==> 5. Tao topic tren broker (ngoai cluster)"
foreach ($topic in @("customers", "orders", "customer-orders-joined", "customer-order-stats")) {
    docker exec broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 `
        --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1
}

Write-Host "==> 6. Build + load 2 image (pipeline runner + Activation API)"
Push-Location $RepoRoot
try {
    docker build -f runner/Dockerfile -t streamflow/pipeline-control-plane:1.0 .
    docker build -f api/Dockerfile -t streamflow/activation-api:latest .
    minikube image load streamflow/pipeline-control-plane:1.0
    minikube image load streamflow/activation-api:latest
} finally {
    Pop-Location
}

Write-Host "==> 7. Deploy Activation API vao cluster"
kubectl apply -f (Join-Path $RepoRoot "k8s\activation-api.yaml")
kubectl wait --for=condition=Ready pod -l app=streamflow-activation-api --timeout=60s

Write-Host ""
Write-Host "==> Xong. Tiep theo (2 terminal rieng):"
Write-Host ""
Write-Host '  # Terminal 1 - port-forward Activation API ra host:'
Write-Host '  kubectl port-forward svc/streamflow-activation-api 7100:80'
Write-Host ""
Write-Host '  # Terminal 2 - kich hoat pipeline (tu tao Deployment moi, khong can kubectl apply):'
Write-Host '  Invoke-RestMethod -Method Post -Uri "http://localhost:7100/pipelines/customer-orders-demo/activate"'
Write-Host '  kubectl get pods -l pipelineId=customer-orders-demo'
Write-Host ""
Write-Host '  # Bom du lieu + xem ket qua (docker exec thang vao container broker, khong con qua k8s):'
Write-Host '  Get-Content data\chap6\customers.keyed.txt | docker exec -i broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic customers --property parse.key=true --property key.separator=|'
Write-Host '  Get-Content data\chap6\orders.keyed.txt | docker exec -i broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders --property parse.key=true --property key.separator=|'
Write-Host '  docker exec broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic customer-orders-joined --from-beginning --timeout-ms 10000'
