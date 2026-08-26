# Dung toan bo he thong THAT trong 1 minikube cluster (Kafka broker + Couchbase + Activation API +
# pod pipeline) - xem docs/plan.md muc "Chuyen han vao trong cluster". Thay the
# ban truoc day dung docker-compose lam ha tang + host.minikube.internal - cach do bi loi that
# (Kafka tra ve advertised.listeners "localhost:9092" cho client trong pod, khong sua duoc ma khong
# doi config chia se voi cac script/tool khac).
#
# Idempotent o muc chap nhan duoc (kubectl apply, --if-not-exists) - KHONG idempotent hoan toan cho
# buoc init Couchbase (cluster-init/bucket-create se loi "da ton tai" neu goi lai tren cluster da
# init roi - bo qua loi do bang tay neu gap, khong anh huong ket qua).
#
# Dung: .\scripts\k8s\setup-minikube-local.ps1
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$InfraDir = Join-Path $RepoRoot "k8s\infra"

function Invoke-KubectlExec([string]$PodSelector, [string[]]$Cmd) {
    $pod = (kubectl get pod -l $PodSelector -o jsonpath='{.items[0].metadata.name}')
    kubectl exec $pod -- @Cmd
}

Write-Host "==> 1. Kafka broker + Couchbase (k8s Deployment/Service that trong cluster)"
kubectl apply -f (Join-Path $InfraDir "kafka-broker.yaml")
kubectl apply -f (Join-Path $InfraDir "couchbase.yaml")
kubectl wait --for=condition=Ready pod -l app=broker --timeout=120s
Write-Host "Cho Couchbase pull image (co the mat vai phut lan dau)..."
kubectl wait --for=condition=Ready pod -l app=couchbase --timeout=400s

Write-Host "==> 2. Init Couchbase (cluster-init + bucket + primary index)"
$cbPod = (kubectl get pod -l app=couchbase -o jsonpath='{.items[0].metadata.name}')
kubectl exec $cbPod -- couchbase-cli cluster-init `
    --cluster-username Administrator --cluster-password password `
    --cluster-ramsize 512 --cluster-index-ramsize 256 --services data,index,query
kubectl exec $cbPod -- couchbase-cli bucket-create `
    -c localhost -u Administrator -p password `
    --bucket streamflow --bucket-type couchbase --bucket-ramsize 256
kubectl exec $cbPod -- cbq -e "http://localhost:8093" -u Administrator -p password `
    -s "CREATE PRIMARY INDEX IF NOT EXISTS ON ``streamflow``;"

Write-Host "==> 3. Nap pipeline config vao Couchbase (doc tu scripts/couchbase/pipelines/*.json)"
$pipelineJson = Get-Content (Join-Path $RepoRoot "scripts\couchbase\pipelines\customer-orders-demo.json") -Raw
$docId = "pipeline::customer-orders-demo"
$upsert = "UPSERT INTO ``streamflow`` (KEY, VALUE) VALUES ('$docId', $pipelineJson);"
$upsert | kubectl exec -i $cbPod -- cbq -e "http://localhost:8093" -u Administrator -p password

Write-Host "==> 4. Tao topic tren broker trong cluster"
$brokerPod = (kubectl get pod -l app=broker -o jsonpath='{.items[0].metadata.name}')
foreach ($topic in @("customers", "orders", "customer-orders-joined", "customer-order-stats")) {
    kubectl exec $brokerPod -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 `
        --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1
}

Write-Host "==> 5. Build + load 2 image (pipeline runner + Activation API)"
Push-Location $RepoRoot
try {
    docker build -f runner/Dockerfile -t streamflow/pipeline-control-plane:1.0 .
    docker build -f api/Dockerfile -t streamflow/activation-api:latest .
    minikube image load streamflow/pipeline-control-plane:1.0
    minikube image load streamflow/activation-api:latest
} finally {
    Pop-Location
}

Write-Host "==> 6. Deploy Activation API vao cluster"
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
Write-Host '  # Bom du lieu + xem ket qua (kubectl exec vao pod broker, khong con docker exec):'
Write-Host '  $brokerPod = kubectl get pod -l app=broker -o jsonpath="{.items[0].metadata.name}"'
Write-Host '  Get-Content data\chap6\customers.keyed.txt | kubectl exec -i $brokerPod -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker:9092 --topic customers --property parse.key=true --property key.separator=|'
Write-Host '  Get-Content data\chap6\orders.keyed.txt | kubectl exec -i $brokerPod -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker:9092 --topic orders --property parse.key=true --property key.separator=|'
Write-Host '  kubectl exec $brokerPod -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker:9092 --topic customer-orders-joined --from-beginning --timeout-ms 10000'
