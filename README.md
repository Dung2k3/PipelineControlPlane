# PipelineControlPlane

Control plane cho hệ thống pipeline Kafka Streams cấu hình qua Couchbase — mỗi pipeline chạy độc lập
trong 1 pod Kubernetes riêng ("Model A: process-per-pipeline", xem chi tiết lý do chọn kiến trúc này ở
[`docs/plan.md`](docs/plan.md)).

Project này được tách ra làm project Gradle độc lập từ `Kafka-Streams-Labs/PipelineControlPlane` (repo
lab `Mastering Kafka Streams and ksqlDB`) để build/đóng gói theo từng runnable module riêng, không còn
phụ thuộc Gradle root của repo lab.

## Cấu trúc module

```
common/   engine dùng chung: đọc/validate/build topology từ PipelineConfig (config, node, validation,
          topology, expression, serdes) + đọc config pipeline từ Couchbase (controlplane.configstore,
          controlplane.couchbase). Chỉ là thư viện (java-library), không tự chạy được.
api/      HTTP API (com.streamflow.controlplane.api.ActivationApiApp) + logic gọi Kubernetes để
          tạo/restart/xoá Deployment (controlplane.k8s.PipelineDeploymentManager). Module duy nhất cần
          io.fabric8:kubernetes-client.
runner/   Entrypoint chạy 1 pipeline thật (com.streamflow.controlplane.Main +
          controlplane.runtime.KafkaStreamsRunner). Không cần kubernetes-client -> image nhẹ hơn.
```

`api` và `runner` đều `implementation project(':common')`. Không có dependency chiều nào giữa `api` và
`runner`.

## Yêu cầu

- Java 17
- Docker (nếu cần build image / chạy hạ tầng local)
- Couchbase + Kafka broker (local qua Docker, hoặc trong cluster — xem [`docs/plan.md`](docs/plan.md)
  mục "chuyển hẳn Kafka + Couchbase vào trong cluster")
- Kubernetes cluster (minikube khi chạy local) nếu muốn `api` thực sự tạo/xoá Deployment

## Build & test

```bash
./gradlew build          # compile + test ca 3 module
./gradlew :common:test :api:test :runner:test
```

## Chạy local (không cần Docker/k8s)

Yêu cầu Couchbase local đã có doc `pipeline::<id>` (xem mục "Seed pipeline config" bên dưới).

```bash
# Chạy 1 pipeline (block terminal, PIPELINE_ID bắt buộc, không có default)
PIPELINE_ID=customer-orders-demo-local ./gradlew :runner:Main

# Chạy Activation API (HTTP server, port mặc định 7100 - xem application.yaml trong common/)
./gradlew :api:ActivationApi
```

## Model B (demo): nhiều pipeline chung 1 JVM

`MultiPipelineMain` (`runner/src/main/java/com/streamflow/controlplane/MultiPipelineMain.java`) là bản
demo đối chiếu với Model A ("1 pod = 1 pipeline") — chạy nhiều `KafkaStreams` instance cùng lúc trong 1
JVM, mỗi pipeline 1 `application.id` riêng. **Chỉ để đo/so sánh resource, không dùng thay `Main.java`
trong production** — 1 JVM crash sẽ kéo sập cả N pipeline cùng lúc, khác Model A mỗi pipeline hỏng độc
lập (xem phần "So sánh resource" bên dưới).

```bash
# Local (không container), PIPELINE_IDS phân cách bằng dấu phay
PIPELINE_IDS=customer-orders-demo,customer-orders-demo1,customer-orders-demo2 ./gradlew :runner:MultiPipelineMain
```

Chạy trong container (dùng lại image `runner/Dockerfile` đã build, override entrypoint sang
`MultiPipelineMain` thay vì `Main`):

```bash
docker run -d --name multi-pipeline-demo \
  --network <network-cua-broker/couchbase> \
  -e COUCHBASE_CONNECTION_STRING=couchbase://couchbase \
  -e COUCHBASE_USERNAME=Administrator -e COUCHBASE_PASSWORD=password -e COUCHBASE_BUCKET=streamflow \
  -e PIPELINE_IDS="customer-orders-demo,customer-orders-demo1,...,customer-orders-demo9" \
  --entrypoint /opt/jre/bin/java \
  streamflow/pipeline-control-plane:1.1 \
  -cp "/app/lib/*" com.streamflow.controlplane.MultiPipelineMain
```

## Seed pipeline config vào Couchbase

```bash
scripts/couchbase/init-cluster.sh          # 1 lần/môi trường: cluster-init + bucket + primary index
scripts/couchbase/seed-pipeline.sh         # nạp scripts/couchbase/pipelines/customer-orders-demo.json
```

(Có bản `.ps1` tương ứng cho PowerShell.)

## Build Docker image

```bash
docker build -f runner/Dockerfile -t streamflow/pipeline-control-plane:1.0 .
docker build -f api/Dockerfile -t streamflow/activation-api:latest .
```

Build context phải là **root project này** (không phải thư mục `runner/`/`api/`) vì Dockerfile cần
`gradlew`/`settings.gradle`/`common/` để build cả cây multi-module.

## Deploy lên Kubernetes (minikube local)

```powershell
.\scripts\k8s\setup-minikube-local.ps1
```

Script tự làm toàn bộ: apply hạ tầng (`k8s/infra/*.yaml`), init Couchbase, tạo topic, build + load 2
image, deploy Activation API (`k8s/activation-api.yaml`).

Sau khi Activation API chạy, kích hoạt 1 pipeline (tự tạo Deployment mới, không cần `kubectl apply`
thủ công):

```bash
curl -X POST http://localhost:7100/pipelines/customer-orders-demo/activate
curl -X DELETE http://localhost:7100/pipelines/customer-orders-demo
```

Nếu Activation API chưa chạy được, có đường fallback thủ công render trực tiếp từ template:

```bash
scripts/k8s/render-pipeline-deployment.sh customer-orders-demo | kubectl apply -f -
```

## So sánh resource: Model A vs Model B

Đo với cùng 1 pipeline (`customer-orders-demo` × 10, có TABLE_JOIN + AGGREGATE cửa sổ, state store
RocksDB), 10 pipeline cho cả 2 phía.

**Đo bằng `docker stats` (không set memory limit, ngay sau khi cả 10 pipeline lên `RUNNING`) — so sánh
tương đối, cùng điều kiện đo cho cả 2 model:**

| | Model A: 10 container / 10 JVM | Model B: 1 container / 10 pipeline chung 1 JVM |
|---|---|---|
| Tổng Memory | ~2269 MiB | **338.9 MiB (~6.7× ít hơn)** |
| Tổng CPU | ~55.6% | **5.6% (~10× ít hơn)** |

**Đo bằng `kubectl top` trên pod thật (có `memory limit`, working-set memory) — chỉ có số của Model A,
vì Model B chưa được deploy lên k8s:**

| Trạng thái | Tổng CPU (10 pod) | Tổng Memory (10 pod) |
|---|---|---|
| Idle | ~293m | ~1864Mi |
| Sau khi bơm 10 customer + 30 order qua join/aggregate | ~262m | ~1891Mi (+~1.4%) |

Batch data nhỏ gần như không làm nhúc nhích resource — chi phí cố định của JVM (JIT, GC threads,
metaspace, Kafka client buffer, ~180Mi/pod) chiếm phần lớn footprint khi traffic thấp. Đây cũng là lý
do Model B tiết kiệm rõ ở quy mô nhỏ: nó gộp chung đúng phần chi phí cố định đó. Với traffic lớn hơn
nhiều, tỷ lệ tiết kiệm của Model B sẽ giảm dần vì lúc đó chi phí xử lý thật (CPU, RocksDB, heap cho
message) mới chiếm chủ đạo — chưa test ở quy mô đó.

**Đánh đổi của Model B** (xem chi tiết trong Javadoc của `MultiPipelineMain`):
- 1 JVM crash (OOM, lỗi ngoài `StreamThread`) = mất cả N pipeline cùng lúc.
- Không tách được resource limit/scale riêng cho từng pipeline (k8s resource quota/HPA hoạt động ở
  cấp pod, không cấp pipeline).
- Dùng `SHUTDOWN_CLIENT` thay vì `SHUTDOWN_APPLICATION` để 1 pipeline lỗi (uncaught exception trong
  `StreamThread`) không kéo sập JVM — nhưng lỗi ngoài phạm vi đó (OOM, bug tầng JVM) vẫn sập tất cả.

## Tài liệu thêm

- [`docs/plan.md`](docs/plan.md) — lịch sử thiết kế đầy đủ: vì sao chọn Model A, các giai đoạn triển
  khai, sự cố thật đã gặp lúc verify trên cluster (đọc trước khi đổi kiến trúc/hạ tầng).
