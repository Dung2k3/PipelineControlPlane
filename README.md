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

## Tài liệu thêm

- [`docs/plan.md`](docs/plan.md) — lịch sử thiết kế đầy đủ: vì sao chọn Model A, các giai đoạn triển
  khai, sự cố thật đã gặp lúc verify trên cluster (đọc trước khi đổi kiến trúc/hạ tầng).
