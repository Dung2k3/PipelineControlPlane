# PipelineControlPlane — kế hoạch triển khai

> **Ghi chú tách project**: file này được copy nguyên trạng từ
> `Kafka-Streams-Labs/docs/PipelineControlPlane/plan.md` tại thời điểm tách `PipelineControlPlane` ra
> thành project Gradle độc lập này (repo `Kafka-Streams-Labs` gốc vẫn còn giữ 1 bản, chưa xoá — xem
> repo đó nếu cần đối chiếu lịch sử commit trước ngày tách). Toàn bộ nội dung bên dưới mô tả quá trình
> ra quyết định **trước khi tách**, nên nhiều đường dẫn/lệnh vẫn viết theo cấu trúc monorepo cũ — quy
> đổi sang cấu trúc mới như sau khi đọc:
> - `PipelineControlPlane/src/main/java/com/streamflow/{config,node,validation,topology,expression,
>   serdes,cache,jdbc}` và `controlplane/{config,configstore,couchbase}` → nay ở module `common/`.
> - `controlplane/{api,k8s}` và `PipelineControlPlane/src/main/resources/k8s/
>   pipeline-deployment.template.yaml` → nay ở module `api/`.
> - `controlplane/{Main.java,runtime}` → nay ở module `runner/`.
> - `./gradlew :PipelineControlPlane:Main` → `./gradlew :runner:Main`; `:PipelineControlPlane:
>   ActivationApi` → `:api:ActivationApi`; `:PipelineControlPlane:test` → `:common:test :api:test
>   :runner:test`; `:PipelineControlPlane:installDist` → `:api:installDist` / `:runner:installDist`.
> - `PipelineControlPlane/Dockerfile` → `runner/Dockerfile`; `PipelineControlPlane/
>   Dockerfile.activation-api` → `api/Dockerfile` (nay có launcher script riêng `./bin/api`, không
>   còn cần chạy `java -cp './lib/*' ...` thủ công như mô tả ở Giai đoạn 2 bên dưới).
> - `PipelineControlPlane/k8s/...` → `k8s/...` (ở root project này).
>
> Lý do tách: đóng gói/deploy độc lập theo từng runnable module (`api`, `runner`), giảm kích thước
> image (`runner` không còn cõng theo `fabric8 kubernetes-client` mà chỉ `api` cần).

## Bối cảnh

`ValidateAndBuildInnerJoinOperation` là demo có scope giới hạn (xem `docs/ValidateAndBuildInnerJoinOperation/
implementation-plan.md` mục 5) — chủ động loại trừ Couchbase thật, multi-instance, và hot-reload trong-JVM.
`PipelineControlPlane` là module **mới**, cho năng lực multi-tenant thật trong production: nhiều pipeline
lấy config từ Couchbase, mỗi pipeline chạy độc lập, deploy/reload qua Kubernetes.

**Cập nhật (quyết định cuối cùng — 2 module độc lập hoàn toàn, không có dependency chiều nào)**: có 2
lần đổi hướng trước khi chốt ở đây, ghi lại để hiểu vì sao code trông "trùng lặp":
1. Ban đầu `PipelineControlPlane` chỉ *tái dùng* code của `ValidateAndBuildInnerJoinOperation` qua
   project dependency (`implementation project(':ValidateAndBuildInnerJoinOperation')`).
2. Sau đó đổi sang **move hẳn** (`git mv`) phần engine dùng chung sang `PipelineControlPlane`, đảo
   ngược dependency (module demo mới là bên phụ thuộc).
3. **Chốt cuối cùng**: giữ `ValidateAndBuildInnerJoinOperation` **độc lập hoàn toàn** — copy (không
   phải move) toàn bộ engine dùng chung trở lại, bỏ hết dependency giữa 2 module theo cả 2 chiều. Lý
   do: người dùng muốn project demo cũ không phụ thuộc gì vào module mới, chấp nhận đánh đổi trùng code
   để đổi lấy sự tách biệt tuyệt đối.

Kết quả: `config/`, `node/` (toàn bộ node kind), `validation/`, `topology/`, `expression/`, `serdes/`,
`cache/RedisConnectionPool`, `jdbc/JdbcConnectionPool` — cùng toàn bộ test tương ứng — tồn tại **y hệt ở
cả 2 module**, mỗi bên 1 bản riêng, không import lẫn nhau. Sửa logic ở 1 trong 2 (vd thêm node kind mới,
sửa bug `JoinNodeBuilder`) **không tự động phản ánh** sang bên còn lại — phải tự đồng bộ tay nếu muốn cả
2 cùng cập nhật. `PipelineControlPlane` phần ở lại/thêm mới, không có trong bản demo:
`controlplane.couchbase.CouchbaseConnection`, `controlplane.configstore.CouchbasePipelineConfigStore`,
`controlplane.Main`, `controlplane.runtime.KafkaStreamsRunner` (Giai đoạn 1).

`PipelineConfigLoadException` mỗi module giữ bản riêng trong đúng package configstore của mình
(`com.streamflow.configstore.PipelineConfigLoadException` ở `ValidateAndBuildInnerJoinOperation` cho
`InMemoryPipelineConfigStore`; `com.streamflow.controlplane.configstore.PipelineConfigLoadException` ở
`PipelineControlPlane` cho `CouchbasePipelineConfigStore`) — tránh split package (2 jar cùng khai báo
`com.streamflow.configstore` nếu để chung tên).

`PipelineConfig` có 2 field cộng thêm, không phá tương thích ngược: `version` (long, mặc định 0) và
`status` (`PipelineStatus`, mặc định `ACTIVE`) — doc JSON cũ không có 2 field này vẫn parse đúng như
trước.

## Quyết định kiến trúc: Model A — process-per-pipeline

Đã cân nhắc so với "fleet + assigner" (nhiều pipeline dùng chung 1 JVM, lease CAS trong Couchbase) và
chọn **process-per-pipeline**: mỗi pipeline = 1 pod Kubernetes riêng, "instance" = **cả cluster** (không
phải 1 JVM). Việc gán pipeline nào chạy ở đâu là **do người vận hành tự cấu hình** (áp đúng Deployment
YAML với `pipelineId` tương ứng, tự chọn `nodeSelector` nếu cần) — không cần tự viết assigner/lease.

Lý do chọn (chi tiết xem lịch sử trao đổi kiến trúc — tóm tắt):

- **Time**: tái dùng gần như nguyên vẹn code hiện có, chỉ thêm store + control plane mỏng.
- **Resource/cô lập lỗi**: k8s cho cô lập theo pod miễn phí (1 pipeline crash không ảnh hưởng pipeline
  khác) — đúng lỗ hổng thấy trong `errorlog.txt` (1 lỗi SpEL/convert kiểu kéo sập cả 1 `StreamThread`,
  và nếu nhiều pipeline dùng chung JVM như hệ thống gốc suy ra từ log thì rủi ro lan rộng hơn nhiều).
- **Cost**: chỉ đáng lo khi số lượng pipeline rất lớn (JVM overhead nhân theo N) — chưa phải vấn đề ở
  quy mô hiện tại, không đáng đánh đổi lấy độ phức tạp của lease/assigner ngay từ đầu (YAGNI, đúng tinh
  thần đã ghi trong `docs/ValidateAndBuildInnerJoinOperation/architecture.md` mục 3).

## Các giai đoạn

### Giai đoạn 0 — Couchbase-backed config store + move engine dùng chung (đã triển khai ở commit này)

- `com.streamflow.controlplane.couchbase.CouchbaseConnection` — singleton `Cluster`/`Bucket`, cấu hình
  qua env (`COUCHBASE_CONNECTION_STRING`, `COUCHBASE_USERNAME`, `COUCHBASE_PASSWORD`, `COUCHBASE_BUCKET`,
  `COUCHBASE_SCOPE`, `COUCHBASE_COLLECTION`) — cùng pattern với `RedisConnectionPool` đã có (nay cũng ở
  module này, xem dưới).
- `com.streamflow.controlplane.configstore.CouchbasePipelineConfigStore` — đọc doc `pipeline::<id>`,
  parse thành `PipelineConfig` bằng `ObjectMapper` riêng (`JavaTimeModule` + `FAIL_ON_UNKNOWN_PROPERTIES
  = false`, chấp nhận field vận hành phát sinh sau này như `createdAt`/`updatedBy` mà không làm load
  thất bại). Không tìm thấy doc / parse lỗi → ném `com.streamflow.controlplane.configstore.
  PipelineConfigLoadException` (bản riêng của module này — xem lý do không move ở mục Bối cảnh).
- Test: `CouchbasePipelineConfigStoreTest` — test phần parse thuần (JSON → `PipelineConfig`, mặc định
  `version`/`status` khi doc cũ chưa có field, bỏ qua field lạ, lỗi JSON hỏng có `pipelineId` trong
  message). **Không** test phần I/O thật tới Couchbase (cần cluster thật) — cùng quy ước với các lab
  `chapN` cần Kafka broker thật, không có unit test cho phần kết nối, chỉ verify thủ công.
- **Copy** (giữ nguyên package `com.streamflow.*`, xuất hiện ở cả 2 module) `config/`, `node/` (toàn bộ
  11 `NodeType`), `validation/`, `topology/`, `expression/`, `serdes/`, `cache/RedisConnectionPool.java`,
  `jdbc/JdbcConnectionPool.java` — cùng toàn bộ test tương ứng (`node/**/*Test.java`,
  `expression/SpelEvaluatorTest.java`) — sang `PipelineControlPlane`, giữ nguyên bản gốc ở
  `ValidateAndBuildInnerJoinOperation` (xem mục Bối cảnh về quyết định cuối "2 module độc lập"). Mỗi
  module tự khai đủ dependency cần cho bản của mình trong `build.gradle` (kafka-streams/kafka-clients,
  jakarta.validation + hibernate-validator + jakarta.el, spring-expression, HikariCP + postgresql,
  jedis) — không có `implementation project(...)` giữa 2 module theo chiều nào.
  `./gradlew :PipelineControlPlane:test :ValidateAndBuildInnerJoinOperation:test` xanh toàn bộ (mỗi
  module ~74-76 test, chạy độc lập được với nhau).

### Giai đoạn 1 — Deploy: 1 pipeline = 1 Deployment (đã triển khai)

- `com.streamflow.controlplane.Main` — entrypoint mới của `PipelineControlPlane` (song song với
  `com.streamflow.Main` bên demo, không phải cùng 1 file): đọc `PIPELINE_ID` bắt buộc từ env (fail-fast
  nếu thiếu — 1 pod luôn ứng với đúng 1 pipeline), load qua `CouchbasePipelineConfigStore`, override
  `bootstrapServers` nếu có env `BOOTSTRAP_SERVERS`, validate, build, chạy qua
  `com.streamflow.controlplane.runtime.KafkaStreamsRunner` (copy hành vi của bản demo — shutdown hook +
  block main thread; giữ nguyên `SHUTDOWN_APPLICATION` vì ở Model A 1 process = 1 pipeline, lỗi chỉ giết
  đúng pod đó, k8s tự restart).
- `PipelineControlPlane/build.gradle`: thêm plugin `application` (`mainClass =
  'com.streamflow.controlplane.Main'`) + task `Main` (JavaExec), mirror đúng cách
  `ValidateAndBuildInnerJoinOperation/build.gradle` đã làm.
- `PipelineControlPlane/Dockerfile` — mirror `ValidateAndBuildInnerJoinOperation/Dockerfile` (multi-stage,
  `installDist`). 1 image dùng chung cho mọi pipeline — `PIPELINE_ID` không bake sẵn trong image, truyền
  vào lúc deploy qua Deployment; các biến `COUCHBASE_*`/`BOOTSTRAP_SERVERS` có default trỏ vào tên service
  trong `docker-compose.yml` (`couchbase`, `broker`), ghi đè được qua env khi deploy k8s thật.
- `PipelineControlPlane/k8s/pipeline-deployment.template.yaml` — template Deployment tham số hoá bằng
  placeholder `${...}`: `nodeSelector` optional (chèn được qua script, đáp ứng yêu cầu chọn worker thủ
  công), `resources.requests/limits`, `terminationGracePeriodSeconds: 60` (đủ để KafkaStreams đóng sạch),
  `strategy.rollingUpdate.maxUnavailable: 0` (khớp Giai đoạn 3 — pod mới phải healthy rồi mới thay pod
  cũ). Label `app=streamflow-pipeline,pipelineId=<id>` để lọc log/metrics theo từng pipeline.
- `scripts/k8s/render-pipeline-deployment.sh` + `.ps1` — render template thành YAML thật (thay
  placeholder, chèn `nodeSelector` nếu truyền tham số thứ 2 dạng `key=value`) rồi in ra stdout để pipe
  thẳng vào `kubectl apply -f -` hoặc lưu file trước khi apply. Người vận hành tự chạy — chưa có operator
  tự động tạo/xoá Deployment theo Couchbase ở giai đoạn này, đúng như đã chốt.
- Verify: `./gradlew :PipelineControlPlane:installDist` chạy sạch (launcher script sinh đúng); render 2
  script với/không `nodeSelector`, `kubectl` (client v1.36.1, không có cluster sống trong sandbox làm
  việc) parse được YAML tới bước cần gọi API cluster — chưa validate được full schema server-side, cần
  người vận hành tự kiểm lại khi có cluster thật (`kubectl apply --dry-run=server`).

### Mô hình vận hành — cái gì làm 1 lần vs. cái gì là "chạy pipeline" thật sự

Dễ nhầm 3 việc này với nhau:

1. **Setup hạ tầng — 1 lần/môi trường**, không lặp lại theo từng pipeline hay mỗi lần chạy:
   `docker compose up -d broker couchbase`, `scripts/couchbase/init-cluster.sh`, tạo topic.
2. **Định nghĩa pipeline — 1 lần/pipeline, hoặc mỗi khi sửa config**: ghi doc vào Couchbase
   (`scripts/couchbase/seed-pipeline.sh` chỉ để có sẵn dữ liệu test cho bước 3 dưới, không phải bước bắt
   buộc mỗi lần chạy — config cứ nằm trong Couchbase, không cần nạp lại trừ khi đổi nội dung).
3. **Chạy pipeline — việc lặp lại thật sự**:
   - Local dev (nhanh, không cần build image/k8s): `PIPELINE_ID=<id> ./gradlew
     :PipelineControlPlane:Main`.
   - Production (Model A đã chốt): `scripts/k8s/render-pipeline-deployment.sh <id> | kubectl apply -f -`
     → tạo 1 Deployment **sống liên tục**, k8s tự restart khi crash. Không phải "chạy lại mỗi lần cần
     pipeline hoạt động" — chỉ động vào khi deploy pipeline mới, đổi config (cần rollout lại), hoặc xoá
     hẳn. Giai đoạn 2 (Activation API) sẽ gộp "đổi config + trigger rollout" thành 1 lệnh `curl`, không
     cần tự tay render/`kubectl apply` nữa.

### Chạy thử local (walkthrough tham khảo, không phải quy trình production)

```bash
docker compose up -d broker couchbase
bash scripts/couchbase/init-cluster.sh
bash scripts/couchbase/seed-pipeline.sh              # nạp pipeline "customer-orders-demo"

bash scripts/chap6/create-topics.sh                  # customers, orders, customer-orders-joined
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic customer-order-stats --partitions 1 --replication-factor 1

PIPELINE_ID=customer-orders-demo ./gradlew :PipelineControlPlane:Main   # block terminal, để nguyên

# terminal khac:
bash scripts/chap6/produce-sample-data.sh             # data chap6 dung lai duoc (dung field customerId/amount)
bash scripts/chap6/consume-output.sh                  # xem topic customer-orders-joined
```

### Giai đoạn 2 — Activation API (control plane mỏng, đã triển khai)

- `com.streamflow.controlplane.api.ActivationApiApp` — HTTP service nhỏ, dùng lại đúng pattern
  `HttpServer` đã có trong `CacheApiApp` (không thêm framework). `POST /pipelines/{id}/activate`:
  1. `CouchbasePipelineConfigStore.load()` — không thấy doc → 404.
  2. `PipelineValidator.validate()` — sai → 400 kèm `pipelineId`/`nodeId`/`field` (đúng shape
     `ValidationException`).
  3. `PipelineTopologyBuilder.build()` **dry-run** — chỉ build `Topology` để xác nhận graph hợp lệ,
     không `start()` — pod thật sự (Giai đoạn 1) vẫn là bên duy nhất chạy pipeline, tự đọc lại config
     ở lần restart tiếp theo. Lỗi build → 400.
  4. `CouchbasePipelineConfigStore.bumpVersion()` — tăng `version` bằng N1QL `UPDATE ... USE KEYS
     $docId SET version = IFMISSINGORNULL(version, 0) + 1 RETURNING version` (atomic phía server,
     không cần tự viết vòng lặp retry CAS-mismatch — khác cách tiếp cận KV get+replace ban đầu cân
     nhắc). Cần `CouchbaseConnection` có thêm `cluster()`/`bucketName()` accessor (Giai đoạn 0 chỉ
     expose `Collection`, chưa đủ cho N1QL).
  5. `PipelineDeploymentManager.ensureDeployed(pipelineId, config)` — dùng
     `io.fabric8:kubernetes-client`. Deployment **đã tồn tại** → patch annotation
     `kubectl.kubernetes.io/restartedAt` vào pod template (đúng cơ chế `kubectl rollout restart` thật,
     k8s không có API "restart" riêng). Deployment **chưa tồn tại** → **tự tạo mới** (xem mục "Cập nhật
     — tự động hoá tạo Deployment" bên dưới). Lỗi bước này **không làm hỏng cả request** — response
     vẫn 200 kèm `k8sDeployed:false` + `k8sError`, vì config đã hợp lệ và version đã bump thành công
     trong Couchbase, chỉ riêng việc tạo/reload k8s thất bại.
- `KubernetesClient` build 1 lần lúc `main()` khởi động — nếu build lỗi (vd không có kubeconfig/không
  chạy trong cluster), log cảnh báo và set `null`, API vẫn phục vụ được validate+bump version, chỉ tắt
  phần trigger k8s (không crash cả service).
- Test: `PipelineDeploymentManagerTest` — dùng `@EnableKubernetesMockClient(crud = true)`
  (`io.fabric8:kubernetes-server-mock`, mock server giả HTTP API k8s, không cần cluster/minikube sống)
  để test thật 3 case: tạo Deployment mới đúng `nodeSelector`/env khi chưa tồn tại, tạo không kèm
  `nodeSelector` khi pipeline không config, patch annotation restart khi đã tồn tại (không tạo lại).
  **Lưu ý dependency đúng**: `io.fabric8:kubernetes-junit-jupiter` KHÔNG chứa
  `@EnableKubernetesMockClient` (artifact đó dành cho test tích hợp với cluster thật) — annotation này
  nằm trong `io.fabric8:kubernetes-server-mock`, xác nhận bằng cách liệt kê class trong jar đã resolve
  trước khi sửa `build.gradle`.
- `bumpVersion()` không có unit test (cần Couchbase Query service thật) — cùng quy ước với `load()` ở
  Giai đoạn 0.
- Deploy: `PipelineControlPlane/Dockerfile.activation-api` (dùng chung `installDist` output với
  Dockerfile chính, chỉ đổi `ENTRYPOINT` sang gọi thẳng `java -cp './lib/*'
  com.streamflow.controlplane.api.ActivationApiApp` — Gradle `application` plugin chỉ sinh 1 launcher
  script/mainClass nên không tái dùng được `bin/PipelineControlPlane`; đã xác nhận classpath đúng bằng
  `javap -cp "lib/*" ...` sau khi `installDist`). `PipelineControlPlane/k8s/activation-api.yaml` —
  KHÔNG templated (service singleton, không theo pipelineId): `ServiceAccount` + `Role`/`RoleBinding`
  (`get/list/patch/update/create` trên `deployments`, không `delete`) + `Deployment` (1 replica)
  + `Service`.

### Cập nhật — tự động hoá tạo Deployment (bỏ bước `kubectl apply` thủ công cho pipeline mới)

Sau khi triển khai xong bản đầu của Giai đoạn 2, phát sinh phản hồi: "user chỉ config cluster/worker
nào chạy pipeline, còn deploy phải tự động" — tức bước `kubectl apply` thủ công cho pipeline **mới**
(Giai đoạn 1 để lại) không nên tồn tại nữa. Đã đổi:

- `PipelineConfig` thêm 2 field optional `nodeSelectorKey`/`nodeSelectorValue` — đây chính là chỗ
  "user config cluster/worker nào" (lưu cùng trong doc Couchbase, không phải tham số dòng lệnh như
  trước). Cả 2 null (mặc định) = không ghim node, để k8s scheduler tự chọn.
- `DeploymentRolloutTrigger` đổi tên thành `PipelineDeploymentManager`, thêm nhánh **create**.
- `ensureDeployed(pipelineId, config)`: `.get()` Deployment trước — null thì tạo mới, khác null thì
  patch-restart như cũ (không tạo lại đè lên).
- RBAC (`activation-api.yaml`) thêm verb `create` — mở rộng có chủ đích, đã xác nhận trước khi đổi
  (không tự ý mở quyền).
- Response `/activate` đổi field: `k8sRolloutTriggered` → `k8sDeployed` (+ `k8sAction`:
  `"created"`/`"restarted"`) để phản ánh đúng 2 nhánh có thể xảy ra.

**Cập nhật tiếp — dùng chung 1 file YAML làm nguồn sự thật (thay vì `DeploymentBuilder` liệt kê tay
từng field)**: bản đầu của nhánh create tự dựng `Deployment` object bằng fabric8 `DeploymentBuilder`,
gõ tay từng field (label, `RollingUpdate maxUnavailable:0`, `terminationGracePeriodSeconds:60`,
resources...) — nghĩa là có **2 nơi định nghĩa "1 Deployment pipeline trông như thế nào"**: file YAML
(chỉ còn ai đọc thủ công) và code Java (đường chạy thật), dễ lệch nhau nếu chỉ sửa 1 bên. Đã sửa lại:

- Template chuyển vị trí, từ `PipelineControlPlane/k8s/pipeline-deployment.template.yaml` (không đóng
  gói vào jar) sang `PipelineControlPlane/src/main/resources/k8s/pipeline-deployment.template.yaml`
  (classpath resource, đọc được lúc runtime kể cả khi chạy trong container chỉ có `installDist`
  output). Bỏ luôn placeholder `${BOOTSTRAP_SERVERS}` khỏi template (khớp quyết định không override
  giá trị này nữa).
- `PipelineDeploymentManager.create()` giờ: đọc text template từ classpath → thay placeholder bằng
  `String.replace` (đơn giản, không cần thư viện template engine) → parse bằng
  `io.fabric8.kubernetes.client.utils.Serialization.unmarshal(yaml, Deployment.class)` ra đúng object
  `Deployment` → set `nodeSelector` trực tiếp trên object đã parse nếu `PipelineConfig` có config (đơn
  giản hơn hẳn tự chèn dòng text có điều kiện vào YAML như cách `render-pipeline-deployment.sh` từng
  làm bằng `awk`) → `.create()`. Không còn `DeploymentBuilder` liệt kê field nào trong class này nữa.
- `scripts/k8s/render-pipeline-deployment.sh`/`.ps1` (đường fallback thủ công) trỏ sang path resource
  mới, cũng bỏ `BOOTSTRAP_SERVERS` khỏi danh sách placeholder — cả 2 đường (tự động qua Activation API,
  thủ công qua script) giờ đọc **đúng 1 file**, không thể lệch nhau.
- Test (`PipelineDeploymentManagerTest`) verify lại qua unmarshal thật: label/env/nodeSelector đúng
  sau khi parse từ YAML, không phải từ builder — vẫn dùng
  `@EnableKubernetesMockClient(crud = true)`, không cần cluster sống.

### Giai đoạn 3 — Reload an toàn (không cần code thêm)

- Xác nhận `KafkaStreamsRunner` hiện tại (shutdown hook + block `Thread.currentThread().join()`) đã đủ
  cho SIGTERM sạch — không sửa code, chỉ đảm bảo `terminationGracePeriodSeconds` đủ lớn.
- Deployment dùng `strategy: RollingUpdate, maxUnavailable: 0` — pod mới phải healthy rồi mới thay pod
  cũ.
- Không có gì để triển khai ở giai đoạn này — 2 cơ chế trên đã có sẵn từ Giai đoạn 1, chỉ cần verify thủ
  công trên cluster thật (đổi config → activate → xem rolling restart không rớt record) khi cần.

### Giai đoạn 4 — Dọn dẹp khi xoá pipeline (đã triển khai, scope thu hẹp có chủ đích)

`DELETE /pipelines/{id}` trong `ActivationApiApp` (`PIPELINE_PATH` pattern, tách riêng khỏi
`ACTIVATE_PATH`). **Quyết định phạm vi**: endpoint này **CHỈ xoá Deployment k8s**
(`PipelineDeploymentManager.undeploy()` — get theo tên `pipeline-<id>`, không tồn tại → 404, tồn tại →
`.delete()`), **không** đụng tới internal topic/consumer group/`state.dir` của Kafka Streams — khác với
`PipelineReset` gốc bên `ValidateAndBuildInnerJoinOperation` (xoá cả 2 thứ đó). Lý do: dọn Kafka cần
`AdminClient` + `bootstrapServers` riêng của từng pipeline và có rủi ro phá huỷ dữ liệu cao hơn hẳn thao
tác k8s thuần, nên giữ tách biệt — muốn dọn Kafka thật thì vẫn chạy `PipelineReset` thủ công
(`PIPELINE_ID=<id> ./gradlew :ValidateAndBuildInnerJoinOperation:resetPipeline`) sau khi gọi
`DELETE /pipelines/{id}`. Response luôn kèm field `note` nhắc rõ điều này để không ai tưởng lầm là đã dọn
sạch Kafka.

- RBAC (`activation-api.yaml`) mở thêm verb `delete` trên `deployments` — trước đó Giai đoạn 2 cố tình
  không có verb này, nay mở có chủ đích cho đúng phạm vi endpoint mới.
- Test: `PipelineDeploymentManagerTest` thêm `undeployDeletesExistingDeployment` +
  `undeployReturnsFalseWhenDeploymentDoesNotExist`, cùng cách dùng `@EnableKubernetesMockClient` như các
  test khác trong file, không cần cluster sống.
- Chưa làm: xoá kèm doc Couchbase (`pipeline::<id>`) — hiện giữ nguyên doc sau khi xoá Deployment (coi
  như lưu lịch sử/audit), có thể thêm query param `?deleteConfig=true` sau nếu cần.

### Giai đoạn 5 — Test & vận hành

- E2E trên dev cluster: tạo pipeline mới trong Couchbase → gọi activate → pod lên đúng, đúng
  `nodeSelector` nếu có set.
- Test reload: sửa config → activate → rolling restart, không mất record giữa chừng.
- Test negative: config sai → activate trả lỗi ngay, pod đang chạy không bị đụng.
- Xác nhận lại 2 sự cố trong `errorlog.txt` không lan rộng: pod crash/disk-full chỉ ảnh hưởng đúng pod đó.

## Cập nhật — chuyển hẳn Kafka + Couchbase vào trong cluster (bỏ mô hình hybrid)

Mô hình dev ban đầu (Kafka/Couchbase ở `docker-compose`, pod pipeline ở minikube, nối qua
`host.minikube.internal`) **gãy thật** khi build+chạy Giai đoạn 2 lần đầu: Kafka trả về
`advertised.listeners = localhost:9092` cho pod (client dùng địa chỉ này cho MỌI request sau bootstrap
đầu tiên, "localhost" trong pod lại trỏ về chính pod đó) — không sửa được mà không đổi
`docker-compose.yml` (ảnh hưởng nhiều script khác đang phụ thuộc `localhost:9092`/`docker exec`). Đã
chuyển hẳn Kafka + Couchbase thành k8s resource thật, cùng cluster với pod pipeline và Activation API —
mọi thứ chỉ còn 1 network, không còn indirection nào để lệch địa chỉ advertise.

- `PipelineControlPlane/k8s/infra/kafka-broker.yaml` — Kafka KRaft 1-node (`apache/kafka:3.9.0`, cùng
  image `docker-compose.yml`), Service tên `broker` (khớp default `bootstrapServers` các pipeline đã
  dùng từ trước). **Không** dùng `0.0.0.0` cho `KAFKA_LISTENERS` — Kafka's `validateValues` từ chối
  `0.0.0.0` trong advertised.listeners kể cả khi chỉ CONTROLLER (không client nào dùng) bind vào đó.
  Bind cả 2 listener thẳng vào Pod IP thật qua Downward API (`$(POD_IP)`, k8s hỗ trợ tham chiếu env
  trước đó trong cùng container) — né toàn bộ nhập nhằng "listeners nào cần advertise".
- `PipelineControlPlane/k8s/infra/couchbase.yaml` — Couchbase (`couchbase/server:community-7.6.2`),
  Service tên `couchbase` (khớp đúng default `COUCHBASE_CONNECTION_STRING`/
  `PIPELINE_COUCHBASE_CONNECTION_STRING` đã có sẵn trong `application.yaml` — không cần đổi gì thêm ở
  đó, chỉ cần Couchbase thật sự chạy đúng chỗ tên đó trỏ tới). Không có PVC — mất state khi pod
  restart, phải init lại (chấp nhận được ở quy mô dev).
- Init/seed/tạo topic giờ qua `kubectl exec` vào đúng pod (`couchbase-cli`, `cbq`,
  `kafka-topics.sh`) — thay `docker exec` vào container compose. Script gộp:
  `scripts/k8s/setup-minikube-local.ps1` (chưa có bản `.sh` tương ứng — làm sau nếu cần, ưu tiên
  PowerShell vì đó là shell chính đang dùng).
- Activation API cũng deploy vào cluster (`PipelineControlPlane/k8s/activation-api.yaml`, đã có sẵn từ
  Giai đoạn 2) — gọi từ host qua `kubectl port-forward svc/streamflow-activation-api 7100:80`.

### Bug thật đã gặp khi verify — ghi lại để không lặp lại

1. **`Dockerfile` bake sẵn `ENV BOOTSTRAP_SERVERS=broker:29092`** từ Giai đoạn 1 (trước khi chốt "không
   override BOOTSTRAP_SERVERS nữa") — `AppConfig.get()` đọc `System.getenv()` trước `application.yaml`,
   nên env thật trong image luôn thắng giá trị đúng đã lưu trong Couchbase, dù Deployment không hề set
   biến này. Đã xoá hết `ENV` default khỏi `Dockerfile` — `application.yaml`/`AppConfig` là nguồn default
   DUY NHẤT, không được duplicate sang Dockerfile nữa.
2. **`minikube image load` (kể cả `--overwrite`) không thực sự ghi đè image đang được container dùng** —
   `minikube image ls` vẫn báo đúng image ID mới, nhưng pod vẫn chạy image ID cũ. Cách chắc ăn: scale
   Deployment về 0 → `minikube image rm` (chỉ xoá được khi không container nào tham chiếu) →
   `minikube image load` → scale lại. Áp dụng mỗi lần rebuild code cần thấy hiệu lực ngay trong
   minikube.
3. **`PIPELINE_COUCHBASE_CONNECTION_STRING` trong `application.yaml` không được cập nhật khi chuyển
   Couchbase vào cluster** — vẫn còn `couchbase://host.minikube.internal` (giá trị đúng cho giai đoạn
   hybrid trước đó) trong lúc pod mới cần `couchbase://couchbase`. Hệ quả: pod mới tạo nối nhầm vào
   Couchbase **cũ** ở docker-compose (vẫn đang chạy song song), đọc phải doc pipeline cũ có
   `bootstrapServers` sai → quay lại đúng lỗi advertised-listener ban đầu dù broker trong cluster đã
   đúng. Bài học chung với bug #1: mọi default đổi theo môi trường (hybrid → in-cluster) phải rà lại
   TẤT CẢ chỗ đã set giá trị đó, không chỉ chỗ vừa sửa.
4. **Debug giả — tiến trình `./gradlew :PipelineControlPlane:ActivationApi` chạy local từ phiên làm việc
   trước đó vẫn chiếm cổng 7100 ở background**, khiến `kubectl port-forward svc/streamflow-activation-api
   7100:80` fail bind (im lặng nếu không đọc kỹ log) nhưng `curl` vẫn nhận được response — từ tiến
   trình local cũ, không phải Activation API thật trong cluster. Toàn bộ kết quả `/activate` test trong
   khoảng thời gian đó là giả — chỉ phát hiện ra khi đọc log port-forward thấy "Unable to listen on
   port 7100" thay vì tin ngay response 200 từ curl. Bài học: **luôn xác nhận port-forward thật sự bind
   được** (đọc log của chính lệnh port-forward) trước khi tin kết quả gọi qua nó, đừng chỉ nhìn response
   code của client.

### Cập nhật — expose broker ra `kafka-ui` (docker-compose)

`kafka-ui` (chạy trong docker-compose, không phải trong cluster) không đọc được broker trong-cluster
qua listener `PLAINTEXT` (advertise Pod IP — không routable từ ngoài cluster). Thêm listener thứ 3
`EXTERNAL` (port 9094) trong `kafka-broker.yaml`, advertise `host.docker.internal:9094` (hostname
Docker Desktop cấp cho MỌI container, không riêng minikube) — client ngoài cluster resolve được, route
về máy host. Cần `kubectl port-forward svc/broker 9094:9094 --address 0.0.0.0` chạy **liên tục** trên
host để request tới `host.docker.internal:9094` thực sự chạm được pod (chưa có cách nào tự động hoá
việc này thành 1 service chạy nền bền — nếu port-forward chết, `kafka-ui` cluster "kube" sẽ mất kết
nối, phải chạy lại lệnh port-forward thủ công). `docker-compose.yml`'s `KAFKA_CLUSTERS_1_BOOTSTRAPSERVERS`
trỏ đúng `host.docker.internal:9094`.

**Lưu ý quan trọng**: mỗi lần broker pod restart (deploy lại manifest, hoặc crash) thì **mất hết
topic** (không PVC) — phải tạo lại topic bằng tay (`kubectl exec` + `kafka-topics.sh --create`), đã
gặp đúng việc này ngay sau khi thêm listener EXTERNAL (phải apply lại Deployment → pod restart → topic
biến mất → `kafka-ui` báo `topicCount: 0` dù cluster "online" bình thường).

### Cập nhật — thêm PVC cho Kafka + Couchbase (bug thật: `AuthenticationFailureException`)

Mất state khi pod restart (đã nói ở trên) **không chỉ là bất tiện phải tạo lại topic** — nó còn gây
crash pipeline theo cách khó đoán: pod Couchbase restart (không do ai đụng vào, tự restart) → mất
sạch cluster-init → Couchbase quay về trạng thái "chưa init" → pod pipeline/Activation API cố kết nối
bằng `Administrator`/`password` → Couchbase từ chối vì user đó **không còn tồn tại nữa** → client nhận
`com.couchbase.client.core.error.AuthenticationFailureException` — đọc qua tưởng sai mật khẩu, thực ra
là "cả cluster quên mất nó được init". Broker cũng mất hết topic cùng lúc (cùng nguyên nhân, không có
PVC).

Đã thêm `PersistentVolumeClaim` cho cả 2 (`couchbase-data` mount `/opt/couchbase/var`, `broker-data`
mount `/tmp/kafka-logs` — **xác nhận đường dẫn thật bằng `kubectl exec ... find`**, không đoán, vì mỗi
image quyết định thư mục data khác nhau). minikube có StorageClass `standard` mặc định (hostPath) nên
PVC tự cấp phát, không cần setup thêm.

**Bẫy gặp phải khi verify PVC**: lần đầu apply PVC, Couchbase crash nội bộ ngay khi boot
(`chronicle` — thành phần lưu metadata nội bộ của Couchbase — báo
`failed_to_start_child,chronicle_agent_sup`, ghi `erl_crash.dump`) dù pod k8s vẫn báo `1/1 Running`
(k8s chỉ thấy tiến trình cha còn sống, không biết app bên trong đang tự crash-loop). Nguyên nhân: gọi
`curl`/`cluster-init` dồn dập ngay khi pod vừa lên, chưa cho Couchbase server boot xong hẳn. Xoá PVC
+ pod, tạo lại, **chờ hẳn admin API trả 200 rồi mới init** (không đoán thời gian, poll thật) — boot
sạch, không crash lại. Bài học: Couchbase (và có thể nhiều app "nặng" khác) cần khoảng lặng thật sự
lúc boot, không chỉ chờ pod `Ready` — `Ready` không đồng nghĩa "app bên trong đã ổn định".

## Việc không nằm trong phạm vi hiện tại (loại trừ có chủ đích)

- Lease/CAS, tự cân bằng pipeline qua nhiều worker ("fleet + assigner") — chỉ đáng làm khi có bằng chứng
  chi phí JVM-per-pipeline thật sự là vấn đề ở quy mô lớn hơn nhiều so với hiện tại.
- Sửa `ValidateAndBuildInnerJoinOperation` để hỗ trợ multi-instance/hot-reload trong-JVM — đã bị loại trừ
  có chủ đích ở đó, không đảo ngược quyết định đó tại đây.
- Backup/snapshot cho PVC của Kafka/Couchbase — có PVC rồi (xem mục "thêm PVC" ở trên) nhưng chưa có cơ
  chế backup, chỉ chống mất state do pod restart thường, không chống mất PV/node.
