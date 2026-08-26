# Build context phai la root project nay (vd: docker build -f runner/Dockerfile .) - can
# settings.gradle/build.gradle/gradlew o root de Gradle build duoc ca cay multi-module.
#
# 1 image dung chung cho moi pipeline (Model A): pipelineId nao chay o container nao la do
# PIPELINE_ID truyen vao luc deploy (k8s Deployment template - xem api/src/main/resources/k8s/), khong
# bake san trong image.

FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

COPY gradlew ./
COPY gradle ./gradle
COPY settings.gradle build.gradle ./
COPY common ./common
COPY runner ./runner

RUN chmod +x gradlew \
    && ./gradlew --no-daemon :runner:installDist

FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app

COPY --from=build /workspace/runner/build/install/runner ./

# KHONG bake ENV default nao o day (tung co BOOTSTRAP_SERVERS=broker:29092 va 4 dong COUCHBASE_* -
# gay bug that: image ENV la bien moi truong that, System.getenv() trong AppConfig doc duoc no
# TRUOC ca application.yaml, nen "broker:29092" am tham de len tren gia tri dung da luu trong
# Couchbase du Deployment khong set BOOTSTRAP_SERVERS. AppConfig/application.yaml (dong goi trong
# jar cua module common) la nguon default DUY NHAT tu gio - khong dupplicate default o Dockerfile nua.
#
# PIPELINE_ID khong co default o dau ca - moi pod phai duoc deploy voi dung 1 PIPELINE_ID cua no
# (PipelineDeploymentManager/api/src/main/resources/k8s/pipeline-deployment.template.yaml), Main.java
# fail-fast neu thieu.

ENTRYPOINT ["./bin/runner"]
