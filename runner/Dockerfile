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

# jlink: dung jdeps quet toan bo jar (app + moi thu vien - Kafka Streams, RocksDB JNI, Couchbase SDK,
# Jackson...) de biet CHINH XAC module JDK nao dang thuc su dung, roi chi link tung ay module thay vi
# ca bo JRE day du (eclipse-temurin:17-jre ~430MB dung het moi module ke ca java.desktop/java.rmi/...
# ma pipeline khong bao gio dung toi). --ignore-missing-deps vi vai lib (Netty, native optional
# codec...) reference class o runtime qua reflection ma jdeps khong lan toi duoc - jlink stage se fail
# ro rang luc runtime (thieu module) neu doan sai, luc do them thu cong vao EXTRA_MODULES ben duoi.
FROM build AS jlink
RUN jdeps \
        --print-module-deps \
        --ignore-missing-deps \
        --multi-release 17 \
        --recursive \
        runner/build/install/runner/lib/*.jar > /tmp/modules.txt \
    && EXTRA_MODULES="java.naming,java.management,java.sql,jdk.unsupported,jdk.crypto.ec,jdk.crypto.cryptoki" \
    && jlink \
        --add-modules "$(cat /tmp/modules.txt),${EXTRA_MODULES}" \
        --strip-debug \
        --no-header-files \
        --no-man-pages \
        --compress=2 \
        --output /opt/jre

# Runtime khong con can JDK/JRE day du - debian-slim (glibc, co /bin/sh cho script khoi dong cua
# Gradle application plugin) + custom JRE tu jlink o tren la du, giam dang ke size/attack surface so
# voi eclipse-temurin:17-jre truoc day.
FROM debian:12-slim AS runtime
ENV JAVA_HOME=/opt/jre
ENV PATH="${JAVA_HOME}/bin:${PATH}"
COPY --from=jlink /opt/jre /opt/jre

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
