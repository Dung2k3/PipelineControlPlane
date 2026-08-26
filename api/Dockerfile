# Build context phai la root project nay (vd: docker build -f api/Dockerfile .).
#
# Module 'api' co plugin 'application' rieng (mainClass = ActivationApiApp) nen installDist tu sinh
# dung launcher script './bin/api' - khac ban goc (khi con chung 1 module PipelineControlPlane) phai
# tu goi 'java -cp lib/*' vi Gradle 'application' plugin chi sinh 1 launcher cho 1 mainClass.

FROM eclipse-temurin:17-jdk AS build
WORKDIR /workspace

COPY gradlew ./
COPY gradle ./gradle
COPY settings.gradle build.gradle ./
COPY common ./common
COPY api ./api

RUN chmod +x gradlew \
    && ./gradlew --no-daemon :api:installDist

FROM eclipse-temurin:17-jre AS runtime
WORKDIR /app

COPY --from=build /workspace/api/build/install/api ./

ENV API_PORT=7100
ENV COUCHBASE_CONNECTION_STRING=couchbase://couchbase
ENV COUCHBASE_USERNAME=Administrator
ENV COUCHBASE_PASSWORD=password
ENV COUCHBASE_BUCKET=streamflow
ENV K8S_NAMESPACE=default

EXPOSE 7100

ENTRYPOINT ["./bin/api"]
