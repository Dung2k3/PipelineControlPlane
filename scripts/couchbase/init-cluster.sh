#!/usr/bin/env bash
# Khoi tao cluster Couchbase 1-node cho dev local: cluster-init + bucket "streamflow" +
# primary index (can cho N1QL UPSERT o seed-pipeline.sh). Day la nguon config that cho
# PipelineControlPlane.CouchbaseConnection - xem docs/plan.md.
#
# Idempotent: bo qua tung buoc neu da co roi, an toan chay lai nhieu lan.
# Yeu cau: `docker compose up -d couchbase` da chay. Lan dau container co the mat 15-30s
# de web console san sang - script tu cho o buoc dau.
set -euo pipefail

CONTAINER="couchbase"
HOST="http://localhost:8091"
ADMIN_USER="Administrator"
ADMIN_PASS="password"
BUCKET="streamflow"

echo "Cho Couchbase web console san sang tren ${HOST}..."
until curl -s -o /dev/null "${HOST}/pools"; do
  sleep 2
done

if curl -s "${HOST}/pools/default" -u "${ADMIN_USER}:${ADMIN_PASS}" | grep -q '"clusterName"'; then
  echo "Cluster da duoc init truoc do - bo qua buoc cluster-init."
else
  echo "Init cluster (admin=${ADMIN_USER}, services data+index+query)..."
  docker exec "${CONTAINER}" couchbase-cli cluster-init \
    --cluster-username "${ADMIN_USER}" \
    --cluster-password "${ADMIN_PASS}" \
    --cluster-ramsize 512 \
    --cluster-index-ramsize 256 \
    --services data,index,query
fi

if docker exec "${CONTAINER}" couchbase-cli bucket-list -c localhost \
    -u "${ADMIN_USER}" -p "${ADMIN_PASS}" | grep -qx "${BUCKET}"; then
  echo "Bucket '${BUCKET}' da ton tai - bo qua."
else
  echo "Tao bucket '${BUCKET}'..."
  docker exec "${CONTAINER}" couchbase-cli bucket-create \
    -c localhost -u "${ADMIN_USER}" -p "${ADMIN_PASS}" \
    --bucket "${BUCKET}" --bucket-type couchbase --bucket-ramsize 256
fi

echo "Tao primary index tren bucket '${BUCKET}'..."
docker exec "${CONTAINER}" cbq -e "http://localhost:8093" -u "${ADMIN_USER}" -p "${ADMIN_PASS}" \
  -s "CREATE PRIMARY INDEX IF NOT EXISTS ON \`${BUCKET}\`;"

echo
echo "Xong. Web console: ${HOST} (user=${ADMIN_USER}, pass=${ADMIN_PASS})"
echo "Bien env cho PipelineControlPlane (gia tri mac dinh cua CouchbaseConnection da khop, khong bat buoc set):"
echo "  COUCHBASE_CONNECTION_STRING=couchbase://localhost"
echo "  COUCHBASE_USERNAME=${ADMIN_USER}"
echo "  COUCHBASE_PASSWORD=${ADMIN_PASS}"
echo "  COUCHBASE_BUCKET=${BUCKET}"
