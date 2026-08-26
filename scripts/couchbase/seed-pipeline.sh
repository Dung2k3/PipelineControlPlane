#!/usr/bin/env bash
# Nap 1 pipeline config vao Couchbase (doc id "pipeline::<pipelineId>") de test
# CouchbasePipelineConfigStore (PipelineControlPlane) that su, khong chi qua unit test.
# Doc noi dung tu scripts/couchbase/pipelines/<pipelineId>.json - sua file JSON do de doi
# noi dung nap, khong sua script nay.
#
# Doi pipeline muon nap qua tham so dau tien (mac dinh: customer-orders-demo).
# Yeu cau: scripts/couchbase/init-cluster.sh da chay xong (co bucket "streamflow" +
# primary index).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="couchbase"
ADMIN_USER="Administrator"
ADMIN_PASS="password"
BUCKET="streamflow"
PIPELINE_ID="${1:-customer-orders-demo}"
SOURCE_JSON="${SCRIPT_DIR}/pipelines/${PIPELINE_ID}.json"

if [ ! -f "${SOURCE_JSON}" ]; then
  echo "Khong tim thay file: ${SOURCE_JSON}" >&2
  echo "Cac pipeline JSON co san:" >&2
  ls "${SCRIPT_DIR}/pipelines/"*.json >&2
  exit 1
fi

DOC_ID="pipeline::${PIPELINE_ID}"
JSON_CONTENT="$(cat "${SOURCE_JSON}")"

echo "Nap '${SOURCE_JSON}' vao doc '${DOC_ID}' (bucket '${BUCKET}')..."
docker exec -i "${CONTAINER}" cbq -e "http://localhost:8093" -u "${ADMIN_USER}" -p "${ADMIN_PASS}" \
  -s "UPSERT INTO \`${BUCKET}\` (KEY, VALUE) VALUES ('${DOC_ID}', ${JSON_CONTENT});"

echo "Xong. Doc trong Couchbase:"
docker exec "${CONTAINER}" cbq -e "http://localhost:8093" -u "${ADMIN_USER}" -p "${ADMIN_PASS}" \
  -s "SELECT * FROM \`${BUCKET}\` USE KEYS '${DOC_ID}';"
