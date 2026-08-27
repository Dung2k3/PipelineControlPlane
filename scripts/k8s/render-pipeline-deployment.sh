#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="${SCRIPT_DIR}/../../api/src/main/resources/k8s/pipeline-deployment.template.yaml"

PIPELINE_ID="${1:?Thieu pipelineId. Dung: $0 <pipelineId> [nodeSelectorKey=value]}"
NODE_SELECTOR_ARG="${2:-}"

IMAGE="${IMAGE:-streamflow/pipeline-control-plane:latest}"
COUCHBASE_CONNECTION_STRING="${COUCHBASE_CONNECTION_STRING:-couchbase://couchbase}"
COUCHBASE_USERNAME="${COUCHBASE_USERNAME:-Administrator}"
COUCHBASE_PASSWORD="${COUCHBASE_PASSWORD:-password}"
COUCHBASE_BUCKET="${COUCHBASE_BUCKET:-streamflow}"

RENDERED="$(sed \
  -e "s#\${PIPELINE_ID}#${PIPELINE_ID}#g" \
  -e "s#\${IMAGE}#${IMAGE}#g" \
  -e "s#\${COUCHBASE_CONNECTION_STRING}#${COUCHBASE_CONNECTION_STRING}#g" \
  -e "s#\${COUCHBASE_USERNAME}#${COUCHBASE_USERNAME}#g" \
  -e "s#\${COUCHBASE_PASSWORD}#${COUCHBASE_PASSWORD}#g" \
  -e "s#\${COUCHBASE_BUCKET}#${COUCHBASE_BUCKET}#g" \
  "${TEMPLATE}")"

if [ -n "${NODE_SELECTOR_ARG}" ]; then
  KEY="${NODE_SELECTOR_ARG%%=*}"
  VALUE="${NODE_SELECTOR_ARG#*=}"
  echo "${RENDERED}" | awk -v key="${KEY}" -v value="${VALUE}" '
    { print }
    /terminationGracePeriodSeconds:/ {
      print "      nodeSelector:"
      print "        " key ": \"" value "\""
    }
  '
else
  echo "${RENDERED}"
fi
