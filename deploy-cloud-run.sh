#!/usr/bin/env bash

set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-canopy-v5}"
REGION="${REGION:-us-central1}"
PROJECT_ID="${PROJECT_ID:-canopy-490503}"
REPOSITORY="${REPOSITORY:-canopy-v5}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${SERVICE_NAME}"
ALLOW_UNAUTHENTICATED="${ALLOW_UNAUTHENTICATED:-false}"
MEMORY="${MEMORY:-2Gi}"
CPU="${CPU:-1}"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "No Google Cloud project is set. Run: gcloud config set project <PROJECT_ID>" >&2
  exit 1
fi

gcloud config set project "${PROJECT_ID}" >/dev/null

echo "Deploying ${SERVICE_NAME} to project ${PROJECT_ID} in ${REGION}"

ENV_VARS=(
  "CANOPY_RUNTIME_MODE=${CANOPY_RUNTIME_MODE:-real}"
  "GCP_PROJECT_ID=${PROJECT_ID}"
  "ETH_PRICE_FALLBACK=3500"
  "POLYGON_PRICE_FALLBACK=0.10"
  "CANOPY_CORRIDOR_BIGQUERY=false"
  "CANOPY_FEE_QUERY_MAX_BYTES_PER_QUERY=${CANOPY_FEE_QUERY_MAX_BYTES_PER_QUERY:-500000000}"
  "CANOPY_MEASURED_QUERY_WINDOWS_HOURS=${CANOPY_MEASURED_QUERY_WINDOWS_HOURS:-24,48}"
  "CANOPY_ACTIVE_TOKENS=${CANOPY_ACTIVE_TOKENS:-}"
  "CANOPY_POLL_INTERVAL_SECONDS=${CANOPY_POLL_INTERVAL_SECONDS:-300}"
  "CANOPY_POLL_BACKOFF_SECONDS=${CANOPY_POLL_BACKOFF_SECONDS:-30}"
  "CANOPY_CORRIDOR_MAX_BYTES_PER_QUERY=${CANOPY_CORRIDOR_MAX_BYTES_PER_QUERY:-250000000}"
  "CANOPY_CORRIDOR_CONFIG_URI=${CANOPY_CORRIDOR_CONFIG_URI:-}"
  "CANOPY_CORRIDOR_CONFIG_REFRESH_SECONDS=${CANOPY_CORRIDOR_CONFIG_REFRESH_SECONDS:-60}"
  "CANOPY_CONTEXT_GRAPH_ENABLED=${CANOPY_CONTEXT_GRAPH_ENABLED:-false}"
  "CANOPY_CONTEXT_GRAPH_TIME_RANGES=${CANOPY_CONTEXT_GRAPH_TIME_RANGES:-1h}"
  "CANOPY_CONTEXT_GRAPH_MAX_BYTES_PER_QUERY=${CANOPY_CONTEXT_GRAPH_MAX_BYTES_PER_QUERY:-1000000000}"
  "CANOPY_CONTEXT_GRAPH_EDGE_MAX_BYTES_PER_QUERY=${CANOPY_CONTEXT_GRAPH_EDGE_MAX_BYTES_PER_QUERY:-750000000}"
  "CANOPY_CONTEXT_GRAPH_GAP_MAX_BYTES_PER_QUERY=${CANOPY_CONTEXT_GRAPH_GAP_MAX_BYTES_PER_QUERY:-250000000}"
  "CANOPY_CONTEXT_GRAPH_ETHEREUM_MODE=${CANOPY_CONTEXT_GRAPH_ETHEREUM_MODE:-transfer_only}"
  "CANOPY_CONTEXT_GRAPH_POLYGON_MODE=${CANOPY_CONTEXT_GRAPH_POLYGON_MODE:-transfer_only}"
  "X402_ENABLED=false"
)

SERVICE_JSON="$(
  gcloud run services describe "${SERVICE_NAME}" \
    --region "${REGION}" \
    --format=json 2>/dev/null || true
)"

EXISTING_MAPBOX_TOKEN="$(
  if [[ -n "${SERVICE_JSON}" ]]; then
    printf '%s' "${SERVICE_JSON}" | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
for env in payload.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [{}])[0].get("env", []):
    if env.get("name") == "VITE_MAPBOX_TOKEN":
        print(env.get("value", ""))
        break
'
  fi
)" || true

if [[ -n "${VITE_MAPBOX_TOKEN:-}" ]]; then
  ENV_VARS+=("VITE_MAPBOX_TOKEN=${VITE_MAPBOX_TOKEN}")
elif [[ -n "${EXISTING_MAPBOX_TOKEN:-}" ]]; then
  ENV_VARS+=("VITE_MAPBOX_TOKEN=${EXISTING_MAPBOX_TOKEN}")
  echo "Preserving existing VITE_MAPBOX_TOKEN from Cloud Run."
else
  echo "VITE_MAPBOX_TOKEN is not set; the deployed UI will keep the map in token-missing mode."
fi

if [[ -z "${CANOPY_CORRIDOR_CONFIG_URI:-}" ]]; then
  echo "CANOPY_CORRIDOR_CONFIG_URI is not set; deployment will use the bundled local corridor config fallback."
fi

gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  bigquery.googleapis.com

if ! gcloud artifacts repositories describe "${REPOSITORY}" --location "${REGION}" >/dev/null 2>&1; then
  gcloud artifacts repositories create "${REPOSITORY}" \
    --repository-format docker \
    --location "${REGION}" \
    --description "Docker images for Canopy v5"
fi

gcloud builds submit --tag "${IMAGE}"

PUBLIC_FLAG="--no-allow-unauthenticated"
if [[ "${ALLOW_UNAUTHENTICATED}" == "true" ]]; then
  PUBLIC_FLAG="--allow-unauthenticated"
fi

ENV_FILE="$(mktemp)"
trap 'rm -f "${ENV_FILE}"' EXIT
python3 - "${ENV_FILE}" "${ENV_VARS[@]}" <<'PY'
import json
import sys

path = sys.argv[1]
items = sys.argv[2:]

with open(path, "w", encoding="utf-8") as handle:
    for item in items:
        key, value = item.split("=", 1)
        handle.write(f"{key}: {json.dumps(value)}\n")
PY

gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE}" \
  --platform managed \
  --region "${REGION}" \
  "${PUBLIC_FLAG}" \
  --min-instances 1 \
  --no-cpu-throttling \
  --memory "${MEMORY}" \
  --cpu "${CPU}" \
  --env-vars-file "${ENV_FILE}"
