# Canopy Route Intelligence Runbook (Cloud Run)

Last verified: 2026-05-07

## Startup command (local)

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8080
```

## Test commands

```bash
.venv/bin/python -m pytest -q tests/test_receipt_contract.py
.venv/bin/python -m pytest -q tests/test_solana_api_endpoints.py
```

## Deploy command (public service)

```bash
SERVICE_NAME=canopy-route-intelligence \
PROJECT_ID=canopy-main \
REGION=us-central1 \
REPOSITORY=canopy-route-intelligence \
ALLOW_UNAUTHENTICATED=true \
./deploy-cloud-run.sh
```

If org policy blocks `allUsers` IAM binding, use:

```bash
gcloud run services update canopy-route-intelligence \
  --region=us-central1 \
  --project=canopy-main \
  --no-invoker-iam-check
```

## Public URLs

- Primary service: `https://canopy-route-intelligence-fw43qa7nca-uc.a.run.app`
- Regional service URL alias: `https://canopy-route-intelligence-935589208391.us-central1.run.app`
- Antler investor demo (kept intact): `https://antler-ic-canopy-papaya-demo-fw43qa7nca-uc.a.run.app`

## Daily monitoring steps

1. Confirm service is healthy:
   ```bash
   curl -sS -o /dev/null -w "%{http_code}\n" https://canopy-route-intelligence-fw43qa7nca-uc.a.run.app
   ```
2. Confirm deployment status:
   ```bash
   gcloud run services list --platform managed --project canopy-main --region us-central1
   ```
3. Spot-check logs for startup/runtime errors:
   ```bash
   gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="canopy-route-intelligence"' --limit 50 --project canopy-main
   ```

## What healthy looks like

- `curl` returns HTTP `200`.
- Cloud Run service status is `True`.
- No repeated startup exceptions in recent logs.

## Alert response steps

1. If `5xx` or `4xx` appears unexpectedly, inspect recent Cloud Run logs.
2. Verify latest revision is serving 100% traffic.
3. Re-deploy from current main branch using deploy command above.
4. Re-run health check and key API smoke checks.

## Known failure modes

- Org policy blocks `allUsers` IAM binding during deploy.
- Missing `VITE_MAPBOX_TOKEN` causes token-missing map mode (UI still serves).
- Missing `CANOPY_CORRIDOR_CONFIG_URI` falls back to bundled local corridor config.

## Recovery procedure

1. Re-run deployment command.
2. If IAM binding fails, run `--no-invoker-iam-check` update command.
3. Validate public URL returns `200`.
4. Verify Antler demo URL still returns `200`.

## Escalation path

- Product/engineering owner: Alan Nguyen.
- Escalate to GCP org policy/IAM admin when public access controls are blocked by organization policies.

## Required environment variables

- `PROJECT_ID` (set to `canopy-main` for current production/public deployment)
- `REGION` (`us-central1`)
- `SERVICE_NAME` (`canopy-route-intelligence`)

Common optional runtime env vars (currently defaulted in deploy script):

- `VITE_MAPBOX_TOKEN`
- `CANOPY_CORRIDOR_CONFIG_URI`
- `CANOPY_RUNTIME_MODE`
- `CANOPY_CORRIDOR_BIGQUERY`

## Security incident notes

- No new secrets were introduced in code changes.
- Public access is configured at Cloud Run service level using invoker IAM check settings due org policy constraints.

## Outcome metric to monitor

- Public endpoint availability (`HTTP 200` uptime).
