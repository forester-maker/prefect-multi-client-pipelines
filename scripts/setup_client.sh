#!/usr/bin/env bash
# Provision one client's isolation unit on the cluster and its work pool in Prefect Cloud.
#   scripts/setup_client.sh acme-manufacturing
#   scripts/setup_client.sh dev            # the shared dev pool for feature branches
set -euo pipefail
cd "$(dirname "$0")/.."
CLIENT="$1"
export PATH="$PWD/.venv/bin:$PATH"
if [[ "$CLIENT" == "dev" ]]; then
  NAMESPACE=client-dev; POOL=client-dev; CONTAINER=dev-lake
else
  CONTAINER=$(.venv/bin/python -c "import yaml,sys;print(yaml.safe_load(open('clients/$CLIENT.yaml'))['lake_container'])")
  NAMESPACE="client-$CLIENT"; POOL="client-$CLIENT"
fi
export CLIENT NAMESPACE POOL CONTAINER
mkdir -p k8s/generated
sed -e "s/\${CLIENT}/$CLIENT/g" -e "s/\${NAMESPACE}/$NAMESPACE/g" -e "s/\${POOL}/$POOL/g" -e "s/\${CONTAINER}/$CONTAINER/g" \
  k8s/client.yaml.tmpl > "k8s/generated/$CLIENT.yaml"
kubectl apply -f "k8s/generated/$CLIENT.yaml"

set -a; source .env; set +a          # service-account key + workspace URL for the workers
API_URL="$PREFECT_API_URL"; API_KEY="$PREFECT_API_KEY"
kubectl -n "$NAMESPACE" create secret generic prefect-api \
  --from-literal=PREFECT_API_URL="$API_URL" --from-literal=PREFECT_API_KEY="$API_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
# The client's source credentials live only in the client's namespace (Key Vault CSI in production).
kubectl -n "$NAMESPACE" create secret generic client-source-credentials \
  --from-literal=ERP_CLIENT_SECRET="erp-$CLIENT-$(openssl rand -hex 6)" \
  --from-literal=SAP_PASSWORD="sap-$CLIENT-$(openssl rand -hex 6)" \
  --dry-run=client -o yaml | kubectl apply -f -

.venv/bin/python scripts/setup_pool.py "$POOL" "$NAMESPACE" "$CONTAINER"
kubectl -n "$NAMESPACE" rollout restart deploy/prefect-worker >/dev/null
kubectl -n "$NAMESPACE" rollout status deploy/prefect-worker --timeout=180s
