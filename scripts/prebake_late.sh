#!/usr/bin/env bash
# Produce an honest "Late, then recovered" run on one client: take its worker
# down across a quarter-hour boundary, then bring it back. The scheduled run
# goes Late (e-mail fires), is picked up when the worker returns, and completes.
#   scripts/prebake_late.sh boreal-energy 18:13 18:18     # UTC hh:mm down / up
set -euo pipefail
CLIENT="$1"; DOWN="$2"; UP="$3"
wait_until() { while [[ "$(date -u +%H:%M)" < "$1" ]]; do sleep 10; done; }
wait_until "$DOWN"; echo "$(date -u +%T) scaling client-$CLIENT worker to 0"
kubectl -n "client-$CLIENT" scale deploy/prefect-worker --replicas=0
wait_until "$UP"; echo "$(date -u +%T) scaling client-$CLIENT worker to 1"
kubectl -n "client-$CLIENT" scale deploy/prefect-worker --replicas=1
kubectl -n "client-$CLIENT" rollout status deploy/prefect-worker --timeout=120s
