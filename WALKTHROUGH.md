# Walkthrough

Six exercises, in the order a platform team usually asks about them. Run commands from the repo root with
`.env` sourced (`set -a && source .env && set +a`) and the venv on your path.

## 1. New client from GitHub to production

1. Copy a client file: `cp clients/boreal-energy.yaml clients/new-client.yaml`, change `name`, `lake_container`, feeds.
2. Provision its infrastructure once: `make client CLIENT=new-client` (namespace, secrets, lake PVC, worker, work pool, queues).
3. Push a branch. Actions runs `python -m pipelines.validate` then deploys to the `client-dev` pool: deployments named `new-client-*-dev`, paused.
4. Merge to main. Actions deploys to pool `client-new-client` with schedules live. Filter the deployments page by `client:new-client`.

Terminal equivalent of the Action: `make deploy ENV=dev` or `make deploy ENV=prod`.

## 2. Large extractions, paged and checkpointed

`src/pipelines/extract.py` is the whole story: fetch a page, write it as a parquet part, commit the checkpoint, ask for the next.

```
prefect deployment run "backfill/acme-manufacturing-backfill" --param fail_at_page=7
```

The task raises after page 7, retries after 5 seconds, and the run log reads "resuming customer_orders from page 8". Pages 1 to 7 are never fetched again.

## 3. Small jobs next to a backfill

```
make run-backfill CLIENT=acme-manufacturing      # ~90 s, its own pod, low-priority queue
make run-backfill CLIENT=acme-manufacturing      # second one is Cancelled: "Deployment concurrency limit reached"
make run-feeds CLIENT=acme-manufacturing         # 50 feeds, one pod, completes while the backfill runs
```

Open the feeds run: one task per feed, twelve at a time; `extract shop_order_operations` retried after a simulated 429.
The other client's worker is untouched because it is a different pool.

## 4. Event-driven transformations

Fire the landed events by hand to show that events, not schedules, drive the chain:

```
python scripts/emit_landed.py acme-manufacturing customer_orders
python scripts/emit_landed.py acme-manufacturing work_orders inventory_parts
```

After the third event the compound trigger starts `acme-manufacturing-silver-transform` once; its `pipelines.layer.ready` event starts `acme-manufacturing-gold-publish`, which leaves a KPI table as an artifact. `make run-feeds` produces the same chain from real data.

## 5. Failure and recovery

Crash the way infrastructure crashes (out of memory; no handler can catch it):

```
prefect deployment run "backfill/boreal-energy-backfill" --job-variable memory_limit=96Mi --job-variable memory_request=64Mi
```

The run shows Crashed; the failure automation e-mails. Evict a pod mid-run instead (`kubectl -n client-acme-manufacturing delete pod <backfill pod>`) and the run goes AwaitingRetry, is re-run in a fresh pod, and completes from its checkpoint.

Take a worker offline across a schedule:

```
kubectl -n client-boreal-energy scale deploy/prefect-worker --replicas=0
make run-feeds CLIENT=boreal-energy             # goes Late within ~15 s, late e-mail fires
kubectl -n client-boreal-energy scale deploy/prefect-worker --replicas=1   # picked up and completed
```

Runs still not started 30 minutes after going Late are cancelled by the `cancel stale late runs` automation so the next schedule covers them. `scripts/prebake_late.sh` does the scale-down and scale-up on a timer.

## 6. Apps and agents

`scripts/agent_shell.sh` opens a Claude Code session whose Prefect MCP server (reads) and `prefect` CLI (run, cancel) both use the service-account key from `.env`. `scripts/agent_demo.py start|watch|cancel` does the same three calls through the Python client. Every action appears in the workspace audit log under the service account.
