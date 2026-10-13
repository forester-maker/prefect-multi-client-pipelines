# Multi-client data pipelines on Prefect Cloud

One orchestrator running the same pipelines for many clients, each client isolated in its own
infrastructure, with one view for the engineers who operate them. Built for a platform team that
extracts ERP and SAP data into each client's lake (ADLS Gen2 in Azure), transforms it with DuckDB,
and publishes results back to the client.

```
                 GitHub ──push──▶ Actions ──deploy──▶ Prefect Cloud (control plane: schedules, states, events, RBAC, audit)
                                                            ▲ outbound HTTPS only
   ┌────────────────────────────────────────────────────────┼────────────────────────────────────────┐
   │ client's cloud subscription                            │                                        │
   │   Kubernetes namespace client-<name> ── worker ────────┘   (polls pool client-<name>)           │
   │     one Job pod per flow run: 50 feeds fan out INSIDE one pod; backfills get their own pod      │
   │     credentials from the client's secret store (k8s Secret here), never stored in Prefect Cloud │
   │   lake container <name>-lake (PVC here): bronze/ silver/ gold/ _checkpoints/                    │
   └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## What it demonstrates

| Requirement | Where it lives |
|---|---|
| New client from GitHub to production, everything in code | `clients/<name>.yaml` + `.github/workflows/deploy.yml` → `src/pipelines/deploy.py`. Branch push = dev pool, paused. Merge to main = client pool, schedules on. |
| Where the cadence of each job is set | `cadence:` on each feed in the client file; the deploy script turns cadences into schedules. |
| Where to see success and failure, with alerts | One dashboard filtered by `client:` tag or work pool; e-mail automations in `scripts/setup_automations.py`. Slack and Teams are the same action with a webhook. |
| Client data never reaches Prefect Cloud | Workers and data stay in the client's subscription. Cloud stores states, parameters and events. `residency: strict` additionally keeps run logs in-cluster (`PREFECT_LOGGING_TO_API_ENABLED=false`). |
| Large and small jobs side by side, no duplicates, no clean-up | Per-pool queues `feeds` (priority 1) and `backfill` (priority 5); deployment concurrency limit 1 with `CANCEL_NEW`; checkpoint-per-page extractor in `src/pipelines/extract.py`. |
| Retry when the source is busy; alert when late | `extract_one` task: 3 retries with exponential backoff against a simulated HTTP 429; the `feed late` automation. |
| Event-driven transforms, fan-in runs once | Feeds emit `pipelines.feed.landed`; a compound trigger waits for all model inputs within 15 minutes and runs silver once; `pipelines.layer.ready` chains gold. |
| 50-million-row extraction | Same extractor, bigger pages: fetch page → write parquet part → commit checkpoint → next page. Memory is one page; a crash resumes from the last committed page (`fail_at_page` parameter shows it). |
| Apps and agents | Prefect MCP server for reads, CLI/API for run and cancel, under a service account with a workspace role. Every action is in the audit log. |

## Repository layout

```
clients/                 one YAML per client (feeds, cadences, lake container, residency)
src/pipelines/config.py  client file schema and validation
src/pipelines/sources.py ERP / SAP clients (simulated here: cursor paging, 429 on busy feeds)
src/pipelines/extract.py paged + checkpointed extractor
src/pipelines/transform.py DuckDB silver / gold
src/pipelines/flows.py   feeds_refresh, backfill_feed, silver_transform, gold_publish
src/pipelines/deploy.py  clients/*.yaml -> Prefect deployments (what Actions runs)
src/pipelines/validate.py CI gate: parse + run in-process, no server needed
k8s/, scripts/           per-client namespace / worker / pool / queues; automations; demo helpers
```

Adding a client is one file. The pipeline code and the image are shared.

## Running it on a local cluster

Tested on docker-desktop's Kubernetes (its node is named `desktop-control-plane`; adjust `make load`
for kind or minikube). You need a Prefect Cloud workspace and a service-account API key.

```
cp .env.example .env            # workspace URL, API key, NOTIFY_EMAIL
uv venv .venv && uv pip install -e ".[dev]"
make validate                   # what CI runs, no server needed
make load                       # build the image and import it into the cluster node
make dev                        # shared dev pool for feature branches
make client CLIENT=acme-manufacturing
make client CLIENT=boreal-energy
make deploy ENV=prod            # same as merging to main
make automations
make run-feeds CLIENT=acme-manufacturing
make run-backfill CLIENT=acme-manufacturing
```

For GitHub Actions, add repository secrets `PREFECT_API_URL` and `PREFECT_API_KEY`.

See `WALKTHROUGH.md` for a guided tour that exercises each capability.
