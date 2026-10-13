"""The four pipeline flows every client gets.

    feeds_refresh     scheduled; fans out one task per feed inside ONE pod
    backfill_feed     on demand; paged + checkpointed, runs in its own pod on the low-priority queue
    silver_transform  event-driven; fires once when all model input feeds have landed
    gold_publish      event-driven; fires when the silver layer is ready

Clients differ only by parameters (which come from clients/<name>.yaml at deploy time).
"""

from __future__ import annotations

from dataclasses import asdict

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.events import emit_event
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.tasks import exponential_backoff

from pipelines.config import Feed
from pipelines.extract import extract_feed
from pipelines.lake import Lake
from pipelines.sources import source_for
from pipelines.transform import gold_site_kpis, silver_operations_snapshot

FEED_PAGE_SIZE = 500


def _landed(client: str, feed: str, summary: dict) -> None:
    """Tell the platform a feed landed. Automations fan these in per client."""
    emit_event(
        event="pipelines.feed.landed",
        resource={
            "prefect.resource.id": f"pipelines.feed.{client}.{feed}",
            "prefect.resource.name": feed,
            "pipelines.client": client,
        },
        payload=summary,
    )


def _layer_ready(client: str, layer: str, rows: int) -> None:
    emit_event(
        event="pipelines.layer.ready",
        resource={"prefect.resource.id": f"pipelines.layer.{client}.{layer}", "pipelines.client": client},
        payload={"layer": layer, "rows": rows},
    )


# --------------------------------------------------------------------------- feeds
@task(
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=5),  # 5s, 10s, 20s
    retry_jitter_factor=0.3,
    task_run_name="extract {feed[name]}",
)
def extract_one(client: str, lake_container: str, feed: dict) -> dict:
    spec = Feed(**feed)
    summary = extract_feed(source_for(spec.source, client), Lake(lake_container), spec, page_size=FEED_PAGE_SIZE)
    _landed(client, spec.name, summary)
    return summary


@flow(
    name="feeds-refresh",
    flow_run_name="{client} feeds {cadence}",
    task_runner=ThreadPoolTaskRunner(max_workers=12),
    log_prints=True,
)
def feeds_refresh(client: str, lake_container: str, cadence: str, feeds: list[dict]) -> dict:
    """Refresh every feed of one cadence for one client. Fifty feeds, one pod, twelve at a time."""
    futures = [extract_one.submit(client, lake_container, f) for f in feeds]
    wait(futures)
    done = [f.result(raise_on_failure=False) for f in futures]
    failed = [f["name"] for f, r in zip(feeds, done) if not isinstance(r, dict)]
    ok = [r for r in done if isinstance(r, dict)]
    create_table_artifact(
        key=f"{client}-feeds-{cadence}",
        table=[{"feed": r["feed"], "rows": r["rows"], "pages": r["pages"]} for r in ok],
        description=f"{client}: {len(ok)}/{len(feeds)} feeds landed ({cadence})",
    )
    print(f"{client}: {len(ok)} feeds landed, {sum(r['rows'] for r in ok):,} rows; failed: {failed or 'none'}")
    if failed:
        raise RuntimeError(f"{len(failed)} feed(s) failed after retries: {failed}")
    return {"landed": len(ok), "rows": sum(r["rows"] for r in ok)}


# ------------------------------------------------------------------------ backfill
@task(retries=2, retry_delay_seconds=5, task_run_name="backfill {feed[name]}")
def backfill_pages(client: str, lake_container: str, feed: dict, rows: int, page_size: int,
                   page_seconds: float, fail_at_page: int | None) -> dict:
    spec = Feed(**{**feed, "busy": False})
    source = source_for(spec.source, client, latency=page_seconds)
    return extract_feed(source, Lake(lake_container), spec, page_size=page_size, rows=rows, fail_at_page=fail_at_page)


@flow(name="backfill", flow_run_name="{client} backfill {feed[name]}", log_prints=True)
def backfill_feed(client: str, lake_container: str, feed: dict, rows: int = 3_000_000, page_size: int = 100_000,
                  page_seconds: float = 3.0, fail_at_page: int | None = None) -> dict:
    """Re-extract one feed from row zero. Same extractor as the 15-minute feeds, bigger pages.

    rows / page_size pages are fetched one at a time; each page is committed to
    the lake and the checkpoint before the next is requested. Set fail_at_page
    to watch a crash resume from the checkpoint instead of restarting.
    """
    summary = backfill_pages(client, lake_container, feed, rows, page_size, page_seconds, fail_at_page)
    _landed(client, feed["name"], summary)
    print(f"{client}: backfilled {summary['rows']:,} rows of {feed['name']} in {summary['pages']} pages")
    return summary


# ------------------------------------------------------------------- transformations
@task(task_run_name="silver operations_snapshot")
def build_silver(lake_container: str, model_inputs: list[str]) -> int:
    return silver_operations_snapshot(Lake(lake_container), model_inputs).num_rows


@flow(name="silver-transform", flow_run_name="{client} silver", log_prints=True)
def silver_transform(client: str, lake_container: str, model_inputs: list[str]) -> int:
    """Runs once per batch of landed inputs, not once per feed (see the fan-in automation)."""
    rows = build_silver(lake_container, model_inputs)
    print(f"{client}: silver operations_snapshot rebuilt from {model_inputs}: {rows:,} rows")
    _layer_ready(client, "silver", rows)
    return rows


@task(task_run_name="gold site_kpis")
def build_gold(lake_container: str) -> list[dict]:
    return gold_site_kpis(Lake(lake_container)).to_pylist()


@flow(name="gold-publish", flow_run_name="{client} gold", log_prints=True)
def gold_publish(client: str, lake_container: str) -> int:
    kpis = build_gold(lake_container)
    create_table_artifact(key=f"{client}-site-kpis", table=kpis, description=f"{client}: site KPIs published to gold")
    create_markdown_artifact(
        key=f"{client}-gold-summary",
        markdown=f"## {client}: gold published\n\n{len(kpis)} sites · {sum(k['parts'] for k in kpis)} parts · "
                 f"{sum(k['parts_at_risk'] for k in kpis)} parts at risk",
    )
    print(f"{client}: gold site_kpis published for {len(kpis)} sites")
    _layer_ready(client, "gold", len(kpis))
    return len(kpis)
