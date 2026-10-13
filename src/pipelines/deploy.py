"""Register every client's deployments from clients/*.yaml.

    python -m pipelines.deploy --env prod            # merge to main   -> client work pools, schedules on
    python -m pipelines.deploy --env dev             # any other branch -> client-dev pool, schedules paused
    python -m pipelines.deploy --env prod --client helix-automotive

Pipeline code is shared and already in the image; a client is parameters,
schedules, queues and tags. This is what GitHub Actions runs.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
from prefect.schedules import Cron, Interval

from pipelines.config import ClientConfig, load_clients
from pipelines.flows import backfill_feed, feeds_refresh, gold_publish, silver_transform

IMAGE = os.environ.get("PIPELINES_IMAGE", "client-pipelines:latest")
DEV_POOL = "client-dev"
NO_DUPLICATES = ConcurrencyLimitConfig(limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW)


def _version() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return "local"


def _quarter_hour_anchor() -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)


def deployments_for(cfg: ClientConfig, env: str):
    prod = env == "prod"
    pool = cfg.pool if prod else DEV_POOL
    sfx = "" if prod else "-dev"
    tags = ["multi-client", f"client:{cfg.name}", f"env:{env}"]
    job_env = {"PIPELINES_CLIENT": cfg.name}
    if cfg.residency == "strict":
        # Run logs stay in the client's cluster; Prefect Cloud sees states, not log lines.
        job_env["PREFECT_LOGGING_TO_API_ENABLED"] = "false"
    common = dict(work_pool_name=pool, tags=tags, job_variables={"env": job_env}, version=_version())
    base = {"client": cfg.name, "lake_container": cfg.lake_container}
    deps = []

    if feeds := cfg.feeds_with_cadence("15m"):
        deps.append(feeds_refresh.to_deployment(
            name=f"{cfg.name}-feeds-15m{sfx}",
            description=f"{cfg.display_name}: {len(feeds)} feeds every 15 minutes, one pod, fan-out inside.",
            work_queue_name="feeds",
            parameters={**base, "cadence": "15m", "feeds": [asdict(f) for f in feeds]},
            schedules=[Interval(timedelta(minutes=15), anchor_date=_quarter_hour_anchor(), timezone="UTC")],
            paused=not prod,
            concurrency_limit=NO_DUPLICATES,
            **common,
        ))
    if feeds := cfg.feeds_with_cadence("daily"):
        deps.append(feeds_refresh.to_deployment(
            name=f"{cfg.name}-feeds-daily{sfx}",
            description=f"{cfg.display_name}: {len(feeds)} daily feeds.",
            work_queue_name="feeds",
            parameters={**base, "cadence": "daily", "feeds": [asdict(f) for f in feeds]},
            schedules=[Cron("0 2 * * *", timezone=cfg.timezone)],
            paused=not prod,
            concurrency_limit=NO_DUPLICATES,
            **common,
        ))
    big = next(f for f in cfg.feeds if f.name == cfg.model_inputs[0])
    deps.append(backfill_feed.to_deployment(
        name=f"{cfg.name}-backfill{sfx}",
        description=f"{cfg.display_name}: paged, checkpointed re-extraction of one feed. Low-priority queue, never duplicated.",
        work_queue_name="backfill",
        parameters={**base, "feed": asdict(big)},
        concurrency_limit=NO_DUPLICATES,
        **common,
    ))
    deps.append(silver_transform.to_deployment(
        name=f"{cfg.name}-silver-transform{sfx}",
        description=f"{cfg.display_name}: DuckDB silver model. Triggered once when all of {cfg.model_inputs} have landed.",
        work_queue_name="default",
        parameters={**base, "model_inputs": cfg.model_inputs},
        **common,
    ))
    deps.append(gold_publish.to_deployment(
        name=f"{cfg.name}-gold-publish{sfx}",
        description=f"{cfg.display_name}: DuckDB gold KPIs. Triggered when the silver layer is ready.",
        work_queue_name="default",
        parameters=base,
        **common,
    ))
    return deps


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", choices=["dev", "prod"], required=True)
    ap.add_argument("--client", help="deploy only this client")
    args = ap.parse_args()
    for cfg in load_clients(only=args.client):
        for dep in deployments_for(cfg, args.env):
            dep_id = dep.apply(image=IMAGE)
            print(f"{args.env:4} {dep.work_pool_name:22} {dep.name:40} {dep_id}")


if __name__ == "__main__":
    main()
