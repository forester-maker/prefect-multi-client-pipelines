"""CI gate: every client file parses, and the extractor runs end to end in-process.

No Prefect server, no credentials beyond dummies, no network. Runs in seconds.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

os.environ.setdefault("ERP_CLIENT_SECRET", "ci-dummy")
os.environ.setdefault("SAP_PASSWORD", "ci-dummy")
os.environ.setdefault("PIPELINES_PAGE_LATENCY", "0")

from pipelines.config import load_clients  # noqa: E402


def main() -> None:
    import pipelines.lake as lake_mod
    from pipelines.extract import extract_feed
    from pipelines.lake import Lake
    from pipelines.sources import source_for
    from pipelines.transform import gold_site_kpis, silver_operations_snapshot

    with tempfile.TemporaryDirectory() as tmp:
        lake_mod.DATA_ROOT = Path(tmp)
        for cfg in load_clients():
            lake = Lake(cfg.lake_container)
            for feed in cfg.feeds:
                if feed.name in cfg.model_inputs:
                    extract_feed(source_for(feed.source, cfg.name), lake, feed, page_size=500)
            silver = silver_operations_snapshot(lake, cfg.model_inputs)
            gold = gold_site_kpis(lake)
            n15 = len(cfg.feeds_with_cadence("15m"))
            print(f"ok  {cfg.name:22} {len(cfg.feeds):3} feeds ({n15} @15m)  silver={silver.num_rows}  gold sites={gold.num_rows}  residency={cfg.residency}")


if __name__ == "__main__":
    main()
