"""Paged, checkpointed extraction.

This is the one piece of code behind both the 15-minute feeds and the
50-million-row backfills. A page is fetched, written to the lake as its own
parquet part, and the checkpoint is advanced. Memory stays flat at one page,
and a crash or a 429 resumes from the last committed page instead of row zero.
"""

from __future__ import annotations

import logging

from pipelines.config import Feed
from pipelines.lake import Checkpoint, Lake
from pipelines.sources import PagedSource

log = logging.getLogger("pipelines.extract")


class SimulatedCrash(RuntimeError):
    """Demo switch: raise mid-backfill to show resume-from-checkpoint."""


def extract_feed(source: PagedSource, lake: Lake, feed: Feed, *, page_size: int, rows: int | None = None,
                 resume: bool = True, fail_at_page: int | None = None) -> dict:
    """Extract ``feed`` into ``lake`` one page at a time. Returns a small summary."""
    target = rows or feed.rows
    cp = lake.read_checkpoint(feed.name) if resume else None
    if cp and cp.target_rows == target and cp.cursor < target:
        log.info("resuming %s from page %d (%d/%d rows already landed)", feed.name, cp.pages_done + 1, cp.rows_done, target)
    else:
        cp = Checkpoint(feed=feed.name, target_rows=target, cursor=0, pages_done=0, rows_done=0)

    while cp.cursor < target:
        page = source.fetch_page(feed.name, cp.cursor, page_size, target, busy=feed.busy)
        part = f"part-{cp.pages_done:05d}"
        lake.write("bronze", feed.name, page.rows, part=part)
        cp = Checkpoint(feed.name, target, page.next_cursor or target, cp.pages_done + 1, cp.rows_done + page.rows.num_rows)
        lake.write_checkpoint(cp)  # commit point: this page is durable before we ask for the next
        log.info("%s page %d landed (%d rows, %d/%d)", feed.name, cp.pages_done, page.rows.num_rows, cp.rows_done, target)
        if fail_at_page and cp.pages_done == fail_at_page:
            raise SimulatedCrash(f"simulated crash after page {fail_at_page} of {feed.name}")

    lake.clear_checkpoint(feed.name)
    return {"feed": feed.name, "rows": cp.rows_done, "pages": cp.pages_done, "path": lake.glob("bronze", feed.name)}
