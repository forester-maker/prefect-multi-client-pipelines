"""Source system clients.

In production these wrap the ERP's OData endpoints and the SAP OData /
RFC endpoints. Here they are in-process simulators with the same shape:
cursor-paged reads, deterministic rows, and the failure modes we care about
(a busy source answering HTTP 429). The extractor code is identical either way.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

import pyarrow as pa

PAGE_LATENCY_SECONDS = float(os.environ.get("PIPELINES_PAGE_LATENCY", "0.15"))
_busy_seen: set[str] = set()


class SourceBusy(Exception):
    """The source answered 429 Too Many Requests. Retry with backoff."""


@dataclass
class Page:
    rows: pa.Table
    next_cursor: int | None


def _credential(env_var: str) -> str:
    # Credentials are injected into the pod from the client's own secret store
    # (Azure Key Vault via the CSI driver, here a Kubernetes Secret). They are
    # never stored in Prefect Cloud and never logged.
    value = os.environ.get(env_var)
    if not value:
        raise RuntimeError(f"missing credential {env_var} in this worker's environment")
    return value


def _synthetic_rows(feed: str, start: int, n: int, site_count: int = 4) -> pa.Table:
    ids = list(range(start, start + n))
    sites = [f"SITE{(i % site_count) + 1:02d}" for i in ids]
    parts = [f"P-{(i * 7919) % 500:04d}" for i in ids]
    qty = [((i * 31) % 97) + 1 for i in ids]
    amount = [round(q * (((i * 13) % 50) + 5) * 1.0, 2) for i, q in zip(ids, qty)]
    status = [("RELEASED", "STARTED", "CLOSED", "PLANNED")[i % 4] for i in ids]
    day = [f"2026-10-{(i % 28) + 1:02d}" for i in ids]
    return pa.table(
        {
            "record_id": pa.array(ids, pa.int64()),
            "site": sites,
            "part_no": parts,
            "qty": pa.array(qty, pa.int32()),
            "amount": pa.array(amount, pa.float64()),
            "status": status,
            "doc_date": day,
            "source_feed": [feed] * n,
        }
    )


class PagedSource:
    """Common cursor-paged read. Subclasses only differ in auth and naming."""

    name = "generic"

    def __init__(self, client: str, credential_env: str, latency: float = PAGE_LATENCY_SECONDS):
        self.client = client
        self.latency = latency  # seconds per page; backfills set this higher to mimic big pages
        self._token = _credential(credential_env)  # held in memory only

    def fetch_page(self, feed: str, cursor: int, page_size: int, total_rows: int, busy: bool = False) -> Page:
        key = f"{self.client}/{feed}"
        if busy and key not in _busy_seen:
            # First attempt against a "busy" feed fails the way a real source does.
            _busy_seen.add(key)
            raise SourceBusy(f"{self.name} answered 429 for {feed} (rate limited)")
        time.sleep(self.latency)
        n = max(0, min(page_size, total_rows - cursor))
        rows = _synthetic_rows(feed, cursor, n)
        nxt = cursor + n
        return Page(rows=rows, next_cursor=nxt if nxt < total_rows else None)


class ERPCloudSource(PagedSource):
    name = "ERP Cloud"

    def __init__(self, client: str):
        super().__init__(client, "ERP_CLIENT_SECRET")


class SAPSource(PagedSource):
    name = "SAP"

    def __init__(self, client: str):
        super().__init__(client, "SAP_PASSWORD")


def source_for(kind: str, client: str, latency: float | None = None) -> PagedSource:
    src = {"erp_cloud": ERPCloudSource, "sap": SAPSource}[kind](client)
    if latency is not None:
        src.latency = latency
    return src
