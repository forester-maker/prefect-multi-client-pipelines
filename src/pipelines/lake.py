"""The client's lake.

Each client has its own ADLS Gen2 container; nothing is shared between clients.
On the demo cluster the container is a PersistentVolumeClaim mounted at /data,
addressed the same way (``<container>/bronze/<feed>/...``). Swapping to real
ADLS Gen2 is a storage adapter (adlfs), not a pipeline change.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DATA_ROOT = Path(os.environ.get("PIPELINES_DATA_DIR", "/data"))


class Lake:
    def __init__(self, container: str):
        self.root = DATA_ROOT / container

    def layer(self, layer: str, feed: str | None = None) -> Path:
        p = self.root / layer / feed if feed else self.root / layer
        p.mkdir(parents=True, exist_ok=True)
        return p

    def write(self, layer: str, feed: str, table: pa.Table, part: str = "snapshot") -> Path:
        path = self.layer(layer, feed) / f"{part}.parquet"
        pq.write_table(table, path)
        return path

    def glob(self, layer: str, feed: str) -> str:
        return str(self.root / layer / feed / "*.parquet")

    # --- checkpoints -------------------------------------------------------
    def checkpoint_path(self, feed: str) -> Path:
        return self.layer("_checkpoints") / f"{feed}.json"

    def read_checkpoint(self, feed: str) -> "Checkpoint | None":
        p = self.checkpoint_path(feed)
        return Checkpoint(**json.loads(p.read_text())) if p.exists() else None

    def write_checkpoint(self, cp: "Checkpoint") -> None:
        self.checkpoint_path(cp.feed).write_text(json.dumps(asdict(cp)))

    def clear_checkpoint(self, feed: str) -> None:
        self.checkpoint_path(feed).unlink(missing_ok=True)


@dataclass
class Checkpoint:
    feed: str
    target_rows: int
    cursor: int        # next row offset to fetch
    pages_done: int
    rows_done: int
