"""Client configuration.

Every client engagement is one YAML file under clients/. That file is the whole
"new client" change: feeds, cadences, which lake container to write to, and the
residency posture. Pipeline code is shared and never changes per client.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

CLIENTS_DIR = Path(__file__).resolve().parents[2] / "clients"
CADENCES = {"15m": 900, "hourly": 3600, "daily": 86400, "weekly": 604800}


@dataclass(frozen=True)
class Feed:
    name: str
    source: str            # "erp_cloud" | "sap"
    cadence: str           # "15m" | "hourly" | "daily" | "weekly"
    rows: int = 1000       # rows the source returns for a normal refresh
    busy: bool = False     # simulate "source busy" (HTTP 429) on first attempt

    @property
    def interval_seconds(self) -> int:
        return CADENCES[self.cadence]


@dataclass(frozen=True)
class ClientConfig:
    name: str
    display_name: str
    lake_container: str
    azure_subscription: str
    timezone: str = "UTC"
    residency: str = "standard"          # "standard" | "strict" (no run logs leave the cluster)
    model_inputs: list[str] = field(default_factory=list)  # feeds the silver model fans in on
    feeds: list[Feed] = field(default_factory=list)

    @property
    def pool(self) -> str:
        return f"client-{self.name}"

    @property
    def namespace(self) -> str:
        return f"client-{self.name}"

    def feeds_with_cadence(self, cadence: str) -> list[Feed]:
        return [f for f in self.feeds if f.cadence == cadence]

    def validate(self) -> None:
        names = [f.name for f in self.feeds]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"{self.name}: duplicate feeds {sorted(dupes)}")
        for f in self.feeds:
            if f.cadence not in CADENCES:
                raise ValueError(f"{self.name}/{f.name}: unknown cadence {f.cadence!r}")
            if f.source not in ("erp_cloud", "sap"):
                raise ValueError(f"{self.name}/{f.name}: unknown source {f.source!r}")
        missing = set(self.model_inputs) - set(names)
        if missing:
            raise ValueError(f"{self.name}: model_inputs not in feeds: {sorted(missing)}")
        if self.residency not in ("standard", "strict"):
            raise ValueError(f"{self.name}: residency must be standard|strict")


def load_client(path: Path) -> ClientConfig:
    raw = yaml.safe_load(path.read_text())
    feeds = [Feed(**f) for f in raw.pop("feeds", [])]
    cfg = ClientConfig(feeds=feeds, **raw)
    cfg.validate()
    return cfg


def load_clients(clients_dir: Path = CLIENTS_DIR, only: str | None = None) -> list[ClientConfig]:
    cfgs = [load_client(p) for p in sorted(clients_dir.glob("*.yaml"))]
    if only:
        cfgs = [c for c in cfgs if c.name == only]
        if not cfgs:
            raise SystemExit(f"no client named {only!r} in {clients_dir}")
    return cfgs
