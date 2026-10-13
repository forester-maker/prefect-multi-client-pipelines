"""Emit feed-landed events by hand, to show the fan-in automation reacting to events alone.

    python scripts/emit_landed.py acme-manufacturing customer_orders
    python scripts/emit_landed.py acme-manufacturing work_orders inventory_parts

The compound trigger for the client needs all three model inputs within 15 minutes;
after the third event it starts the silver transform once, which then triggers gold.
"""

import sys

from prefect.events import emit_event

client, feeds = sys.argv[1], sys.argv[2:]
for feed in feeds:
    emit_event(
        event="pipelines.feed.landed",
        resource={"prefect.resource.id": f"pipelines.feed.{client}.{feed}", "prefect.resource.name": feed, "pipelines.client": client},
        payload={"feed": feed, "rows": 0, "source": "manual (demo)"},
    )
    print(f"emitted pipelines.feed.landed for {client}/{feed}")
