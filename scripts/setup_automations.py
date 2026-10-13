"""Platform automations. Idempotent: an automation with the same name is replaced.

Per client (from clients/*.yaml):
  pipelines/<client>: silver fan-in     all model-input feeds landed within 15 min -> run silver ONCE
  pipelines/<client>: gold after silver silver layer ready                        -> run gold
Platform-wide (anything tagged multi-client):
  pipelines: feed late                  a scheduled run goes Late                 -> e-mail
  pipelines: run failed or crashed      Failed / Crashed                          -> e-mail
  pipelines: cancel stale late runs     Late and still not Running after 30 min   -> cancel (no thundering herd after an outage)

Needs PREFECT_API_URL / PREFECT_API_KEY of the workspace and NOTIFY_EMAIL.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prefect.client.orchestration import get_client  # noqa: E402

from pipelines.config import load_clients  # noqa: E402

EMAIL = os.environ["NOTIFY_EMAIL"]  # who gets Late / Failed / Crashed e-mails
ENV = os.environ.get("PIPELINES_ENV", "prod")
SFX = "" if ENV == "prod" else "-dev"
TAGGED = {"prefect.resource.role": "tag", "prefect.resource.id": "prefect.tag.multi-client"}


def feed_trigger(client: str, feed: str) -> dict:
    return {
        "type": "event", "posture": "Reactive", "threshold": 1, "within": 0,
        "match": {"prefect.resource.id": f"pipelines.feed.{client}.{feed}"},
        "expect": ["pipelines.feed.landed"],
    }


def run(deployment_id: str) -> dict:
    return {"type": "run-deployment", "source": "selected", "deployment_id": deployment_id, "parameters": {}}


def email(subject: str, body: str) -> dict:
    return {"type": "send-email-notification", "emails": [EMAIL], "subject": subject, "body": body}


def per_client(cfg, dep_ids: dict[str, str]) -> list[dict]:
    silver = dep_ids[f"{cfg.name}-silver-transform{SFX}"]
    gold = dep_ids[f"{cfg.name}-gold-publish{SFX}"]
    return [
        {
            "name": f"pipelines/{cfg.name}: silver fan-in",
            "description": f"Run the silver model once when ALL of {cfg.model_inputs} have landed (within 15 min), not once per feed.",
            "enabled": True,
            "trigger": {"type": "compound", "require": "all", "within": 900,
                        "triggers": [feed_trigger(cfg.name, f) for f in cfg.model_inputs]},
            "actions": [run(silver)],
        },
        {
            "name": f"pipelines/{cfg.name}: gold after silver",
            "description": "Publish gold when the silver layer is ready.",
            "enabled": True,
            "trigger": {"type": "event", "posture": "Reactive", "threshold": 1, "within": 0,
                        "match": {"prefect.resource.id": f"pipelines.layer.{cfg.name}.silver"},
                        "expect": ["pipelines.layer.ready"]},
            "actions": [run(gold)],
        },
    ]


PLATFORM = [
    {
        "name": "pipelines: feed late",
        "description": "A scheduled run was not picked up on time (worker down, pool saturated, pod pending).",
        "enabled": True,
        "trigger": {"type": "event", "posture": "Reactive", "threshold": 1, "within": 0,
                    "match": {"prefect.resource.id": "prefect.flow-run.*"}, "match_related": TAGGED,
                    "expect": ["prefect.flow-run.Late"], "for_each": ["prefect.resource.id"]},
        "actions": [email("[Pipelines] LATE: {{ flow_run.name }}",
                          "{{ flow_run.name }} ({{ deployment.name }}) was due at {{ flow_run.expected_start_time }} and has not started.\n"
                          "Work pool: {{ work_pool.name }}\n\nOpen: {{ flow_run|ui_url }}")],
    },
    {
        "name": "pipelines: run failed or crashed",
        "description": "A run ended Failed or Crashed after its retries.",
        "enabled": True,
        "trigger": {"type": "event", "posture": "Reactive", "threshold": 1, "within": 0,
                    "match": {"prefect.resource.id": "prefect.flow-run.*"}, "match_related": TAGGED,
                    "expect": ["prefect.flow-run.Failed", "prefect.flow-run.Crashed"], "for_each": ["prefect.resource.id"]},
        "actions": [email("[Pipelines] {{ flow_run.state.name }}: {{ flow_run.name }}",
                          "{{ flow_run.name }} ({{ deployment.name }}) ended {{ flow_run.state.name }}.\n\n"
                          "{{ flow_run.state.message }}\n\nOpen: {{ flow_run|ui_url }}")],
    },
    {
        "name": "pipelines: cancel stale late runs",
        "description": "After an outage, runs that were due more than 30 min ago are cancelled instead of all starting at once; the next scheduled run covers them.",
        "enabled": True,
        "trigger": {"type": "event", "posture": "Proactive", "threshold": 1, "within": 1800,
                    "match": {"prefect.resource.id": "prefect.flow-run.*"}, "match_related": TAGGED,
                    "after": ["prefect.flow-run.Late"],
                    "expect": ["prefect.flow-run.Pending", "prefect.flow-run.Running", "prefect.flow-run.Completed",
                               "prefect.flow-run.Failed", "prefect.flow-run.Crashed", "prefect.flow-run.Cancelled",
                               "prefect.flow-run.Cancelling"],
                    "for_each": ["prefect.resource.id"]},
        "actions": [{"type": "cancel-flow-run", "source": "inferred"}],
    },
]


async def main() -> None:
    async with get_client() as client:
        deps = await client.read_deployments()
        dep_ids = {d.name: str(d.id) for d in deps}
        wanted = list(PLATFORM)
        for cfg in load_clients():
            try:
                wanted += per_client(cfg, dep_ids)
            except KeyError as e:
                print(f"skip {cfg.name}: deployment {e} not found (deploy first)")
        existing = (await client.request("POST", "/automations/filter", json={})).json()
        by_name = {a["name"]: a["id"] for a in existing}
        for auto in wanted:
            if auto["name"] in by_name:
                await client.request("DELETE", f"/automations/{by_name[auto['name']]}")
            r = await client.request("POST", "/automations/", json=auto)
            r.raise_for_status()
            print(f"{'replaced' if auto['name'] in by_name else 'created ':8} {auto['name']}")


if __name__ == "__main__":
    asyncio.run(main())
