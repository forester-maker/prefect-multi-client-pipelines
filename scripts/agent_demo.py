"""What an app or agent does through the API, with exactly the permissions of its service account.

    python scripts/agent_demo.py start  <client>     # start the client's 15-minute feeds now
    python scripts/agent_demo.py watch  <run-id>     # follow state changes until terminal
    python scripts/agent_demo.py cancel <run-id>     # cancel (the worker tears down the pod)

Every call is recorded in the workspace audit log against the service account.
"""

import asyncio
import sys
import time

from prefect import get_client
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterName
from prefect.states import Cancelling


async def start(client: str) -> None:
    async with get_client() as c:
        dep = (await c.read_deployments(deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=[f"{client}-feeds-15m"]))))[0]
        run = await c.create_flow_run_from_deployment(dep.id, tags=["triggered-by:agent"])
        print(f"started {run.name} ({run.id}) on {dep.work_pool_name}")


async def watch(run_id: str) -> None:
    async with get_client() as c:
        last = None
        while True:
            run = await c.read_flow_run(run_id)
            if run.state_name != last:
                print(f"{time.strftime('%H:%M:%S')}  {run.name}: {run.state_name}")
                last = run.state_name
            if run.state and run.state.is_final():
                break
            await asyncio.sleep(2)


async def cancel(run_id: str) -> None:
    async with get_client() as c:
        await c.set_flow_run_state(run_id, Cancelling(message="cancelled by agent"), force=True)
        print(f"cancel requested for {run_id}")


if __name__ == "__main__":
    cmd, arg = sys.argv[1], sys.argv[2]
    asyncio.run({"start": start, "watch": watch, "cancel": cancel}[cmd](arg))
