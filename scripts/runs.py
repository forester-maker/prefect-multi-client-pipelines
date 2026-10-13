"""List recent non-scheduled the platform team runs (newest first)."""
import asyncio, sys
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterTags, FlowRunFilterState, FlowRunFilterStateName
from prefect.client.schemas.sorting import FlowRunSort

async def m(limit=int(sys.argv[1]) if len(sys.argv) > 1 else 15):
    async with get_client() as c:
        f = FlowRunFilter(tags=FlowRunFilterTags(all_=["multi-client"]),
                          state=FlowRunFilterState(name=FlowRunFilterStateName(not_any_=["Scheduled"])))
        runs = await c.read_flow_runs(flow_run_filter=f, sort=FlowRunSort.EXPECTED_START_TIME_DESC, limit=limit)
        for r in runs:
            t = (r.start_time or r.expected_start_time)
            print(f"{str(t)[11:19]}  {r.name:42} {r.state_name:10} {(r.work_pool_name or '')[:22]:22} {(r.state.message or '')[:70]}")
asyncio.run(m())
