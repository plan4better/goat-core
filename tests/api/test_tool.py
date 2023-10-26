import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_join(client: AsyncClient, fixture_join):
    assert fixture_join["job_id"] is not None

@pytest.mark.asyncio
async def test_aggregate_points(client: AsyncClient, fixture_aggregation_points):
    assert fixture_aggregation_points["job_id"] is not None