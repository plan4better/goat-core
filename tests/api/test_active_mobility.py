import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_isochrone_active_mobility(client: AsyncClient, fixture_isochrone_active_mobility):
    assert fixture_isochrone_active_mobility["job_id"] is not None