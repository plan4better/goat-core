import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_compute_isochrone_pt(client: AsyncClient, fixture_isochrone_pt):
    assert fixture_isochrone_pt["job_id"] is not None

@pytest.mark.asyncio
async def test_compute_isochrone_car(client: AsyncClient, fixture_isochrone_car):
    assert fixture_isochrone_car["job_id"] is not None

@pytest.mark.asyncio
async def test_oev_gueteklasse(client: AsyncClient, fixture_oev_gueteklasse):
    assert fixture_oev_gueteklasse["job_id"] is not None