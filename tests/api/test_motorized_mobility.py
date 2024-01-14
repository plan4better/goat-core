import pytest
from httpx import AsyncClient

from src.core.config import settings
from src.schemas.motorized_mobility import station_config_example
from tests.utils import check_job_status


@pytest.mark.asyncio
async def test_compute_isochrone_pt(client: AsyncClient, fixture_isochrone_pt):
    assert fixture_isochrone_pt["job_id"] is not None


@pytest.mark.asyncio
async def test_compute_isochrone_car(client: AsyncClient, fixture_isochrone_car):
    assert fixture_isochrone_car["job_id"] is not None


@pytest.mark.asyncio
async def test_oev_gueteklasse(
    client: AsyncClient, fixture_add_polygon_layer_to_project
):
    project_id = fixture_add_polygon_layer_to_project["project_id"]
    reference_layer_project_id = fixture_add_polygon_layer_to_project[
        "layer_project_id"
    ]

    payload = {
        "time_window": {
            "weekday": "weekday",
            "from_time": 25200,
            "to_time": 32400,
        },
        "reference_area_layer_project_id": reference_layer_project_id,
        "station_config": station_config_example,
    }

    response = await client.post(
        f"{settings.API_V2_STR}/motorized-mobility/oev-gueteklassen?project_id={project_id}",
        json=payload,
    )
    assert response.status_code == 201
    assert response.json()["job_id"] is not None

    job = await check_job_status(client, response.json()["job_id"])
    assert job["status_simple"] == "finished"


@pytest.mark.asyncio
async def test_trip_count_station(
    client: AsyncClient, fixture_add_polygon_layer_to_project
):
    project_id = fixture_add_polygon_layer_to_project["project_id"]
    reference_layer_project_id = fixture_add_polygon_layer_to_project[
        "layer_project_id"
    ]

    payload = {
        "reference_area_layer_project_id": reference_layer_project_id,
        "time_window": {
            "weekday": "weekday",
            "from_time": 25200,
            "to_time": 32400,
        },
    }

    response = await client.post(
        f"{settings.API_V2_STR}/motorized-mobility/trip-count-station?project_id={project_id}",
        json=payload,
    )
    assert response.status_code == 201
    assert response.json()["job_id"] is not None

    job = await check_job_status(client, response.json()["job_id"])
    assert job["status_simple"] == "finished"
