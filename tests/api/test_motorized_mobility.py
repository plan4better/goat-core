import pytest
from httpx import AsyncClient

from src.core.config import settings
from src.schemas.oev_gueteklasse import station_config_example
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
            "weekday": "sunday",
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
    # Check if job is finished
    assert job["status_simple"] == "finished"


async def test_single_isochrone_public_transport(
    client: AsyncClient, fixture_create_project
):
    project_id = fixture_create_project["id"]
    params = {
        "starting_points": {"latitude": [53.55390], "longitude": [10.01770]},
        "routing_type": {
            "mode": [
                "bus",
                "tram",
                "rail",
                "subway",
                "ferry",
                "cable_car",
                "gondola",
                "funicular",
            ],
            "egress_mode": "walk",
            "access_mode": "walk",
        },
        "travel_cost": {
            "max_traveltime": 60,
            "steps": 5,
        },
        "time_window": {
            "weekday": "weekday",
            "from_time": 25200,  # 7 AM
            "to_time": 39600,  # 9 AM
        },
        "isochrone_type": "polygon",
    }
    response = await client.post(
        f"{settings.API_V2_STR}/motorized-mobility/pt/isochrone?project_id={project_id}",
        json=params,
    )
    assert response.status_code == 201
    # Check if job is finished
    job = await check_job_status(client, response.json()["job_id"])
    # Check if job is finished
    assert job["status_simple"] == "finished"


async def test_nearby_station_access(client: AsyncClient, fixture_create_project):
    project_id = fixture_create_project["id"]
    params = {
        "starting_points": {"latitude": [52.5200], "longitude": [13.4050]},
        "access_mode": "walking",
        "speed": 5,
        "max_traveltime": 10,
        "mode": ["bus", "tram", "rail", "subway"],
        "time_window": {"weekday": "weekday", "from_time": 25200, "to_time": 32400},
    }
    response = await client.post(
        f"{settings.API_V2_STR}/motorized-mobility/nearby-station-access?project_id={project_id}",
        json=params,
    )
    assert response.status_code == 201
    # Check if job is finished
    job = await check_job_status(client, response.json()["job_id"])
    # Check if job is finished
    assert job["status_simple"] == "finished"
