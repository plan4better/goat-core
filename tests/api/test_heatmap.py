import pytest
from httpx import AsyncClient

from src.core.config import settings
from src.db.session import AsyncSession
from tests.utils import check_job_status

@pytest.mark.asyncio
async def test_heatmap_gravity_active_mobility(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_add_aggregate_point_layer_to_project,
):
    project_id = fixture_add_aggregate_point_layer_to_project["project_id"]
    layer_project_id = fixture_add_aggregate_point_layer_to_project["source_layer_project_id"]

    # Produce heatmap request payload
    params = {
        "routing_type": "walking",
        "impedance_function": "gaussian",
        "opportunities": [
            {
                "opportunity_layer_project_id": layer_project_id,
                "max_traveltime": 30,
                "sensitivity": 300000,
                "destination_potential_column": None,
            }
        ],
    }

    # Call endpoint
    response = await client.post(
        f"{settings.API_V2_STR}/active-mobility/heatmap-gravity?project_id={project_id}",
        json=params,
    )
    assert response.status_code == 201

    # Check if job is finished
    job = await check_job_status(client, response.json()["job_id"])
    assert job["status_simple"] == "finished"


@pytest.mark.asyncio
async def test_heatmap_closest_average_active_mobility(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_add_aggregate_point_layer_to_project,
):
    project_id = fixture_add_aggregate_point_layer_to_project["project_id"]
    layer_project_id = fixture_add_aggregate_point_layer_to_project["source_layer_project_id"]

    # Produce heatmap request payload
    params = {
        "routing_type": "walking",
        "opportunities": [
            {
                "opportunity_layer_project_id": layer_project_id,
                "max_traveltime": 30,
                "number_of_destinations": 10,
            }
        ],
    }

    # Call endpoint
    response = await client.post(
        f"{settings.API_V2_STR}/active-mobility/heatmap-closest-average?project_id={project_id}",
        json=params,
    )
    assert response.status_code == 201

    # Check if job is finished
    job = await check_job_status(client, response.json()["job_id"])
    assert job["status_simple"] == "finished"
