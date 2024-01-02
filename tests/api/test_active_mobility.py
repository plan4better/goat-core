import pytest
from httpx import AsyncClient

from src.core.config import settings
from tests.utils import check_job_status


@pytest.mark.asyncio
async def test_single_isochrone_active_mobility_lat_lon(client: AsyncClient, fixture_create_project):
    project_id = fixture_create_project["id"]
    params = {
        "starting_points": {"latitude": [13.4050], "longitude": [52.5200]},
        "routing_type": "walking",
        "travel_cost": {
            "max_traveltime": 30,
            "traveltime_step": 10,
            "speed": 5,
        },
        "isochrone_type": "polygon",
        "polygon_difference": True,
    }
    response = await client.post(
        f"{settings.API_V2_STR}/active-mobility/isochrone?project_id={project_id}",
        json=params,
    )
    assert response.status_code == 201
    # Check if job is finished
    job = await check_job_status(client, response.json()["job_id"])
    # Check if job is finished
    assert job["status_simple"] == "finished"

    #TODO: It is bit hard to test the result of the isochrone calculation. 
    # What we could do though is to check if the data is inside the respective table. 
    # We could also measure the area of the isochrone and see if it corresponds to an expected value.

#TODO: Add more test cases
