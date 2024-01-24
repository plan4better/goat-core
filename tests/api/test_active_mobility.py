import random

import pytest
from httpx import AsyncClient

from src.core.config import settings
from src.db.session import AsyncSession
from tests.utils import check_job_status


@pytest.mark.asyncio
async def test_single_isochrone_active_mobility_lat_lon(
    client: AsyncClient,
    fixture_create_project,
):
    project_id = fixture_create_project["id"]

    # Produce isochrone request payload
    params = {
        "starting_points": {
            "latitude": [51.201582802561035],
            "longitude": [9.481917667178564],
        },
        "routing_type": "walking",
        "travel_cost": {
            "max_traveltime": 30,
            "traveltime_step": 5,
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


@pytest.mark.asyncio
async def test_single_isochrone_active_mobility_dynamic(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_project,
    fixture_create_user,
):
    project_id = fixture_create_project["id"]
    user_id = fixture_create_user["id"]

    # Produce a random point within the network region
    lat, long = (
        await db_session.execute(
            """
                WITH geofence AS (
                    SELECT ST_Union(geom) AS geom
                    FROM temporal.network_region
                ),
                point AS (
                    SELECT (ST_Dump(ST_GeneratePoints(geofence.geom, 1))).geom AS geom
                    FROM geofence
                )
                SELECT ST_Y(point.geom), ST_X(point.geom) FROM point;
            """
        )
    ).fetchall()[0]

    # Produce a random isochrone request payload
    max_traveltime = random.randint(5, 30)
    traveltime_step = random.randint(1, int(max_traveltime / 2))
    params = {
        "starting_points": {"latitude": [lat], "longitude": [long]},
        "routing_type": random.choice(["walking", "bicycle", "pedelec"]),
        "travel_cost": {
            "max_traveltime": max_traveltime,
            "traveltime_step": traveltime_step,
            "speed": random.randint(1, 25),
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

    # Check if the resulting isochrone was actually saved to the database
    # And if the number of rows in the table equals the expected number of incremental polygons
    result_table = (
        f"{settings.USER_DATA_SCHEMA}.polygon_{str(user_id).replace('-', '')}"
    )
    num_rows = len(
        (await db_session.execute(f"SELECT * FROM {result_table};")).fetchall()
    )
    assert num_rows == (max_traveltime / traveltime_step)

    # TODO: We could also measure the area of the isochrone and see if it corresponds to an expected value.
