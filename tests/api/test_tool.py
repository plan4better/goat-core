import pytest
from httpx import AsyncClient

from src.core.config import settings
from src.schemas.toolbox_base import ColumnStatisticsOperation
from tests.utils import check_if_job_finished


@pytest.mark.asyncio
async def test_join(client: AsyncClient, fixture_add_join_layers_to_project):

    layer_id_table, layer_id_gpkg, project_id = fixture_add_join_layers_to_project.values()
    #TODO: Check the produced results
    for operation in ColumnStatisticsOperation:
        # Request join endpoint
        params = {
            "target_layer_project_id": layer_id_gpkg,
            "target_field": "zipcode",
            "join_layer_project_id": layer_id_table,
            "join_field": "plz",
            "column_statistics": {
                "operation": operation.value,
                "field": "events",
            },
            "result_target": {
                "layer_name": f"{operation.value} result layer",
            },
        }

        response = await client.post(
            f"{settings.API_V2_STR}/tool/join?project_id={project_id}", json=params
        )
        assert response.status_code == 201
        job = await check_if_job_finished(client, response.json()["job_id"])
        assert job["status_simple"] == "finished"

@pytest.mark.asyncio
async def test_join_filter(client: AsyncClient, fixture_add_join_layers_to_project):
    layer_id_table, layer_id_gpkg, project_id = fixture_add_join_layers_to_project.values()

    # Update target layer project and add filter plz=80799
    response = await client.put(
        f"{settings.API_V2_STR}/project/{project_id}/layer/{layer_id_gpkg}",
        json={
            "query": {"op": "=", "args": [{"property": "plz"}, "80799"]},
        },
    )
    assert response.status_code == 200

    # Update join layer project and add filter to events > 500
    response = await client.put(
        f"{settings.API_V2_STR}/project/{project_id}/layer/{layer_id_table}",
        json={
            "query": {"op": ">", "args": [{"property": "events"}, "500"]},
        },
    )
    assert response.status_code == 200

    # Request join endpoint
    params = {
        "target_layer_project_id": layer_id_gpkg,
        "target_field": "zipcode",
        "join_layer_project_id": layer_id_table,
        "join_field": "plz",
        "column_statistics": {
            "operation": "sum",
            "field": "events",
        },
        "result_target": {
            "layer_name": "join result layer",
        },
    }
    response = await client.post(
        f"{settings.API_V2_STR}/tool/join?project_id={project_id}", json=params
    )
    assert response.status_code == 201
    job = await check_if_job_finished(client, response.json()["job_id"])
    assert job["status_simple"] == "finished"



@pytest.mark.asyncio
async def test_aggregate_points(client: AsyncClient, fixture_aggregation_points):
    assert fixture_aggregation_points["job_id"] is not None
