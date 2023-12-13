import os
from uuid import uuid4

import pytest
from httpx import AsyncClient

from src.core.config import settings
from src.schemas.layer import AreaStatisticsOperation, ColumnStatisticsOperation
from tests.utils import get_with_wrong_id


@pytest.mark.asyncio
async def test_files_upload(client: AsyncClient, fixture_upload_files):
    assert fixture_upload_files is not None


@pytest.mark.asyncio
async def test_files_upload_invalid(client: AsyncClient, fixture_upload_file_invalid):
    job_id = fixture_upload_file_invalid

    # Check if folder was deleted with the data using os
    assert os.path.exists(f"{settings.DATA_DIR}/{job_id}") is False


@pytest.mark.asyncio
async def test_create_internal_layer(
    client: AsyncClient, fixture_create_internal_layers
):
    assert fixture_create_internal_layers is not None


@pytest.mark.asyncio
async def test_create_external_layers(
    client: AsyncClient, fixture_create_external_layers
):
    assert fixture_create_external_layers is not None


@pytest.mark.asyncio
async def test_get_internal_layer(
    client: AsyncClient, fixture_create_internal_feature_layer
):
    layer_id = fixture_create_internal_feature_layer["id"]
    response = await client.get(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 200
    assert response.json()["id"] == layer_id


@pytest.mark.asyncio
async def test_get_external_layer(client: AsyncClient, fixture_create_external_layer):
    layer_id = fixture_create_external_layer["id"]
    response = await client.get(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 200
    assert response.json()["id"] == layer_id


@pytest.mark.asyncio
async def test_get_layer_wrong_id(client: AsyncClient, fixture_create_external_layer):
    await get_with_wrong_id(client, "layer")


@pytest.mark.asyncio
async def test_update_internal_layer(
    client: AsyncClient, fixture_create_internal_feature_layer
):
    layer_id = fixture_create_internal_feature_layer["id"]
    layer_dict = fixture_create_internal_feature_layer
    layer_dict["name"] = "Updated name"
    layer_dict["description"] = "Updated description"
    layer_dict["tags"] = ["Update tag 1", "Update tag 2"]
    layer_dict["thumbnail_url"] = "https://updated-example.com"
    response = await client.put(
        f"{settings.API_V2_STR}/layer/{layer_id}", json=layer_dict
    )
    updated_layer = response.json()
    assert response.status_code == 200
    assert updated_layer["name"] == "Updated name"
    assert updated_layer["description"] == "Updated description"
    assert set(updated_layer["tags"]) == {"Update tag 1", "Update tag 2"}
    assert updated_layer["thumbnail_url"] == "https://updated-example.com"


@pytest.mark.asyncio
async def test_delete_internal_layers(
    client: AsyncClient, fixture_delete_internal_layers
):
    return


@pytest.mark.asyncio
async def test_delete_external_layers(
    client: AsyncClient, fixture_delete_external_layers
):
    return


@pytest.mark.asyncio
async def test_get_feature_cnt(
    client: AsyncClient, fixture_create_internal_feature_layer
):
    layer_id = fixture_create_internal_feature_layer["id"]
    query = '{"op": "=", "args": [{"property": "category"}, "bus_stop"]}'
    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/feature-count?query={str(query)}"
    )
    assert response.status_code == 200
    assert response.json()["filtered_count"] == 2
    assert response.json()["total_count"] == 26


@pytest.mark.asyncio
async def test_get_area_statistics(
    client: AsyncClient, fixture_create_internal_feature_polygon_layer
):
    layer_id = fixture_create_internal_feature_polygon_layer["id"]
    expected_result = {
        "sum": 5813696342.208582,
        "mean": 2906848171.10429092,
        "min": 83206235.43294483,
        "max": 5730490106.775637
    }

    query = '{"op": ">", "args": [{"property": "id"}, "3"]}'
    # Request each statistical operation
    for operation in AreaStatisticsOperation:
        response = await client.get(
            f"{settings.API_V2_STR}/layer/{layer_id}/area/{operation.value}?query={str(query)}"
        )
        assert response.status_code == 200
        # Check if results are same as expected results
        assert response.json()[operation.value] == expected_result[operation.value]


@pytest.mark.asyncio
async def test_get_area_statistics_no_query(
    client: AsyncClient, fixture_create_internal_feature_polygon_layer
):
    layer_id = fixture_create_internal_feature_polygon_layer["id"]

    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/area/sum"
    )
    assert response.status_code == 200
    return

@pytest.mark.asyncio
async def test_get_wrong_area_statistics_wrong_geom_type(
    client: AsyncClient, fixture_create_internal_feature_layer
):
    layer_id = fixture_create_internal_feature_layer["id"]
    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/area/sum"
    )
    assert response.status_code == 422
    return

@pytest.mark.asyncio
async def test_get_unique_values_layer_pagination(
    client: AsyncClient, fixture_create_internal_feature_layer
):
    layer_id = fixture_create_internal_feature_layer["id"]
    column = "name"

    # Request the first 5 unique values
    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/unique-values/{column}?page=1&size=5&order=descendent"
    )
    assert response.status_code == 200
    first_five = response.json()
    assert len(first_five) == 5

    # Request the next 5 unique values
    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/unique-values/{column}?page=2&size=5"
    )
    assert response.status_code == 200
    next_five = response.json()
    assert len(next_five) == 5

    # Check that no value of the first five is in the next five
    assert set(first_five).isdisjoint(next_five)

    # Request the first 10 unique values
    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/unique-values/{column}?page=1&size=10"
    )
    assert response.status_code == 200
    first_ten = response.json()
    assert len(first_ten) == 10

    # Check that the first and next five are the same as the 10 unique values
    assert {**first_five, **next_five} == first_ten

    return


@pytest.mark.asyncio
async def test_get_unique_values_layer_query(
    client: AsyncClient, fixture_create_internal_feature_layer
):
    layer_id = fixture_create_internal_feature_layer["id"]
    column = "name"
    query = '{"op": "=", "args": [{"property": "category"}, "bus_stop"]}'

    # Request the first 5 unique values
    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/unique-values/{column}?query={str(query)}&page=1&size=5"
    )
    assert response.status_code == 200
    values = response.json()
    assert len(values) == 1
    assert values["Röthensteig"] == 2
    return


@pytest.mark.asyncio
async def test_get_unique_values_wrong_layer_id(
    client: AsyncClient, fixture_create_internal_feature_layer
):
    layer_id = uuid4()
    column = "name"

    # Request the first 5 unique values
    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/unique-values/{column}?page=1&size=5"
    )
    assert response.status_code == 404
    return


@pytest.mark.asyncio
async def test_get_unique_values_wrong_layer_type(
    client: AsyncClient, fixture_create_external_layer
):
    layer_id = fixture_create_external_layer["id"]
    column = "name"

    # Request the first 5 unique values
    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/unique-values/{column}?page=1&size=5"
    )
    assert response.status_code == 422
    return


@pytest.mark.asyncio
async def test_get_unique_value_wrong_column_name(
    client: AsyncClient, fixture_create_internal_feature_layer
):
    layer_id = fixture_create_internal_feature_layer["id"]
    column = "wrong_column"

    # Request the first 5 unique values
    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/unique-values/{column}?page=1&size=5"
    )
    assert response.status_code == 404
    return


@pytest.mark.asyncio
async def test_get_statistics_column(
    client: AsyncClient, fixture_create_internal_table_layer
):
    layer_id = fixture_create_internal_table_layer["id"]
    column = "einwohnerzahl_ewz"

    base_results = {
        "max": 3677472,
        "min": 34091,
        "mean": 208092.81,
    }
    results = {
        "quantile": {
            **base_results,
            "breaks": [88430, 122724, 155900, 203831, 288097],
        },
        "standard_deviation": {
            **base_results,
            "breaks": [
                85339.48205224,
                330846.13794776,
                576352.7938432801,
                821859.4497388001,
            ],
        },
        "equal_interval": {
            **base_results,
            "breaks": [641321, 1248551, 1855781, 2463011, 3070241],
        },
        "heads_and_tails": {
            **base_results,
            "breaks": [
                208092.81,
                383477.3106060606,
                720935.3636363636,
                1668162.6666666667,
                2765703.5,
            ],
        },
    }

    # Request each statistical operation
    for operation in ColumnStatisticsOperation:
        if operation.value == ColumnStatisticsOperation.standard_deviation.value:
            # There is no breaks parameter for standard deviation
            response = await client.get(
                f"{settings.API_V2_STR}/layer/{layer_id}/class-breaks/{operation.value}/{column}?stripe_zeros=true"
            )
        else:
            response = await client.get(
                f"{settings.API_V2_STR}/layer/{layer_id}/class-breaks/{operation.value}/{column}?breaks=5&stripe_zeros=true"
            )
        assert response.status_code == 200
        # Check that the results are the same as the expected results. Avoid checking the breaks for standard deviation as they can slighly differ.
        if operation.value != ColumnStatisticsOperation.standard_deviation.value:
            assert response.json() == results[operation.value]
    return


@pytest.mark.asyncio
async def test_get_statistics_column_wrong_layer_id(
    client: AsyncClient, fixture_create_internal_table_layer
):
    layer_id = uuid4()
    column = "einwohnerzahl_ewz"

    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/class-breaks/quantile/{column}?breaks=5&stripe_zeros=true"
    )
    assert response.status_code == 404
    return


# Get not existing column name
@pytest.mark.asyncio
async def test_get_statistics_column_wrong_column_name(
    client: AsyncClient, fixture_create_internal_table_layer
):
    layer_id = fixture_create_internal_table_layer["id"]
    column = "wrong_column"

    response = await client.get(
        f"{settings.API_V2_STR}/layer/{layer_id}/class-breaks/quantile/{column}?breaks=5&stripe_zeros=true"
    )
    assert response.status_code == 404
    return


# Some further test cases
"""Valid File Import
Use an invalid job ID for import
Ensure the API responds with the proper HTTP error (404 Not Found)

Create multiple layers
Use the "get-by-ids" endpoint to retrieve them in bulk using their IDs
Ensure all are returned as expected
Retrieve Layers with Filters

Create multiple layers with varying attributes (different layer_type, feature_layer_type, etc.)
Use the layered retrieval endpoint with different combinations of filters
Ensure the results match the expected filtered layers"""
