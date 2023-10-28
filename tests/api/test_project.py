import pytest
from httpx import AsyncClient
from src.core.config import settings
from tests.utils import get_with_wrong_id
from src.schemas.project import initial_view_state_example


@pytest.mark.asyncio
async def test_create_project(client: AsyncClient, fixture_create_project):
    assert fixture_create_project["id"] is not None


@pytest.mark.asyncio
async def test_get_project(
    client: AsyncClient,
    fixture_create_project,
):
    response = await client.get(
        f"{settings.API_V2_STR}/project/{fixture_create_project['id']}",
    )
    assert response.status_code == 200
    assert response.json()["id"] == fixture_create_project["id"]





@pytest.mark.asyncio
async def test_get_projects(
    client: AsyncClient,
    fixture_create_projects,
):
    response = await client.get(
        f"{settings.API_V2_STR}/project?order_by=created_at&order=descendent&search=test&@page=1&size=50",
    )
    assert response.status_code == 200
    assert len(response.json()["items"]) == len(fixture_create_projects)


@pytest.mark.asyncio
async def test_get_project_wrong_id(client: AsyncClient, fixture_create_project):
    await get_with_wrong_id(client, "project")


@pytest.mark.asyncio
async def test_update_project(client: AsyncClient, fixture_create_project):
    response = await client.put(
        f"{settings.API_V2_STR}/project/{fixture_create_project['id']}", json={"name": "test2"}
    )
    assert response.status_code == 200
    assert response.json()["name"] == "test2"


@pytest.mark.asyncio
async def test_delete_project(
    client: AsyncClient,
    fixture_create_project,
):
    response = await client.delete(
        f"{settings.API_V2_STR}/project/{fixture_create_project['id']}",
    )
    assert response.status_code == 204

    # Check if project is deleted
    response = await client.get(
        f"{settings.API_V2_STR}/project/{fixture_create_project['id']}",
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_initial_view_state(
    client: AsyncClient,
    fixture_create_project,
):
    response = await client.get(
        f"{settings.API_V2_STR}/project/{fixture_create_project['id']}/initial-view-state",
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_update_initial_view_state(client: AsyncClient, fixture_create_project):
    initial_view_state = initial_view_state_example
    initial_view_state["latitude"] = initial_view_state_example["latitude"] + 2
    initial_view_state["longitude"] = initial_view_state_example["longitude"] + 2
    initial_view_state["zoom"] = initial_view_state_example["zoom"] + 2

    response = await client.put(
        f"{settings.API_V2_STR}/project/{fixture_create_project['id']}/initial-view-state",
        json=initial_view_state,
    )
    assert response.status_code == 200
    updated_initial_view_state = response.json()
    assert updated_initial_view_state["latitude"] == initial_view_state["latitude"]
    assert updated_initial_view_state["longitude"] == initial_view_state["longitude"]
    assert updated_initial_view_state["zoom"] == initial_view_state["zoom"]


@pytest.mark.asyncio
async def test_create_layer_project(client: AsyncClient, fixture_create_layer_project):
    assert fixture_create_layer_project["project_id"] is not None


@pytest.mark.asyncio
async def test_get_layer_project(client: AsyncClient, fixture_create_layer_project):
    response = await client.get(
        f"{settings.API_V2_STR}/project/{fixture_create_layer_project['project_id']}/layer",
    )
    assert response.status_code == 200
    assert response.json()[0]["id"] == fixture_create_layer_project["layer_project"][0]["id"]


# TODO: Add test for style and query
@pytest.mark.asyncio
async def test_update_layer_project(client: AsyncClient, fixture_create_layer_project):
    project_id = fixture_create_layer_project["project_id"]
    layer_id = fixture_create_layer_project["layer_project"][0]["id"]
    response = await client.put(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_id={layer_id}",
        json={"name": "test2"},
    )
    assert response.status_code == 200
    assert response.json()["name"] == "test2"


@pytest.mark.asyncio
async def test_delete_layer_project(client: AsyncClient, fixture_create_layer_project):
    project_id = fixture_create_layer_project["project_id"]
    layer_id = fixture_create_layer_project["layer_project"][0]["id"]

    # Delete layer
    response = await client.delete(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_id={layer_id}",
    )
    assert response.status_code == 204

    # Check if layer is deleted
    response = await client.get(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_id}",
    )
    assert response.status_code == 200
    assert len(response.json()) == 0