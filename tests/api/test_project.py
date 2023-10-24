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
        f"{settings.API_V2_STR}/project/{fixture_create_project['id']}/initial-view-state", json=initial_view_state
    )
    assert response.status_code == 200
    updated_initial_view_state = response.json()
    assert updated_initial_view_state["latitude"] == initial_view_state["latitude"]
    assert updated_initial_view_state["longitude"] == initial_view_state["longitude"]
    assert updated_initial_view_state["zoom"] == initial_view_state["zoom"]


@pytest.mark.asyncio
async def test_create_layer_project(client: AsyncClient, fixture_create_layer_project):
    assert fixture_create_layer_project["id"] is not None

# Get layers from project

# Get by IDs

# Get initial view state

# Update initial view state

# Add layer in project

# Get layers from project

# Update layer in project

# Delete layer in project