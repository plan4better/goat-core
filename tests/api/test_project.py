import pytest
from httpx import AsyncClient

from src.core.config import settings


@pytest.mark.asyncio
async def test_create_project(client: AsyncClient, create_project):
    assert create_project["id"] is not None


@pytest.mark.asyncio
async def test_get_project(
    client: AsyncClient,
    create_project,
):
    response = await client.get(
        f"{settings.API_V2_STR}/project/{create_project['id']}",
    )
    assert response.status_code == 200
    assert response.json()["id"] == create_project["id"]


@pytest.mark.asyncio
async def test_get_projects(
    client: AsyncClient,
    create_projects,
):
    response = await client.get(
        f"{settings.API_V2_STR}/project?order_by=created_at&order=descendent&search=test&@page=1&size=50",
    )
    assert response.status_code == 200
    assert len(response.json()["items"]) == len(create_projects)


@pytest.mark.asyncio
async def test_update_project(client: AsyncClient, create_project):
    response = await client.put(
        f"{settings.API_V2_STR}/project/{create_project['id']}", json={"name": "test2"}
    )
    assert response.status_code == 200
    assert response.json()["name"] == "test2"


@pytest.mark.asyncio
async def test_delete_project(
    client: AsyncClient,
    create_project,
):
    response = await client.delete(
        f"{settings.API_V2_STR}/project/{create_project['id']}",
    )
    assert response.status_code == 204
