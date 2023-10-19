import pytest
from httpx import AsyncClient

from src.core.config import settings


@pytest.mark.asyncio
async def test_create_folder(client: AsyncClient, create_folder):
    assert create_folder["id"] is not None


@pytest.mark.asyncio
async def test_get_folder(
    client: AsyncClient,
    create_folder,
):
    response = await client.get(
        f"{settings.API_V2_STR}/folder/{create_folder['id']}",
    )
    assert response.status_code == 200
    assert response.json()["id"] == create_folder["id"]

@pytest.mark.asyncio
async def test_get_folders(
    client: AsyncClient,
    create_folders,
):
    response = await client.get(
        f"{settings.API_V2_STR}/folder?order_by=created_at&order=descendent&search=test&@page=1&size=50",
    )
    assert response.status_code == 200
    assert len(response.json()["items"]) == len(create_folders)

@pytest.mark.asyncio
async def test_update_folder(
    client: AsyncClient,
    create_folder
):
    response = await client.put(
        f"{settings.API_V2_STR}/folder/{create_folder['id']}",
        json={"name": "test2"}
    )
    assert response.status_code == 200
    assert response.json()["name"] == "test2"

@pytest.mark.asyncio
async def test_delete_folder(
    client: AsyncClient,
    create_folder,
):
    response = await client.delete(
        f"{settings.API_V2_STR}/folder/{create_folder['id']}",
    )
    assert response.status_code == 204
