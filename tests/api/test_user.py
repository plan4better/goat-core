import pytest
from httpx import AsyncClient

from src.core.config import settings


async def test_get_user(client: AsyncClient, fixture_create_user):
    response = await client.get(
        f"{settings.API_V2_STR}/user",
    )
    assert response.status_code == 200
    assert response.json()["id"] == fixture_create_user["id"]

@pytest.mark.asyncio
async def test_create_user(client: AsyncClient, fixture_create_user):
    assert fixture_create_user["id"] is not None


@pytest.mark.asyncio
async def test_delete_user(
    client: AsyncClient,
    fixture_create_user,
):
    response = await client.delete(
        f"{settings.API_V2_STR}/user",
    )
    assert response.status_code == 204

    # Check if user is deleted
    response = await client.get(
        f"{settings.API_V2_STR}/user",
    )
    assert response.status_code == 404
