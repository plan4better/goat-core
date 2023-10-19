import pytest
from httpx import AsyncClient
from src.core.config import settings
from tests.utils import check_if_job_finished

@pytest.mark.asyncio
async def test_files_validate_valid(client: AsyncClient, validate_files):
   assert validate_files is not None

@pytest.mark.asyncio
async def test_files_import(client: AsyncClient, validate_files):
    validate_job_ids = validate_files
    assert validate_job_ids is not None

    # Hit endpoint to upload file
    for validate_job_id in validate_job_ids:
        response = await client.post(
            f"{settings.API_V2_STR}/layer/file-import",
            json={"upload_job_id": validate_job_id},
        )
        assert response.status_code == 201
        job_id = response.json()["job_id"]
        await check_if_job_finished(client, job_id)

@pytest.mark.asyncio
async def test_files_validate_invalid(client: AsyncClient, validate_file_invalid):
    assert validate_file_invalid is not None

