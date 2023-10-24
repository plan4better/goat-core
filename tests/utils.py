import asyncio
import os
from httpx import AsyncClient
from src.core.config import settings
from src.schemas.job import JobStatusType
from typing import List
from src.schemas.layer import request_examples as layer_request_examples
from uuid import UUID, uuid4

async def check_if_job_finished(client: AsyncClient, job_id: str):
    """Check if job is finished."""

    # Get job status recursively until they return status_simplified = finished
    max_retries = 60
    retry_delay = 1  # delay in seconds
    retries = 0

    while retries < max_retries:
        response = await client.get(f"{settings.API_V2_STR}/job/{job_id}")
        assert response.status_code == 200
        job = response.json()
        if job["status_simple"] not in [JobStatusType.running.value, JobStatusType.pending.value]:
            break
        else:
            await asyncio.sleep(retry_delay)
            retries += 1

    # Make sure that the job finished within the allowed retries
    assert retries < max_retries, f"Job {job_id} did not finish within the allowed retries."
    # Make sure job is finished
    assert job["status_simple"] == JobStatusType.finished.value
    # Make sure each job_step is finished
    for job_step in job["status"]:
        assert job["status"][job_step]["status"] == JobStatusType.finished.value

    return job

async def check_if_job_failed(client: AsyncClient, job_id: str):
    """Check if job is failed."""

    # Get job status recursively until they return status_simplified = finished
    max_retries = 10
    retry_delay = 1  # delay in seconds
    retries = 0

    while retries < max_retries:
        response = await client.get(f"{settings.API_V2_STR}/job/{job_id}")
        assert response.status_code == 200
        job = response.json()
        if job["status_simple"] not in [JobStatusType.running.value, JobStatusType.pending.value]:
            break
        else:
            await asyncio.sleep(retry_delay)
            retries += 1

    # Make sure that the job finished within the allowed retries
    assert retries < max_retries, f"Job {job_id} did not finish within the allowed retries."
    # Make sure job is failed
    assert job["status_simple"] == JobStatusType.failed.value
    # Make sure that one job_step is failed
    for job_step in job["status"]:
        if job["status"][job_step]["status"] == JobStatusType.failed.value:
            break

    return job


async def get_files_to_test(file_type: str) -> List[str]:
    """Get list of files based on file_type."""
    data_dir = f"tests/data/layers/valid/{file_type}"
    return os.listdir(data_dir)


async def upload_and_get_job_id(client: AsyncClient, file_type: str, filename: str) -> int:
    """Upload a single file and get its job ID."""
    data_dir = f"tests/data/layers/valid/{file_type}"
    with open(os.path.join(data_dir, filename), "rb") as f:
        response = await client.post(
            f"{settings.API_V2_STR}/layer/file-validate", files={"file": f}
        )
    assert response.status_code == 201
    return response.json()["job_id"]


async def check_single_job_status(client: AsyncClient, job_id: int):
    """Check the status of a single job and assert file existence."""
    job = await check_if_job_finished(client, job_id)
    assert os.path.exists(
        os.path.join(settings.DATA_DIR, str(job_id), f"file.{job['response']['file_ending']}")
    )

async def validate_valid_file(client: AsyncClient, file_type: str):
    """Validate valid file."""

    if file_type == "point":
        job_id = await upload_and_get_job_id(client, "point", "valid.geojson")
    elif file_type == "no_geometry":
        job_id = await upload_and_get_job_id(client, "no_geometry", "valid.csv")
    else:
        raise ValueError("file_type must be either point or table")
    await check_single_job_status(client, job_id)
    return job_id

async def validate_valid_files(client: AsyncClient, file_type: str):
    """Validate valid files."""
    files = await get_files_to_test(file_type)

    job_ids = [
        await upload_and_get_job_id(client, file_type, filename) for filename in files
    ]

    for job_id in job_ids:
        await check_single_job_status(client, job_id)

    return job_ids

async def validate_invalid_file(client: AsyncClient, file_type: str):
    """Validate invalid file."""

    # Get files to test
    data_dir = "tests/data/layers/invalid/" + file_type

    # Upload file by file and validate. Get response.
    response = await client.post(
        f"{settings.API_V2_STR}/layer/file-validate",
        files={"file": open(data_dir, "rb")},
    )
    assert response.status_code == 201
    job_id = response.json()["job_id"]

    # Revise job status for each job
    await check_if_job_failed(client, job_id)

    return job_id

async def upload_file(client: AsyncClient, validate_job_id: int):
    """Upload a single file and get its job ID."""

    response = await client.post(
        f"{settings.API_V2_STR}/layer/file-import",
        json={"validate_job_id": validate_job_id},
    )
    assert response.status_code == 201
    job_id = response.json()["job_id"]
    await check_if_job_finished(client, job_id)
    return job_id

async def get_with_wrong_id(client: AsyncClient, item: str):
    """Get item with wrong ID."""

    id = uuid4()
    response = await client.get(
        f"{settings.API_V2_STR}/{item}/{id}",
    )
    assert response.status_code == 404