import asyncio
import os
import random
import string
from typing import List
from uuid import uuid4

from httpx import AsyncClient

from src.core.config import settings
from src.schemas.job import JobStatusType


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
        if job["status_simple"] not in [
            JobStatusType.running.value,
            JobStatusType.pending.value,
        ]:
            break
        else:
            await asyncio.sleep(retry_delay)
            retries += 1

    # Make sure that the job finished within the allowed retries
    assert (
        retries < max_retries
    ), f"Job {job_id} did not finish within the allowed retries."
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
        if job["status_simple"] not in [
            JobStatusType.running.value,
            JobStatusType.pending.value,
        ]:
            break
        else:
            await asyncio.sleep(retry_delay)
            retries += 1

    # Make sure that the job finished within the allowed retries
    assert (
        retries < max_retries
    ), f"Job {job_id} did not finish within the allowed retries."
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


async def upload_file(client: AsyncClient, file_type: str, filename: str) -> int:
    """Upload a single file and get its job ID."""
    data_dir = f"tests/data/layers/valid/{file_type}"
    with open(os.path.join(data_dir, filename), "rb") as f:
        response = await client.post(
            f"{settings.API_V2_STR}/layer/file-upload", files={"file": f}
        )
    assert response.status_code == 201
    return response.json()


async def upload_valid_file(client: AsyncClient, file_type: str):
    """Validate valid file."""

    if file_type == "point":
        response = await upload_file(client, "point", "valid.geojson")
    elif file_type == "no_geometry":
        response = await upload_file(client, "no_geometry", "valid.csv")
    else:
        raise ValueError("file_type must be either point or table")
    return response


async def upload_valid_files(client: AsyncClient, file_type: str):
    """Validate valid files."""
    files = await get_files_to_test(file_type)

    dataset_ids = []
    for filename in files:
        metadata = await upload_file(client, file_type, filename)
        dataset_ids.append(metadata["dataset_id"])

    return dataset_ids


async def upload_invalid_file(client: AsyncClient, file_type: str):
    """Validate invalid file."""

    # Get files to test
    data_dir = "tests/data/layers/invalid/" + file_type

    # Upload file by file and validate. Get response.
    response = await client.post(
        f"{settings.API_V2_STR}/layer/file-upload",
        files={"file": open(data_dir, "rb")},
    )
    assert response.status_code == 422

    return response.json()


async def get_with_wrong_id(client: AsyncClient, item: str):
    """Get item with wrong ID."""

    id = uuid4()
    response = await client.get(
        f"{settings.API_V2_STR}/{item}/{str(id)}",
    )
    assert response.status_code == 404


def generate_random_string(length):
    # Define the characters to use in the string
    characters = string.ascii_letters + string.digits
    # Generate the random string
    random_string = "".join(random.choice(characters) for i in range(length))
    return random_string
