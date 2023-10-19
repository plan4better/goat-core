import asyncio
import os
from httpx import AsyncClient
from src.core.config import settings
from src.schemas.job import JobStatusType

async def check_if_job_finished(client: AsyncClient, job_id: str):
    """Check if job is finished."""

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

async def validate_valid_files(client: AsyncClient, file_type: str):
    """Validate valid files."""

    # Get files to test
    data_dir = "tests/data/layers/valid/" + file_type
    files = os.listdir(data_dir)

    # Upload file by file and validate. Get response.
    job_ids = []
    for file in files:
        response = await client.post(
            f"{settings.API_V2_STR}/layer/file-validate",
            files={"file": open(os.path.join(data_dir, file), "rb")},
        )
        assert response.status_code == 201
        job_id = response.json()["job_id"]
        job_ids.append(job_id)

    # Revise job status for each job
    for job_id in job_ids:
        job = await check_if_job_finished(client, job_id)
        # Check if file is in the data folder
        assert os.path.exists(os.path.join(settings.DATA_DIR, str(job_id), "file." + job["response"]["file_ending"]))
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