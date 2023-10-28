from fastapi import APIRouter, Depends, Path, Query, Body, HTTPException
from src.crud.crud_job import job as crud_job
from src.db.models.job import Job
from sqlalchemy.ext.asyncio import AsyncSession
from src.endpoints.deps import get_db, get_user_id
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from pydantic import UUID4
from src.schemas.job import JobType, JobStatusType
from typing import List
from datetime import datetime
from src.schemas.common import OrderEnum

router = APIRouter()


@router.get(
    "/{id}",
    response_model=Job,
    response_model_exclude_none=True,
    status_code=200,
    summary="Get a job by its ID.",
)
async def get_job(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    user_id: UUID4 = Depends(get_user_id),
):
    """Retrieve a job by its ID."""
    job = await crud_job.get_by_multi_keys(db=async_session, keys={"id": id, "user_id": user_id})

    if job == []:
        raise HTTPException(status_code=404, detail="Job not found")

    return job[0]


@router.get(
    "",
    response_model=Page[Job],
    response_model_exclude_none=True,
    status_code=200,
    summary="Retrieve a list of jobs using different filters.",
)
async def read_jobs(
    async_session: AsyncSession = Depends(get_db),
    page_params: PaginationParams = Depends(),
    user_id: UUID4 = Depends(get_user_id),
    job_type: List[JobType]
    | None = Query(
        None,
        description="Job type to filter by. Can be multiple. If not specified, all job types will be returned.",
    ),
    project_id: UUID4
    | None = Query(
        None,
        description="Project ID to filter by. If not specified, all projects will be returned.",
    ),
    start_data: datetime
    | None = Query(
        None,
        description="Specify the start date to filter the jobs. If not specified, all jobs will be returned.",
    ),
    end_data: datetime
    | None = Query(
        None,
        description="Specify the end date to filter the jobs. If not specified, all jobs will be returned.",
    ),
    read: bool
    | None = Query(
        False,
        description="Specify if the job should be read. If not specified, all jobs will be returned.",
    ),
    order_by: str = Query(
        None,
        description="Specify the column name that should be used to order. You can check the Layer model to see which column names exist.",
        example="created_at",
    ),
    order: OrderEnum = Query(
        "descendent",
        description="Specify the order to apply. There are the option ascendent or descendent.",
        example="descendent",
    ),
):
    """Retrieve a list of jobs using different filters."""

    return await crud_job.get_by_date(
        async_session=async_session,
        user_id=user_id,
        page_params=page_params,
        project_id=project_id,
        job_type=job_type,
        start_data=start_data,
        end_data=end_data,
        read=read,
        order_by=order_by,
        order=order,
    )


@router.put(
    "/read",
    response_model=List[Job],
    response_model_exclude_none=True,
    status_code=200,
    summary="Mark jobs as read.",
)
async def mark_jobs_as_read(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    job_ids: List[UUID4] = Body(
        ...,
        description="List of job IDs to mark as read.",
        example=["7e5eeb1f-3605-4ff7-87f8-2aed7094e4de", "c9d2884c-0e01-4d7a-b595-5c20be857ec5"],
    ),
):
    """Mark jobs as read."""
    return await crud_job.mark_as_read(
        async_session=async_session, user_id=user_id, job_ids=job_ids
    )


@router.put(
    "/kill/{job_id}",
    response_model=Job,
    response_model_exclude_none=True,
    status_code=200,
    summary="Kill a job.",
)
async def kill_job(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    job_id: UUID4 = Path(
        ...,
        description="Job ID to kill.",
        example="7e5eeb1f-3605-4ff7-87f8-2aed7094e4de",
    ),
):
    """Kill a job. It will let the job finish already started tasks and then kill it. All data produced by the job will be deleted."""

    job = await crud_job.get_by_multi_keys(db=async_session, keys={"id": job_id, "user_id": user_id})

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    job = job[0]

    if job.status_simple not in [JobStatusType.pending.value, JobStatusType.running.value]:
        raise HTTPException(status_code=400, detail="Job is not pending or running. Therefore it cannot be killed.")

    return await crud_job.update(db=async_session, db_obj=job, obj_in={"status_simple": "killed"})
