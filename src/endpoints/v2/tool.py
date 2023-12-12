from src.schemas.tool import (
    IAggregationPoint,
    IJoin,
    request_examples_aggregation,
    request_examples_join,
)
from src.schemas.job import JobType
from src.crud.crud_tool import tool as crud_tool
from src.crud.crud_job import job as crud_job
from uuid import UUID
from uuid import uuid4
from sqlalchemy.ext.asyncio import AsyncSession
from src.endpoints.deps import get_db, get_user_id
from fastapi import APIRouter, Depends, Body, BackgroundTasks, Query

from src.schemas.toolbox_base import IToolResponse

router = APIRouter()


@router.post(
    "/join",
    summary="Join two layers.",
    response_model=IToolResponse,
    status_code=201,
)
async def join(
    *,
    background_tasks: BackgroundTasks,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    project_id: str = Query(
        ...,
        title="Project ID of the project that contains the layers.",
        description="The project ID of the project that contains the layers.",
    ),
    params: IJoin = Body(
        ...,
        examples=request_examples_join,
        description="The join paramaters.",
    ),
):
    """Join two layers based on a matching column and create a new layer containing the attributes of the target layer and the new column for the statistics results."""

    # Create job and check if user can create a new job
    job = await crud_job.check_and_create(
        async_session=async_session,
        user_id=user_id,
        job_type=JobType.join,
    )

    await crud_tool.join(
        background_tasks=background_tasks,
        job_id=job.id,
        async_session=async_session,
        user_id=user_id,
        project_id=project_id,
        params=params,
    )
    return {"job_id": job.id}


@router.post(
    "/aggregate-points",
    summary="Aggregate points",
    response_model=IToolResponse,
    status_code=201,
)
async def aggregate_points(
    *,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    params: IAggregationPoint = Body(
        ...,
        examples=request_examples_aggregation,
        description="The aggregation parameters.",
    ),
):
    """Aggregate points and compute statistics on a group by column."""
    return {"job_id": uuid4()}
