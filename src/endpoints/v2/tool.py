from fastapi import APIRouter, Depends, Body
from src.schemas.tool import (
    IAggregationPoint,
    IJoin,
    request_examples_aggregation,
    request_examples_join,
)
from src.schemas.job import JobType
from src.crud.crud_tool import CRUDTool
from uuid import UUID
from uuid import uuid4
from sqlalchemy.ext.asyncio import AsyncSession
from src.endpoints.deps import get_db, get_user_id

from src.core.tool import start_calculation
from src.schemas.toolbox_base import IToolResponse
from src.schemas.toolbox_base import CommonToolParams

router = APIRouter()


@router.post(
    "/join",
    summary="Join two layers.",
    response_model=IToolResponse,
    status_code=201,
)
async def join(
    common: CommonToolParams = Depends(),
    params: IJoin = Body(
        ...,
        examples=request_examples_join,
        description="The join paramaters.",
    ),
):
    """Join two layers based on a matching column and create a new layer containing the attributes of the target layer and the new column for the statistics results."""

    return await start_calculation(
        job_type=JobType.join,
        tool_class=CRUDTool,
        crud_method="join_run",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
    )


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
