from fastapi import APIRouter, Body, Depends

from src.core.tool import start_calculation
from src.crud.crud_isochrone import CRUDIsochroneActiveMobility
from src.endpoints.deps import get_http_client
from src.schemas.active_mobility import (
    IIsochroneActiveMobility,
)
from src.schemas.active_mobility import (
    request_examples as active_mobility_request_examples,
)
from src.schemas.job import JobType
from src.schemas.toolbox_base import CommonToolParams, IToolResponse

router = APIRouter()


@router.post(
    "/isochrone",
    summary="Compute isochrones for active mobility",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_active_mobility_isochrone(
    *,
    common: CommonToolParams = Depends(),
    params: IIsochroneActiveMobility = Body(
        ...,
        examples=active_mobility_request_examples["isochrone_active_mobility"],
        description="The isochrone parameters.",
    ),
):
    """Compute isochrones for active mobility."""

    return await start_calculation(
        job_type=JobType.isochrone_active_mobility,
        tool_class=CRUDIsochroneActiveMobility,
        crud_method="run_isochrone",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
        http_client=get_http_client(),
    )
