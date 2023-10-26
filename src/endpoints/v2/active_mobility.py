# Standard Libraries
from uuid import UUID, uuid4

# Third-party Libraries
from fastapi import APIRouter, Body, Depends
from sqlalchemy.ext.asyncio import AsyncSession

# Project-specific Modules
from src.endpoints.deps import get_db, get_user_id
from src.schemas.active_mobility import (
    IIsochroneActiveMobility,
    request_examples as active_mobility_request_examples,
)
from src.schemas.toolbox_base import IToolResponse


router = APIRouter()

@router.post(
    "/isochrone",
    summary="Compute isochrones for active mobility",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_active_mobility_isochrone(
    *,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    params: IIsochroneActiveMobility = Body(
        ...,
        examples=active_mobility_request_examples["isochrone_active_mobility"],
        description="The isochrone parameters.",
    ),
):
    """Compute isochrones for active mobility."""
    return {"job_id": uuid4()}
