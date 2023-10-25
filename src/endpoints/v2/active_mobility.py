from src.schemas.active_mobility import (
    IIsochroneActiveMobility,
    request_examples as active_mobility_request_examples,
)
from uuid import UUID
from uuid import uuid4
from src.core.config import settings
from sqlalchemy.ext.asyncio import AsyncSession
from src.endpoints.deps import get_db, get_user_id
from fastapi import APIRouter, Depends, HTTPException, Body, status
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
