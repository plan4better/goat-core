from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, status
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from pydantic import UUID4
from sqlalchemy import select

from src.crud.crud_scenario import scenario as crud_scenario
from src.db.models.scenario import Scenario
from src.db.session import AsyncSession
from src.endpoints.deps import get_db, get_user_id
from src.schemas.common import OrderEnum
from src.schemas.scenario import (
    IScenarioCreate,
    IScenarioUpdate,
)
from src.schemas.scenario import (
    request_examples as scenario_request_examples,
)

router = APIRouter()


@router.get(
    "",
    summary="Retrieve a list of scenarios",
    response_model=Page[Scenario],
    status_code=200,
)
async def read_scenarios(
    async_session: AsyncSession = Depends(get_db),
    page_params: PaginationParams = Depends(),
    user_id: UUID4 = Depends(get_user_id),
    search: str = Query(None, description="Searches the name of the scenario"),
    order_by: str = Query(
        None,
        description="Specify the column name that should be used to order",
        example="created_at",
    ),
    order: OrderEnum = Query(
        "descendent",
        description="Specify the order to apply. There are the option ascendent or descendent.",
        example="descendent",
    ),
):
    """Retrieve a list of scenarios."""
    query = select(Scenario).where(Scenario.user_id == user_id)
    scenarios = await crud_scenario.get_multi(
        db=async_session,
        query=query,
        page_params=page_params,
        search_text={"name": search} if search else {},
        order_by=order_by,
        order=order,
    )

    return scenarios


@router.get(
    "/{scenario_id}",
    summary="Retrieve a scenario by id",
    response_model=Scenario,
    status_code=200,
)
async def read_scenario_by_id(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
):
    """Retrieve a scenario by id."""
    scenario = await crud_scenario.get(async_session, id=id)

    if scenario is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Scenario not found"
        )
    return scenario


@router.post(
    "",
    summary="Create scenario",
    status_code=201,
    response_model=Scenario,
    response_model_exclude_none=True,
)
async def create_scenario(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    *,
    scenario_in: IScenarioCreate = Body(
        ...,
        example=scenario_request_examples["create"],
        description="Scenario to create",
    ),
):
    """Create scenario."""

    return await crud_scenario.create(
        db=async_session,
        obj_in=Scenario(**scenario_in.dict(exclude_none=True), user_id=user_id),
    )


@router.put(
    "/{scenario_id}",
    summary="Update scenario",
    status_code=201,
)
async def update_scenario(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    scenario_id: UUID4 = Path(
        ...,
        description="The ID of the scenario to update",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    scenario_in: IScenarioUpdate = Body(
        ...,
        example=scenario_request_examples["update"],
        description="Scenario to update",
    ),
):
    """Update scenario."""
    db_obj = await crud_scenario.get(async_session, id=scenario_id)
    return await crud_scenario.update(
        db=async_session,
        db_obj=db_obj,
        obj_in=scenario_in,
    )


@router.delete(
    "/{scenario_id}",
    summary="Delete scenario",
    status_code=204,
)
async def delete_scenario(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    scenario_id: UUID4 = Path(
        ...,
        description="The ID of the scenario to delete",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
):
    """Delete scenario."""

    await crud_scenario.remove(db=async_session, id=scenario_id)
    return None
