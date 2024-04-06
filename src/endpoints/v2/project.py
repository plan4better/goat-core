from typing import List

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, status
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from pydantic import UUID4

from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.crud.crud_project import project as crud_project
from src.crud.crud_user_project import user_project as crud_user_project
from src.core.chart import read_chart_data
from src.db.models._link_model import UserProjectLink
from src.db.models.project import Project
from src.db.session import AsyncSession
from src.endpoints.deps import get_db, get_user_id
from src.schemas.common import ContentIdList, OrderEnum
from src.schemas.project import (
    IExternalImageryProjectRead,
    IExternalVectorTileProjectRead,
    IFeatureToolProjectRead,
    IFeatureScenarioProjectRead,
    IFeatureStandardProjectRead,
    InitialViewState,
    IProjectBaseUpdate,
    IProjectCreate,
    IProjectRead,
    ITableProjectRead,
)
from src.schemas.project import (
    request_examples as project_request_examples,
)

router = APIRouter()


### Project endpoints
@router.post(
    "",
    summary="Create a new project",
    response_model=IProjectRead,
    response_model_exclude_none=True,
    status_code=201,
)
async def create_project(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    *,
    project_in: IProjectCreate = Body(
        ..., example=project_request_examples["create"], description="Project to create"
    ),
):
    """This will create an empty project with a default initial view state. The project does not contains layers or reports."""

    # Create project
    return await crud_project.create(
        async_session=async_session,
        project_in=Project(**project_in.dict(exclude_none=True), user_id=user_id),
        initial_view_state=project_in.initial_view_state,
    )

@router.get(
    "/{id}",
    summary="Retrieve a project by its ID",
    response_model=IProjectRead,
    response_model_exclude_none=True,
    status_code=200,
)
async def read_project(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
):
    """Retrieve a project by its ID."""

    # Get project
    project = await crud_project.get(async_session, id=id)
    if project is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )
    return IProjectRead(**project.dict())


@router.get(
    "",
    summary="Retrieve a list of projects",
    response_model=Page[IProjectRead],
    response_model_exclude_none=True,
    status_code=200,
)
async def read_projects(
    async_session: AsyncSession = Depends(get_db),
    page_params: PaginationParams = Depends(),
    folder_id: UUID4 | None = Query(None, description="Folder ID"),
    user_id: UUID4 = Depends(get_user_id),
    search: str = Query(None, description="Searches the name of the project"),
    order_by: str = Query(
        None,
        description="Specify the column name that should be used to order. You can check the Project model to see which column names exist.",
        example="created_at",
    ),
    order: OrderEnum = Query(
        "descendent",
        description="Specify the order to apply. There are the option ascendent or descendent.",
        example="descendent",
    ),
):
    """Retrieve a list of projects."""

    projects = await crud_project.get_projects(
        async_session=async_session,
        user_id=user_id,
        folder_id=folder_id,
        page_params=page_params,
        search=search,
        order_by=order_by,
        order=order,
    )

    return projects


@router.post(
    "/get-by-ids",
    summary="Retrieve a list of projects by their IDs",
    response_model=Page[IProjectRead],
    response_model_exclude_none=True,
    status_code=200,
)
async def read_projects_by_ids(
    async_session: AsyncSession = Depends(get_db),
    page_params: PaginationParams = Depends(),
    user_id: UUID4 = Depends(get_user_id),
    ids: ContentIdList = Body(
        ...,
        example=project_request_examples["get"],
        description="List of project IDs to retrieve",
    ),
):
    """Retrieve a list of projects by their IDs."""

    # Get projects by ids
    projects = await crud_project.get_projects(
        async_session=async_session,
        user_id=user_id,
        page_params=page_params,
        ids=ids.ids,
    )

    return projects


@router.put(
    "/{id}",
    response_model=IProjectRead,
    response_model_exclude_none=True,
    status_code=200,
)
async def update_project(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    project_in: IProjectBaseUpdate = Body(
        ..., example=project_request_examples["update"], description="Project to update"
    ),
):
    """Update base attributes of a project by its ID."""

    # Update project
    project = await crud_project.update_base(
        async_session=async_session,
        id=id,
        project=project_in,
    )
    return project


@router.delete(
    "/{id}",
    response_model=None,
    status_code=204,
)
async def delete_project(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
):
    """Delete a project by its ID."""

    # Get project
    project = await crud_project.get(async_session, id=id)
    if project is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )

    # Delete project
    await crud_project.delete(db=async_session, id=id)
    return


@router.get(
    "/{id}/initial-view-state",
    response_model=InitialViewState,
    response_model_exclude_none=True,
    status_code=200,
)
async def read_project_initial_view_state(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
):
    """Retrieve initial view state of a project by its ID."""

    # Get initial view state
    user_project = await crud_user_project.get_by_multi_keys(
        async_session, keys={"user_id": user_id, "project_id": id}
    )
    return user_project[0].initial_view_state


@router.put(
    "/{id}/initial-view-state",
    response_model=InitialViewState,
    response_model_exclude_none=True,
    status_code=200,
)
async def update_project_initial_view_state(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    initial_view_state: InitialViewState = Body(
        ...,
        example=project_request_examples["initial_view_state"],
        description="Initial view state to update",
    ),
):
    """Update initial view state of a project by its ID."""

    # Update project
    user_project = await crud_user_project.update_initial_view_state(
        async_session,
        user_id=user_id,
        project_id=id,
        initial_view_state=initial_view_state,
    )
    return user_project.initial_view_state


@router.post(
    "/{id}/layer",
    response_model=List[
        IFeatureStandardProjectRead
        | IFeatureToolProjectRead
        | IFeatureScenarioProjectRead
        | ITableProjectRead
        | IExternalVectorTileProjectRead
        | IExternalImageryProjectRead
    ],
    response_model_exclude_none=True,
    status_code=200,
)
async def add_layers_to_project(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    layer_ids: List[UUID4] = Query(
        ...,
        description="List of layer IDs to add to the project",
        example=["3fa85f64-5717-4562-b3fc-2c963f66afa6"],
    ),
):
    """Add layers to a project by its ID."""

    # Add layers to project
    layers_project = await crud_layer_project.create(
        async_session=async_session,
        project_id=id,
        layer_ids=layer_ids,
    )

    return layers_project


@router.get(
    "/{id}/layer",
    response_model=List[
        IFeatureStandardProjectRead
        | IFeatureToolProjectRead
        | IFeatureScenarioProjectRead
        | ITableProjectRead
        | IExternalVectorTileProjectRead
        | IExternalImageryProjectRead
    ],
    response_model_exclude_none=True,
    status_code=200,
)
async def get_layers_from_project(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
):
    """Get layers from a project by its ID."""

    # Get all layers from project
    layers_project = await crud_layer_project.get_layers(
        async_session,
        project_id=id,
    )
    return layers_project


@router.get(
    "/{id}/layer/{layer_project_id}",
    response_model=IFeatureStandardProjectRead
    | IFeatureToolProjectRead
    | IFeatureScenarioProjectRead
    | ITableProjectRead
    | IExternalVectorTileProjectRead
    | IExternalImageryProjectRead,
    response_model_exclude_none=True,
    status_code=200,
)
async def get_layer_from_project(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    layer_project_id: int = Path(
        ...,
        description="Layer project ID to get",
        example="1",
    ),
):
    layer_project = await crud_layer_project.get_by_ids(
        async_session, ids=[layer_project_id]
    )
    return layer_project[0]


@router.put(
    "/{id}/layer/{layer_project_id}",
    response_model=IFeatureStandardProjectRead
    | IFeatureToolProjectRead
    | IFeatureScenarioProjectRead
    | ITableProjectRead
    | IExternalVectorTileProjectRead
    | IExternalImageryProjectRead,
    response_model_exclude_none=True,
    status_code=200,
)
async def update_layer_in_project(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    layer_project_id: int = Path(
        ...,
        description="Layer Project ID to update",
        example="1",
    ),
    layer_in: dict = Body(
        ...,
        examples=project_request_examples["update_layer"],
        description="Layer to update",
    ),
):
    """Update layer in a project by its ID."""

    # NOTE: Avoid getting layer_id from layer_in as the authorization is running against the query params.

    # Update layer in project
    layer_project = await crud_layer_project.update(
        async_session=async_session,
        id=layer_project_id,
        layer_in=layer_in,
    )
    # Update the last updated at of the project
    # Get project to update it
    project = await crud_project.get(async_session, id=id)

    # Update project updated_at
    await crud_project.update(
        async_session,
        db_obj=project,
        obj_in={"updated_at": layer_project.updated_at},
    )

    # Get layers in project
    return layer_project


@router.delete(
    "/{id}/layer",
    response_model=None,
    status_code=204,
)
async def delete_layer_from_project(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the project",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    layer_project_id: int = Query(
        ...,
        description="Layer ID to delete",
        example="1",
    ),
):
    """Delete layer from a project by its ID."""

    # Get layer project
    layer_project = await crud_layer_project.get(async_session, id=layer_project_id)
    if layer_project is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Layer project relation not found",
        )

    # Delete layer from project
    await crud_layer_project.delete(
        db=async_session,
        id=layer_project.id,
    )

    # Delete layer from project layer order
    project = await crud_project.get(async_session, id=id)
    layer_order = project.layer_order.copy()
    layer_order.remove(layer_project.id)

    await crud_project.update(
        async_session,
        db_obj=project,
        obj_in={"layer_order": layer_order},
    )

    return None

@router.get(
    "/{id}/layer/{layer_project_id}/chart-data",
    response_model=dict,
    response_model_exclude_none=True,
    status_code=200,
)
async def get_chart_data(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the project to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    layer_project_id: int = Path(
        ...,
        description="Layer Project ID to get chart data",
        example="1",
    ),
):
    """Get chart data from a layer in a project by its ID."""

    # Get chart data
    return await read_chart_data(
        async_session=async_session,
        project_id=id,
        layer_project_id=layer_project_id,
    )