# Standard Libraries
import json
import os
from typing import Any, Dict, List, Optional

# Third-party Libraries
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    File,
    HTTPException,
    Path,
    Query,
    UploadFile,
    status,
)
from fastapi.responses import FileResponse, JSONResponse
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from pydantic import UUID4
from sqlalchemy import and_, or_, select
from src.core.config import settings
from uuid import UUID

# Local application imports
from src.core.content import (
    read_content_by_id,
    read_contents_by_ids,
)
from src.crud.crud_job import job as crud_job
from src.crud.crud_layer import CRUDLayerExport, CRUDLayerImport
from src.crud.crud_layer import layer as crud_layer
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.db.models.layer import (
    FeatureType,
    FeatureUploadType,
    FileUploadType,
    Layer,
    LayerType,
    TableUploadType,
)
from src.db.session import AsyncSession
from src.endpoints.deps import get_db, get_user_id
from src.schemas.common import ContentIdList, OrderEnum
from src.schemas.error import HTTPErrorHandler, http_error_handler
from src.schemas.job import JobType
from src.schemas.layer import (
    AreaStatisticsOperation,
    ComputeBreakOperation,
    IFileUploadMetadata,
    IInternalLayerCreate,
    IInternalLayerExport,
    ILayerExternalCreate,
    ILayerRead,
    IUniqueValue,
    MaxFileSizeType,
)
from src.schemas.layer import request_examples as layer_request_examples
from src.utils import build_where, check_file_size

router = APIRouter()


@router.post(
    "/file-upload",
    summary="Upload file to server and validate",
    response_model=IFileUploadMetadata,
    status_code=201,
)
async def file_upload(
    *,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    file: UploadFile | None = File(None, description="File to upload. "),
):
    """
    Upload file and validate.
    """

    file_ending = os.path.splitext(file.filename)[-1][1:]
    # Check if file is feature or table
    if file_ending in TableUploadType.__members__:
        layer_type = LayerType.table.value
    elif file_ending in FeatureUploadType.__members__:
        layer_type = LayerType.feature.value
    else:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"File type not allowed. Allowed file types are: {', '.join(FileUploadType.__members__.keys())}",
        )

    if (
        await check_file_size(file=file, max_size=MaxFileSizeType[file_ending].value)
        is False
    ):
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File size too large. Max file size is {round(MaxFileSizeType[file_ending].value / 1048576, 2)} MB",
        )

    # Run the validation
    metadata = await crud_layer.upload_file(
        async_session=async_session,
        layer_type=layer_type,
        user_id=user_id,
        file=file,
    )
    return metadata


@router.post(
    "/internal",
    summary="Create a new internal layer",
    response_class=JSONResponse,
    status_code=201,
    description="Generate a new layer from a file that was previously uploaded using the file-upload endpoint.",
)
async def create_layer_internal(
    background_tasks: BackgroundTasks,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    project_id: Optional[UUID] = Query(
        None,
        description="The ID of the project to add the layer to",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    layer_in: IInternalLayerCreate = Body(
        ...,
        examples=layer_request_examples["create_internal"],
        description="Layer to create",
    ),
):
    # Check if user owns folder by checking if it exists
    folder_path = os.path.join(settings.DATA_DIR, user_id, str(layer_in.dataset_id))
    if os.path.exists(folder_path) is False:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found or not owned by user.",
        )

    # Get metadata from file in folder
    with open(os.path.join(folder_path, "metadata.json"), "r") as f:
        file_metadata = json.loads(json.load(f))

    # Create job and check if user can create a new job
    job = await crud_job.check_and_create(
        async_session=async_session,
        user_id=user_id,
        job_type=JobType.file_import,
        project_id=project_id,
    )

    # Run the import
    await CRUDLayerImport(
        background_tasks=background_tasks,
        async_session=async_session,
        user_id=user_id,
        job_id=job.id,
    ).import_file(
        file_metadata=file_metadata,
        layer_in=layer_in,
        project_id=project_id,
    )
    return {"job_id": job.id}


@router.post(
    "/internal/{id}/export",
    summary="Export a layer to a file",
    response_class=FileResponse,
    status_code=201,
    description="Export a layer to a zip file.",
)
async def export_layer(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    id: UUID4 = Path(
        ...,
        description="The ID of the layer to export",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    layer_in: IInternalLayerExport = Body(
        ...,
        examples=layer_request_examples["export_internal"],
        description="Layer to export",
    ),
):
    # Run the export
    crud_export = CRUDLayerExport(
        id=id,
        async_session=async_session,
        user_id=user_id,
    )
    zip_file_path = await http_error_handler(
        crud_export.export_file_run,
        layer_in=layer_in,
    )
    # Return file
    file_name = os.path.basename(zip_file_path)
    return FileResponse(zip_file_path, media_type="application/zip", filename=file_name)


@router.post(
    "/external",
    summary="Create a new external layer",
    response_model=ILayerRead,
    status_code=201,
    description="Generate a new layer based on a URL that is stored on an external server.",
)
async def create_layer_external(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    layer_in: ILayerExternalCreate = Body(
        ...,
        examples=layer_request_examples["create_external"],
        description="Layer to create",
    ),
):
    """Create a new external layer."""

    layer_in = Layer(**layer_in.dict(), user_id=user_id)
    layer = await crud_layer.create(db=async_session, obj_in=layer_in)
    return layer


@router.get(
    "/{id}",
    summary="Retrieve a layer by its ID",
    response_model=ILayerRead,
    response_model_exclude_none=True,
    status_code=200,
)
async def read_layer(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
):
    """Retrieve a layer by its ID."""
    return await read_content_by_id(
        async_session=async_session, id=id, model=Layer, crud_content=crud_layer
    )


@router.post(
    "/get-by-ids",
    summary="Retrieve a list of layers by their IDs",
    response_model=Page[ILayerRead],
    response_model_exclude_none=True,
    status_code=200,
)
async def read_layers_by_ids(
    async_session: AsyncSession = Depends(get_db),
    page_params: PaginationParams = Depends(),
    ids: ContentIdList = Body(
        ...,
        example=layer_request_examples["get"],
        description="List of layer IDs to retrieve",
    ),
):
    return await read_contents_by_ids(
        async_session=async_session,
        ids=ids,
        model=Layer,
        crud_content=crud_layer,
        page_params=page_params,
    )


@router.get(
    "",
    response_model=Page[ILayerRead],
    response_model_exclude_none=True,
    status_code=200,
    summary="Retrieve a list of layers using different filters",
)
async def read_layers(
    async_session: AsyncSession = Depends(get_db),
    page_params: PaginationParams = Depends(),
    folder_id: UUID4 | None = Query(None, description="Folder ID"),
    user_id: UUID4 = Depends(get_user_id),
    layer_type: List[LayerType] | None = Query(
        None,
        description="Layer type to filter by. Can be multiple. If not specified, all layer types will be returned.",
    ),
    feature_layer_type: List[FeatureType] | None = Query(
        None,
        description="Feature layer type. Can be multiple. If not specified, all feature layer types will be returned. Can only be used if 'layer_type' contains 'feature'.",
    ),
    search: str = Query(
        None,
        description="Searches the 'name' column of the layer. It will convert the text into lower case and see if the passed text is part of the name.",
        example="Münch",
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
    """This endpoints returns a list of layers based one the specified filters."""

    # Additional server side validation for feature_layer_type
    if feature_layer_type is not None and LayerType.feature not in layer_type:
        raise HTTPException(
            status_code=400,
            detail="Feature layer type can only be set when layer type is feature",
        )
    # TODO: Put this in CRUD layer
    if folder_id is None:
        sql_and_filters = [Layer.user_id == user_id]
    else:
        sql_and_filters = [Layer.user_id == user_id, Layer.folder_id == folder_id]

    # Add conditions to filter by layer_type and feature_layer_type
    if layer_type is not None:
        sql_and_filters.append(or_(Layer.type.in_(layer_type)))

    if feature_layer_type is not None:
        sql_and_filters.append(or_(Layer.feature_layer_type.in_(feature_layer_type)))

    # Build query
    query = select(Layer).where(and_(*sql_and_filters))

    # Build params
    params = {
        "search_text": {"name": search} if search else None,
        "order_by": order_by,
        "order": order,
    }

    # Filter out None values
    params = {k: v for k, v in params.items() if v is not None}

    layers = await crud_layer.get_multi(
        async_session,
        query=query,
        page_params=page_params,
        **params,
    )

    return layers


@router.put(
    "/{id}",
    response_model=ILayerRead,
    response_model_exclude_none=True,
    status_code=200,
)
async def update_layer(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    layer_in: Dict[Any, Any] = Body(
        ..., examples=layer_request_examples["update"], description="Layer to update"
    ),
):
    with HTTPErrorHandler():
        return await crud_layer.update(
            async_session=async_session,
            id=id,
            layer_in=layer_in,
        )


@router.delete(
    "/{id}",
    response_model=None,
    summary="Delete a layer and its data in case of an internal layer.",
    status_code=204,
)
async def delete_layer(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
):
    """Delete a layer and its data in case of an internal layer."""

    with HTTPErrorHandler():
        await crud_layer.delete(
            async_session=async_session,
            id=id,
        )
    return


@router.get(
    "/{id}/feature-count",
    summary="Get feature count",
    response_class=JSONResponse,
    status_code=200,
)
async def get_feature_count(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    query: str = Query(
        None,
        description="CQL2-Filter in JSON format",
        example='{"op": "=", "args": [{"property": "category"}, "bus_stop"]}',
    ),
):
    """Get feature count. Based on the passed CQL-filter."""

    # Get layer
    layer = await crud_layer.get_internal(
        async_session=async_session,
        id=id,
    )
    where_query = build_where(
        layer.id, layer.table_name, query, layer.attribute_mapping
    )
    count = await crud_layer_project.get_feature_cnt(
        async_session=async_session,
        layer_project=layer,
        where_query=where_query,
    )

    # Return result
    return count


@router.get(
    "/{id}/area/{operation}",
    summary="Get area statistics of a layer",
    response_class=JSONResponse,
    status_code=200,
)
async def get_area_statistics(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    operation: AreaStatisticsOperation = Path(
        ...,
        description="The operation to perform",
        example="sum",
    ),
    query: str = Query(
        None,
        description="CQL2-Filter in JSON format",
        example='{"op": ">", "args": [{"property": "id"}, "10"]}',
    ),
):
    """Get statistics on the area size of a polygon layer. The area is computed using geography datatype and the unit is m²."""

    statistics = await crud_layer.get_area_statistics(
        async_session=async_session,
        id=id,
        operation=operation,
        query=query,
    )

    # Return result
    return statistics


@router.get(
    "/{id}/unique-values/{column_name}",
    summary="Get unique values of a column",
    response_model=Page[IUniqueValue],
    status_code=200,
)
async def get_unique_values(
    async_session: AsyncSession = Depends(get_db),
    page_params: PaginationParams = Depends(),
    id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    column_name: str = Path(
        ...,
        description="The column name to get the unique values from",
        example="name",
    ),
    query: str = Query(
        None,
        description="CQL2-Filter in JSON format",
        example={"op": "=", "args": [{"property": "category"}, "bus_stop"]},
    ),
    order: OrderEnum = Query(
        "descendent",
        description="Specify the order to apply. There are the option ascendent or descendent.",
        example="descendent",
    ),
):
    """Get unique values of a column. Based on the passed CQL-filter and order."""

    values = await crud_layer.get_unique_values(
        async_session=async_session,
        id=id,
        column_name=column_name,
        query=query,
        page_params=page_params,
        order=order,
    )

    # Return result
    return values


@router.get(
    "/{id}/class-breaks/{operation}/{column_name}",
    summary="Get statistics of a column",
    response_class=JSONResponse,
    status_code=200,
)
async def class_breaks(
    async_session: AsyncSession = Depends(get_db),
    id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    operation: ComputeBreakOperation = Path(
        ...,
        description="The operation to perform",
        example="quantile",
    ),
    column_name: str = Path(
        ...,
        description="The column name to get the statistics from. It needs to be a number column.",
        example="name",
    ),
    breaks: int | None = Query(
        None,
        description="Number of class breaks to create",
        example=5,
    ),
    query: str | None = Query(
        None,
        description="CQL2-Filter in JSON format",
        example={"op": "=", "args": [{"property": "category"}, "bus_stop"]},
    ),
    stripe_zeros: bool | None = Query(
        True,
        description="Stripe zeros from the column before performing the operation",
        example=True,
    ),
):
    """Get statistics of a column. Based on the saved layer filter in the project."""

    with HTTPErrorHandler():
        statistics = await crud_layer.get_class_breaks(
            async_session=async_session,
            id=id,
            operation=operation,
            column_name=column_name,
            breaks=breaks,
            query=query,
            stripe_zeros=stripe_zeros,
        )

    # Return result
    return statistics
