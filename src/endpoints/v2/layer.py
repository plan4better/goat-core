# Standard Libraries
import os
from typing import List
from uuid import uuid4

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
from fastapi.responses import JSONResponse
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from pydantic import UUID4
from sqlalchemy import and_, or_, select, text

# Local application imports
from src.core.content import (
    read_content_by_id,
    read_contents_by_ids,
    update_content_by_id,
)
from src.crud.crud_job import job as crud_job
from src.crud.crud_layer import layer as crud_layer
from src.db.models.layer import FeatureLayerType, Layer, LayerType
from src.db.session import AsyncSession
from src.endpoints.deps import get_db, get_user_id
from src.schemas.common import ContentIdList, OrderEnum
from src.schemas.job import JobStatusType, JobType
from src.schemas.layer import (
    CQLQuery,
    FeatureLayerUploadType,
    FileUploadType,
    IInternalLayerCreate,
    ILayerExternalCreate,
    ILayerRead,
    ILayerUpdate,
    IValidateJobId,
    MaxFileSizeType,
    TableUploadType,
)
from src.schemas.layer import request_examples as layer_request_examples
from src.utils import build_where, check_file_size, get_user_table, search_value
import json

router = APIRouter()


@router.post(
    "/file-validate",
    summary="Upload file to server and validate",
    response_class=JSONResponse,
    status_code=201,
)
async def file_validate(
    *,
    background_tasks: BackgroundTasks,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    file: UploadFile | None = File(None, description="File to upload. "),
):
    """
    Upload file and validate.
    """

    file_ending = os.path.splitext(file.filename)[-1][1:]
    # Check if file is feature_layer or table_layer
    if file_ending in TableUploadType.__members__:
        layer_type = LayerType.table.value
    elif file_ending in FeatureLayerUploadType.__members__:
        layer_type = LayerType.feature_layer.value
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

    # Create job and check if user can create a new job
    job = await crud_job.check_and_create(
        async_session=async_session,
        user_id=user_id,
        job_type=JobType.file_validate,
    )

    # Run the validation
    await crud_layer.validate_file(
        background_tasks=background_tasks,
        async_session=async_session,
        user_id=user_id,
        job_id=job.id,
        layer_type=layer_type,
        file=file,
    )
    return {"job_id": job.id}


@router.post(
    "/file-import",
    summary="Import previously uploaded file using Ogr2ogr",
    response_class=JSONResponse,
    status_code=201,
)
async def file_import(
    *,
    background_tasks: BackgroundTasks,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    validate_job_id: IValidateJobId = Body(
        ...,
        example=layer_request_examples["validate_job_id"],
        description="Upload job ID",
    ),
):
    """
    Import file using ogr2ogr.
    """

    # Check if file upload job exists, is finished and owned by the user
    validate_job = await crud_job.get_by_multi_keys(
        db=async_session,
        keys={
            "id": validate_job_id.validate_job_id,
            "user_id": user_id,
            "type": JobType.file_validate.value,
            "status_simple": JobStatusType.finished.value,
        },
    )
    if validate_job == []:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File upload job not found or not finished.",
        )
    validate_job = validate_job[0]
    # Create job and check if user can create a new job
    job = await crud_job.check_and_create(
        async_session=async_session,
        user_id=user_id,
        job_type=JobType.file_import,
    )

    # Create layer_id
    layer_id = str(uuid4())

    # Run the import
    await crud_layer.import_file(
        background_tasks=background_tasks,
        async_session=async_session,
        user_id=user_id,
        job_id=job.id,
        layer_id=layer_id,
        validate_job=validate_job,
        file_path=validate_job.response["file_path"],
    )

    return {"job_id": job.id}


## Layer endpoints
@router.post(
    "/internal",
    summary="Create a new internal layer",
    response_model=ILayerRead,
    status_code=201,
    description="Generate a new layer from a file that was previously uploaded using the file-upload endpoint.",
)
async def create_layer_internal(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    layer_in: IInternalLayerCreate = Body(
        ...,
        examples=layer_request_examples["create_internal"],
        description="Layer to create",
    ),
):
    """Create a new internal layer."""
    layer = await crud_layer.create_internal(
        async_session=async_session, user_id=user_id, layer_in=layer_in
    )
    return layer


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
    layer_type: List[LayerType]
    | None = Query(
        None,
        description="Layer type to filter by. Can be multiple. If not specified, all layer types will be returned.",
    ),
    feature_layer_type: List[FeatureLayerType]
    | None = Query(
        None,
        description="Feature layer type. Can be multiple. If not specified, all feature layer types will be returned. Can only be used if 'layer_type' contains 'feature_layer'.",
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
    if feature_layer_type is not None and LayerType.feature_layer not in layer_type:
        raise HTTPException(
            status_code=400,
            detail="Feature layer type can only be set when layer type is feature_layer",
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
    layer_in: ILayerUpdate = Body(
        ..., examples=layer_request_examples["update"], description="Layer to update"
    ),
):
    return await update_content_by_id(
        async_session=async_session,
        id=id,
        model=Layer,
        crud_content=crud_layer,
        content_in=layer_in,
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

    layer = await crud_layer.get(async_session, id=id)
    if layer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Layer not found"
        )

    # Check if internal or external layer
    if layer.type in [LayerType.table.value, LayerType.feature_layer.value]:
        # Delete layer data
        await crud_layer.delete_layer_data(async_session=async_session, layer=layer)
    
    # Delete layer metadata
    await crud_layer.delete(
        db=async_session,
        id=id,
    )
    return


@router.get(
    "/{id}/unique-values/{column_name}",
    summary="Get unique values of a column",
    response_class=JSONResponse,
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
        "",
        description="CQL2-Filter in JSON format",
        example={"op": "=", "args": [{"property": "category"}, "bus_stop"]},
    ),
    order: OrderEnum = Query(
        "descendent",
        description="Specify the order to apply. There are the option ascendent or descendent.",
        example="descendent",
    ),
):
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
