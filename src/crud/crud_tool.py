from typing import List
from uuid import UUID

from fastapi import BackgroundTasks, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import SQLModel

from src.core.config import settings
from src.core.job import job_init, run_background_or_immediately
from src.crud.crud_layer import layer as crud_layer
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.crud.crud_project import project as crud_project
from src.db.models.layer import FeatureType, Layer, LayerType, ToolType
from src.schemas.job import JobStatusType
from src.schemas.layer import IFeatureLayerIndicatorCreate, OgrPostgresType
from src.schemas.style import base_properties
from src.schemas.tool import IJoin
from src.schemas.toolbox_base import MaxFeatureCnt
from src.utils import (
    build_where_clause,
    get_result_column,
    get_statistics_sql,
    get_user_table,
    search_value,
)


class CRUDTool:
    async def create_feature_layer_tool(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        project_id: UUID,
        layer_in: IFeatureLayerIndicatorCreate,
    ):
        # Check layer name and alter if needed
        new_layer_name = await crud_layer_project.check_and_alter_layer_name(
            async_session=async_session,
            project_id=project_id,
            layer_name=layer_in.name,
        )
        layer_in.name = new_layer_name

        # Get project to put the new layer in the same folder as the project
        project = await crud_project.get(async_session, id=project_id)

        # TODO: Compute properties dynamically and create thumbnail dynamically
        properties = base_properties["feature"]["tool"]["join"][
            layer_in.feature_layer_geometry_type
        ]
        thumbnail = (
            "https://goat-app-assets.s3.eu-central-1.amazonaws.com/logos/goat_green.png"
        )
        layer = Layer(
            **layer_in.dict(exclude_none=True),
            folder_id=project.folder_id,
            user_id=user_id,
            type=LayerType.feature,
            feature_layer_type=FeatureType.tool,
            properties=properties,
            thumbnail_url=thumbnail,
        )
        # Get extent, size and properties
        layer.extent = await crud_layer.get_feature_layer_extent(
            async_session=async_session, layer=layer
        )
        layer.size = await crud_layer.get_feature_layer_size(
            async_session=async_session, layer=layer
        )
        layer = await crud_layer.create(
            db=async_session,
            obj_in=layer,
        )

        # Add layer to project
        await crud_layer_project.create(
            async_session=async_session,
            project_id=project_id,
            layer_ids=[layer.id],
        )
        return {"status": JobStatusType.finished.value}

    async def check_max_feature_cnt(self, layers_project: List[BaseModel] | List[SQLModel] | List[dict], tool_type: ToolType):
        for layer_project in layers_project:
            # Check if BaseModel or SQLModel
            if isinstance(layer_project, BaseModel) or isinstance(layer_project, SQLModel):
                feature_cnt = layer_project.filtered_count or layer_project.total_count
            elif isinstance(layer_project, dict):
                feature_cnt = layer_project.filtered_count or layer_project.total_count
            else:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="The layer_project is not of type BaseModel, SQLModel or dict.",
                )
            if feature_cnt > MaxFeatureCnt[tool_type.value].value:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"The operation cannot be performed on more than {MaxFeatureCnt[tool_type.value].value} features.",
                )


    @run_background_or_immediately(settings)
    @job_init()
    async def join(
        self,
        job_id: UUID,
        background_tasks: BackgroundTasks,
        async_session: AsyncSession,
        user_id: UUID,
        project_id: UUID,
        params: IJoin,
    ):
        # Get target layer
        target_layer_project = await crud_layer_project.get_internal(
            async_session=async_session,
            id=params.target_layer_project_id,
            project_id=project_id,
        )
        # Get join layer
        join_layer_project = await crud_layer_project.get_internal(
            async_session=async_session,
            id=params.join_layer_project_id,
            project_id=project_id,
        )
        # Check Max feature count
        await self.check_max_feature_cnt(
            layers_project=[target_layer_project, join_layer_project],
            tool_type=ToolType.join,
        )

        # Check that the target layer is a feature layer. This is done because tool layers are currently a subset of feature layers.
        if target_layer_project.type != LayerType.feature.value:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="The operation can only be performed on feature layers.",
            )

        # Get translated fields
        mapped_target_field = search_value(
            target_layer_project.attribute_mapping, params.target_field
        )
        mapped_join_field = search_value(
            join_layer_project.attribute_mapping, params.join_field
        )
        mapped_statistics_field = search_value(
            join_layer_project.attribute_mapping, params.column_statistics.field
        )

        # Check if mapped_target_field and mapped_join_field are having the same type
        if mapped_target_field.split("_")[0] != mapped_join_field.split("_")[0]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Mapped target field and mapped join field are not having the same type.",
            )

        # Check if mapped statistics field is float, integer or biginteger
        mapped_statistics_field_type = mapped_statistics_field.split("_")[0]
        if mapped_statistics_field_type not in [
            OgrPostgresType.Integer,
            OgrPostgresType.Real,
            OgrPostgresType.Integer64,
        ]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Mapped statistics field is not {OgrPostgresType.Integer}, {OgrPostgresType.Real}, {OgrPostgresType.Integer64}. The operation cannot be performed on the {mapped_statistics_field_type} type.",
            )

        # Get result column name
        result_column = get_result_column(
            attribute_mapping=target_layer_project.attribute_mapping,
            base_column_name=params.column_statistics.operation.value,
            datatype=mapped_statistics_field.split("_")[0],
        )
        new_layer_attribute_mapping = target_layer_project.attribute_mapping.copy()
        new_layer_attribute_mapping.update(result_column)

        # Create new layer
        layer_in = IFeatureLayerIndicatorCreate(
            name=params.result_target.layer_name,
            feature_layer_geometry_type=target_layer_project.feature_layer_geometry_type,
            attribute_mapping=new_layer_attribute_mapping,
            tool_type=ToolType.join.value,
        )

        # Update user_id in target_layer_projet to meet the user_id of the user sending the request
        copy_target_layer_project = target_layer_project.copy(
            update={"user_id": user_id}
        )
        result_table = get_user_table(copy_target_layer_project)

        # Create insert statement
        insert_columns = (
            ", ".join(target_layer_project.attribute_mapping.keys())
            + ", "
            + list(result_column.keys())[0]
        )
        select_columns = ", ".join(
            f"{target_layer_project.table_name}." + value for value in ["geom"] + list(target_layer_project.attribute_mapping.keys())
        )
        insert_statement = f"INSERT INTO {result_table} (layer_id, geom, {insert_columns})"

        # Get statistics column query
        statistics_column_query = get_statistics_sql(
            f"{join_layer_project.table_name}." + mapped_statistics_field, operation=params.column_statistics.operation
        )

        # Build combined where query
        where_query = build_where_clause([target_layer_project.where_query, join_layer_project.where_query])

        # Create query
        sql_query = (
            insert_statement
            + f"""
            SELECT '{layer_in.id}', {select_columns}, {statistics_column_query}
            FROM {target_layer_project.table_name}
            LEFT JOIN {join_layer_project.table_name}
            ON {target_layer_project.table_name}.{mapped_target_field} = {join_layer_project.table_name}.{mapped_join_field}
            {where_query}
            GROUP BY {select_columns}
        """
        )

        # Execute query
        await async_session.execute(text(sql_query))

        # Create layer and add layer to project
        layer = await self.create_feature_layer_tool(
            async_session=async_session,
            user_id=user_id,
            project_id=project_id,
            layer_in=layer_in,
        )
        return layer


tool = CRUDTool()
