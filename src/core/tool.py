from typing import List
from uuid import UUID

from fastapi import BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from sqlmodel import SQLModel

from src.core.config import settings
from src.crud.crud_job import job as crud_job
from src.crud.crud_layer import layer as crud_layer
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.crud.crud_project import project as crud_project
from src.db.models.layer import FeatureType, Layer, LayerType, ToolType
from src.schemas.job import JobStatusType, JobType
from src.schemas.layer import IFeatureLayerToolCreate, UserDataTable
from src.schemas.style import base_properties
from src.schemas.toolbox_base import MaxFeatureCnt


async def start_calculation(
    job_type: JobType,
    tool_class: object,
    crud_method: str,
    async_session: AsyncSession,
    user_id: UUID,
    background_tasks: BackgroundTasks,
    project_id: UUID,
    params: BaseModel,
):
    # Create job and check if user can create a new job
    job = await crud_job.check_and_create(
        async_session=async_session,
        user_id=user_id,
        job_type=job_type,
    )

    # Init class
    tool = tool_class(
        job_id=job.id,
        background_tasks=background_tasks,
        async_session=async_session,
        user_id=user_id,
        project_id=project_id,
    )

    # Execute the CRUD method
    await getattr(tool, crud_method)(
        params=params,
    )

    return {"job_id": job.id}


class CRUDToolBase:
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        self.job_id = job_id
        self.background_tasks = background_tasks
        self.async_session = async_session
        self.user_id = user_id
        self.project_id = project_id

    async def create_feature_layer_tool(
        self,
        layer_in: IFeatureLayerToolCreate,
    ):
        # Check layer name and alter if needed
        new_layer_name = await crud_layer_project.check_and_alter_layer_name(
            async_session=self.async_session,
            project_id=self.project_id,
            layer_name=layer_in.name,
        )
        layer_in.name = new_layer_name

        # Get project to put the new layer in the same folder as the project
        project = await crud_project.get(self.async_session, id=self.project_id)

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
            user_id=self.user_id,
            type=LayerType.feature,
            feature_layer_type=FeatureType.tool,
            properties=properties,
            thumbnail_url=thumbnail,
        )

        # Get extent, size and properties
        layer.size = await crud_layer.get_feature_layer_size(
            async_session=self.async_session, layer=layer
        )
        layer.extent = await crud_layer.get_feature_layer_extent(
            async_session=self.async_session, layer=layer
        )
        # Raise error if extent or size is None
        if layer.size is None:
            raise Exception("The layer size is None.")
        if layer.extent is None:
            raise Exception("The layer extent is None.")

        layer = await crud_layer.create(
            db=self.async_session,
            obj_in=layer,
        )

        # Add layer to project
        await crud_layer_project.create(
            async_session=self.async_session,
            project_id=self.project_id,
            layer_ids=[layer.id],
        )
        return {"status": JobStatusType.finished.value}

    async def check_max_feature_cnt(
        self,
        layers_project: List[BaseModel] | List[SQLModel] | List[dict],
        tool_type: ToolType,
    ):
        for layer_project in layers_project:
            # Check if BaseModel or SQLModel
            if isinstance(layer_project, BaseModel) or isinstance(
                layer_project, SQLModel
            ):
                feature_cnt = layer_project.filtered_count or layer_project.total_count
            elif isinstance(layer_project, dict):
                feature_cnt = layer_project.filtered_count or layer_project.total_count
            else:
                raise Exception(
                    "The layer_project is not of type BaseModel, SQLModel or dict."
                )
            if feature_cnt > MaxFeatureCnt[tool_type.value].value:
                raise Exception(
                    f"The operation cannot be performed on more than {MaxFeatureCnt[tool_type.value].value} features."
                )

    async def delete_orphan_data(self):
        # Delete orphan data from user tables
        user_id = self.user_id

        for table in UserDataTable:
            table_name = f"{table.value}_{str(user_id).replace('-', '')}"

            # Build condition for layer filtering
            if table == UserDataTable.no_geometry:
                condition = f"WHERE l.type = '{LayerType.table.value}'"
            else:
                condition = f"WHERE l.feature_layer_geometry_type = '{table.value}'"

            # Delete orphan data that don't exists in layer table and check for data not older then 30 minuts
            sql_delete_orphan_data = f"""
            WITH layer_ids_to_check AS (
                SELECT DISTINCT layer_id
                FROM {settings.USER_DATA_SCHEMA}."{table_name}"
                WHERE created_at > CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '30 minutes'
            ),
            to_delete AS (
                SELECT x.layer_id
                FROM layer_ids_to_check x
                LEFT JOIN
                (
                    SELECT l.id
                    FROM {settings.CUSTOMER_SCHEMA}.layer l
                    {condition}
                    AND l.user_id = '{str(user_id)}'
                ) l
                ON x.layer_id = l.id
                WHERE l.id IS NULL
            )
            DELETE FROM {settings.USER_DATA_SCHEMA}."{table_name}" x
            USING to_delete d
            WHERE x.layer_id = d.layer_id;
            """
            await self.async_session.execute(text(sql_delete_orphan_data))
        return
