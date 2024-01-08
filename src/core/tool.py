from typing import List
from uuid import UUID, uuid4

from fastapi import BackgroundTasks
from httpx import AsyncClient
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
from src.schemas.error import (
    AreaSizeError,
    ColumnTypeError,
    FeatureCountError,
    GeometryTypeError,
    LayerExtentError,
    LayerProjectTypeError,
    LayerSizeError,
)
from src.schemas.job import JobStatusType, JobType, Msg, MsgType
from src.schemas.layer import (
    FeatureGeometryType,
    IFeatureLayerToolCreate,
    OgrPostgresType,
    UserDataGeomType,
    UserDataTable,
)
from src.schemas.style import base_properties
from src.schemas.tool import IToolParam
from src.schemas.toolbox_base import (
    ColumnStatisticsOperation,
    GeofenceTable,
    MaxFeatureCnt,
    MaxSizeReferenceArea,
)
from src.utils import build_where_clause, search_value


async def start_calculation(
    job_type: JobType,
    tool_class: object,
    crud_method: str,
    async_session: AsyncSession,
    user_id: UUID,
    background_tasks: BackgroundTasks,
    project_id: UUID,
    params: BaseModel,
    http_client: AsyncClient = None,
):
    # Create job and check if user can create a new job
    job = await crud_job.check_and_create(
        async_session=async_session,
        user_id=user_id,
        job_type=job_type,
    )

    # Init class
    tool = (
        tool_class(
            job_id=job.id,
            background_tasks=background_tasks,
            async_session=async_session,
            user_id=user_id,
            project_id=project_id,
        )
        if http_client is None
        else tool_class(
            job_id=job.id,
            background_tasks=background_tasks,
            async_session=async_session,
            user_id=user_id,
            project_id=project_id,
            http_client=http_client,
        )
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

    async def get_layers_project(self, params: IToolParam):
        # Get all params that have the name layer_project_id and build a dict using the variable name as key
        layer_project_ids = {}
        for key, value in params.dict().items():
            if key.endswith("_layer_project_id") and value is not None:
                layer_project_ids[key] = value

        # Get all layers_project
        layers_project = {}
        for layer_name in layer_project_ids:
            # Get layer_project
            layer_project_id = layer_project_ids[layer_name]
            input_layer_types = params.input_layer_types
            layer_project = await crud_layer_project.get_internal(
                async_session=self.async_session,
                id=layer_project_id,
                project_id=self.project_id,
                expected_layer_types=input_layer_types[layer_name].layer_types,
                expected_geometry_types=input_layer_types[
                    layer_name
                ].feature_layer_geometry_types,
            )
            layers_project[layer_name] = layer_project

        # Check Max feature count
        await self.check_max_feature_cnt(
            layers_project=list(layers_project.values()),
            tool_type=params.tool_type,
        )
        return layers_project

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
            raise LayerSizeError("The layer size is None.")
        if layer.extent is None:
            raise LayerExtentError("The layer extent is None.")

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
                raise LayerProjectTypeError(
                    "The layer_project is not of type BaseModel, SQLModel or dict."
                )
            if feature_cnt > MaxFeatureCnt[tool_type.value].value:
                raise FeatureCountError(
                    f"The operation cannot be performed on more than {MaxFeatureCnt[tool_type.value].value} features."
                )

    async def check_reference_area_size(
        self,
        layer_project: BaseModel | SQLModel | dict,
        tool_type: MaxSizeReferenceArea,
    ):
        # Build where query for layer
        where_query = build_where_clause([layer_project.where_query])

        # Check if layer has polygon geoms
        if (
            layer_project.feature_layer_geometry_type.value
            != UserDataGeomType.polygon.value
        ):
            raise GeometryTypeError(
                "Operation not supported. The layer does not contain polygon geometries. Pick a layer with polygon geometries."
            )

        # Call SQL function
        sql_query = """
            SELECT *
            FROM basic.area_statistics(:operation, :table_name, :where_query)
        """
        res = await self.async_session.execute(
            sql_query,
            {
                "operation": ColumnStatisticsOperation.sum.value,
                "table_name": layer_project.table_name,
                "where_query": where_query,
            },
        )
        res = res.fetchall()
        area = res[0][0]["sum"] / 1000000
        if area > MaxSizeReferenceArea[tool_type.value].value:
            raise AreaSizeError(
                f"The operation cannot be performed on more than {MaxSizeReferenceArea[tool_type.value].value} km2."
            )
        return

    async def check_reference_area_geofence(
        self,
        layer_project: BaseModel | SQLModel | dict,
        tool_type: MaxSizeReferenceArea,
    ):
        # Build where query for layer
        where_query = build_where_clause([layer_project.where_query])
        geofence_table = GeofenceTable[tool_type.value].value
        # Check if layer has polygon geoms
        sql = f"""
            WITH to_test AS
            (
                SELECT *
                FROM {layer_project.table_name}
                {where_query}
            )
            SELECT COUNT(*)
            FROM to_test t
            WHERE NOT EXISTS (
                SELECT 1
                FROM {geofence_table} AS g
                WHERE ST_WITHIN(t.geom, g.geom)
            )
        """
        # Execute query
        cnt_not_within = await self.async_session.execute(sql)
        cnt_not_within = cnt_not_within.scalar()

        if cnt_not_within > 0:
            return Msg(
                type=MsgType.warning,
                text=f"{cnt_not_within} features are not within the geofence.",
            )
        return Msg(type=MsgType.info, text="All features are within the geofence.")

    async def check_reference_area(
        self, layer_project_id: int, tool_type: MaxSizeReferenceArea
    ):
        # Get layer project
        layer_project = await crud_layer_project.get_internal(
            async_session=self.async_session,
            id=layer_project_id,
            project_id=self.project_id,
            expected_layer_types=[LayerType.feature],
            expected_geometry_types=[FeatureGeometryType.polygon],
        )
        # Check if the feature count is below the limit
        await self.check_max_feature_cnt(
            layers_project=[layer_project], tool_type=tool_type
        )
        # Check if the reference area size is below the limit
        await self.check_reference_area_size(
            layer_project=layer_project, tool_type=tool_type
        )
        # Check if the reference area is within in geofence
        return {
            "msg": await self.check_reference_area_geofence(
                layer_project=layer_project, tool_type=tool_type
            ),
            "layer_project": layer_project,
        }

    async def check_column_statistics(
        self,
        layer_project: BaseModel,
        column_statistics_field: str,
    ):
        # Check if field is $area and geometry type is polygon
        if column_statistics_field == "$area":
            if layer_project.feature_layer_geometry_type != FeatureGeometryType.polygon:
                raise GeometryTypeError(
                    "The layer does not contain polygon geometries and therefore $area cannot be computed. Pick a layer with polygon geometries."
                )
            return {
                "mapped_statistics_field": "area",
                "mapped_statistics_field_type": OgrPostgresType.Real,
            }

        mapped_statistics_field = search_value(
            layer_project.attribute_mapping, column_statistics_field
        )
        # Check if mapped statistics field is float, integer or biginteger
        mapped_statistics_field_type = mapped_statistics_field.split("_")[0]
        if mapped_statistics_field_type not in [
            OgrPostgresType.Integer,
            OgrPostgresType.Real,
            OgrPostgresType.Integer64,
        ]:
            raise ColumnTypeError(
                f"Mapped statistics field is not {OgrPostgresType.Integer}, {OgrPostgresType.Real}, {OgrPostgresType.Integer64}. The operation cannot be performed on the {mapped_statistics_field_type} type."
            )
        return {
            "mapped_statistics_field": mapped_statistics_field,
            "mapped_statistics_field_type": mapped_statistics_field_type,
        }

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

    async def create_distributed_polygon_table(
        self,
        layer_project: BaseModel,
    ):
        # Create table name
        table_suffix = str(uuid4()).replace("-", "")
        temp_polygons = f"temporal.temp_polygons_{table_suffix}"

        # Create distributed polygon table using sql
        where_query_polygon = "WHERE " + layer_project.where_query.replace("'", "''")
        await self.async_session.execute(
            f"""SELECT basic.create_distributed_polygon_table(
                '{layer_project.table_name}',
                '{'id, ' + ', '.join(list(layer_project.attribute_mapping.keys()))}',
                '{where_query_polygon}',
                30,
                '{temp_polygons}'
            )"""
        )
        # Commit changes
        await self.async_session.commit()
        return temp_polygons

    async def create_distributed_point_table(
        self,
        layer_project: BaseModel,
    ):
        # Create temp table name for points
        table_suffix = str(uuid4()).replace("-", "")
        temp_points = f"temporal.temp_points_{table_suffix}"

        # Create distributed point table using sql
        where_query_point = "WHERE " + layer_project.where_query.replace("'", "''")
        await self.async_session.execute(
            f"""SELECT basic.create_distributed_point_table(
                '{layer_project.table_name}',
                '{', '.join(list(layer_project.attribute_mapping.keys()))}',
                '{where_query_point}',
                '{temp_points}'
            )"""
        )
        # Commit changes
        await self.async_session.commit()
        return temp_points
