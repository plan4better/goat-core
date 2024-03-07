from typing import List
from uuid import UUID

from fastapi import BackgroundTasks
from fastapi_pagination import Params as PaginationParams
from httpx import AsyncClient
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import SQLModel

from src.core.job import CRUDFailedJob
from src.crud.crud_job import job as crud_job
from src.crud.crud_layer import layer as crud_layer
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.crud.crud_project import project as crud_project
from src.db.models.layer import FeatureType, Layer, LayerType, ToolType
from src.schemas.common import OrderEnum
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
    ComputeBreakOperation,
    FeatureGeometryType,
    IFeatureLayerToolCreate,
    OgrPostgresType,
    UserDataGeomType,
)
from src.schemas.style import (
    get_base_style,
    get_tool_style_with_breaks,
    get_tool_style_ordinal,
    custom_styles,
)
from src.schemas.tool import IToolParam
from src.schemas.toolbox_base import (
    ColumnStatisticsOperation,
    ColumnStatistic,
    GeofenceTable,
    MaxFeatureCnt,
    MaxFeaturePolygonArea,
)
from src.utils import build_where_clause, get_random_string, search_value


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
        project_id=project_id,
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


def convert_geom_measurement_field(field):
    if field.endswith("$intersected_area"):
        field = f"ST_AREA({field.replace('$intersected_area', 'geom')}::geography)"
    elif field.endswith("$length"):
        field = f"ST_LENGTH({field.replace('$length', 'geom')}::geography)"
    return field


def get_statistics_sql(field, operation):
    # Check if field endswith $intersected_area
    field = convert_geom_measurement_field(field)

    if operation == ColumnStatisticsOperation.count:
        query = f"COUNT({field})"
    elif operation == ColumnStatisticsOperation.sum:
        query = f"SUM({field})"
    elif operation == ColumnStatisticsOperation.mean:
        query = f"AVG({field})"
    elif operation == ColumnStatisticsOperation.median:
        query = f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {field})"
    elif operation == ColumnStatisticsOperation.min:
        query = f"MIN({field})"
    elif operation == ColumnStatisticsOperation.max:
        query = f"MAX({field})"
    else:
        raise ValueError(f"Unsupported operation {operation}")

    return query


class CRUDToolBase(CRUDFailedJob):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id)
        self.project_id = project_id

    async def get_layers_project(self, params: IToolParam):
        # Get all params that have the name layer_project_id and build a dict using the variable name as key
        layer_project_ids = {}
        for key, value in params.dict().items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if sub_key.endswith("layer_project_id") and sub_value is not None:
                        layer_project_ids[sub_key] = sub_value
            elif key.endswith("layer_project_id") and value is not None:
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

        # If geometry is of type polygon and tool type is in MaxFeaturePolygonArea
        for layer_project in layers_project.values():
            # Check for each feature layer of type polygon if the tool type is in MaxFeaturePolygonArea
            if layer_project.type == LayerType.feature:
                if (
                    layer_project.feature_layer_geometry_type
                    == FeatureGeometryType.polygon
                    and params.tool_type in MaxFeaturePolygonArea.__members__
                ):
                    # Check reference area size
                    await self.check_reference_area_size(
                        layer_project=layer_project,
                        tool_type=params.tool_type,
                    )

        return layers_project

    async def create_feature_layer_tool(
        self,
        layer_in: IFeatureLayerToolCreate,
        params: IToolParam,
    ):
        # Get project to put the new layer in the same folder as the project
        project = await crud_project.get(self.async_session, id=self.project_id)

        # Check layer name and alter if needed
        new_layer_name = await crud_layer_project.check_and_alter_layer_name(
            async_session=self.async_session,
            folder_id=project.folder_id,
            layer_name=layer_in.name,
            project_id=self.project_id,
        )
        layer_in.name = new_layer_name

        # TODO: Compute properties dynamically and create thumbnail dynamically
        properties = get_base_style(layer_in.feature_layer_geometry_type)
        thumbnail_url = (
            "https://goat-app-assets.s3.eu-central-1.amazonaws.com/logos/goat_green.png"
        )
        layer = Layer(
            **layer_in.dict(exclude_none=True),
            folder_id=project.folder_id,
            user_id=self.user_id,
            type=LayerType.feature,
            feature_layer_type=FeatureType.tool,
            properties=properties,
            thumbnail_url=thumbnail_url,
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

        # Create layer
        layer = await crud_layer.create(
            db=self.async_session,
            obj_in=layer,
        )
        await crud_layer.label_cluster_keep(
            async_session=self.async_session,
            layer=layer,
        )

        # Create style for layer
        # Request scale breaks in case of color_scale
        properties = None
        if layer.tool_type in custom_styles:
            properties = custom_styles[layer.tool_type][
                layer.feature_layer_geometry_type
            ]

        elif hasattr(params, "properties_base"):
            if (
                params.properties_base.get("color_scale")
                and params.properties_base.get("color_field").get("type") == "number"
                and params.properties_base["color_field"]["name"] in (layer_in.attribute_mapping.values())
            ):
                # Get unique values for color field
                unique_values = await crud_layer.get_unique_values(
                    async_session=self.async_session,
                    id=layer.id,
                    column_name=params.properties_base["color_field"]["name"],
                    order=OrderEnum.descendent.value,
                    query=None,
                    page_params=PaginationParams(page=1, size=7),
                )
                if params.properties_base.get("breaks") is None:
                    # Get len propertes as breaks
                    breaks = len(unique_values.items)
                else:
                    # Get breaks from params if len is less then number of unique values
                    breaks = params.properties_base["breaks"] if len(unique_values.items) > params.properties_base["breaks"] else len(unique_values.items)

                if breaks > 2:
                    # Get unique unique scale breaks
                    operation = params.properties_base.get("color_scale")
                    # Get scale breaks
                    color_scale_breaks = await crud_layer.get_class_breaks(
                        async_session=self.async_session,
                        id=layer.id,
                        operation=ComputeBreakOperation(operation),
                        column_name=params.properties_base["color_field"]["name"],
                        stripe_zeros=True,
                        breaks=breaks,
                        query=None,
                    )
                    # Get properties
                    properties = get_tool_style_with_breaks(
                        feature_geometry_type=layer.feature_layer_geometry_type,
                        color_field=params.properties_base["color_field"],
                        color_scale_breaks=color_scale_breaks,
                        color_range_type=params.properties_base["color_range_type"],
                    )
            elif (
                params.properties_base.get("color_scale")
                and params.properties_base.get("color_field").get("type") == "string"
            ):
                # Check if layer has max nine unique values in color_field
                unique_values = await crud_layer.get_unique_values(
                    async_session=self.async_session,
                    id=layer.id,
                    column_name=params.properties_base["color_field"]["name"],
                    order=OrderEnum.descendent.value,
                    query=None,
                    page_params=PaginationParams(page=1, size=7),
                )
                # Get properties
                unique_values = [item.value for item in unique_values.items]
                properties = get_tool_style_ordinal(
                    feature_geometry_type=layer.feature_layer_geometry_type,
                    color_range_type=params.properties_base["color_range_type"],
                    color_field=params.properties_base["color_field"],
                    unique_values=unique_values,
                )

        if properties is None:
            properties = get_base_style(layer_in.feature_layer_geometry_type)

        # Update layer with properties and thumbnail
        layer = await crud_layer.update(
            async_session=self.async_session,
            id=layer.id,
            layer_in={"properties": properties},
        )

        # Label cluster_keep
        if layer.type == LayerType.feature:
            await crud_layer.label_cluster_keep(self.async_session, layer)

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
        tool_type: ToolType,
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
        if area > MaxFeaturePolygonArea[tool_type.value].value:
            raise AreaSizeError(
                f"The operation cannot be performed on more than {MaxFeaturePolygonArea[tool_type.value].value} km2."
            )
        return

    async def check_reference_area_geofence(
        self,
        layer_project: BaseModel | SQLModel | dict,
        tool_type: MaxFeaturePolygonArea,
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

    async def check_reference_area(self, layer_project_id: int, tool_type: ToolType):
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

    async def check_is_number(self, data_type: str):
        """Check if the data type is a number."""
        if data_type not in [
            OgrPostgresType.Integer,
            OgrPostgresType.Real,
            OgrPostgresType.Integer64,
        ]:
            raise ColumnTypeError(
                f"Field has to be {OgrPostgresType.Integer}, {OgrPostgresType.Real}, {OgrPostgresType.Integer64}."
            )

    async def check_column_statistics(
        self,
        layer_project: BaseModel,
        column_statistics: ColumnStatistic,
    ):
        """Check if the column statistics field is valid and return the mapped statistics field and the mapped statistics field type."""

        # Check if field is $intersected_area and geometry type is polygon
        column_statistics_field = column_statistics.field
        column_statistics_operation = column_statistics.operation

        if column_statistics_field == "$intersected_area":
            if layer_project.feature_layer_geometry_type != FeatureGeometryType.polygon:
                raise GeometryTypeError(
                    "The layer does not contain polygon geometries and therefore $intersected_area cannot be computed. Pick a layer with polygon geometries."
                )
            return {
                "mapped_statistics_field": "$intersected_area",
                "mapped_statistics_field_type": OgrPostgresType.Real.value,
            }

        # Get mapped field
        mapped_statistics_field = search_value(
            layer_project.attribute_mapping, column_statistics_field
        )
        mapped_statistics_field_type = mapped_statistics_field.split("_")[0]

        # Check if mapped statistics field is float, integer or biginteger if the operation is not count
        if column_statistics_operation != ColumnStatisticsOperation.count:
            await self.check_is_number(mapped_statistics_field_type)

        return {
            "mapped_statistics_field": mapped_statistics_field,
            "mapped_statistics_field_type": mapped_statistics_field_type,
        }

    async def check_column_same_type(
        self, layers_project: BaseModel, columns: list[str]
    ):
        """Check if all columns are having the same type"""

        # Check if len layers_project and columns are the same
        if len(layers_project) != len(columns):
            raise ValueError("The number of columns and layers are not the same.")

        # Populate mapped_field_type array
        mapped_field_type = []
        for i in range(len(layers_project)):
            layer_project = layers_project[i]
            column = columns[i]

            # Get mapped field
            mapped_field = search_value(layer_project.attribute_mapping, column)
            mapped_field_type.append(mapped_field.split("_")[0])

        # Check if all mapped_field_type are the same
        if len(set(mapped_field_type)) != 1:
            raise ColumnTypeError("The columns are not having the same type.")

    async def create_temp_table_name(self, prefix: str):
        # Create temp table name
        table_suffix = str(self.job_id).replace("-", "")
        temp_table = f"temporal.{prefix}_{get_random_string(6)}_{table_suffix}"
        return temp_table

    async def create_distributed_polygon_table(
        self,
        layer_project: BaseModel,
    ):
        # Create table name
        temp_polygons = await self.create_temp_table_name("polygons")

        # Create distributed polygon table using sql
        where_query_polygon = "WHERE " + layer_project.where_query.replace("'", "''")
        arr_columns = ["id"] + list(layer_project.attribute_mapping.keys())

        await self.async_session.execute(
            f"""SELECT basic.create_distributed_polygon_table(
                '{layer_project.table_name}',
                '{', '.join(arr_columns)}',
                '{where_query_polygon}',
                30,
                '{temp_polygons}'
            )"""
        )
        # Commit changes
        await self.async_session.commit()
        return temp_polygons

    async def create_distributed_line_table(
        self,
        layer_project: BaseModel,
    ):
        # Create temp table name for lines
        temp_lines = await self.create_temp_table_name("lines")

        # Create distributed line table using sql
        where_query_line = "WHERE " + layer_project.where_query.replace("'", "''")
        arr_columns = ["id"] + list(layer_project.attribute_mapping.keys())

        await self.async_session.execute(
            f"""SELECT basic.create_distributed_line_table(
                '{layer_project.table_name}',
                '{', '.join(arr_columns)}',
                '{where_query_line}',
                '{temp_lines}'
            )"""
        )
        # Commit changes
        await self.async_session.commit()
        return temp_lines

    async def create_distributed_point_table(
        self,
        layer_project: BaseModel,
    ):
        # Create temp table name for points
        temp_points = await self.create_temp_table_name("points")

        # Create distributed point table using sql
        where_query_point = "WHERE " + layer_project.where_query.replace("'", "''")
        arr_columns = ["id"] + list(layer_project.attribute_mapping.keys())

        await self.async_session.execute(
            f"""SELECT basic.create_distributed_point_table(
                '{layer_project.table_name}',
                '{', '.join(arr_columns)}',
                '{where_query_point}',
                '{temp_points}'
            )"""
        )
        # Commit changes
        await self.async_session.commit()
        return temp_points

    async def create_temp_table_layer(self, layer_project: BaseModel):
        """Create a temp table for the layer_project."""

        temp_geometry_layer = await self.create_temp_table_name("layer")
        where_query = "WHERE " + layer_project.where_query
        sql_temp_geometry_layer = f"""
            CREATE TABLE {temp_geometry_layer} AS
            SELECT *
            FROM {layer_project.table_name}
            {where_query}
        """
        await self.async_session.execute(sql_temp_geometry_layer)
        await self.async_session.commit()
        return temp_geometry_layer
