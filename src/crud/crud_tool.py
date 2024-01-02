from sqlalchemy import text

from src.core.config import settings
from src.core.job import job_init, job_log, run_background_or_immediately
from src.core.tool import CRUDToolBase
from src.db.models.layer import ToolType
from src.schemas.error import ColumnTypeError
from src.schemas.job import JobStatusType, JobType
from src.schemas.layer import (
    FeatureGeometryType,
    IFeatureLayerToolCreate,
)
from src.schemas.tool import IAggregationPoint, IAggregationPolygon, IJoin
from src.schemas.toolbox_base import DefaultResultLayerName
from src.utils import (
    build_where_clause,
    get_result_column,
    get_statistics_sql,
    get_user_table,
    search_value,
)


# TODO: Refactor give each tools its own class
class CRUDTool(CRUDToolBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    @job_log(JobType.join.value)
    async def join(
        self,
        params: IJoin,
    ):
        # Get layers
        layers_project = await self.get_layers_project(
            params=params,
        )
        target_layer_project = layers_project["target_layer_project_id"]
        join_layer_project = layers_project["join_layer_project_id"]

        # Get translated fields
        mapped_target_field = search_value(
            target_layer_project.attribute_mapping, params.target_field
        )
        mapped_join_field = search_value(
            join_layer_project.attribute_mapping, params.join_field
        )

        # Check if mapped_target_field and mapped_join_field are having the same type
        if mapped_target_field.split("_")[0] != mapped_join_field.split("_")[0]:
            raise ColumnTypeError(
                "Mapped target field and mapped join field are not having the same type."
            )

        # Check if mapped statistics field is float, integer or biginteger
        mapped_statistics_field = await self.check_column_statistics(
            layer_project=join_layer_project,
            column_statistics_field=params.column_statistics.field,
        )
        mapped_statistics_field = mapped_statistics_field["mapped_statistics_field"]

        # Get result column name
        result_column = get_result_column(
            attribute_mapping=target_layer_project.attribute_mapping,
            base_column_name=params.column_statistics.operation.value,
            datatype=mapped_statistics_field.split("_")[0],
        )
        new_layer_attribute_mapping = target_layer_project.attribute_mapping.copy()
        new_layer_attribute_mapping.update(result_column)

        # Create new layer
        layer_in = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.join.value,
            feature_layer_geometry_type=target_layer_project.feature_layer_geometry_type,
            attribute_mapping=new_layer_attribute_mapping,
            tool_type=ToolType.join.value,
        )

        # Update user_id in target_layer_projet to meet the user_id of the user sending the request
        copy_target_layer_project = target_layer_project.copy(
            update={"user_id": self.user_id}
        )
        result_table = get_user_table(copy_target_layer_project)

        # Create insert statement
        insert_columns = (
            ", ".join(target_layer_project.attribute_mapping.keys())
            + ", "
            + list(result_column.keys())[0]
        )
        select_columns = ", ".join(
            f"{target_layer_project.table_name}." + value
            for value in ["geom"] + list(target_layer_project.attribute_mapping.keys())
        )
        insert_statement = (
            f"INSERT INTO {result_table} (layer_id, geom, {insert_columns})"
        )

        # Get statistics column query
        statistics_column_query = get_statistics_sql(
            f"{join_layer_project.table_name}." + mapped_statistics_field,
            operation=params.column_statistics.operation,
        )

        # Build combined where query
        where_query = build_where_clause(
            [target_layer_project.where_query, join_layer_project.where_query]
        )

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
        await self.async_session.execute(text(sql_query))

        # Create new layer
        await self.create_feature_layer_tool(
            layer_in=layer_in,
        )
        return {
            "status": JobStatusType.finished.value,
            "msg": "Layers where successfully joined.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def join_run(self, params: IJoin):
        return await self.join(params=params)

    async def join_fail(self, params: IJoin):
        await self.delete_orphan_data()


class CRUDAggregateBase(CRUDToolBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    async def prepare_aggregation(
        self, params: IAggregationPoint | IAggregationPolygon
    ):
        # Get layers
        layers_project = await self.get_layers_project(
            params=params,
        )
        source_layer_project = layers_project["source_layer_project_id"]
        aggregation_layer_project = layers_project.get("aggregation_layer_project_id")

        # Check if mapped statistics field is float, integer or biginteger
        result_check_statistics_field = await self.check_column_statistics(
            layer_project=source_layer_project,
            column_statistics_field=params.column_statistics.field,
        )
        mapped_statistics_field = result_check_statistics_field[
            "mapped_statistics_field"
        ]

        # Create distributed point table or polygon table depend on the geometry type
        if (
            source_layer_project.feature_layer_geometry_type
            == FeatureGeometryType.point
        ):
            temp_source = await self.create_distributed_point_table(
                layer_project=source_layer_project,
            )
        elif (
            source_layer_project.feature_layer_geometry_type
            == FeatureGeometryType.polygon
        ):
            temp_source = await self.create_distributed_polygon_table(
                layer_project=source_layer_project,
            )

        # Check if aggregation_layer_project_id exists
        if aggregation_layer_project:
            # Create distributed polygon table
            temp_aggregation = await self.create_distributed_polygon_table(
                layer_project=aggregation_layer_project,
            )
            attribute_mapping_aggregation = (
                aggregation_layer_project.attribute_mapping.copy()
            )
            # Build select columns
            select_columns_arr = ["geom"] + list(attribute_mapping_aggregation.keys())
            select_columns = ", ".join(
                f"{aggregation_layer_project.table_name}.{column}"
                for column in select_columns_arr
            )
        else:
            attribute_mapping_aggregation = {"text_attr1": f"h3_{params.h3_resolution}"}

        result_column = get_result_column(
            attribute_mapping=attribute_mapping_aggregation,
            base_column_name=params.column_statistics.operation.value,
            datatype=result_check_statistics_field["mapped_statistics_field_type"],
        )

        # Build group by columns
        if params.source_group_by_field:
            # Extend result column with jsonb
            result_column.update(
                get_result_column(
                    attribute_mapping=attribute_mapping_aggregation,
                    base_column_name=f"{params.column_statistics.operation.value}_grouped",
                    datatype="jsonb",
                )
            )
            # Build group by columns
            group_by_columns = ", ".join(
                f"{temp_source}.{search_value(source_layer_project.attribute_mapping, column)}"
                for column in params.source_group_by_field
            )
            group_by_select_columns = ", ".join(
                f"{temp_source}.{search_value(source_layer_project.attribute_mapping, column)}::text"
                for column in params.source_group_by_field
            )
            group_column_name = f"ARRAY_TO_STRING(ARRAY[{group_by_select_columns}], '_') AS group_column_name"

        # Get statistics column query
        statistics_column_query = get_statistics_sql(
            f"{temp_source}." + mapped_statistics_field,
            operation=params.column_statistics.operation,
        )

        # Create insert statement
        insert_columns_arr = (
            ["geom"]
            + list(attribute_mapping_aggregation.keys())
            + list(result_column.keys())
        )
        insert_columns = ", ".join(insert_columns_arr)

        # Create new layer
        layer_in = IFeatureLayerToolCreate(
            name=DefaultResultLayerName[params.tool_type].value,
            feature_layer_geometry_type=FeatureGeometryType.polygon,
            attribute_mapping={**attribute_mapping_aggregation, **result_column},
            tool_type=params.tool_type,
        )

        return {
            "aggregation_layer_project": locals().get(
                "aggregation_layer_project", None
            ),
            "layer_in": layer_in,
            "temp_source": temp_source,
            "temp_aggregation": locals().get("temp_aggregation", None),
            "group_by_columns": locals().get("group_by_columns", None),
            "group_column_name": locals().get("group_column_name", None),
            "statistics_column_query": statistics_column_query,
            "insert_columns": insert_columns,
            "select_columns": locals().get("select_columns", None),
            "result_check_statistics_field": result_check_statistics_field,
        }


class CRUDAggregatePoint(CRUDAggregateBase):
    """Tool aggregation points."""

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
        self.result_table = (
            f"{settings.USER_DATA_SCHEMA}.polygon_{str(self.user_id).replace('-', '')}"
        )

    @job_log(job_step_name="aggregation")
    async def aggregate_point(self, params: IAggregationPoint):
        # Prepare aggregation
        aggregation = await self.prepare_aggregation(params=params)
        aggregation_layer_project = aggregation["aggregation_layer_project"]
        layer_in = aggregation["layer_in"]
        temp_source = aggregation["temp_source"]
        temp_aggregation = aggregation["temp_aggregation"]
        group_by_columns = aggregation["group_by_columns"]
        group_column_name = aggregation["group_column_name"]
        statistics_column_query = aggregation["statistics_column_query"]
        insert_columns = aggregation["insert_columns"]
        select_columns = aggregation["select_columns"]

        # Create query
        if aggregation_layer_project:
            # Define subquery for grouped by id only
            sql_query_total_stats = f"""
                SELECT {temp_aggregation}.id, {statistics_column_query} AS stats
                FROM {temp_aggregation}, {temp_source}
                WHERE ST_Intersects({temp_aggregation}.geom, {temp_source}.geom)
                AND {temp_aggregation}.h3_3 = {temp_source}.h3_3
                GROUP BY {temp_aggregation}.id
            """

            if params.source_group_by_field:
                # Define subquery for grouped by id and group_by_field
                sql_query_group_stats = f"""
                    SELECT id, JSONB_OBJECT_AGG(group_column_name, stats) AS stats
                    FROM
                    (
                        SELECT {temp_aggregation}.id, {group_column_name}, {statistics_column_query} AS stats
                        FROM {temp_aggregation}, {temp_source}
                        WHERE ST_Intersects({temp_aggregation}.geom, {temp_source}.geom)
                        AND {temp_aggregation}.h3_3 = {temp_source}.h3_3
                        GROUP BY {temp_aggregation}.id, {group_by_columns}
                    ) AS to_group
                    GROUP BY id
                """

                # Build combined query with two left joins
                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    WITH total_stats AS
                    (
                        {sql_query_total_stats}
                    ),
                    grouped_stats AS
                    (
                        {sql_query_group_stats}
                    ),
                    first_join AS
                    (
                        SELECT total_stats.id, total_stats.stats AS total_stats, grouped_stats.stats AS grouped_stats
                        FROM grouped_stats, total_stats
                        WHERE grouped_stats.id = total_stats.id
                    )
                    SELECT '{layer_in.id}', {select_columns}, first_join.total_stats, first_join.grouped_stats
                    FROM {aggregation_layer_project.table_name}
                    LEFT JOIN first_join
                    ON {aggregation_layer_project.table_name}.id = first_join.id
                    WHERE {aggregation_layer_project.table_name}.layer_id = '{aggregation_layer_project.layer_id}'
                """
            else:
                # Build combined query with one left join
                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    WITH total_stats AS
                    (
                        {sql_query_total_stats}
                    )
                    SELECT '{layer_in.id}', {select_columns}, total_stats.stats AS total_stats
                    FROM {aggregation_layer_project.table_name}
                    LEFT JOIN total_stats
                    ON {aggregation_layer_project.table_name}.id = total_stats.id
                    WHERE {aggregation_layer_project.table_name}.layer_id = '{aggregation_layer_project.layer_id}'
                """
        else:
            # If aggregation_layer_project_id does not exist the h3 grid will be taken for the intersection
            sql_query_total_stats = f"""
                SELECT h3_lat_lng_to_cell(geom::point, {params.h3_resolution}) h3_index, {statistics_column_query} AS stats
                FROM {temp_source}
                GROUP BY h3_lat_lng_to_cell(geom::point, {params.h3_resolution})
            """

            if params.source_group_by_field:
                # Define subquery for grouped by id and group_by_field
                sql_query_group_stats = f"""
                    SELECT h3_index, JSONB_OBJECT_AGG(group_column_name, stats) AS stats
                    FROM
                    (
                        SELECT h3_lat_lng_to_cell(geom::point, {params.h3_resolution}) h3_index, {group_column_name}, {statistics_column_query} AS stats
                        FROM {temp_source}
                        GROUP BY h3_lat_lng_to_cell(geom::point, {params.h3_resolution}), {group_by_columns}
                    ) AS to_group
                    GROUP BY h3_index
                """

                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    WITH total_stats AS
                    (
                        {sql_query_total_stats}
                    ),
                    grouped_stats AS
                    (
                        {sql_query_group_stats}
                    )
                    SELECT '{layer_in.id}', ST_SETSRID(h3_cell_to_boundary(total_stats.h3_index)::geometry, 4326),
                    total_stats.h3_index, total_stats.stats AS total_stats, grouped_stats.stats AS grouped_stats
                    FROM grouped_stats, total_stats
                    WHERE grouped_stats.h3_index = total_stats.h3_index
                """
            else:
                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    WITH total_stats AS
                    (
                        {sql_query_total_stats}
                    )
                    SELECT '{layer_in.id}', ST_SETSRID(h3_cell_to_boundary(h3_index)::geometry, 4326), h3_index, total_stats.stats AS total_stats
                    FROM total_stats
                """
        # Execute query
        await self.async_session.execute(text(sql_query))

        # Create new layer
        await self.create_feature_layer_tool(
            layer_in=layer_in,
        )
        return {
            "status": JobStatusType.finished.value,
            "msg": "Points where successfully aggregated.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def aggregate_point_run(self, params: IAggregationPoint):
        return await self.aggregate_point(params=params)

    async def aggregate_point_fail(self, params: IAggregationPoint):
        await self.delete_orphan_data()


class CRUDAggregatePolygon(CRUDAggregateBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
        self.result_table = (
            f"{settings.USER_DATA_SCHEMA}.polygon_{str(self.user_id).replace('-', '')}"
        )

    @job_log(job_step_name="aggregation")
    async def aggregate_polygon(self, params: IAggregationPolygon):
        # Prepare aggregation
        aggregation = await self.prepare_aggregation(params=params)
        aggregation_layer_project = aggregation["aggregation_layer_project"]
        layer_in = aggregation["layer_in"]
        temp_source = aggregation["temp_source"]
        temp_aggregation = aggregation["temp_aggregation"]
        group_by_columns = aggregation["group_by_columns"]
        group_column_name = aggregation["group_column_name"]
        statistics_column_query = aggregation["statistics_column_query"]
        insert_columns = aggregation["insert_columns"]
        select_columns = aggregation["select_columns"]
        mapped_statistics_field = aggregation["result_check_statistics_field"]["mapped_statistics_field"]


        if params.weigthed_by_intersecting_area:
            statistics_column_query = f"{statistics_column_query} * (ST_AREA(ST_INTERSECTION({temp_aggregation}.geom, {temp_source}.geom)::geography))"

        if aggregation_layer_project:
            # Define subquery for grouped by id only
            sql_query_total_stats = f"""
                SELECT {temp_aggregation}.id, {statistics_column_query} AS stats
                FROM {temp_aggregation}, {temp_source}
                WHERE ST_Intersects({temp_aggregation}.geom, {temp_source}.geom)
                AND {temp_aggregation}.h3_3 = {temp_source}.h3_3
                GROUP BY {temp_aggregation}.id
            """

            if params.source_group_by_field:
                # Define subquery for grouped by id and group_by_field
                sql_query_group_stats = f"""
                    SELECT id, JSONB_OBJECT_AGG(group_column_name, stats) AS stats
                    FROM
                    (
                        SELECT {temp_aggregation}.id, {group_column_name}, {statistics_column_query} AS stats
                        FROM {temp_aggregation}, {temp_source}
                        WHERE ST_Intersects({temp_aggregation}.geom, {temp_source}.geom)
                        AND {temp_aggregation}.h3_3 = {temp_source}.h3_3
                        GROUP BY {temp_aggregation}.id, {group_by_columns}
                    ) AS to_group
                    GROUP BY id
                """

                # Build combined query with two left joins
                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    WITH total_stats AS
                    (
                        {sql_query_total_stats}
                    ),
                    grouped_stats AS
                    (
                        {sql_query_group_stats}
                    ),
                    first_join AS
                    (
                        SELECT total_stats.id, total_stats.stats AS total_stats, grouped_stats.stats AS grouped_stats
                        FROM grouped_stats, total_stats
                        WHERE grouped_stats.id = total_stats.id
                    )
                    SELECT '{layer_in.id}', {select_columns}, first_join.total_stats, first_join.grouped_stats
                    FROM {aggregation_layer_project.table_name}
                    LEFT JOIN first_join
                    ON {aggregation_layer_project.table_name}.id = first_join.id
                    WHERE {aggregation_layer_project.table_name}.layer_id = '{aggregation_layer_project.layer_id}'
                """
            else:
                # Build combined query with one left join
                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    WITH total_stats AS
                    (
                        {sql_query_total_stats}
                    )
                    SELECT '{layer_in.id}', {select_columns}, total_stats.stats AS total_stats
                    FROM {aggregation_layer_project.table_name}
                    LEFT JOIN total_stats
                    ON {aggregation_layer_project.table_name}.id = total_stats.id
                    WHERE {aggregation_layer_project.table_name}.layer_id = '{aggregation_layer_project.layer_id}'
                """
        else:
            print()

        # Execute query
        await self.async_session.execute(text(sql_query))

        # Create new layer
        await self.create_feature_layer_tool(
            layer_in=layer_in,
        )

        return {
            "status": JobStatusType.finished.value,
            "msg": "Polygons where successfully aggregated.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def aggregate_polygon_run(self, params: IAggregationPolygon):
        return await self.aggregate_polygon(params=params)
