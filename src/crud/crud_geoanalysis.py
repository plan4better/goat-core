from sqlalchemy import text

from src.core.config import settings
from src.core.job import job_init, job_log, run_background_or_immediately
from src.core.tool import CRUDToolBase
from src.schemas.job import JobStatusType
from src.schemas.layer import (
    FeatureGeometryType,
    IFeatureLayerToolCreate,
)
from src.schemas.tool import IAggregationPoint, IAggregationPolygon
from src.schemas.toolbox_base import ColumnStatisticsOperation, DefaultResultLayerName
from src.utils import (
    convert_geom_measurement_field,
    get_result_column,
    get_statistics_sql,
    search_value,
)

class CRUDAggregateBase(CRUDToolBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
        self.table_name_total_stats = (
            f"temporal.total_stats_{str(self.job_id).replace('-', '')}"
        )
        self.table_name_grouped_stats = (
            f"temporal.grouped_stats_{str(self.job_id).replace('-', '')}"
        )

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
            "mapped_statistics_field": mapped_statistics_field,
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
                CREATE TABLE {self.table_name_total_stats} AS
                SELECT {temp_aggregation}.id, {statistics_column_query} AS stats
                FROM {temp_aggregation}, {temp_source}
                WHERE ST_Intersects({temp_aggregation}.geom, {temp_source}.geom)
                AND {temp_aggregation}.h3_3 = {temp_source}.h3_3
                GROUP BY {temp_aggregation}.id
            """
            await self.async_session.execute(sql_query_total_stats)
            await self.async_session.execute(
                f"CREATE INDEX ON {self.table_name_total_stats} (id);"
            )

            if params.source_group_by_field:
                # Define subquery for grouped by id and group_by_field
                sql_query_group_stats = f"""
                    CREATE TABLE {self.table_name_grouped_stats} AS
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
                await self.async_session.execute(text(sql_query_group_stats))
                await self.async_session.execute(
                    f"CREATE INDEX ON {self.table_name_grouped_stats} (id);"
                )

                # Build combined query with two left joins
                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    WITH first_join AS
                    (
                        SELECT t.id, t.stats AS total_stats, g.stats AS grouped_stats
                        FROM {self.table_name_grouped_stats} g, {self.table_name_total_stats} t
                        WHERE g.id = t.id
                    )
                    SELECT '{layer_in.id}', {select_columns}, f.total_stats, f.grouped_stats
                    FROM {aggregation_layer_project.table_name}
                    LEFT JOIN first_join f
                    ON {aggregation_layer_project.table_name}.id = f.id
                    WHERE {aggregation_layer_project.table_name}.layer_id = '{aggregation_layer_project.layer_id}'
                """
            else:
                # Build combined query with one left join
                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    SELECT '{layer_in.id}', {select_columns}, t.stats AS total_stats
                    FROM {aggregation_layer_project.table_name}
                    LEFT JOIN {self.table_name_total_stats} t
                    ON {aggregation_layer_project.table_name}.id = t.id
                    WHERE {aggregation_layer_project.table_name}.layer_id = '{aggregation_layer_project.layer_id}'
                """
        else:
            # If aggregation_layer_project_id does not exist the h3 grid will be taken for the intersection
            sql_query_total_stats = f"""
                CREATE TABLE {self.table_name_total_stats} AS
                SELECT h3_lat_lng_to_cell(geom::point, {params.h3_resolution}) h3_index, {statistics_column_query} AS stats
                FROM {temp_source}
                GROUP BY h3_lat_lng_to_cell(geom::point, {params.h3_resolution})
            """
            await self.async_session.execute(sql_query_total_stats)
            await self.async_session.execute(
                f"CREATE INDEX ON {self.table_name_total_stats} (h3_index);"
            )

            if params.source_group_by_field:
                # Define subquery for grouped by id and group_by_field
                sql_query_group_stats = f"""
                    CREATE TABLE {self.table_name_grouped_stats} AS
                    SELECT h3_index, JSONB_OBJECT_AGG(group_column_name, stats) AS stats
                    FROM
                    (
                        SELECT h3_lat_lng_to_cell(geom::point, {params.h3_resolution}) h3_index, {group_column_name}, {statistics_column_query} AS stats
                        FROM {temp_source}
                        GROUP BY h3_lat_lng_to_cell(geom::point, {params.h3_resolution}), {group_by_columns}
                    ) AS to_group
                    GROUP BY h3_index
                """
                await self.async_session.execute(sql_query_group_stats)
                await self.async_session.execute(
                    f"CREATE INDEX ON {self.table_name_grouped_stats} (h3_index);"
                )

                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    SELECT '{layer_in.id}', ST_SETSRID(h3_cell_to_boundary(t.h3_index)::geometry, 4326),
                    t.h3_index, t.stats AS total_stats, g.stats AS grouped_stats
                    FROM {self.table_name_total_stats} t, {self.table_name_grouped_stats} g
                    WHERE t.h3_index = g.h3_index
                """
            else:
                sql_query = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    SELECT '{layer_in.id}', ST_SETSRID(h3_cell_to_boundary(h3_index)::geometry, 4326),
                    h3_index, t.stats AS total_stats
                    FROM {self.table_name_total_stats} t
                """
        # Execute query
        await self.async_session.execute(sql_query)

        # Create new layer
        await self.create_feature_layer_tool(
            layer_in=layer_in,
        )

        # Delete temporary tables
        await self.delete_temp_tables()

        return {
            "status": JobStatusType.finished.value,
            "msg": "Points where successfully aggregated.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def aggregate_point_run(self, params: IAggregationPoint):
        return await self.aggregate_point(params=params)

    async def aggregate_point_fail(self, params: IAggregationPoint):
        # Delete orphan data
        await self.delete_orphan_data()
        # Delete temporary tables
        await self.delete_temp_tables()


class CRUDAggregatePolygon(CRUDAggregateBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
        self.result_table = (
            f"{settings.USER_DATA_SCHEMA}.polygon_{str(self.user_id).replace('-', '')}"
        )
        self.table_name_pre_grouped = (
            f"temporal.h3_pregrouped_{str(self.job_id).replace('-', '')}"
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
        mapped_statistics_field = aggregation["result_check_statistics_field"][
            "mapped_statistics_field"
        ]
        if aggregation_layer_project:
            if params.weigthed_by_intersecting_area:
                statistics_column_query = f"{statistics_column_query} * SUM(ST_AREA(ST_INTERSECTION({temp_aggregation}.geom, {temp_source}.geom)) / ST_AREA({temp_source}.geom))"

            # Define subquery for grouped by id only
            sql_query_total_stats = f"""
                CREATE TABLE {self.table_name_total_stats} AS
                SELECT {temp_aggregation}.id, round(({statistics_column_query})::numeric, 6) AS stats
                FROM {temp_aggregation}, {temp_source}
                WHERE ST_Intersects({temp_aggregation}.geom, {temp_source}.geom)
                AND {temp_aggregation}.h3_3 = {temp_source}.h3_3
                GROUP BY {temp_aggregation}.id
            """
            await self.async_session.execute(sql_query_total_stats)
            await self.async_session.execute(
                f"CREATE INDEX ON {self.table_name_total_stats} (id);"
            )

            if params.source_group_by_field:
                # Define subquery for grouped by id and group_by_field

                sql_query_group_stats = f"""
                    CREATE TABLE {self.table_name_grouped_stats} AS
                    SELECT id, JSONB_OBJECT_AGG(group_column_name, stats) AS stats
                    FROM
                    (
                        SELECT {temp_aggregation}.id, {group_column_name}, round(({statistics_column_query})::numeric, 6) AS stats
                        FROM {temp_aggregation}, {temp_source}
                        WHERE ST_Intersects({temp_aggregation}.geom, {temp_source}.geom)
                        AND {temp_aggregation}.h3_3 = {temp_source}.h3_3
                        GROUP BY {temp_aggregation}.id, {group_by_columns}
                    ) AS to_group
                    GROUP BY id
                """
                await self.async_session.execute(text(sql_query_group_stats))
                await self.async_session.execute(
                    f"CREATE INDEX ON {self.table_name_grouped_stats} (id);"
                )

                # Build combined query with two left joins
                sql_query_combine = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    WITH first_join AS
                    (
                        SELECT t.id, t.stats AS total_stats, g.stats AS grouped_stats
                        FROM {self.table_name_grouped_stats} g, {self.table_name_total_stats} t
                        WHERE g.id = t.id
                    )
                    SELECT '{layer_in.id}', {select_columns}, f.total_stats, f.grouped_stats
                    FROM {aggregation_layer_project.table_name}
                    LEFT JOIN first_join f
                    ON {aggregation_layer_project.table_name}.id = f.id
                    WHERE {aggregation_layer_project.table_name}.layer_id = '{aggregation_layer_project.layer_id}'
                """
            else:
                # Build combined query with one left join
                sql_query_combine = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    SELECT '{layer_in.id}', {select_columns}, t.stats AS total_stats
                    FROM {aggregation_layer_project.table_name}
                    LEFT JOIN {self.table_name_total_stats} t
                    ON {aggregation_layer_project.table_name}.id = t.id
                    WHERE {aggregation_layer_project.table_name}.layer_id = '{aggregation_layer_project.layer_id}'
                """
        else:
            # Get average edge length of h3 grid
            avg_edge_length = await self.async_session.execute(
                f"SELECT h3_get_hexagon_edge_length_avg({params.h3_resolution}, 'm')"
            )
            avg_edge_length = avg_edge_length.scalars().first()

            # Build group by fields
            group_by_columns_subquery = ""
            if params.source_group_by_field:
                group_by_columns.replace(f"{temp_source}.", "")
                group_by_columns_subquery = group_by_columns.replace(
                    f"{temp_source}.", "p."
                )
                group_column_name = group_column_name.replace(f"{temp_source}.", "p.")

            # Build statistics column query
            if params.weigthed_by_intersecting_area:
                first_statistic_column_query = """* SUM(
                    (CASE WHEN ST_WITHIN(j.geom, p.geom) THEN 1
                    WHEN ST_Intersects(j.geom, p.geom) THEN ST_AREA(ST_Intersection(j.geom, p.geom)) / ST_AREA(j.geom)
                    ELSE 0
                    END))
                """
                if (
                    params.column_statistics.operation.value
                    == ColumnStatisticsOperation.count.value
                ):
                    statistics_val = "1"
                    statistics_sql = "SUM(val)"
                else:
                    if mapped_statistics_field == "$intersected_area":
                        statistics_val = convert_geom_measurement_field(
                            "j." + mapped_statistics_field
                        )
                    else:
                        statistics_val = "p." + mapped_statistics_field

                    statistics_sql = get_statistics_sql(
                        "val", params.column_statistics.operation
                    )

            # Pregroup the data
            group_column_name_with_comma = (
                f"{group_column_name}, " if group_column_name else ""
            )
            group_by_columns_subquery_with_comma = (
                f"{group_by_columns_subquery}, " if group_by_columns_subquery else ""
            )

            sql_query_pre_grouped = f"""
                CREATE TABLE {self.table_name_pre_grouped} AS
                SELECT h3_target, {group_column_name_with_comma}
                (ARRAY_AGG({statistics_val}))[1] {first_statistic_column_query} AS val
                FROM (
                    SELECT *,
                    (ST_DUMP(ST_BUFFER(geom::geography, {avg_edge_length})::geometry)).geom AS buffer_geom
                    FROM {temp_source}
                ) p
                LEFT JOIN LATERAL (
                    SELECT h3_target, ST_SETSRID(h3_cell_to_boundary(h3_target)::geometry, 4326) AS geom
                    FROM
                    (
                        SELECT CASE WHEN h3_polygon_to_cells IS NULL
                        THEN h3_lat_lng_to_cell(ST_CENTROID(p.buffer_geom)::point, {params.h3_resolution})
                        ELSE h3_polygon_to_cells
                        END AS h3_target
                        FROM
                        (
                            SELECT h3_polygon_to_cells
                            FROM h3_polygon_to_cells(p.buffer_geom::polygon, ARRAY[]::polygon[], {params.h3_resolution})
                        ) x
                    ) y
                ) j ON TRUE
                WHERE ST_Intersects(j.geom, p.geom)
                GROUP BY h3_target, {group_by_columns_subquery_with_comma} p.id;
            """
            await self.async_session.execute(sql_query_pre_grouped)
            await self.async_session.execute(
                f"CREATE INDEX ON {self.table_name_pre_grouped} (h3_target);"
            )

            # Compute total stats
            sql_query_total_stats = f"""
                CREATE TABLE {self.table_name_total_stats} AS
                SELECT h3_target::text, ROUND({statistics_sql}::numeric, 6) AS stats
                FROM {self.table_name_pre_grouped}
                GROUP BY h3_target;
            """
            await self.async_session.execute(sql_query_total_stats)
            await self.async_session.execute(
                f"CREATE INDEX ON {self.table_name_total_stats} (h3_target);"
            )

            if params.source_group_by_field:
                # Compute grouped stats
                sql_query_group_stats = f"""
                    CREATE TABLE {self.table_name_grouped_stats} AS
                    SELECT h3_target, JSONB_OBJECT_AGG(group_column_name, stats) AS stats
                    FROM
                    (
                        SELECT h3_target::text, group_column_name, ROUND({statistics_sql}::numeric, 6) AS stats
                        FROM {self.table_name_pre_grouped}
                        GROUP BY h3_target, group_column_name
                    ) AS to_group
                    GROUP BY h3_target;
                """
                await self.async_session.execute(text(sql_query_group_stats))
                await self.async_session.execute(
                    f"CREATE INDEX ON {self.table_name_grouped_stats} (h3_target);"
                )

                sql_query_combine = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    SELECT '{layer_in.id}', ST_SETSRID(h3_cell_to_boundary(t.h3_target::h3index)::geometry, 4326),
                    t.h3_target, t.stats as total_stats, g.stats AS grouped_stats
                    FROM {self.table_name_grouped_stats} g, {self.table_name_total_stats} t
                    WHERE g.h3_target = t.h3_target;
                """
            else:
                sql_query_combine = f"""
                    INSERT INTO {self.result_table} (layer_id, {insert_columns})
                    SELECT '{layer_in.id}', ST_SETSRID(h3_cell_to_boundary(h3_target::h3index)::geometry, 4326),
                    h3_target, stats AS total_stats
                    FROM {self.table_name_total_stats}
                """

        # Execute combined query
        await self.async_session.execute(sql_query_combine)

        # Create new layer
        await self.create_feature_layer_tool(
            layer_in=layer_in,
        )
        # Delete temporary tables
        await self.delete_temp_tables()

        return {
            "status": JobStatusType.finished.value,
            "msg": "Polygons where successfully aggregated.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def aggregate_polygon_run(self, params: IAggregationPolygon):
        return await self.aggregate_polygon(params=params)

    async def aggregate_polygon_fail(self, params: IAggregationPolygon):
        # Delete orphan data
        await self.delete_orphan_data()
        # Delete temporary tables
        await self.delete_temp_tables()
