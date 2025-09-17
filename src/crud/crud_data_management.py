from sqlalchemy import text

from src.core.config import settings
from src.core.job import job_init, job_log, run_background_or_immediately
from src.core.layer import get_user_table
from src.core.tool import CRUDToolBase
from src.db.models.layer import ToolType
from src.schemas.job import JobStatusType, JobType
from src.schemas.layer import (
    IFeatureLayerToolCreate,
)
from src.schemas.tool import IJoin, IJoinClassical, DuplicateHandling, JoinType
from src.schemas.toolbox_base import DefaultResultLayerName
from src.utils import (
    build_where_clause,
    get_result_column,
    search_value,
)


class CRUDJoin(CRUDToolBase):
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

        # Check if mapped statistics field is float, integer or biginteger
        mapped_statistics_field = await self.check_column_statistics(
            layer_project=join_layer_project,
            column_name=params.column_statistics.field,
            operation=params.column_statistics.operation,
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
            job_id=self.job_id,
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
            "target_layer." + value
            for value in ["geom"] + list(target_layer_project.attribute_mapping.keys())
        )
        insert_statement = (
            f"INSERT INTO {result_table} (layer_id, geom, {insert_columns})"
        )

        # Get statistics column query
        statistics_column_query = self.get_statistics_sql(
            "join_layer." + mapped_statistics_field,
            operation=params.column_statistics.operation,
        )

        # Build combined where query
        where_query = build_where_clause(
            [
                target_layer_project.where_query.replace(
                    f"{target_layer_project.table_name}.", "target_layer."
                ),
                join_layer_project.where_query.replace(
                    f"{join_layer_project.table_name}.", "join_layer."
                ),
            ],
        )

        # Create query
        sql_query = (
            insert_statement
            + f"""
            SELECT '{layer_in.id}', {select_columns}, {statistics_column_query}
            FROM {target_layer_project.table_name} target_layer
            LEFT JOIN {join_layer_project.table_name} join_layer
            ON target_layer.{mapped_target_field}::text = join_layer.{mapped_join_field}::text
            {where_query}
            GROUP BY {select_columns}
        """
        )

        # Execute query
        await self.async_session.execute(text(sql_query))

        # Create new layer
        await self.create_feature_layer_tool(
            layer_in=layer_in,
            params=params,
        )
        return {
            "status": JobStatusType.finished.value,
            "msg": "Layers where successfully joined.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def join_run(self, params: IJoin):
        return await self.join(params=params)


class CRUDJoinClassical(CRUDToolBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
    
    @job_log(JobType.join_classical.value)
    async def join_classical(
        self,
        params: IJoinClassical,
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

        # Create attribute mapping for the result layer (combine both layers' attributes)
        # Prefix join layer attributes to avoid column name conflicts
        join_layer_attribute_mapping = {
            f"join_{k}": v for k, v in join_layer_project.attribute_mapping.items()
        }
        new_layer_attribute_mapping = target_layer_project.attribute_mapping.copy()
        new_layer_attribute_mapping.update(join_layer_attribute_mapping)

        # Create new layer
        layer_in = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.join_classical.value,
            feature_layer_geometry_type=target_layer_project.feature_layer_geometry_type,
            attribute_mapping=new_layer_attribute_mapping,
            tool_type=ToolType.join_classical.value,
            job_id=self.job_id,
        )

        # Update user_id in target_layer_projet to meet the user_id of the user sending the request
        copy_target_layer_project = target_layer_project.copy(
            update={"user_id": self.user_id}
        )
        result_table = get_user_table(copy_target_layer_project)

        # Select columns from both tables
        target_columns = list(target_layer_project.attribute_mapping.keys())
        join_columns_with_prefix = list(join_layer_attribute_mapping.keys())
        join_columns_original = list(join_layer_project.attribute_mapping.keys())
        
        # Create statement for duplicate handling
        distinct_clause = ""
        group_by_clause = ""
        
        # Process duplicates according to the chosen method
        if params.handle_duplicates == DuplicateHandling.keep_first:
            distinct_clause = "DISTINCT ON (target_layer.id)"
        elif params.handle_duplicates == DuplicateHandling.aggregate:
            # We'll use GROUP BY on the target layer's id and aggregate join fields with first_value
            group_by_clause = f"GROUP BY target_layer.id, target_layer.geom, {', '.join(['target_layer.' + col for col in target_columns])}"

        # Create insert statement
        insert_columns = ", ".join(target_columns + join_columns_with_prefix)
        
        # Select columns - target layer columns directly, join layer columns with prefixes
        select_target_columns = ", ".join(
            f"target_layer.{col}" for col in ["geom"] + target_columns
        )
        
        # Prepare select columns for join layer based on duplicate handling
        if params.handle_duplicates == DuplicateHandling.aggregate:
            # Use first_value when aggregating
            select_join_columns = ", ".join(
                f"first(join_layer.{orig_col}) AS {prefix_col}"
                for orig_col, prefix_col in zip(join_columns_original, join_columns_with_prefix)
            )
        else:
            # Direct selection
            select_join_columns = ", ".join(
                f"join_layer.{orig_col} AS {prefix_col}"
                for orig_col, prefix_col in zip(join_columns_original, join_columns_with_prefix)
            )
        
        # Create the full select list
        select_columns = f"{select_target_columns}, {select_join_columns}"
        
        # Build the INSERT statement
        insert_statement = (
            f"INSERT INTO {result_table} (layer_id, geom, {insert_columns})"
        )

        # Build combined where query
        where_query = build_where_clause(
            [
                target_layer_project.where_query.replace(
                    f"{target_layer_project.table_name}.", "target_layer."
                ),
                join_layer_project.where_query.replace(
                    f"{join_layer_project.table_name}.", "join_layer."
                ),
            ],
        )

        # Determine join type
        join_type_sql = ""
        if params.join_type == JoinType.left:
            join_type_sql = "LEFT"
        elif params.join_type == JoinType.right:
            join_type_sql = "RIGHT"
        elif params.join_type == JoinType.inner:
            join_type_sql = "INNER"
        
        # Determine join condition based on spatial_join flag
        join_condition = ""
        if params.spatial_join:
            join_condition = "ST_Intersects(target_layer.geom, join_layer.geom)"
        else:
            join_condition = f"target_layer.{mapped_target_field}::text = join_layer.{mapped_join_field}::text"

        # Create query
        sql_query = (
            insert_statement
            + f"""
            SELECT {distinct_clause} '{layer_in.id}', {select_columns}
            FROM {target_layer_project.table_name} target_layer
            {join_type_sql} JOIN {join_layer_project.table_name} join_layer
            ON {join_condition}
            {where_query}
            {group_by_clause}
        """
        )

        # Execute query
        await self.async_session.execute(text(sql_query))

        # Create new layer
        await self.create_feature_layer_tool(
            layer_in=layer_in,
            params=params,
        )
        return {
            "status": JobStatusType.finished.value,
            "msg": "Layers where successfully joined with classical join.",
        }
    
    @run_background_or_immediately(settings)
    @job_init()
    async def join_classical_run(self, params: IJoinClassical):
        return await self.join_classical(params=params)
