from sqlalchemy import text

from src.core.config import settings
from src.core.job import job_init, job_log, run_background_or_immediately
from src.core.tool import CRUDToolBase, get_statistics_sql
from src.db.models.layer import ToolType
from src.schemas.error import ColumnTypeError
from src.schemas.job import JobStatusType, JobType
from src.schemas.layer import (
    IFeatureLayerToolCreate,
)
from src.schemas.tool import IJoin
from src.schemas.toolbox_base import DefaultResultLayerName
from src.utils import (
    build_where_clause,
    get_result_column,
    search_value,
)
from src.core.layer import get_user_table


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
