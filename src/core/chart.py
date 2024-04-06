from sqlmodel import SQLModel
from pydantic import BaseModel
from uuid import UUID
from src.utils import search_value
from src.crud.crud_layer_project import layer_project as crud_layer_project
from sqlalchemy.ext.asyncio import AsyncSession
from src.schemas.toolbox_base import ColumnStatisticsOperation
from src.core.tool import get_statistics_sql
from src.db.models.layer import ToolType


async def read_chart_data(
    async_session: AsyncSession, project_id: UUID, layer_project_id: int
):

    # Get layer project data
    layer_project = await crud_layer_project.get_internal(
        async_session=async_session, project_id=project_id, id=layer_project_id
    )

    # Make sure that layer is aggregation_point or aggregation_polygon
    if layer_project.tool_type not in [ToolType.aggregate_point, ToolType.aggregate_polygon]:
        raise ValueError("Layer is not aggregation point or aggregation polygon")

    # Get chart data
    charts = layer_project.charts
    operation = charts["operation"]
    x_label = charts["x_label"]
    y_label = charts["y_label"]
    group_by = charts["group_by"]

    # Get y_query
    x_label_mapped = search_value(layer_project.attribute_mapping, x_label)
    y_label_mapped = search_value(layer_project.attribute_mapping, y_label)
    y_query = get_statistics_sql(y_label_mapped, operation)

    # Replace count with sum in case operation is count
    if operation == ColumnStatisticsOperation.count:
        y_query = y_query.replace("COUNT", "SUM")

    if not group_by:
        # Get data from layer
        sql = f"""
        WITH data AS (
            SELECT {x_label_mapped} AS x, {y_query} AS y
            FROM {layer_project.table_name}
            WHERE layer_id = '{layer_project.layer_id}'
            GROUP BY {x_label_mapped}
        )
        SELECT ARRAY_AGG(x), ARRAY_AGG(y)
        FROM data
        """
    else:
        # Define statistics query
        y_query = get_statistics_sql("value", operation)

        # Cast value inside query to float
        y_query = y_query.replace("value", "value::float")
        data_column = search_value(
            layer_project.attribute_mapping, operation + "_grouped"
        )

        # Build query
        sql = f"""
        WITH unnested AS (
            SELECT {x_label_mapped} x, key AS group, {y_query} AS y
            FROM {layer_project.table_name}, LATERAL JSONB_EACH({data_column})
            WHERE layer_id = '{layer_project.layer_id}'
            GROUP BY {x_label_mapped}, key
            ORDER BY {x_label_mapped}, key
        ),
        grouped AS
        (
            SELECT ARRAY_AGG(x) AS x, ARRAY_AGG(y) as y, "group"
            FROM unnested
            GROUP BY "group"
        )
        SELECT ARRAY_AGG(x), ARRAY_AGG(y), ARRAY_AGG("group")
        FROM grouped
        """

    result = await async_session.execute(sql)
    data = result.fetchall()
    data = {"x": data[0][0], "y": data[0][1], "group": data[0][2] if group_by else None}
    return data


class Chart:
    def __init__(self, job_id, async_session, user_id):
        self.job_id = job_id
        self.async_session = async_session
        self.user_id = user_id

    async def create_chart(
        self,
        layer: SQLModel,
        layer_project: BaseModel,
        operation: ColumnStatisticsOperation,
        x_label: str,
        y_label: str,
        group_by: str = None,
    ):

        # Map columns
        chart_data = {
            "charts": {
                "operation": operation.value,
                "x_label": x_label,
                "y_label": y_label,
                "group_by": group_by,
            }
        }

        # Update layer project with chart data
        await crud_layer_project.update(
            async_session=self.async_session, id=layer_project.id, layer_in=chart_data
        )
