from uuid import UUID
from src.db.session import AsyncSession
from src.schemas.motorized_mobility import AreaLimitPerTool, CountLimitPerTool
from src.schemas.layer import IndicatorType
from sqlalchemy import text
from fastapi import HTTPException, status
from src.db.models.layer import Layer
from src.core.config import settings


async def check_reference_area(
    async_session: AsyncSession,
    user_id: UUID,
    reference_area: Layer,
    operation_type: IndicatorType,
    filter: dict = None,
) -> str:
    """Check if reference area is too large for specific indicator."""

    table_name = "polygon" + "_" + str(user_id).replace("-", "")

    # Check if layer is feature layer
    if reference_area.type != "feature_layer":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Layer {reference_area.id} is not a feature layer.",
        )

    # Check if layer object is a polygon
    if reference_area.feature_layer_geometry_type != "polygon":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Layer {reference_area.id} is not a polygon layer.",
        )

    # If layer has a filter use it
    if hasattr(reference_area, "query"):
        # TODO: Convert JSON-filter to SQL
        filter = reference_area.query
        sql_query = f"""SELECT count(*) cnt, sum(ST_AREA(geom)) AS area FROM {settings.USER_DATA_SCHEMA}.{table_name} WHERE layer_id = '{reference_area.id}' AND {filter}"""
    else:
        sql_query = f"""SELECT count(*) cnt, sum(ST_AREA(geom)) AS area FROM {settings.USER_DATA_SCHEMA}.{table_name} WHERE layer_id = '{reference_area.id}'"""

    result = await async_session.execute(text(sql_query))
    result = result.all()
    cnt, area = result[0]

    if cnt > getattr(CountLimitPerTool, operation_type).value:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"There are too many reference areas. The maximum number is {getattr(CountLimitPerTool, operation_type.value).value}. Please select fewer features.",
        )

    if area > getattr(AreaLimitPerTool, operation_type).value:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"reference area is too large for this indicator. The maximum size is {getattr(AreaLimitPerTool, operation_type.value).value}. Please select a smaller area.",
        )
    
    reference_area_sql = (
        f"""SELECT geom FROM {settings.USER_DATA_SCHEMA}.{table_name} WHERE layer_id = '{reference_area.id}'"""
    )



    return reference_area_sql