from src.schemas.motorized_mobility import (
    CalculateOevGueteklassenParameters,
    oev_gueteklasse_config_example,
    oev_gueteklasse_station_config_layer_base, 
    oev_gueteklasse_config_layer_base,
)
from src.db.session import AsyncSession
from fastapi import Body, Depends, HTTPException, APIRouter
from src.endpoints.deps import get_db, get_user_id
from uuid import UUID
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.crud.crud_layer import layer as crud_layer
from src.core.indicator import check_reference_area
from sqlalchemy import select
from src.db.models.layer import Layer
from datetime import datetime
from src.crud.crud_motorized_mobility import crud_oev_gueteklasse
router = APIRouter()

@router.post("/oev-gueteklassen")
async def calculate_oev_gueteklassen(
    *,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    params: CalculateOevGueteklassenParameters = Body(..., example=oev_gueteklasse_config_example),
):
    """
    ÖV-Güteklassen (The public transport quality classes) is an indicator for access to public transport.
    The indicator makes it possible to identify locations which, thanks to their good access to public transport, have great potential as focal points for development.
    The calculation in an automated process from the data in the electronic timetable (GTFS).
    """
    if params.start_time >= params.end_time:
        raise HTTPException(status_code=422, detail="Start time must be before end time")

    if params.project_id:
        # Get filter query for project
        layer = await crud_layer_project.get_by_ids(async_session=async_session, project_id=params.project_id, layer_ids=[params.reference_area])
    else:
        layer = await crud_layer.get(db=async_session, id=params.reference_area)

    if not layer:
        raise HTTPException(status_code=404, detail="Reference area not found or not found in project")

    reference_area_sql = await check_reference_area(
        async_session=async_session,
        user_id=user_id,
        reference_area=layer,
        operation_type="oev_gueteklasse",
    )

    # Get stations in study area
    layer_stations = await crud_oev_gueteklasse.get_oev_gueteklasse_station_category(
        async_session=async_session,
        user_id=user_id,
        params=params,
        reference_area_sql=reference_area_sql,
    )

    # Compute stations category
    await crud_oev_gueteklasse.compute_station_buffer(
        async_session=async_session,
        user_id=user_id,
        params=params,
        reference_area_id=layer_stations.id,
    )
    # TODO: Save layer in layer table and create a layer for the stations
    # TODO: Send task into background    

    #TODO: Add layer to project if project_id is given

    return {"layer_id": 1}
