from uuid import UUID, uuid4
from fastapi import APIRouter, Body, Depends
from src.core.tool import start_calculation
from src.crud.crud_isochrone import CRUDIsochronePT
from src.crud.crud_trip_count_station import CRUDTripCountStation
from src.crud.crud_oev_gueteklasse import CRUDOevGueteklasse
from src.crud.crud_nearby_station_access import CRUDNearbyStationAccess
from src.db.session import AsyncSession
from src.endpoints.deps import get_db, get_http_client, get_user_id
from src.schemas.job import JobType
from src.schemas.motorized_mobility import (
    IIsochroneCar,
    IIsochronePT,
    IOevGueteklasse,
    request_example_oev_gueteklasse,
    request_examples_isochrone_car,
    request_examples_isochrone_pt,
    request_example_nearby_station_access,
    ITripCountStation,
    INearbyStationAccess,
)
from src.schemas.toolbox_base import IToolResponse
from src.schemas.toolbox_base import CommonToolParams


router = APIRouter()


@router.post(
    "/pt/isochrone",
    summary="Compute isochrones for public transport.",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_pt_isochrone(
    *,
    common: CommonToolParams = Depends(),
    params: IIsochronePT = Body(
        ...,
        examples=request_examples_isochrone_pt,
        description="The isochrone parameters.",
    ),
):
    """Compute isochrones for public transport."""
    return await start_calculation(
        job_type=JobType.isochrone_pt,
        tool_class=CRUDIsochronePT,
        crud_method="run_isochrone",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
        http_client=get_http_client(),
    )


@router.post(
    "/car/isochrone",
    summary="Compute isochrones for car.",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_car_isochrone(
    *,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    params: IIsochroneCar = Body(
        ...,
        examples=request_examples_isochrone_car,
        description="The isochrone parameters.",
    ),
):
    """Compute isochrones for car."""
    return {"job_id": uuid4()}


@router.post(
    "/oev-gueteklassen",
    summary="Calculate ÖV-Güteklassen.",
    response_model=IToolResponse,
    status_code=201,
)
async def calculate_oev_gueteklassen(
    *,
    common: CommonToolParams = Depends(),
    params: IOevGueteklasse = Body(..., examples=request_example_oev_gueteklasse),
):
    """
    ÖV-Güteklassen (The public transport quality classes) is an indicator for access to public transport.
    The indicator makes it possible to identify locations which, thanks to their good access to public transport, have great potential as focal points for development.
    The calculation in an automated process from the data in the electronic timetable (GTFS).
    """

    return await start_calculation(
        job_type=JobType.oev_gueteklasse,
        tool_class=CRUDOevGueteklasse,
        crud_method="oev_gueteklasse_run",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
    )

@router.post(
    "/trip-count-station",
    summary="Calculate trip count per station.",
    response_model=IToolResponse,
    status_code=201,
)
async def calculate_trip_count_station(
    *,
    common: CommonToolParams = Depends(),
    params: ITripCountStation = Body(
        ..., examples=request_example_oev_gueteklasse
    ),
):
    """Calculates the number of trips per station and public transport mode."""

    return await start_calculation(
        job_type=JobType.trip_count_station,
        tool_class=CRUDTripCountStation,
        crud_method="trip_count_run",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
    )

@router.post(
    "/nearby-station-access",
    summary="Get public transport stops and their trips that are accessible by walking/cycling.",
    response_model=IToolResponse,
    status_code=201,
)
async def nearby_station_access(
    *,
    common: CommonToolParams = Depends(),
    params: INearbyStationAccess = Body(
        ...,
        examples=request_example_nearby_station_access,
        description="The isochrone parameters.",
    ),
):
    """Get public transport stops and their trips that are accessible by walking/cycling."""
    return await start_calculation(
        job_type=JobType.nearby_station_access,
        tool_class=CRUDNearbyStationAccess,
        crud_method="nearby_station_access_run",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
    )
