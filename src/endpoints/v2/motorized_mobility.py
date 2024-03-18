from uuid import UUID, uuid4
from fastapi import APIRouter, Body, Depends
from src.core.tool import start_calculation
from src.crud.crud_catchment_area import CRUDCatchmentAreaPT
from src.crud.crud_trip_count_station import CRUDTripCountStation
from src.crud.crud_oev_gueteklasse import CRUDOevGueteklasse
from src.crud.crud_nearby_station_access import CRUDNearbyStationAccess
from src.db.session import AsyncSession
from src.endpoints.deps import get_db, get_http_client, get_user_id
from src.schemas.job import JobType
from src.schemas.catchment_area import (
    ICatchmentAreaPT,
    ICatchmentAreaCar,
    request_examples_catchment_area_pt,
    request_examples_catchment_area_car,
)
from src.schemas.oev_gueteklasse import (
    IOevGueteklasse,
    request_example_oev_gueteklasse,
)
from src.schemas.trip_count_station import ITripCountStation
from src.schemas.nearby_station_access import (
    INearbyStationAccess,
    request_example_nearby_station_access,
)
from src.schemas.heatmap import (
    IHeatmapGravityMotorized,
    IHeatmapClosestAverageMotorized,
    IHeatmapConnectivityMotorized,
)
from src.schemas.toolbox_base import IToolResponse
from src.schemas.toolbox_base import CommonToolParams


router = APIRouter()


@router.post(
    "/pt/catchment-area",
    summary="Compute catchment areas for public transport.",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_pt_catchment_area(
    *,
    common: CommonToolParams = Depends(),
    params: ICatchmentAreaPT = Body(
        ...,
        examples=request_examples_catchment_area_pt,
        description="The catchment area parameters.",
    ),
):
    """Compute catchment areas for public transport."""
    return await start_calculation(
        job_type=JobType.catchment_area_pt,
        tool_class=CRUDCatchmentAreaPT,
        crud_method="run_catchment_area",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
        http_client=get_http_client(),
    )


@router.post(
    "/car/catchment-area",
    summary="Compute catchment areas for car.",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_car_catchment_area(
    *,
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID = Depends(get_user_id),
    params: ICatchmentAreaCar = Body(
        ...,
        examples=request_examples_catchment_area_car,
        description="The catchment area parameters.",
    ),
):
    """Compute catchment areas for car."""
    return {"job_id": uuid4()}


@router.post(
    "/oev-gueteklassen",
    summary="Calculate ÖV-Güteklassen.",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_oev_gueteklassen(
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
async def compute_trip_count_station(
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
async def compute_nearby_station_access(
    *,
    common: CommonToolParams = Depends(),
    params: INearbyStationAccess = Body(
        ...,
        examples=request_example_nearby_station_access,
        description="The catchment area parameters.",
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

@router.post(
    "/heatmap-gravity",
    summary="Compute gravity-based heatmap for motorized mobility",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_motorized_mobility_heatmap_gravity(
    *,
    common: CommonToolParams = Depends(),
    params: IHeatmapGravityMotorized = Body(
        ...,
        examples={},
        description="The gravity-based heatmap parameters.",
    ),
):
    """Compute gravity-based heatmap for motorized mobility."""

    return await start_calculation(
        job_type=JobType.heatmap_gravity_motorized_mobility,
        tool_class=CRUDCatchmentAreaPT,
        crud_method="run_heatmap",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
    )


@router.post(
    "/heatmap-closest-average",
    summary="Compute closest-average-based heatmap for motorized mobility",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_motorized_mobility_heatmap_closest_average(
    *,
    common: CommonToolParams = Depends(),
    params: IHeatmapClosestAverageMotorized = Body(
        ...,
        examples={},
        description="The closest-average-based heatmap parameters.",
    ),
):
    """Compute closest-average-based heatmap for motorized mobility."""

    return await start_calculation(
        job_type=JobType.heatmap_closest_average_motorized_mobility,
        tool_class=CRUDCatchmentAreaPT,
        crud_method="run_heatmap",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
    )


@router.post(
    "/heatmap-connectivity",
    summary="Compute connectivity-based heatmap for motorized mobility",
    response_model=IToolResponse,
    status_code=201,
)
async def compute_motorized_mobility_heatmap_connectivity(
    *,
    common: CommonToolParams = Depends(),
    params: IHeatmapConnectivityMotorized = Body(
        ...,
        examples={},
        description="The connectivity-based heatmap parameters.",
    ),
):
    """Compute connectivity-based heatmap for motorized mobility."""

    return await start_calculation(
        job_type=JobType.heatmap_connectivity_motorized_mobility,
        tool_class=CRUDCatchmentAreaPT,
        crud_method="run_heatmap",
        async_session=common.async_session,
        user_id=common.user_id,
        background_tasks=common.background_tasks,
        project_id=common.project_id,
        params=params,
    )
