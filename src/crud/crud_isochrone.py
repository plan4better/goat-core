import time

from httpx import AsyncClient

from src.core.config import settings
from src.core.job import job_init, job_log, run_background_or_immediately
from src.core.tool import CRUDToolBase
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.jsoline import generate_jsolines
from src.schemas.active_mobility import (
    IIsochroneActiveMobility,
    TravelTimeCostActiveMobility,
)
from src.schemas.error import (
    OutOfGeofenceError,
    R5EndpointError,
    R5IsochroneComputeError,
    RoutingEndpointError,
    SQLError,
)
from src.schemas.job import JobStatusType
from src.schemas.layer import IFeatureLayerToolCreate, UserDataGeomType
from src.schemas.motorized_mobility import IIsochroneCar, IIsochronePT
from src.schemas.toolbox_base import (
    DefaultResultLayerName,
    IsochroneGeometryTypeMapping,
)
from src.utils import decode_r5_grid, write_isochrone_result


class CRUDIsochroneBase(CRUDToolBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
        self.table_starting_points = (
            f"{settings.USER_DATA_SCHEMA}.point_{str(self.user_id).replace('-', '')}"
        )

    async def create_or_return_layer_starting_points(
        self, params: IIsochroneActiveMobility | IIsochroneCar | IIsochronePT
    ):
        # Check if starting points are a layer
        if params.starting_points.layer_project_id:
            layer = await crud_layer_project.get(
                db=self.async_session, id=params.starting_points.layer_project_id
            )
            return layer

        # Create layer object
        layer = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.isochrone_starting_points.value,
            feature_layer_geometry_type=UserDataGeomType.point.value,
            attribute_mapping={},
            tool_type=params.tool_type.value,
        )

        # Check if starting points are within the geofence
        for i in range(0, len(params.starting_points.latitude), 500):
            # Create insert query
            lats = params.starting_points.latitude[i : i + 500]
            lons = params.starting_points.longitude[i : i + 500]
            sql = f"""
                WITH to_test AS
                (
                    SELECT ST_SETSRID(ST_MAKEPOINT(lon, lat), 4326) AS geom
                    FROM UNNEST(ARRAY{str(lats)}) AS lat,
                    UNNEST(ARRAY{str(lons)}) AS lon
                )
                SELECT COUNT(*)
                FROM to_test t
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {params.geofence_table} AS g
                    WHERE ST_INTERSECTS(t.geom, g.geom)
                )
            """
            # Execute query
            cnt_not_intersecting = await self.async_session.execute(sql)
            cnt_not_intersecting = cnt_not_intersecting.scalars().first()

            if cnt_not_intersecting > 0:
                raise OutOfGeofenceError(
                    f"There are {cnt_not_intersecting} starting points that are not within the geofence. Please check your starting points."
                )

        # Save data into user data tables in batches of 500
        for i in range(0, len(params.starting_points.latitude), 500):
            # Create insert query
            lats = params.starting_points.latitude[i : i + 500]
            lons = params.starting_points.longitude[i : i + 500]
            sql = f"""
                INSERT INTO {self.table_starting_points} (layer_id, geom)
                SELECT '{layer.id}', ST_SETSRID(ST_MAKEPOINT(lon, lat), 4326) AS geom
                FROM UNNEST(ARRAY{str(lats)}) AS lat,
                UNNEST(ARRAY{str(lons)}) AS lon
            """
            # Execute query
            await self.async_session.execute(sql)

        return layer


class CRUDIsochroneActiveMobility(CRUDIsochroneBase):
    def __init__(
        self,
        job_id,
        background_tasks,
        async_session,
        user_id,
        project_id,
        http_client: AsyncClient,
    ):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

        self.http_client = http_client

    @job_log(job_step_name="isochrone")
    async def isochrone(
        self,
        params: IIsochroneActiveMobility,
    ):
        """Compute active mobility isochrone using GOAT Routing endpoint."""

        # Create layer to store isochrone starting points if required
        await self.create_or_return_layer_starting_points(params=params)

        # Create feature layer to store computed isochrone output
        layer_isochrone = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.isochrone_active_mobility.value,
            feature_layer_geometry_type=IsochroneGeometryTypeMapping[
                params.isochrone_type.value
            ],
            attribute_mapping={"integer_attr1": "travel_cost"},
            tool_type=params.tool_type.value,
        )
        result_table = f"{settings.USER_DATA_SCHEMA}.{layer_isochrone.feature_layer_geometry_type.value}_{str(self.user_id).replace('-', '')}"

        # Construct request payload
        request_payload = {
            "starting_points": {
                "latitude": params.starting_points.latitude,
                "longitude": params.starting_points.longitude,
            },
            "routing_type": params.routing_type.value,
            "travel_cost": {
                "max_traveltime": params.travel_cost.max_traveltime,
                "traveltime_step": params.travel_cost.traveltime_step,
                "speed": params.travel_cost.speed,
            }
            if type(params.travel_cost) == TravelTimeCostActiveMobility
            else {
                "max_distance": params.travel_cost.max_distance,
                "distance_step": params.travel_cost.distance_step,
            },
            "isochrone_type": params.isochrone_type.value,
            "polygon_difference": params.polygon_difference,
            "result_table": result_table,
            "layer_id": str(layer_isochrone.id),
        }

        try:
            # Call GOAT Routing endpoint to compute isochrone
            response = await self.http_client.post(
                url=f"{settings.GOAT_ROUTING_URL}/isochrone",
                json=request_payload,
            )
            if response.status_code != 201:
                raise Exception(response.text)
        except Exception as e:
            raise RoutingEndpointError(
                f"Error while calling the routing endpoint: {str(e)}"
            )

        return {
            "status": JobStatusType.finished.value,
            "msg": "Active mobility isochrone was successfully computed.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def run_isochrone(self, params: IIsochroneActiveMobility):
        return await self.isochrone(params=params)

    async def join_fail(self, params: IIsochroneActiveMobility):
        await self.delete_orphan_data()


class CRUDIsochronePT(CRUDIsochroneBase):
    def __init__(
        self,
        job_id,
        background_tasks,
        async_session,
        user_id,
        project_id,
        http_client: AsyncClient,
    ):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

        self.http_client = http_client
        self.NUM_RETRIES = 10  # Number of times to retry calling the R5 endpoint
        self.RETRY_DELAY = 2  # Number of seconds to wait between retries

    @job_log(job_step_name="isochrone")
    async def isochrone(
        self,
        params: IIsochronePT,
    ):
        """Compute public transport isochrone using R5 routing endpoint."""

        # Create layer to store isochrone starting points if required
        await self.create_or_return_layer_starting_points(params=params)

        # Create feature layer to store computed isochrone output
        layer_isochrone = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.isochrone_pt.value,
            feature_layer_geometry_type=IsochroneGeometryTypeMapping[
                params.isochrone_type.value
            ],
            attribute_mapping={"integer_attr1": "travel_cost"},
            tool_type=params.tool_type.value,
        )
        result_table = f"{settings.USER_DATA_SCHEMA}.{layer_isochrone.feature_layer_geometry_type.value}_{str(self.user_id).replace('-', '')}"

        # Compute isochrone for each starting point
        for i in range(0, len(params.starting_points.latitude)):
            # Identify relevant R5 region & bundle for this isochrone starting point
            sql_get_region_mapping = f"""
                SELECT r5_region_id, r5_bundle_id, r5_host
                FROM {settings.REGION_MAPPING_PT_TABLE}
                WHERE ST_INTERSECTS(
                    ST_SETSRID(
                        ST_MAKEPOINT(
                            {params.starting_points.longitude[i]},
                            {params.starting_points.latitude[i]}
                        ),
                        4326
                    ),
                    ST_SetSRID(geom, 4326)
                );
            """
            r5_region_id, r5_bundle_id, r5_host = (
                await self.async_session.execute(sql_get_region_mapping)
            ).fetchall()[0]

            # Get relevant region bounds for this starting point
            # TODO Compute buffer distance dynamically?
            sql_get_region_bounds = f"""
                SELECT ST_XMin(b.geom), ST_YMin(b.geom), ST_XMax(b.geom), ST_YMax(b.geom)
                FROM (
                    SELECT ST_Envelope(
                        ST_Buffer(
                            ST_SetSRID(
                                ST_MakePoint(
                                    {params.starting_points.longitude[i]},
                                    {params.starting_points.latitude[i]}),
                                4326
                            )::geography,
                            100000
                        )::geometry
                    ) AS geom
                ) b;
            """
            xmin, ymin, xmax, ymax = (
                await self.async_session.execute(sql_get_region_bounds)
            ).fetchall()[0]

            # Construct request payload
            request_payload = {
                "accessModes": params.routing_type.access_mode.value.upper(),
                "transitModes": ",".join(params.routing_type.mode).upper(),
                "bikeSpeed": params.bike_speed,
                "walkSpeed": params.walk_speed,
                "bikeTrafficStress": params.bike_traffic_stress,
                "date": params.time_window.weekday_date,
                "fromTime": params.time_window.from_time,
                "toTime": params.time_window.to_time,
                "maxTripDurationMinutes": params.travel_cost.max_traveltime,
                "decayFunction": {
                    "type": "logistic",
                    "standard_deviation_minutes": params.decay_function.standard_deviation_minutes,
                    "width_minutes": params.decay_function.width_minutes,
                },
                "destinationPointSetIds": [],
                "bounds": {
                    "north": ymax,
                    "south": ymin,
                    "east": xmax,
                    "west": xmin,
                },
                "directModes": params.routing_type.access_mode.value.upper(),
                "egressModes": params.routing_type.egress_mode.value.upper(),
                "fromLat": params.starting_points.latitude[i],
                "fromLon": params.starting_points.longitude[i],
                "zoom": params.zoom,
                "maxBikeTime": params.max_bike_time,
                "maxRides": params.max_rides,
                "maxWalkTime": params.max_walk_time,
                "monteCarloDraws": params.monte_carlo_draws,
                "percentiles": params.percentiles,
                "variantIndex": settings.R5_VARIANT_INDEX,
                "workerVersion": settings.R5_WORKER_VERSION,
                "regionId": r5_region_id,
                "projectId": r5_region_id,
                "bundleId": r5_bundle_id,
            }

            result = None
            try:
                # Call R5 endpoint multiple times for upto 20 seconds / 10 retries
                for i in range(self.NUM_RETRIES):
                    # Call R5 endpoint to compute isochrone
                    response = await self.http_client.post(
                        url=f"{r5_host}/api/analysis",
                        json=request_payload,
                        headers={"Authorization": settings.R5_AUTHORIZATION},
                    )
                    if response.status_code == 202:
                        # Engine is still processing request, retry shortly
                        if i == self.NUM_RETRIES - 1:
                            raise Exception(
                                "R5 engine took too long to process request."
                            )
                        time.sleep(self.RETRY_DELAY)
                        continue
                    elif response.status_code == 200:
                        # Engine has finished processing request, break
                        result = response.content
                        break
                    else:
                        raise Exception(response.text)
            except Exception as e:
                raise R5EndpointError(f"Error while calling the R5 endpoint: {str(e)}")

            isochrone_grid = None
            isochrone_shapes = None
            try:
                # Decode R5 response data
                isochrone_grid = decode_r5_grid(result)

                # Convert grid data returned by R5 to valid isochrone geometry
                isochrone_shapes = generate_jsolines(
                    grid=isochrone_grid,
                    travel_time=params.travel_cost.max_traveltime,
                    percentile=5,
                    step=params.travel_cost.traveltime_step,
                )
            except Exception as e:
                raise R5IsochroneComputeError(
                    f"Error while processing R5 isochrone grid: {str(e)}"
                )

            try:
                # Save result to database
                await write_isochrone_result(
                    async_session=self.async_session,
                    isochrone_type=params.isochrone_type.value,
                    layer_id=str(layer_isochrone.id),
                    result_table=result_table,
                    shapes=isochrone_shapes,
                    grid=isochrone_grid,
                )
            except Exception as e:
                raise SQLError(
                    f"Error while saving R5 isochrone result to database: {str(e)}"
                )

        return {
            "status": JobStatusType.finished.value,
            "msg": "Public transport isochrone was successfully computed.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def run_isochrone(self, params: IIsochronePT):
        return await self.isochrone(params=params)

    async def join_fail(self, params: IIsochronePT):
        await self.delete_orphan_data()
