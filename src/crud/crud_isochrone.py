from httpx import AsyncClient

from src.core.config import settings
from src.core.job import job_init, job_log, run_background_or_immediately
from src.core.tool import CRUDToolBase
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.schemas.active_mobility import (
    IIsochroneActiveMobility,
    TravelTimeCostActiveMobility,
)
from src.schemas.error import OutOfGeofenceError, RoutingEndpointError
from src.schemas.job import JobStatusType
from src.schemas.layer import IFeatureLayerToolCreate, UserDataGeomType
from src.schemas.motorized_mobility import IIsochroneCar, IIsochronePT
from src.schemas.toolbox_base import (
    DefaultResultLayerName,
    IsochroneGeometryTypeMapping,
)


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
                    SELECT ST_SETSRID(ST_MAKEPOINT(lat, lon), 4326) AS geom
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
                SELECT '{layer.id}', ST_SETSRID(ST_MAKEPOINT(lat, lon), 4326) AS geom
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
        request_body = {
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
            result = await self.http_client.post(
                url=f"{settings.GOAT_ROUTING_URL}/isochrone",
                json=request_body,
            )
            if result.status_code != 201:
                raise Exception(result.text)
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
    async def __init__(
        self, job_id, background_tasks, async_session, user_id, project_id
    ):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    @job_log(job_step_name="isochrone")
    async def isochrone(
        self,
        params: IIsochronePT,
    ):
        await self.create_or_return_layer_starting_points(params=params)

        return {
            "status": JobStatusType.finished.value,
            "msg": "Public transport isochrone was successfully computed.",
        }
