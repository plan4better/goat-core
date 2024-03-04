from datetime import timedelta

from src.core.tool import CRUDToolBase
from src.schemas.nearby_station_access import INearbyStationAccess
from src.schemas.toolbox_base import DefaultResultLayerName
from src.schemas.layer import IFeatureLayerToolCreate, UserDataGeomType
from src.core.config import settings
from src.schemas.error import SQLError
from src.schemas.job import JobStatusType
from src.core.job import job_log, job_init, run_background_or_immediately
from src.schemas.isochrone import (
    IsochroneNearbyStationAccess,
    IsochroneTypeActiveMobility,
    IsochroneTravelTimeCostActiveMobility,
)
from src.endpoints.deps import get_http_client
from src.crud.crud_isochrone import CRUDIsochroneActiveMobility


class CRUDNearbyStationAccess(CRUDToolBase):
    """CRUD for Nearby Station Access."""

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
        self.result_table = (
            f"{settings.USER_DATA_SCHEMA}.point_{str(self.user_id).replace('-', '')}"
        )

    @job_log(job_step_name="nearby_station_access")
    async def nearby_station_access(self, params: INearbyStationAccess):
        """Computes an isochrone based on provided parameters, then identifies stations within this isochrone area
        and computes the frequency of routes serving these stations."""

        # Create feature layer to store computed nearby stations output
        layer_stations = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.nearby_station_access.value,
            feature_layer_geometry_type=UserDataGeomType.point.value,
            attribute_mapping={
                "text_attr1": "stop_name",
                "integer_attr1": "access_time",
                "integer_attr2": "agg_frequency",
                "jsonb_attr1": "routes",
            },
            tool_type=params.tool_type.value,
            job_id=self.job_id,
        )
        result_table = f"{settings.USER_DATA_SCHEMA}.{layer_stations.feature_layer_geometry_type.value}_{str(self.user_id).replace('-', '')}"

        try:
            # Create result table to store isochrone geometry
            isochrone_table = f"temporal.temp_{str(self.job_id).replace('-', '')}"
            sql_create_temp_table = f"""
                CREATE TABLE {isochrone_table} (
                    id serial,
                    layer_id text,
                    geom geometry,
                    integer_attr1 smallint
                );
            """
            await self.async_session.execute(sql_create_temp_table)
            await self.async_session.commit()
        except Exception as e:
            await self.async_session.rollback()
            raise SQLError(e)

        # Create active mobility isochrone request payload
        isochrone_request = IsochroneNearbyStationAccess(
            starting_points=params.starting_points,
            routing_type=params.access_mode,
            travel_cost=IsochroneTravelTimeCostActiveMobility(
                max_traveltime=params.max_traveltime,
                steps=params.max_traveltime,
                speed=params.speed,
            ),
            isochrone_type=IsochroneTypeActiveMobility.polygon,
            polygon_difference=True,
        )

        # Compute isochrone
        await CRUDIsochroneActiveMobility(
            job_id=self.job_id,
            background_tasks=self.background_tasks,
            async_session=self.async_session,
            user_id=self.user_id,
            project_id=self.project_id,
            http_client=get_http_client()
        ).isochrone(
            params=isochrone_request,
            result_params={
                "result_table": isochrone_table,
                "layer_id": str(layer_stations.id),
                "starting_points_layer_name": DefaultResultLayerName.nearby_station_access_starting_points,
            },
        )

        # Run query to find nearby stations, compute route frequencies and insert into result table
        sql_compute_nearby_station_access = f"""
            WITH stop AS (
                SELECT stop_id, stop_name, access_time, geom, h3_3, unpacked.KEY AS route_type, unpacked.value AS routes
                FROM basic.station_route_count(
                    '{isochrone_table}',
                    '',
                    '{str(timedelta(seconds=params.time_window.from_time))}'::interval,
                    '{str(timedelta(seconds=params.time_window.to_time))}'::interval,
                    {params.time_window.weekday_integer}
                ),
                LATERAL jsonb_each(route_ids) unpacked
            ),
            service AS (
                SELECT sr.stop_id, sr.stop_name, sr.access_time, sr.geom, sr.route_id, sr.route_type, count(sr.route_id) AS trip_cnt
                FROM (
                    SELECT stop_id, stop_name, access_time, geom, route_type, jsonb_array_elements_text(routes) as route_id
                    FROM stop
                ) sr
                GROUP BY sr.stop_id, sr.stop_name, sr.access_time, sr.geom, sr.route_id, sr.route_type
            ),
            frequency AS (
                SELECT s.stop_id, s.stop_name, s.access_time, s.geom, r.route_short_name,
                    s.route_type, trip_cnt, ROUND({params.time_window.duration_minutes} / trip_cnt) AS frequency
                FROM service s
                INNER JOIN basic.routes r ON r.route_id = s.route_id
            )
            INSERT INTO {result_table} (layer_id, geom, text_attr1, integer_attr1, integer_attr2, jsonb_attr1)
            SELECT '{str(layer_stations.id)}', geom, stop_name, access_time, ROUND({params.time_window.duration_minutes} / sum(trip_cnt)) AS agg_frequency,
                jsonb_agg(jsonb_build_object('route_short_name', route_short_name, 'route_type', route_type, 'frequency', frequency)) AS routes
            FROM frequency
            GROUP BY stop_id, stop_name, access_time, geom
            ORDER BY stop_name, access_time;
        """
        try:
            await self.async_session.execute(sql_compute_nearby_station_access)
            await self.async_session.commit()
        except Exception as e:
            await self.async_session.rollback()
            raise SQLError(e)

        try:
            # Delete temporary isochrone result table
            await self.async_session.execute(f"DROP TABLE IF EXISTS {isochrone_table};")
        except Exception as e:
            await self.async_session.rollback()
            raise SQLError(e)

        await self.create_feature_layer_tool(
            layer_in=layer_stations,
            params=params,
        )

        #TODO: Return the job id.
        #TO BE DISCUSSED: For the tests we should consider mocking the isochrone request as otherswise it is very hard to test the isochrone in isolation.
        return {
            "status": JobStatusType.finished.value,
            "msg": "Nearby station access created.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def nearby_station_access_run(self, params: INearbyStationAccess):
        return await self.nearby_station_access(params=params)
