import json
from datetime import timedelta
from pydantic import BaseModel

from src.core.tool import CRUDToolBase
from src.schemas.motorized_mobility import (
    IOevGueteklasse,
    ITripCountStation,
    public_transport_types,
    INearbyStationAccess,
)
from src.utils import build_where_clause
from src.db.models.layer import ToolType
from src.schemas.toolbox_base import DefaultResultLayerName
from src.schemas.layer import IFeatureLayerToolCreate, UserDataGeomType
from src.core.config import settings
from src.schemas.error import SQLError
from src.schemas.job import JobStatusType
from src.core.job import job_log, job_init, run_background_or_immediately
from src.core.config import settings
from src.schemas.active_mobility import IIsochroneActiveMobility, IsochroneType, TravelTimeCostActiveMobility
from src.endpoints.deps import get_http_client
from src.crud.crud_isochrone import CRUDIsochroneActiveMobility


class CRUDOevGueteklasse(CRUDToolBase):
    """CRUD for OEV-Gueteklasse."""

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
        self.table_stations = (
            f"{settings.USER_DATA_SCHEMA}.point_{str(self.user_id).replace('-', '')}"
        )
        self.table_oev_gueteklasse = (
            f"{settings.USER_DATA_SCHEMA}.polygon_{str(self.user_id).replace('-', '')}"
        )

    @job_log(job_step_name="station_category")
    async def get_oev_gueteklasse_station_category(
        self,
        params: IOevGueteklasse,
        reference_layer_project: BaseModel,
        station_category_layer: BaseModel,
    ):
        """Get station category."""

        where_query = build_where_clause([reference_layer_project.where_query])
        query = f"""
            INSERT INTO {self.table_stations}({', '.join(station_category_layer.attribute_mapping.keys())}, layer_id, geom)
            WITH stations AS (
                SELECT stop_id, stop_name, (oev_gueteklasse ->> 'frequency')::float AS frequency,
                (oev_gueteklasse ->> '_class')::integer AS _class, '{str(station_category_layer.id)}'::uuid AS layer_id, geom AS geom
                FROM basic.count_public_transport_services_station(
                    '{reference_layer_project.table_name}',
                    :where_query,
                    '{str(timedelta(seconds=params.time_window.from_time))}',
                    '{str(timedelta(seconds=params.time_window.to_time))}',
                    {params.time_window.weekday_integer}
                ) s, LATERAL basic.oev_guetklasse_station_category(trip_cnt, '{json.dumps(params.station_config.dict())}'::jsonb,
                {params.time_window.from_time}, {params.time_window.to_time}) oev_gueteklasse
            )
            SELECT *
            FROM stations
        """
        await self.async_session.execute(query, {"where_query": where_query})
        await self.async_session.commit()
        return {
            "status": JobStatusType.finished.value,
            "msg": "Station category created.",
        }

    @job_log(job_step_name="station_buffer")
    async def compute_station_buffer(
        self,
        station_category_layer: BaseModel,
        buffer_layer: BaseModel,
        station_config: dict,
    ) -> dict:
        """Compute station buffer."""

        # Create temp table names
        table_suffix = str(self.job_id).replace("-", "")
        temp_buffered_stations = f"temporal.temp_buffered_stations_{table_suffix}"
        temp_union_buffer = f"temporal.temp_union_buffered_stations_{table_suffix}"

        try:
            # Create temp distributed table for buffered stations
            await self.async_session.execute(
                f"DROP TABLE IF EXISTS {temp_buffered_stations};"
            )
            await self.async_session.execute(
                f"""
                CREATE TABLE {temp_buffered_stations}
                (
                    stop_id TEXT,
                    pt_class integer,
                    buffer_size integer,
                    geom geometry,
                    h3_3 integer
                )
            """
            )
            await self.async_session.execute(
                f"SELECT create_distributed_table('{temp_buffered_stations}', 'h3_3')"
            )

            # Buffer the stations in their respective intervals
            await self.async_session.execute(
                f"""INSERT INTO {temp_buffered_stations}
                SELECT text_attr1 AS stop_id,  REPLACE(j.value::TEXT, '"', '')::integer AS pt_class,
                j.KEY::integer buffer_size, ST_BUFFER(s.geom::geography, j.KEY::integer)::geometry AS geom,
                basic.to_short_h3_3(h3_lat_lng_to_cell(geom::point, 3)::bigint)
                FROM {self.table_stations} s
                , LATERAL jsonb_each('{json.dumps(station_config)}'::jsonb -> 'classification' -> s.integer_attr1::text) j
                WHERE layer_id = '{station_category_layer.id}';
                """
            )
            await self.async_session.execute(
                f"""CREATE INDEX ON {temp_buffered_stations} USING GIST(h3_3, geom);"""
            )

            # Union the buffers. It is not made use of citus distribution column here as it is a challenge to group over shards efficiently.
            await self.async_session.execute(
                f"DROP TABLE IF EXISTS {temp_union_buffer};"
            )
            await self.async_session.execute(
                f"""CREATE TABLE {temp_union_buffer} AS
                WITH clustered_buffer AS
                (
                    SELECT s.geom, s.pt_class,
                    ST_ClusterDBSCAN(geom, eps := 0, minpoints := 1) OVER (PARTITION BY pt_class) AS cluster_id
                    FROM {temp_buffered_stations} s
                )
                SELECT b.pt_class, b.geom
                FROM clustered_buffer b
                WHERE cluster_id IS NULL
                UNION ALL
                SELECT b.pt_class, ST_UNION(b.geom) AS geom
                FROM clustered_buffer b
                WHERE cluster_id IS NOT NULL
                GROUP BY b.pt_class, cluster_id;"""
            )
            await self.async_session.execute(
                f"""CREATE INDEX ON {temp_union_buffer} USING GIST(geom);"""
            )
            # Create difference between different buffers*
            await self.async_session.execute(
                f"""
                INSERT INTO {self.table_oev_gueteklasse} (text_attr1, layer_id, geom)
                SELECT a.pt_class::text, '{buffer_layer.id}', CASE WHEN j.geom IS NULL THEN a.geom ELSE j.geom END AS geom
                FROM {temp_union_buffer} a
                LEFT JOIN LATERAL
                (
                    SELECT ST_DIFFERENCE(a.geom, c.geom) AS geom
                    FROM (
                        SELECT ST_UNION(b.geom) geom
                        FROM {temp_union_buffer} b
                        WHERE a.pt_class > b.pt_class
                        AND ST_Intersects(a.geom, b.geom)
                    ) c
                ) j ON TRUE;
                """
            )
            # Drop temp tables
            await self.async_session.execute(
                f"DROP TABLE IF EXISTS {temp_buffered_stations};"
            )
            await self.async_session.execute(
                f"DROP TABLE IF EXISTS {temp_union_buffer};"
            )
            await self.async_session.commit()
        except Exception as e:
            # Drop temp tables
            await self.async_session.execute(
                f"DROP TABLE IF EXISTS {temp_buffered_stations};"
            )
            await self.async_session.execute(
                f"DROP TABLE IF EXISTS {temp_union_buffer};"
            )
            await self.async_session.commit()
            raise SQLError(e)
        return {
            "status": JobStatusType.finished.value,
            "msg": "Station buffers are created.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def oev_gueteklasse_run(self, params: IOevGueteklasse):
        """Compute ÖV-Güteklassen."""

        # Check if reference layer qualifies for ÖV-Güteklassen
        layer_project = await self.get_layers_project(
            params=params,
        )
        reference_layer_project = layer_project["reference_area_layer_project_id"]

        # Create layer object
        station_category_layer = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.oev_gueteklasse_station.value,
            feature_layer_geometry_type=UserDataGeomType.point.value,
            attribute_mapping={
                "text_attr1": "stop_name",
                "text_attr2": "stop_id",
                "float_attr1": "frequency",
                "integer_attr1": "pt_class",
            },
            tool_type=params.tool_type.value,
            job_id=self.job_id,
        )

        # Get station category
        await self.get_oev_gueteklasse_station_category(
            params=params,
            reference_layer_project=reference_layer_project,
            station_category_layer=station_category_layer,
        )

        # Create layer for buffer results
        buffer_layer = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.oev_gueteklasse.value,
            feature_layer_geometry_type=UserDataGeomType.polygon.value,
            attribute_mapping={"text_attr1": "pt_class"},
            tool_type=ToolType.oev_gueteklasse.value,
            job_id=self.job_id,
        )

        # Compute station buffer
        await self.compute_station_buffer(
            station_category_layer=station_category_layer,
            buffer_layer=buffer_layer,
            station_config=params.station_config.dict(),
        )

        # Create result layers
        await self.create_feature_layer_tool(
            layer_in=buffer_layer,
            params=params,
        )
        await self.create_feature_layer_tool(
            layer_in=station_category_layer,
            params=params,
        )
        return {
            "status": JobStatusType.finished.value,
            "msg": "ÖV-Güteklassen created.",
        }


class CRUDTripCountStation(CRUDToolBase):
    """CRUD for OEV-Gueteklasse."""

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
        self.result_table = (
            f"{settings.USER_DATA_SCHEMA}.point_{str(self.user_id).replace('-', '')}"
        )

    @job_log(job_step_name="trip_count_station")
    async def trip_count(self, params: ITripCountStation):
        # Get Layer
        layer_project = await self.get_layers_project(params=params)
        layer_project = layer_project["reference_area_layer_project_id"]

        # Create result layer object
        pt_modes = list(public_transport_types.keys())
        pt_modes.append("total")

        # Populate attribute mapping with pt_modes as integer
        attribute_mapping = {
            f"integer_attr{i+1}": pt_mode for i, pt_mode in enumerate(pt_modes)
        }
        attribute_mapping = {
            "text_attr1": "stop_id",
            "text_attr2": "stop_name",
            "jsonb_attr1": "trip_cnt",
        } | attribute_mapping

        result_layer = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.trip_count_station.value,
            feature_layer_geometry_type=UserDataGeomType.point.value,
            attribute_mapping=attribute_mapping,
            tool_type=ToolType.trip_count_station.value,
            job_id=self.job_id,
        )

        # Create mapping for transport modes
        flat_mode_mapping = {}
        for outer_key, inner_dict in public_transport_types.items():
            for inner_key in inner_dict:
                flat_mode_mapping[str(inner_key)] = outer_key

        # Get trip count using sql function
        sql_query = f"""
            INSERT INTO {self.result_table}(layer_id, geom, {', '.join(result_layer.attribute_mapping.keys())})
            SELECT '{str(result_layer.id)}', s.geom, s.stop_id, s.stop_name, s.trip_cnt,
            (summarized ->> 'bus')::integer AS bus, (summarized ->> 'tram')::integer AS tram, (summarized ->> 'metro')::integer AS metro,
            (summarized ->> 'rail')::integer AS rail, (summarized ->> 'other')::integer AS other,
            (summarized ->> 'bus')::integer + (summarized ->> 'tram')::integer + (summarized ->> 'metro')::integer +
            (summarized ->> 'rail')::integer + (summarized ->> 'other')::integer AS total
            FROM basic.count_public_transport_services_station(
                '{layer_project.table_name}',
                :where_query,
                '{str(timedelta(seconds=params.time_window.from_time))}',
                '{str(timedelta(seconds=params.time_window.to_time))}',
                {params.time_window.weekday_integer}
            ) s, LATERAL basic.summarize_trip_count(trip_cnt, '{json.dumps(flat_mode_mapping)}'::JSONB) summarized
        """
        where_query = build_where_clause([layer_project.where_query])
        await self.async_session.execute(sql_query, {"where_query": where_query})
        await self.async_session.commit()

        # Create result layer
        await self.create_feature_layer_tool(layer_in=result_layer, params=params)
        return {
            "status": JobStatusType.finished.value,
            "msg": "Trip count created.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def trip_count_run(self, params: ITripCountStation):
        return await self.trip_count(params=params)


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
        isochrone_request = IIsochroneActiveMobility(
            starting_points=params.starting_points,
            routing_type=params.access_mode,
            travel_cost=TravelTimeCostActiveMobility(
                max_traveltime=params.max_traveltime,
                steps=params.max_traveltime,
                speed=params.speed,
            ),
            isochrone_type=IsochroneType.polygon,
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
        sql_compute_nearby_stations = f"""
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
            GROUP BY stop_id, stop_name, access_time, geom;
        """
        try:
            await self.async_session.execute(sql_compute_nearby_stations)
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
