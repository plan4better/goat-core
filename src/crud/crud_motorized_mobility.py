from datetime import timedelta
from geojson import FeatureCollection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from src.schemas.motorized_mobility import StationConfig
from src.crud.crud_layer import layer as crud_layer
from uuid import UUID
import json


class CRUDMotorizedMobility:
    async def get_oev_gueteklasse_station_category(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        start_time: str,
        end_time: str,
        weekday: int,
        reference_area_sql: str,
        station_config: StationConfig,
    ):
        """Get count of public transport stations for every service."""

        attribute_mapping = {
            "text_attr1": "stop_id",
            "text_attr2": "stop_name",
        }


        await async_session.execute(
            text(
                f"""
            CREATE TABLE basic.stations AS
            SELECT stop_id, stop_name, trip_cnt::jsonb, geom AS geom, trip_ids, (oev_gueteklasse ->> 'frequency')::float AS frequency, oev_gueteklasse ->> '_class' AS _class
            FROM basic.count_public_transport_services_station(
                '{timedelta(seconds=start_time)}',
                '{timedelta(seconds=end_time)}',
                {weekday},
                '{reference_area_sql}'
            ), LATERAL basic.oev_guetklasse_station_category(trip_cnt, '{json.dumps(station_config)}'::jsonb, {start_time}::numeric, {end_time}::numeric) oev_gueteklasse
            WHERE (oev_gueteklasse ->> 'frequency') <> '0'
            """
            ),
        )
        await async_session.commit()

        # TODO: Save directly in layer table and create a layer for the stations
        return

    async def compute_station_buffer(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        station_config: dict,
    ) -> dict:
        """Compute station buffer."""

        # TODO: Save directly in layer table and create a layer for the stations
        await async_session.execute(
            text(
                """
                DROP TABLE IF EXISTS basic.oev_gueteklasse;
                """
            ),
        )
        await async_session.commit()

        await async_session.execute(
            text(
                f"""
                CREATE TABLE basic.oev_gueteklasse AS
                SELECT *
                FROM basic.oev_gueteklasse_buffer('{json.dumps(station_config["classification"])}'::jsonb)
                """
            ),
        )
        await async_session.commit()

    async def compute_oev_gueteklassen(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        start_time: str,
        end_time: str,
        weekday: int,
        reference_area_sql: str,
        station_config: dict,
    ) -> FeatureCollection:
        """
        Calculate the OEV-Gueteklassen for a given time period and weekday.
        """

        # Get stations in study area
        await self.get_oev_gueteklasse_station_category(
            async_session=async_session,
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            weekday=weekday,
            reference_area_sql=reference_area_sql,
            station_config=station_config,
        )

        await self.compute_station_buffer(
            async_session=async_session,
            user_id=user_id,
            station_config=station_config,
        )
    #     return stations_count


motorized_mobility = CRUDMotorizedMobility()
