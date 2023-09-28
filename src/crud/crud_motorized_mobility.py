import json
from datetime import datetime, timedelta
from uuid import UUID

from geojson import FeatureCollection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

from src.crud.crud_layer import layer as crud_layer
from src.db.models import Layer
from src.schemas.motorized_mobility import (
    StationConfig,
    oev_gueteklasse_station_config_layer_base,
    CalculateOevGueteklassenParameters
)
from src.utils import get_user_table, get_layer_columns


class CRUDOevGueteklasse:
    """CRUD for OEV-Gueteklasse."""
    def __init__(self):
        pass

    async def get_oev_gueteklasse_station_category(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        params: CalculateOevGueteklassenParameters,
        reference_table_name: str,
    ):
        """Get count of public transport stations for every service."""

        # Create Layer for stations
        layer = Layer(
            **oev_gueteklasse_station_config_layer_base,
            user_id=user_id,
            data_reference_year=datetime.now().year,
            folder_id=params.folder_id,
        )
        layer = await crud_layer.create(db=async_session, obj_in=layer)

        # Get user table
        user_table = get_user_table(user_id, layer.feature_layer_geometry_type)
        original_columns, table_columns = get_layer_columns(layer.attribute_mapping, ["geom", "layer_id"])

        query = f"""
            INSERT INTO {user_table}({table_columns})
            WITH stations AS (
                SELECT stop_id, stop_name, trip_cnt::jsonb, trip_ids,
                (oev_gueteklasse ->> 'frequency')::float AS frequency, 
                (oev_gueteklasse ->> '_class')::integer AS _class, geom AS geom, 
                '{str(layer.id)}'::uuid AS layer_id
                FROM basic.count_public_transport_services_station(
                    '{str(timedelta(seconds=params.start_time))}',
                    '{str(timedelta(seconds=params.end_time))}',
                    {params.weekday},
                    '{reference_table_name}'
                ) s, LATERAL basic.oev_guetklasse_station_category(trip_cnt, '{json.dumps(params.station_config.dict())}'::jsonb, 
                {params.start_time}, {params.end_time}) oev_gueteklasse
            )
            SELECT {original_columns}
            FROM stations
        """

        # Replace variables with actual values
        params = {
            "layer_id": layer.id,
            "start_time_str": str(timedelta(seconds=params.start_time)),
            "end_time_str": str(timedelta(seconds=params.end_time)),
            "weekday": params.weekday,
            "reference_table_name": reference_table_name,
            #"station_config_json": json.dumps(params.station_config.dict()),
            "start_time_numeric": params.start_time,
            "end_time_numeric": params.end_time,
        }
        # Execute query

        await async_session.execute(query, params)
        await async_session.commit()

        return layer

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

crud_oev_gueteklasse = CRUDOevGueteklasse()
