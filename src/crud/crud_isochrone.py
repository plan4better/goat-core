from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.schemas.active_mobility import (
    IIsochroneActiveMobility,
)
from src.schemas.motorized_mobility import IIsochroneCar, IIsochronePT
from src.schemas.job import JobStatusType
from src.schemas.layer import IFeatureLayerToolCreate, UserDataGeomType
from src.core.tool import CRUDToolBase
from src.core.config import settings


class CRUDIsochrone(CRUDToolBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)
        self.table_starting_points = (
            f"{settings.USER_DATA_SCHEMA}.point_{str(self.user_id).replace('-', '')}"
        )

    async def create_layer_starting_points(
        self, params: IIsochroneActiveMobility | IIsochroneCar | IIsochronePT
    ):
        # Create layer object
        layer = IFeatureLayerToolCreate(
            name="Starting Points",
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
                raise Exception(
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

    async def active_mobility(
        self,
        params: IIsochroneActiveMobility,
    ):
        # Check if starting points are a layer
        if params.starting_points.layer_project_id:
            await crud_layer_project.get(
                db=self.async_session, id=params.starting_points.layer_project_id
            )
        else:
            # If starting points are not a layer then create a new layer and save x,y to the db
            await self.create_layer_starting_points(
                params=params
            )

        #TODO: Call isochrone routing endpoint
        #TODO: Create layer
        #TODO: Add layer_ids to job
        return {"status": JobStatusType.finished.value}
