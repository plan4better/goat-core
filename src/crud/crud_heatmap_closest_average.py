from src.crud.crud_heatmap import CRUDHeatmapBase
from src.schemas.heatmap import (
    IHeatmapClosestAverageActive,
    OpportunityClosestAverage,
    ActiveRoutingHeatmapType,
)
from src.core.config import settings
from src.schemas.job import JobStatusType
from src.core.job import job_init, job_log, run_background_or_immediately
from src.schemas.layer import IFeatureLayerToolCreate, FeatureGeometryType
from src.schemas.toolbox_base import (
    DefaultResultLayerName,
)


class CRUDHeatmapClosestAverageBase(CRUDHeatmapBase):
    TRAVELTIME_MATRIX_TABLE = {
        ActiveRoutingHeatmapType.walking: "basic.traveltime_matrix_walking",
        ActiveRoutingHeatmapType.bicycle: "basic.traveltime_matrix_bicycle",
    }

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    def build_query(
        self,
        params: IHeatmapClosestAverageActive,
        opportunity_layer: OpportunityClosestAverage,
        opportunity_table: str,
        result_table: str,
        result_layer_id: str,
    ):
        """Builds SQL query to compute closest-average-based heatmap."""

        query = f"""
            WITH opportunity_cells AS (
                SELECT id, h3_lat_lng_to_cell(geom::point, 10) AS h3_index, h3_3
                FROM {opportunity_table}
            ),
            sub_matrix AS (
                SELECT matrix.orig_id, matrix.dest_id, matrix.traveltime
                FROM opportunity_cells p, {self.TRAVELTIME_MATRIX_TABLE[params.routing_type]} matrix
                WHERE matrix.h3_3 IN (SELECT DISTINCT(h3_3) FROM opportunity_cells)
                AND matrix.orig_id = p.h3_index
                AND matrix.traveltime <= {opportunity_layer.max_traveltime}
                UNION ALL
                SELECT h3_index AS orig_id, ARRAY[h3_index] AS dest_id, 0 AS traveltime
                FROM opportunity_cells
            ),
            unnested AS (
                SELECT matrix.orig_id, dest_id.value AS dest_id, matrix.traveltime,
                    ROW_NUMBER() OVER(PARTITION BY matrix.orig_id ORDER BY matrix.traveltime) as rn
                FROM sub_matrix matrix
                JOIN LATERAL UNNEST(matrix.dest_id) dest_id(value) ON TRUE
            ),
            filtered AS (
                SELECT orig_id, dest_id, traveltime
                FROM unnested
                WHERE rn <= {opportunity_layer.number_of_destinations}
            ),
            grouped AS (
                SELECT dest_id, AVG(traveltime) AS accessibility
                FROM filtered
                GROUP BY dest_id
            )
            INSERT INTO {result_table} (layer_id, geom, text_attr1, float_attr1)
            SELECT '{result_layer_id}', ST_SetSRID(h3_cell_to_boundary(dest_id)::geometry, 4326),
                dest_id, accessibility
            FROM grouped;
        """
        return query


class CRUDHeatmapClosestAverageActiveMobility(CRUDHeatmapClosestAverageBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    @job_log(job_step_name="heatmap_closest_average")
    async def heatmap(self, params: IHeatmapClosestAverageActive):
        """Compute closest-average-based heatmap for active mobility."""

        # Fetch opportunity tables
        opportunity_layers = await self.fetch_opportunity_layers(params)

        # Initialize result table
        result_table = f"{settings.USER_DATA_SCHEMA}.{FeatureGeometryType.polygon.value}_{str(self.user_id).replace('-', '')}"

        # Iterate over tables and compute heatmap
        for layer in opportunity_layers:
            # Create feature layer to store computed heatmap output
            layer_heatmap = IFeatureLayerToolCreate(
                name=DefaultResultLayerName.heatmap_closest_average_active_mobility.value,
                feature_layer_geometry_type=FeatureGeometryType.polygon,
                attribute_mapping={
                    "text_attr1": "h3_index",
                    "float_attr1": "accessibility"
                },
                tool_type=params.tool_type.value,
                job_id=self.job_id,
            )

            # Compute heatmap & write to output table
            await self.async_session.execute(self.build_query(
                params=params,
                opportunity_layer=layer["config"],
                opportunity_table=layer["table"],
                result_table=result_table,
                result_layer_id=str(layer_heatmap.id),
            ))

            # Register feature layer
            await self.create_feature_layer_tool(
                layer_in=layer_heatmap,
                params=params,
            )

        return {
            "status": JobStatusType.finished.value,
            "msg": "Closest-average-based heatmap for active mobility was successfully computed.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def run_heatmap(self, params: IHeatmapClosestAverageActive):
        return await self.heatmap(params=params)
