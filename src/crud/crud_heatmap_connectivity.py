from src.crud.crud_heatmap import CRUDHeatmapBase
from src.schemas.heatmap import (
    IHeatmapConnectivityActive,
    ActiveRoutingHeatmapType,
)
from src.core.config import settings
from src.schemas.job import JobStatusType
from src.core.job import job_init, job_log, run_background_or_immediately
from src.schemas.layer import IFeatureLayerToolCreate, FeatureGeometryType
from src.schemas.toolbox_base import (
    DefaultResultLayerName,
)
from src.schemas.project import IFeatureStandardProjectRead


class CRUDHeatmapConnectivityBase(CRUDHeatmapBase):
    TRAVELTIME_MATRIX_TABLE = {
        ActiveRoutingHeatmapType.walking: "basic.traveltime_matrix_walking",
        ActiveRoutingHeatmapType.bicycle: "basic.traveltime_matrix_bicycle",
    }
    H3_10_CELL_AREA_SQM = 11285.625000840355

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    async def compute(
        self,
        params: IHeatmapConnectivityActive,
        reference_area_layer: IFeatureStandardProjectRead,
        result_table: str,
        result_layer_id: str,
    ):
        """Computes connectivity-based heatmap."""

        # Fetch H3_3 cells within reference area (we run the computation cell-by-cell for performance reasons)
        h3_3_cells = (await self.async_session.execute(
            f"""
                WITH reference_area AS (
                    SELECT ST_Union(geom) AS geom
                    FROM {reference_area_layer.table_name}
                    WHERE {reference_area_layer.where_query}
                )
                SELECT h3_short
                FROM reference_area ra,
                LATERAL temporal.fill_polygon_h3_3(ra.geom);
            """
        )).fetchall()
        h3_3_cells = [row[0] for row in h3_3_cells]

        # Compute connectivity-based heatmap and write to result table
        for h3_3 in h3_3_cells:
            sql_compute_heatmap = f"""
                WITH reference_area AS (
                    SELECT ST_Union(geom) AS geom
                    FROM {reference_area_layer.table_name}
                    WHERE {reference_area_layer.where_query}
                ),
                origin_cells_h3_10 AS (
                    SELECT h3_index
                    FROM reference_area ra,
                    LATERAL temporal.fill_polygon_h3_10(ra.geom)
                ),
                sub_matrix AS (
                    SELECT matrix.orig_id, (ARRAY_LENGTH(matrix.dest_id, 1) * {self.H3_10_CELL_AREA_SQM}) AS area_sqm
                    FROM origin_cells_h3_10 o, {self.TRAVELTIME_MATRIX_TABLE[params.routing_type]} matrix
                    WHERE matrix.h3_3 = {h3_3}
                    AND matrix.orig_id = o.h3_index
                    AND matrix.traveltime <= {params.max_traveltime}
                    UNION ALL
                    SELECT h3_index AS orig_id, {self.H3_10_CELL_AREA_SQM} AS area_sqm
                    FROM origin_cells_h3_10
                ),
                grouped AS (
                    SELECT orig_id, SUM(area_sqm) AS accessibility
                    FROM sub_matrix
                    GROUP BY orig_id
                )
                INSERT INTO {result_table} (layer_id, geom, text_attr1, float_attr1)
                SELECT '{result_layer_id}', ST_SetSRID(h3_cell_to_boundary(orig_id)::geometry, 4326),
                    orig_id, accessibility
                FROM grouped;
            """
            await self.async_session.execute(sql_compute_heatmap)


class CRUDHeatmapConnectivityActiveMobility(CRUDHeatmapConnectivityBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    @job_log(job_step_name="heatmap_connectivity")
    async def heatmap(self, params: IHeatmapConnectivityActive):
        """Compute connectivity-based heatmap for active mobility."""

        # Fetch reference area table
        reference_area_layer = await self.get_layers_project(params)

        # Initialize result table
        result_table = f"{settings.USER_DATA_SCHEMA}.{FeatureGeometryType.polygon.value}_{str(self.user_id).replace('-', '')}"

        # Create feature layer to store computed heatmap output
        layer_heatmap = IFeatureLayerToolCreate(
            name=DefaultResultLayerName.heatmap_connectivity_active_mobility.value,
            feature_layer_geometry_type=FeatureGeometryType.polygon,
            attribute_mapping={
                "text_attr1": "h3_index",
                "float_attr1": "accessibility"
            },
            tool_type=params.tool_type.value,
            job_id=self.job_id,
        )

        # Compute heatmap & write to output table
        await self.compute(
            params=params,
            reference_area_layer=reference_area_layer["reference_area_layer_project_id"],
            result_table=result_table,
            result_layer_id=str(layer_heatmap.id),
        )

        # Register feature layer
        await self.create_feature_layer_tool(
            layer_in=layer_heatmap,
            params=params,
        )

        return {
            "status": JobStatusType.finished.value,
            "msg": "Connectivity-based heatmap for active mobility was successfully computed.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def run_heatmap(self, params: IHeatmapConnectivityActive):
        return await self.heatmap(params=params)
