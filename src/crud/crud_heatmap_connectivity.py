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


class CRUDHeatmapConnectivityBase(CRUDHeatmapBase):
    TRAVELTIME_MATRIX_TABLE = {
        ActiveRoutingHeatmapType.walking: "basic.traveltime_matrix_walking",
        ActiveRoutingHeatmapType.bicycle: "basic.traveltime_matrix_bicycle",
    }

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    def build_query(
        self,
        params: IHeatmapConnectivityActive,
        reference_area_table: str,
        result_table: str,
        result_layer_id: str,
    ):
        """Builds SQL query to compute connectivity-based heatmap."""

        query = f"""
            INSERT INTO {result_table} (layer_id, geom, text_attr1, float_attr1)
            SELECT '{result_layer_id}', ST_SetSRID(h3_cell_to_boundary(matrix.orig_id)::geometry, 4326),
                matrix.orig_id, SUM((ARRAY_LENGTH(matrix.dest_id, 1) * ((3 * SQRT(3) / 2) * POWER(h3_get_hexagon_edge_length_avg(10, 'm'), 2))))
            FROM {reference_area_table} o, {self.TRAVELTIME_MATRIX_TABLE[params.routing_type]} matrix
            WHERE matrix.h3_3 = o.h3_3
            AND matrix.orig_id = o.h3_index
            AND matrix.traveltime <= {params.max_traveltime}
            GROUP BY matrix.orig_id;
        """
        return query


class CRUDHeatmapConnectivityActiveMobility(CRUDHeatmapConnectivityBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    @job_log(job_step_name="heatmap_connectivity")
    async def heatmap(self, params: IHeatmapConnectivityActive):
        """Compute connectivity-based heatmap for active mobility."""

        # Fetch reference area table
        reference_area_layer = await self.get_layers_project(params)
        reference_area_table = await self.create_table_polygons_to_h3_grid(
            [reference_area_layer["reference_area_layer_project_id"]]
        )

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
        await self.async_session.execute(self.build_query(
            params=params,
            reference_area_table=reference_area_table,
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
            "msg": "Connectivity-based heatmap for active mobility was successfully computed.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def run_heatmap(self, params: IHeatmapConnectivityActive):
        return await self.heatmap(params=params)
