from src.core.tool import CRUDToolBase
from src.schemas.heatmap import (
    IHeatmapGravityActive,
    IHeatmapGravityMotorized,
    IHeatmapClosestAverageActive,
    IHeatmapClosestAverageMotorized,
    IHeatmapConnectivityActive,
    IHeatmapConnectivityMotorized,
)
from src.core.config import settings
from src.schemas.job import JobStatusType
from src.core.job import job_init, job_log, run_background_or_immediately
from src.schemas.layer import IFeatureLayerToolCreate, FeatureGeometryType
from src.schemas.toolbox_base import (
    DefaultResultLayerName,
    MaxFeaturePolygonArea,
)
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.db.models.layer import LayerType


class CRUDHeatmapBase(CRUDToolBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    async def fetch_opportunity_layers(self, params: IHeatmapGravityActive | IHeatmapGravityMotorized |
        IHeatmapClosestAverageActive | IHeatmapClosestAverageMotorized | IHeatmapConnectivityActive |
        IHeatmapConnectivityMotorized,
    ):
        opportunity_layers = []

        # Iterate over opportunity layers supplied by user
        for layer in params.opportunities:
            # Get project for this layer
            input_layer_types = params.input_layer_types
            layer_project = await crud_layer_project.get_internal(
                async_session=self.async_session,
                id=layer.opportunity_layer_project_id,
                project_id=self.project_id,
                expected_layer_types=input_layer_types[
                    "opportunity_layer_project_id"
                ].layer_types,
                expected_geometry_types=input_layer_types[
                    "opportunity_layer_project_id"
                ].feature_layer_geometry_types,
            )

            # Check Max feature count
            await self.check_max_feature_cnt(
                layers_project=[layer_project],
                tool_type=params.tool_type,
            )

            # Check for each feature layer of type polygon if the tool type is in MaxFeaturePolygonArea
            if layer_project.type == LayerType.feature:
                if (
                    layer_project.feature_layer_geometry_type == FeatureGeometryType.polygon
                    and params.tool_type in MaxFeaturePolygonArea.__members__
                ):
                    # Check reference area size
                    await self.check_reference_area_size(
                        layer_project=layer_project,
                        tool_type=params.tool_type,
                    )

            # Create temporary distributed table to store data from the opportunity layer
            temp_table = await self.create_distributed_point_table(layer_project)

            opportunity_layers.append({
                "config": layer,
                "table": temp_table,
            })

        return opportunity_layers


class CRUDHeatmapGravityActiveMobility(CRUDHeatmapBase):
    def __init__(
        self,
        job_id,
        background_tasks,
        async_session,
        user_id,
        project_id,
    ):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    @job_log(job_step_name="heatmap_gravity")
    async def heatmap(self, params: IHeatmapGravityActive):
        """Compute gravity-based heatmap for active mobility."""

        # Fetch opportunity tables
        opportunity_layers = await self.fetch_opportunity_layers(params)

        # Initialize result table
        result_table = f"{settings.USER_DATA_SCHEMA}.{FeatureGeometryType.polygon.value}_{str(self.user_id).replace('-', '')}"

        # Iterate over tables and compute heatmap
        for layer in opportunity_layers:
            # Create feature layer to store computed heatmap output
            layer_heatmap = IFeatureLayerToolCreate(
                name=DefaultResultLayerName.heatmap_gravity_active_mobility.value,
                feature_layer_geometry_type=FeatureGeometryType.polygon,
                attribute_mapping={
                    "text_attr1": "h3_index",
                    "float_attr1": "accessibility"
                },
                tool_type=params.tool_type.value,
                job_id=self.job_id,
            )

            # Compute heatmap & write to output table
            potential_column: str = layer["config"].destination_potential_column \
                if layer["config"].destination_potential_column else "1"
            sql_compute_heatmap = f"""
                WITH poi_cells AS (
                    SELECT id, h3_lat_lng_to_cell(geom::point, 10) AS h3_index, {potential_column} AS potential, h3_3
                    FROM {layer["table"]}
                ),
                sub_matrix AS (
                    SELECT matrix.orig_id, p.potential, UNNEST(matrix.dest_id) AS dest_id, matrix.traveltime
                    FROM poi_cells p,
                        basic.traveltime_matrix_walking matrix
                    WHERE matrix.h3_3 IN (SELECT DISTINCT(h3_3) FROM poi_cells)
                    AND matrix.orig_id = p.h3_index
                    UNION ALL
                    SELECT h3_index AS orig_id, potential, h3_index AS dest_id, 0 AS traveltime
                    FROM poi_cells
                ),
                grouped AS (
                    SELECT dest_id, SUM(EXP(1) ^ ((((traveltime * 60) ^ 2) * -1) / {layer["config"].sensitivity}) * potential) AS accessibility
                    FROM sub_matrix
                    GROUP BY dest_id
                )
                INSERT INTO {result_table} (layer_id, geom, text_attr1, float_attr1)
                SELECT '{str(layer_heatmap.id)}', ST_SetSRID(h3_cell_to_boundary(dest_id)::geometry, 4326),
                    dest_id, accessibility
                FROM grouped;
            """
            await self.async_session.execute(sql_compute_heatmap)

            # Register feature layer
            await self.create_feature_layer_tool(
                layer_in=layer_heatmap,
                params=params,
            )

        return {
            "status": JobStatusType.finished.value,
            "msg": "Gravity-based heatmap for active mobility was successfully computed.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def run_heatmap(self, params: IHeatmapGravityActive):
        return await self.heatmap(params=params)
