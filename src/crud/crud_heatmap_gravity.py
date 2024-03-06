from src.crud.crud_heatmap import CRUDHeatmapBase
from src.schemas.heatmap import (
    IHeatmapGravityActive,
    OpportunityGravityBased,
    ImpedanceFunctionType,
    ActiveRoutingHeatmapType,
)
from src.core.config import settings
from src.schemas.job import JobStatusType
from src.core.job import job_init, job_log, run_background_or_immediately
from src.schemas.layer import IFeatureLayerToolCreate, FeatureGeometryType
from src.schemas.toolbox_base import (
    DefaultResultLayerName,
)


class CRUDHeatmapGravityBase(CRUDHeatmapBase):
    TRAVELTIME_MATRIX_TABLE = {
        ActiveRoutingHeatmapType.walking: "basic.traveltime_matrix_walking",
        ActiveRoutingHeatmapType.bicycle: "basic.traveltime_matrix_bicycle",
    }

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    # TODO: Verify function formulas
    def build_impedance_function(self, type: ImpedanceFunctionType, sensitivity: int):
        """Builds impedance function used to compute gravity-based heatmaps."""

        if type == ImpedanceFunctionType.gaussian:
            return f"SUM(EXP(1) ^ ((((traveltime * 60) ^ 2) * -1) / {sensitivity}) * potential)"
        elif type == ImpedanceFunctionType.linear:
            return "SUM(potential / (traveltime * 60))"
        elif type == ImpedanceFunctionType.exponential:
            return "SUM(EXP(1) ^ ((traveltime * 60) * -1) * potential)"
        elif type == ImpedanceFunctionType.power:
            return "SUM(potential / ((traveltime * 60) ^ 2))"
        else:
            raise ValueError(f"Unknown impedance function type: {type}")

    def build_query(
        self,
        params: IHeatmapGravityActive,
        opportunity_layer: OpportunityGravityBased,
        opportunity_table: str,
        result_table: str,
        result_layer_id: str,
    ):
        """Builds SQL query to compute gravity-based heatmap."""

        potential_column = opportunity_layer.destination_potential_column \
            if opportunity_layer.destination_potential_column else "1"
        impedance_function = self.build_impedance_function(
            params.impedance_function,
            opportunity_layer.sensitivity,
        )
        query = f"""
            WITH opportunity_cells AS (
                SELECT id, h3_lat_lng_to_cell(geom::point, 10) AS h3_index,
                    {potential_column} AS potential, h3_3
                FROM {opportunity_table}
            ),
            sub_matrix AS (
                SELECT matrix.orig_id, p.potential, UNNEST(matrix.dest_id) AS dest_id, matrix.traveltime
                FROM opportunity_cells p, {self.TRAVELTIME_MATRIX_TABLE[params.routing_type]} matrix
                WHERE matrix.h3_3 IN (SELECT DISTINCT(h3_3) FROM opportunity_cells)
                AND matrix.orig_id = p.h3_index
                AND matrix.traveltime <= {opportunity_layer.max_traveltime}
                UNION ALL
                SELECT h3_index AS orig_id, potential, h3_index AS dest_id, 0 AS traveltime
                FROM opportunity_cells
            ),
            grouped AS (
                SELECT dest_id, {impedance_function} AS accessibility
                FROM sub_matrix
                GROUP BY dest_id
            )
            INSERT INTO {result_table} (layer_id, geom, text_attr1, float_attr1)
            SELECT '{result_layer_id}', ST_SetSRID(h3_cell_to_boundary(dest_id)::geometry, 4326),
                dest_id, accessibility
            FROM grouped;
        """
        return query


class CRUDHeatmapGravityActiveMobility(CRUDHeatmapGravityBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
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
            "msg": "Gravity-based heatmap for active mobility was successfully computed.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def run_heatmap(self, params: IHeatmapGravityActive):
        return await self.heatmap(params=params)
