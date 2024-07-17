from typing import List

from src.core.config import settings
from src.core.job import job_init, job_log, run_background_or_immediately
from src.crud.crud_heatmap import CRUDHeatmapBase
from src.schemas.heatmap import (
    TRAVELTIME_MATRIX_RESOLUTION,
    TRAVELTIME_MATRIX_TABLE,
    ActiveRoutingHeatmapType,
    IHeatmapGravityActive,
    IHeatmapGravityMotorized,
    ImpedanceFunctionType,
    MotorizedRoutingHeatmapType,
)
from src.schemas.job import JobStatusType
from src.schemas.layer import FeatureGeometryType, IFeatureLayerToolCreate
from src.schemas.toolbox_base import DefaultResultLayerName


class CRUDHeatmapGravity(CRUDHeatmapBase):

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    async def create_distributed_opportunity_table(
        self,
        routing_type: ActiveRoutingHeatmapType | MotorizedRoutingHeatmapType,
        layers: List[dict],
    ):
        """Create distributed table for user-specified opportunities."""

        # Create temp table name for points
        temp_points = await self.create_temp_table_name("points")

        append_to_existing = False
        for layer in layers:
            # Create distributed point table using sql
            where_query_point = "WHERE " + layer["where_query"].replace("'", "''")
            potential_column = (
                1
                if not layer["layer"].destination_potential_column
                else layer["layer"].destination_potential_column
            )

            await self.async_session.execute(
                f"""SELECT basic.create_heatmap_gravity_opportunity_table(
                    '{layer["table_name"]}',
                    {layer["layer"].max_traveltime},
                    {layer["layer"].sensitivity},
                    {potential_column}::text,
                    '{where_query_point}',
                    '{temp_points}',
                    {TRAVELTIME_MATRIX_RESOLUTION[routing_type]},
                    {append_to_existing}
                )"""
            )

            await self.async_session.commit()
            append_to_existing = True

        return temp_points

    # TODO: Verify function formulas
    def build_impedance_function(
        self,
        type: ImpedanceFunctionType,
        max_traveltime: int,
        max_sensitivity: float,
    ):
        """Builds impedance function used to compute heatmap gravity."""

        max_traveltime = float(max_traveltime)

        if type == ImpedanceFunctionType.gaussian:
            return f"SUM((EXP(1) ^ ((((traveltime / {max_traveltime}) ^ 2) * -1) / (sensitivity / {max_sensitivity}))) * potential)"
        elif type == ImpedanceFunctionType.linear:
            return f"SUM((1 - (traveltime / {max_traveltime})) * potential)"
        elif type == ImpedanceFunctionType.exponential:
            return f"SUM((EXP(1) ^ (((sensitivity / {max_sensitivity}) * -1) * (traveltime / {max_traveltime}))) * potential)"
        elif type == ImpedanceFunctionType.power:
            return f"SUM(((traveltime / {max_traveltime}) ^ ((sensitivity / {max_sensitivity}) * -1)) * potential)"
        else:
            raise ValueError(f"Unknown impedance function type: {type}")

    def build_query(
        self,
        params: IHeatmapGravityActive | IHeatmapGravityMotorized,
        opportunity_table: str,
        max_traveltime: int,
        max_sensitivity: float,
        result_table: str,
        result_layer_id: str,
    ):
        """Builds SQL query to compute heatmap gravity."""

        impedance_function = self.build_impedance_function(
            type=params.impedance_function,
            max_traveltime=max_traveltime,
            max_sensitivity=max_sensitivity,
        )

        query = f"""
            INSERT INTO {result_table} (layer_id, geom, text_attr1, float_attr1)
            SELECT '{result_layer_id}', ST_SetSRID(h3_cell_to_boundary(dest_id.value)::geometry, 4326), dest_id.value,
                {impedance_function} AS accessibility
            FROM
            (
                SELECT matrix.orig_id, matrix.dest_id, CAST(matrix.traveltime AS float) AS traveltime,
                    opportunity.sensitivity, opportunity.potential
                FROM {opportunity_table} opportunity, {TRAVELTIME_MATRIX_TABLE[params.routing_type]} matrix
                WHERE matrix.h3_3 = opportunity.h3_3
                AND matrix.orig_id = opportunity.h3_index
                AND matrix.traveltime <= opportunity.max_traveltime
            ) sub_matrix
            JOIN LATERAL UNNEST(sub_matrix.dest_id) dest_id(value) ON TRUE
            GROUP BY dest_id.value;
        """

        return query

    @job_log(job_step_name="heatmap_gravity")
    async def heatmap(self, params: IHeatmapGravityActive | IHeatmapGravityMotorized):
        """Compute heatmap gravity."""

        # Fetch opportunity tables
        layers = await self.fetch_opportunity_layers(params)
        opportunity_table = await self.create_distributed_opportunity_table(
            params.routing_type,
            layers,
        )

        # Initialize result table
        result_table = f"{settings.USER_DATA_SCHEMA}.{FeatureGeometryType.polygon.value}_{str(self.user_id).replace('-', '')}"

        # Create feature layer to store computed heatmap output
        layer_heatmap = IFeatureLayerToolCreate(
            name=(
                DefaultResultLayerName.heatmap_gravity_active_mobility.value
                if type(params.routing_type) == ActiveRoutingHeatmapType
                else DefaultResultLayerName.heatmap_gravity_motorized_mobility.value
            ),
            feature_layer_geometry_type=FeatureGeometryType.polygon,
            attribute_mapping={
                "text_attr1": "h3_index",
                "float_attr1": "accessibility",
            },
            tool_type=params.tool_type.value,
            job_id=self.job_id,
        )

        # Get max traveltime & sensitivity for normalization
        max_traveltime = max([layer["layer"].max_traveltime for layer in layers])

        # Compute heatmap & write to result table
        await self.async_session.execute(
            self.build_query(
                params=params,
                opportunity_table=opportunity_table,
                max_traveltime=max_traveltime,
                max_sensitivity=settings.HEATMAP_GRAVITY_MAX_SENSITIVITY,
                result_table=result_table,
                result_layer_id=str(layer_heatmap.id),
            )
        )

        # Register feature layer
        await self.create_feature_layer_tool(
            layer_in=layer_heatmap,
            params=params,
        )

        return {
            "status": JobStatusType.finished.value,
            "msg": "Heatmap gravity was successfully computed.",
        }

    @run_background_or_immediately(settings)
    @job_init()
    async def run_heatmap(
        self, params: IHeatmapGravityActive | IHeatmapGravityMotorized
    ):
        return await self.heatmap(params=params)
