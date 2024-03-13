from src.crud.crud_heatmap import CRUDHeatmapBase
from src.schemas.heatmap import (
    IHeatmapGravityActive,
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
from typing import List


class CRUDHeatmapGravityBase(CRUDHeatmapBase):
    TRAVELTIME_MATRIX_TABLE = {
        ActiveRoutingHeatmapType.walking: "basic.traveltime_matrix_walking",
        ActiveRoutingHeatmapType.bicycle: "basic.traveltime_matrix_bicycle",
    }

    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    async def create_distributed_opportunity_table(self, layers: List[dict]):
        # Create temp table name for points
        temp_points = await self.create_temp_table_name("points")

        append_to_existing = False
        for layer in layers:
            # Create distributed point table using sql
            where_query_point = "WHERE " + layer["where_query"].replace("'", "''")
            potential_column = 1 if not layer["layer"].destination_potential_column \
                else layer["layer"].destination_potential_column

            await self.async_session.execute(
                f"""SELECT basic.create_heatmap_gravity_opportunity_table(
                    '{layer["table_name"]}'::text,
                    {layer["layer"].max_traveltime}::smallint,
                    {layer["layer"].sensitivity}::int,
                    {potential_column}::text,
                    '{where_query_point}'::text,
                    '{temp_points}'::text,
                    {append_to_existing}::boolean
                )"""
            )
            await self.async_session.commit()
            append_to_existing = True

        return temp_points

    # TODO: Verify function formulas
    def build_impedance_function(self, type: ImpedanceFunctionType):
        """Builds impedance function used to compute gravity-based heatmaps."""

        if type == ImpedanceFunctionType.gaussian:
            return "SUM(EXP(1) ^ ((((traveltime * 60) ^ 2) * -1) / sensitivity) * potential)"
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
        opportunity_table: str,
        result_table: str,
        result_layer_id: str,
    ):
        """Builds SQL query to compute gravity-based heatmap."""

        impedance_function = self.build_impedance_function(params.impedance_function)

        query = f"""
            INSERT INTO {result_table} (layer_id, geom, text_attr1, float_attr1)
            SELECT '{result_layer_id}', ST_SetSRID(h3_cell_to_boundary(dest_id.value)::geometry, 4326), dest_id.value,
                {impedance_function} AS accessibility
            FROM
            (
                SELECT matrix.orig_id, matrix.dest_id, matrix.traveltime, opportunity.sensitivity, opportunity.potential
                FROM {opportunity_table} opportunity, basic.traveltime_matrix_walking matrix
                WHERE matrix.h3_3 = opportunity.h3_3
                AND matrix.orig_id = opportunity.h3_index
                AND matrix.traveltime <= opportunity.max_traveltime
            ) sub_matrix
            JOIN LATERAL UNNEST(sub_matrix.dest_id) dest_id(value) ON TRUE
            GROUP BY dest_id.value;
        """

        return query


class CRUDHeatmapGravityActiveMobility(CRUDHeatmapGravityBase):
    def __init__(self, job_id, background_tasks, async_session, user_id, project_id):
        super().__init__(job_id, background_tasks, async_session, user_id, project_id)

    @job_log(job_step_name="heatmap_gravity")
    async def heatmap(self, params: IHeatmapGravityActive):
        """Compute gravity-based heatmap for active mobility."""

        # Fetch opportunity tables
        layers = await self.fetch_opportunity_layers(params)
        opportunity_table = await self.create_distributed_opportunity_table(layers)

        # Initialize result table
        result_table = f"{settings.USER_DATA_SCHEMA}.{FeatureGeometryType.polygon.value}_{str(self.user_id).replace('-', '')}"

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

        # Compute heatmap & write to result table
        await self.async_session.execute(self.build_query(
            params=params,
            opportunity_table=opportunity_table,
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
