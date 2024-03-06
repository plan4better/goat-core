from src.core.tool import CRUDToolBase
from src.schemas.heatmap import (
    IHeatmapGravityActive,
    IHeatmapGravityMotorized,
    IHeatmapClosestAverageActive,
    IHeatmapClosestAverageMotorized,
    IHeatmapConnectivityActive,
    IHeatmapConnectivityMotorized,
)
from src.schemas.layer import FeatureGeometryType
from src.schemas.toolbox_base import (
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
