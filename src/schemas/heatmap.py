from enum import Enum
from typing import List
from uuid import UUID

from pydantic import BaseModel, Field, validator

from src.core.config import settings
from src.schemas.colors import ColorRangeType
from src.schemas.layer import ToolType
from src.schemas.toolbox_base import (
    DefaultResultLayerName,
    input_layer_type_point,
    input_layer_type_polygon,
)


class ActiveRoutingHeatmapType(str, Enum):
    """Supported active mobility types for heatmaps."""

    walking = "walking"
    bicycle = "bicycle"
    pedelec = "pedelec"


class MotorizedRoutingHeatmapType(str, Enum):
    """Supported motorized mobility types for heatmaps."""

    public_transport = "public_transport"
    car = "car"


TRAVELTIME_MATRIX_TABLE = {
    ActiveRoutingHeatmapType.walking: "basic.traveltime_matrix_walking",
    ActiveRoutingHeatmapType.bicycle: "basic.traveltime_matrix_bicycle",
    ActiveRoutingHeatmapType.pedelec.value: "basic.traveltime_matrix_pedelec",
    MotorizedRoutingHeatmapType.public_transport.value: "basic.traveltime_matrix_pt",
    MotorizedRoutingHeatmapType.car.value: "basic.traveltime_matrix_car",
}


TRAVELTIME_MATRIX_RESOLUTION = {
    ActiveRoutingHeatmapType.walking.value: 10,
    ActiveRoutingHeatmapType.bicycle.value: 9,
    ActiveRoutingHeatmapType.pedelec.value: 9,
    MotorizedRoutingHeatmapType.public_transport.value: 9,
    MotorizedRoutingHeatmapType.car.value: 8,
}


class ImpedanceFunctionType(str, Enum):
    """Impedance function type schema."""

    gaussian = "gaussian"
    linear = "linear"
    exponential = "exponential"
    power = "power"


class MaxTravelTimeTransportMode(int, Enum):
    """Max travel time transport mode schema."""

    walking = 30
    bicycle = 30
    pedelec = 30
    public_transport = 60
    car = 60


class OpportunityBase(BaseModel):
    opportunity_layer_project_id: int = Field(
        ...,
        title="Layer Project ID of layer containing opportunities.",
        description="The ID of the layer project that contains the opportunities.",
    )
    max_traveltime: int = Field(
        ...,
        title="Max Travel Time",
        description="The maximum travel time in minutes.",
        ge=1,
        le=60,
    )


class OpportunityClosestAverage(OpportunityBase):
    """Opportunity object for the gravity based heatmap."""

    number_of_destinations: int = Field(
        ...,
        title="Number of Destinations",
        description="The number of destinations to be included in the average.",
    )


class OpportunityGravityBased(OpportunityBase):
    """Opportunity object for the gravity based heatmap."""

    sensitivity: float = Field(
        ...,
        title="Sensitivity parameter for the opportunities and impedance function.",
        description="The sensitivity parameter for the opportunities and impedance function.",
    )
    destination_potential_column: str | None = Field(
        None,
        title="Destination Potential Column",
        description="The column name of the destination potential.",
    )

    # Ensure sensitivity doesn't exceed the configured limit
    @validator("sensitivity", pre=True, always=True)
    def valid_sensitivity(cls, v):
        if v > settings.HEATMAP_GRAVITY_MAX_SENSITIVITY:
            raise ValueError(
                f"The sensitivity must not exceed {settings.HEATMAP_GRAVITY_MAX_SENSITIVITY}."
            )
        return v


class HeatmapGravityBase(BaseModel):
    """Gravity based heatmap schema."""

    impedance_function: ImpedanceFunctionType = Field(
        ...,
        title="Impedance Function",
        description="The impedance function of the heatmap.",
    )
    # TODO: Limit 10 opportunities layers
    opportunities: List[OpportunityGravityBased] = Field(
        ...,
        title="Opportunities",
        description="The opportunities the heatmap should be calculated for heatmap.",
    )
    scenario_id: UUID | None = Field(
        None,
        title="Scenario ID",
        description="The ID of the scenario that is to be applied on the input layer or base network.",
    )

    def validate_max_traveltime(routing_type, values):
        max_traveltime = MaxTravelTimeTransportMode[routing_type].value
        for opportunity in values.get("opportunities"):
            if opportunity.max_traveltime > max_traveltime:
                raise ValueError(
                    f"Max supported travel time for {routing_type} is {max_traveltime} minutes."
                )
        return routing_type

    @property
    def input_layer_types(self):
        return {"opportunity_layer_project_id": input_layer_type_point}


class HeatmapClosestAverageBase(BaseModel):
    """Closest average based heatmap schema."""

    opportunities: List[OpportunityClosestAverage] = Field(
        ...,
        title="Opportunities",
        description="The opportunities the heatmap should be calculated for heatmap.",
    )
    scenario_id: UUID | None = Field(
        None,
        title="Scenario ID",
        description="The ID of the scenario that is to be applied on the input layer or base network.",
    )

    def validate_max_traveltime(routing_type, values):
        max_traveltime = MaxTravelTimeTransportMode[routing_type].value
        for opportunity in values.get("opportunities"):
            if opportunity.max_traveltime > max_traveltime:
                raise ValueError(
                    f"Max supported travel time for {routing_type} is {max_traveltime} minutes."
                )
        return routing_type

    @property
    def input_layer_types(self):
        return {"opportunity_layer_project_id": input_layer_type_point}


class HeatmapConnectivityBase(BaseModel):
    """Connectivity based heatmap schema."""

    reference_area_layer_project_id: int = Field(
        ...,
        title="The layer project serving reference Area for the calculation.",
        description="The reference area for the connectivity heatmap.",
    )
    max_traveltime: int = Field(
        ...,
        title="Max Travel Time",
        description="The maximum travel time in minutes.",
        ge=1,
        le=60,
    )
    scenario_id: UUID | None = Field(
        None,
        title="Scenario ID",
        description="The ID of the scenario that is to be applied on the input layer or base network.",
    )

    def validate_max_traveltime(routing_type, values):
        max_traveltime = MaxTravelTimeTransportMode[routing_type].value
        if values.get("max_traveltime") > max_traveltime:
            raise ValueError(
                f"Max supported travel time for {routing_type} is {max_traveltime} minutes."
            )
        return routing_type

    @property
    def input_layer_types(self):
        return {"reference_area_layer_project_id": input_layer_type_polygon}


class IHeatmapGravityActive(HeatmapGravityBase):
    """Gravity based heatmap for active mobility schema."""

    routing_type: ActiveRoutingHeatmapType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )

    @validator("routing_type")
    def validate_routing_type(cls, routing_type, values):
        return super().validate_max_traveltime(routing_type, values)

    @property
    def tool_type(self):
        return ToolType.heatmap_gravity_active_mobility

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.heatmap_gravity_active_mobility: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {
                    "name": "accessibility",
                    "type": "number",
                },
                "color_scale": "quantile",
            }
        }


class IHeatmapGravityMotorized(HeatmapGravityBase):
    """Gravity based heatmap for motorized mobility schema."""

    routing_type: MotorizedRoutingHeatmapType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )

    @validator("routing_type")
    def validate_routing_type(cls, routing_type, values):
        return super().validate_max_traveltime(routing_type, values)

    @property
    def tool_type(self):
        return ToolType.heatmap_gravity_motorized_mobility

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.heatmap_gravity_active_mobility: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {
                    "name": "accessibility",
                    "type": "number",
                },
                "color_scale": "quantile",
            }
        }


class IHeatmapClosestAverageActive(HeatmapClosestAverageBase):
    """Closest average based heatmap for active mobility schema."""

    routing_type: ActiveRoutingHeatmapType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )

    @validator("routing_type")
    def validate_routing_type(cls, routing_type, values):
        return super().validate_max_traveltime(routing_type, values)

    @property
    def tool_type(self):
        return ToolType.heatmap_closest_average_active_mobility

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.heatmap_closest_average_active_mobility: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {
                    "name": "accessibility",
                    "type": "number",
                },
                "color_scale": "quantile",
            }
        }


class IHeatmapClosestAverageMotorized(HeatmapClosestAverageBase):
    """Closest average based heatmap for motorized mobility schema."""

    routing_type: MotorizedRoutingHeatmapType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )

    @validator("routing_type")
    def validate_routing_type(cls, routing_type, values):
        return super().validate_max_traveltime(routing_type, values)

    @property
    def tool_type(self):
        return ToolType.heatmap_closest_average_motorized_mobility

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.heatmap_closest_average_motorized_mobility: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {
                    "name": "accessibility",
                    "type": "number",
                },
                "color_scale": "quantile",
            }
        }


class IHeatmapConnectivityActive(HeatmapConnectivityBase):
    """Connectivity based heatmap for active mobility schema."""

    routing_type: ActiveRoutingHeatmapType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )

    @validator("routing_type")
    def validate_routing_type(cls, routing_type, values):
        return super().validate_max_traveltime(routing_type, values)

    @property
    def tool_type(self):
        return ToolType.heatmap_connectivity_active_mobility

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.heatmap_connectivity_active_mobility: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {
                    "name": "accessibility",
                    "type": "number",
                },
                "color_scale": "quantile",
            }
        }


class IHeatmapConnectivityMotorized(HeatmapConnectivityBase):
    """Connectivity based heatmap for motorized mobility schema."""

    routing_type: MotorizedRoutingHeatmapType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )

    @validator("routing_type")
    def validate_routing_type(cls, routing_type, values):
        return super().validate_max_traveltime(routing_type, values)

    @property
    def tool_type(self):
        return ToolType.heatmap_connectivity_motorized_mobility

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.heatmap_connectivity_motorized_mobility: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {
                    "name": "accessibility",
                    "type": "number",
                },
                "color_scale": "quantile",
            }
        }
