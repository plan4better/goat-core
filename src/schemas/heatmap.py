from enum import Enum
from pydantic import Field, BaseModel
from typing import List
from src.schemas.layer import ToolType
from src.schemas.toolbox_base import input_layer_type_point


# TODO: Validate the traveltime based on the mode.
class ActiveRoutingHeatmapType(str, Enum):
    """Supported routing modes for active mobility and the heatmap."""

    walking = "walking"
    bicycle = "bicycle"


class MotorizedRoutingHeatmapType(str, Enum):
    """Supported routing modes for the heatmap."""

    public_transport = "public_transport"
    car = "car"


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


class HeatmapGravityBase(BaseModel):
    """Gravity based heatmap schema."""

    impedance_function: ImpedanceFunctionType = Field(
        ...,
        title="Impedance Function",
        description="The impedance function of the heatmap.",
    )
    #TODO: Limit 10 opportunities layers
    opportunities: List[OpportunityGravityBased] = Field(
        ...,
        title="Opportunities",
        description="The opportunities the heatmap should be calculated for heatmap.",
    )

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

    @property
    def input_layer_types(self):
        return {"opportunity_layer_project_id": input_layer_type_point}


class RoutingTypeActive(BaseModel):
    """Routing type for active mobility schema."""

    routing_type: ActiveRoutingHeatmapType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )


class RoutingTypeMotorized(BaseModel):
    """Routing type for motorized mobility schema."""

    routing_type: MotorizedRoutingHeatmapType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )


class IHeatmapGravityActive(RoutingTypeActive, HeatmapGravityBase):
    """Gravity based heatmap for active mobility schema."""

    @property
    def tool_type(self):
        return ToolType.heatmap_gravity_active_mobility



class IHeatmapGravityMotorized(RoutingTypeMotorized, HeatmapGravityBase):
    """Gravity based heatmap for motorized mobility schema."""

    @property
    def tool_type(self):
        return ToolType.heatmap_gravity_motorized_mobility



class IHeatmapClosestAverageActive(RoutingTypeActive, HeatmapClosestAverageBase):
    """Closest average based heatmap for active mobility schema."""

    @property
    def tool_type(self):
        return ToolType.heatmap_closest_average_active_mobility


class IHeatmapClosestAverageMotorized(RoutingTypeMotorized, HeatmapClosestAverageBase):
    """Closest average based heatmap for motorized mobility schema."""

    @property
    def tool_type(self):
        return ToolType.heatmap_closest_average_motorized_mobility


class IHeatmapConnectivityActive(RoutingTypeActive, HeatmapConnectivityBase):
    """Connectivity based heatmap for active mobility schema."""

    @property
    def tool_type(self):
        return ToolType.heatmap_connectivity_active_mobility


class IHeatmapConnectivityMotorized(RoutingTypeMotorized, HeatmapConnectivityBase):
    """Connectivity based heatmap for motorized mobility schema."""

    @property
    def tool_type(self):
        return ToolType.heatmap_connectivity_motorized_mobility
