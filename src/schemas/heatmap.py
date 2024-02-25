from enum import Enum
from pydantic import Field, BaseModel
from typing import List

class RoutingHeatmapType(str, Enum):
    """Supported routing modes for the heatmap."""

    walking = "walking"
    bicycle = "bicycle"
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


class HeatmapBase(BaseModel):

    routing_type: RoutingHeatmapType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the heatmap.",
    )

class IHeatmapGravityBased(HeatmapBase):
    """Gravity based heatmap schema."""

    impedance_function: ImpedanceFunctionType = Field(
        ...,
        title="Impedance Function",
        description="The impedance function of the heatmap.",
    )
    opportunities: List[OpportunityGravityBased] = Field(
        ...,
        title="Opportunities",
        description="The opportunities the heatmap should be calculated for heatmap.",
    )

class IHeatmapClosestAverage(HeatmapBase):
    """Closest average based heatmap schema."""

    opportunities: List[OpportunityClosestAverage] = Field(
        ...,
        title="Opportunities",
        description="The opportunities the heatmap should be calculated for heatmap.",
    )
    
