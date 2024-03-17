from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field, validator

from src.schemas.layer import ToolType
from src.schemas.colors import ColorRangeType

from typing import List, Optional
from src.schemas.toolbox_base import (
    IsochroneStartingPointsBase,
    PTTimeWindow,
    input_layer_type_point,
    check_starting_points,
    DefaultResultLayerName,
)


"""Isochrone starting point validators."""


class IsochroneStartingPointsActiveMobility(IsochroneStartingPointsBase):
    """Model for the active mobility isochrone starting points."""

    # Check that the starting points for active mobility are below 1000
    check_starting_points = check_starting_points(1000)


class IsochroneStartingPointsMotorizedMobility(IsochroneStartingPointsBase):
    """Model for the active mobility isochrone starting points."""

    # Check that the starting points for motorized mobility is 1
    check_starting_points = check_starting_points(1)


"""Isochrone routing mode schemas."""


class IsochroneRoutingModeActiveMobility(str, Enum):
    """Routing active mobility type schema."""

    walking = "walking"
    bicycle = "bicycle"
    pedelec = "pedelec"


class IsochroneRoutingModePT(str, Enum):
    """Routing public transport mode schema."""

    bus = "bus"
    tram = "tram"
    rail = "rail"
    subway = "subway"
    ferry = "ferry"
    cable_car = "cable_car"
    gondola = "gondola"
    funicular = "funicular"


class IsochroneRoutingEgressModePT(str, Enum):
    """Routing public transport egress mode schema."""

    walk = "walk"
    bicycle = "bicycle"


class IsochroneRoutingAccessModePT(str, Enum):
    """Routing public transport access mode schema."""

    walk = "walk"
    bicycle = "bicycle"
    car = "car"


class IsochroneRoutingModeConfigPT(BaseModel):
    """Routing public transport type schema."""

    mode: List[IsochroneRoutingModePT] = Field(
        ...,
        title="Mode",
        description="The mode of the public transport.",
    )
    egress_mode: IsochroneRoutingEgressModePT = Field(
        ...,
        title="Egress Mode",
        description="The egress mode of the public transport.",
    )
    access_mode: IsochroneRoutingAccessModePT = Field(
        ...,
        title="Access Mode",
        description="The access mode of the public transport.",
    )


class IsochroneRoutingTypeCar(str, Enum):
    """Routing car type schema."""

    car_peak = "car_peak"


"""Isochrone travel cost schemas."""


class IsochroneTravelTimeCostActiveMobility(BaseModel):
    """Travel time cost schema."""

    max_traveltime: int = Field(
        ...,
        title="Max Travel Time",
        description="The maximum travel time in minutes.",
        ge=1,
        le=45,
    )
    steps: int = Field(
        ...,
        title="Steps",
        description="The number of steps.",
    )
    speed: int = Field(
        ...,
        title="Speed",
        description="The speed in km/h.",
        ge=1,
        le=25,
    )

    # Ensure the number of steps doesn't exceed the maximum traveltime
    @validator("steps", pre=True, always=True)
    def valid_num_steps(cls, v):
        if v > 45:
            raise ValueError(
                "The number of steps must not exceed the maximum traveltime."
            )
        return v


# TODO: Check how to treat miles
class IsochroneTravelDistanceCostActiveMobility(BaseModel):
    """Travel distance cost schema."""

    max_distance: int = Field(
        ...,
        title="Max Distance",
        description="The maximum distance in meters.",
        ge=50,
        le=20000,
    )
    steps: int = Field(
        ...,
        title="Steps",
        description="The number of steps.",
    )

    # Ensure the number of steps doesn't exceed the maximum distance
    @validator("steps", pre=True, always=True)
    def valid_num_steps(cls, v):
        if v > 20000:
            raise ValueError(
                "The number of steps must not exceed the maximum distance."
            )
        return v


class IsochroneTravelTimeCostMotorizedMobility(BaseModel):
    """Travel time cost schema."""

    max_traveltime: int = Field(
        ...,
        title="Max Travel Time",
        description="The maximum travel time in minutes.",
        ge=1,
        le=60,
    )
    steps: int = Field(
        ...,
        title="Steps",
        description="The number of steps.",
    )


"""Isochrone decay function schemas."""


class IsochroneDecayFunctionTypePT(Enum):
    LOGISTIC = "logistic"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    STEP = "step"


class IsochroneDecayFunctionPT(BaseModel):
    type: Optional[IsochroneDecayFunctionTypePT] = Field(
        IsochroneDecayFunctionTypePT.LOGISTIC, description="Decay function type"
    )
    standard_deviation_minutes: Optional[int] = Field(
        12, description="Standard deviation in minutes"
    )
    width_minutes: Optional[int] = Field(10, description="Width in minutes")


"""Isochrone type schemas."""


class IsochroneTypeActiveMobility(str, Enum):
    """Isochrone type schema for active mobility."""

    polygon = "polygon"
    network = "network"
    rectangular_grid = "rectangular_grid"


class IsochroneTypePT(str, Enum):
    """Isochrone type schema for public transport."""

    polygon = "polygon"
    rectangular_grid = "rectangular_grid"


"""User-configured isochrone payload schemas."""


class IIsochroneActiveMobility(BaseModel):
    """Model for the active mobility isochrone"""

    starting_points: IsochroneStartingPointsActiveMobility = Field(
        ...,
        title="Starting Points",
        description="The starting points of the isochrone.",
    )
    routing_type: IsochroneRoutingModeActiveMobility = Field(
        ...,
        title="Routing Type",
        description="The routing type of the isochrone.",
    )
    travel_cost: (
        IsochroneTravelTimeCostActiveMobility
        | IsochroneTravelDistanceCostActiveMobility
    ) = Field(
        ...,
        title="Travel Cost",
        description="The travel cost of the isochrone.",
    )
    scenario_id: UUID | None = Field(
        None,
        title="Scenario ID",
        description="The ID of the scenario that is used for the routing.",
    )
    isochrone_type: IsochroneTypeActiveMobility = Field(
        ...,
        title="Return Type",
        description="The return type of the isochrone.",
    )
    polygon_difference: bool | None = Field(
        None,
        title="Polygon Difference",
        description="If true, the polygons returned will be the geometrical difference of two following calculations.",
    )

    @property
    def tool_type(self):
        return ToolType.isochrone_active_mobility

    @property
    def geofence_table(self):
        mode = ToolType.isochrone_active_mobility.value.replace("isochrone_", "")
        return f"basic.geofence_{mode}"

    @property
    def input_layer_types(self):
        return {"layer_project_id": input_layer_type_point}

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.isochrone_active_mobility: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {"name": "travel_cost", "type": "number"},
                "color_scale": "ordinal",
            }
        }


class IIsochronePT(BaseModel):
    """Model for the public transport isochrone"""

    starting_points: IsochroneStartingPointsMotorizedMobility = Field(
        ...,
        title="Starting Points",
        description="The starting points of the isochrone.",
    )
    routing_type: IsochroneRoutingModeConfigPT = Field(
        ...,
        title="Routing Type",
        description="The routing type of the isochrone.",
    )
    travel_cost: IsochroneTravelTimeCostMotorizedMobility = Field(
        ...,
        title="Travel Cost",
        description="The travel cost of the isochrone.",
    )
    time_window: PTTimeWindow = Field(
        ...,
        title="Time Window",
        description="The time window of the isochrone.",
    )
    isochrone_type: IsochroneTypePT = Field(
        ...,
        title="Return Type",
        description="The return type of the isochrone.",
    )

    decay_function: IsochroneDecayFunctionPT = Field(
        IsochroneDecayFunctionPT(),
        title="Decay Function",
        description="The decay function of the isochrone.",
    )

    # Defaults - not currently user configurable
    walk_speed: float = 1.39
    max_walk_time: int = 20
    bike_speed: float = 4.166666666666667
    max_bike_time: int = 20
    bike_traffic_stress: int = 4
    max_rides: int = 4
    zoom: int = 9
    percentiles: List[int] = [5]
    monte_carlo_draws: int = 200

    @property
    def tool_type(self):
        return ToolType.isochrone_pt

    @property
    def geofence_table(self):
        mode = ToolType.isochrone_pt.value.replace("isochrone_", "")
        return f"basic.geofence_{mode}"

    @property
    def input_layer_types(self):
        return {"layer_project_id": input_layer_type_point}

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.isochrone_pt: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {"name": "travel_cost", "type": "number"},
                "color_scale": "ordinal",
            }
        }


class IIsochroneCar(BaseModel):
    """Model for the car isochrone"""

    starting_points: IsochroneStartingPointsMotorizedMobility = Field(
        ...,
        title="Starting Points",
        description="The starting points of the isochrone.",
    )
    routing_type: IsochroneRoutingTypeCar = Field(
        ...,
        title="Routing Type",
        description="The routing type of the isochrone.",
    )
    travel_cost: IsochroneTravelTimeCostMotorizedMobility = Field(
        ...,
        title="Travel Cost",
        description="The travel cost of the isochrone.",
    )

    @property
    def tool_type(self):
        return ToolType.isochrone_car

    @property
    def geofence_table(self):
        mode = ToolType.isochrone_car.value.value.replace("isochrone_", "")
        return f"basic.geofence_{mode}"

    @property
    def input_layer_types(self):
        return {"layer_project_id": input_layer_type_point}

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.isochrone_car: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {"name": "travel_cost", "type": "number"},
                "color_scale": "ordinal",
            }
        }


class IsochroneNearbyStationAccess(BaseModel):
    """Model for the nearby stations (active mobility) isochrone"""

    starting_points: IsochroneStartingPointsActiveMobility = Field(
        ...,
        title="Starting Points",
        description="The starting points of the isochrone.",
    )
    routing_type: IsochroneRoutingModeActiveMobility = Field(
        ...,
        title="Routing Type",
        description="The routing type of the isochrone.",
    )
    travel_cost: (
        IsochroneTravelTimeCostActiveMobility
        | IsochroneTravelDistanceCostActiveMobility
    ) = Field(
        ...,
        title="Travel Cost",
        description="The travel cost of the isochrone.",
    )
    scenario_id: UUID | None = Field(
        None,
        title="Scenario ID",
        description="The ID of the scenario that is used for the routing.",
    )
    isochrone_type: IsochroneTypeActiveMobility = Field(
        ...,
        title="Return Type",
        description="The return type of the isochrone.",
    )
    polygon_difference: bool | None = Field(
        None,
        title="Polygon Difference",
        description="If true, the polygons returned will be the geometrical difference of two following calculations.",
    )

    @property
    def tool_type(self):
        return ToolType.isochrone_nearby_station_access

    @property
    def geofence_table(self):
        mode = ToolType.isochrone_pt.value.replace("isochrone_", "")
        return f"basic.geofence_{mode}"

    @property
    def input_layer_types(self):
        return {"layer_project_id": input_layer_type_point}

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.nearby_station_access: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {"name": "travel_cost", "type": "number"},
                "color_scale": "quantile",
                "breaks": self.travel_cost.steps,
            }
        }


request_examples_isochrone_active_mobility = {
    "isochrone_active_mobility": {
        "single_point_walking": {
            "summary": "Single point isochrone walking",
            "value": {
                "starting_points": {"latitude": [52.5200], "longitude": [13.4050]},
                "routing_type": "walking",
                "travel_cost": {
                    "max_traveltime": 30,
                    "steps": 10,
                    "speed": 5,
                },
                "isochrone_type": "polygon",
                "polygon_difference": True,
            },
        },
        "single_point_cycling": {
            "summary": "Single point isochrone cycling",
            "value": {
                "starting_points": {"latitude": [52.5200], "longitude": [13.4050]},
                "routing_type": "bicycle",
                "travel_cost": {
                    "max_traveltime": 15,
                    "steps": 5,
                    "speed": 15,
                },
                "isochrone_type": "polygon",
                "polygon_difference": True,
            },
        },
        "single_point_walking_scenario": {
            "summary": "Single point isochrone walking",
            "value": {
                "starting_points": {"latitude": [52.5200], "longitude": [13.4050]},
                "routing_type": "walking",
                "travel_cost": {
                    "max_traveltime": 30,
                    "steps": 10,
                    "speed": 5,
                },
                "scenario_id": "e7dcaae4-1750-49b7-89a5-9510bf2761ad",
                "isochrone_type": "polygon",
                "polygon_difference": True,
            },
        },
        "multi_point_walking": {
            "summary": "Multi point isochrone walking",
            "value": {
                "starting_points": {
                    "latitude": [
                        52.5200,
                        52.5210,
                        52.5220,
                        52.5230,
                        52.5240,
                        52.5250,
                        52.5260,
                        52.5270,
                        52.5280,
                        52.5290,
                    ],
                    "longitude": [
                        13.4050,
                        13.4060,
                        13.4070,
                        13.4080,
                        13.4090,
                        13.4100,
                        13.4110,
                        13.4120,
                        13.4130,
                        13.4140,
                    ],
                },
                "routing_type": "walking",
                "travel_cost": {
                    "max_traveltime": 30,
                    "steps": 10,
                    "speed": 5,
                },
            },
        },
        "multi_point_cycling": {
            "summary": "Multi point isochrone cycling",
            "value": {
                "starting_points": {
                    "latitude": [
                        52.5200,
                        52.5210,
                        52.5220,
                        52.5230,
                        52.5240,
                        52.5250,
                        52.5260,
                        52.5270,
                        52.5280,
                        52.5290,
                    ],
                    "longitude": [
                        13.4050,
                        13.4060,
                        13.4070,
                        13.4080,
                        13.4090,
                        13.4100,
                        13.4110,
                        13.4120,
                        13.4130,
                        13.4140,
                    ],
                },
                "routing_type": "bicycle",
                "travel_cost": {
                    "max_traveltime": 15,
                    "steps": 5,
                    "speed": 15,
                },
            },
        },
        "layer_based_walking": {
            "summary": "Layer based isochrone walking",
            "value": {
                "starting_points": {
                    "layer_id": "39e16c27-2b03-498e-8ccc-68e798c64b8d"  # Sample UUID for the layer
                },
                "routing_type": "walking",
                "travel_cost": {
                    "max_traveltime": 30,
                    "steps": 10,
                    "speed": 5,
                },
            },
        },
    }
}


request_examples_isochrone_pt = {
    # 1. Isochrone for public transport with all modes
    "all_modes_pt": {
        "summary": "Isochrone using all PT modes",
        "value": {
            "starting_points": {"latitude": [52.5200], "longitude": [13.4050]},
            "routing_type": {
                "mode": [
                    "bus",
                    "tram",
                    "rail",
                    "subway",
                ],
                "egress_mode": "walk",
                "access_mode": "walk",
            },
            "travel_cost": {"max_traveltime": 40, "steps": 10},
            "time_window": {"weekday": "weekday", "from_time": 25200, "to_time": 32400},
            "isochrone_type": "polygon",
        },
    },
    # 2. Isochrone for public transport excluding bus mode
    "exclude_bus_mode_pt": {
        "summary": "Isochrone excluding bus mode",
        "value": {
            "starting_points": {"latitude": [52.5200], "longitude": [13.4050]},
            "routing_type": {
                "mode": [
                    "tram",
                    "rail",
                    "subway",
                ],
                "egress_mode": "walk",
                "access_mode": "walk",
            },
            "travel_cost": {"max_traveltime": 35, "steps": 5},
            "time_window": {"weekday": "weekday", "from_time": 25200, "to_time": 32400},
            "isochrone_type": "polygon",
        },
    },
}


request_examples_isochrone_car = {
    # 1. Isochrone for car
    "single_point_car": {
        "summary": "Isochrone for a single starting point using car",
        "value": {
            "starting_points": {"latitude": [52.5200], "longitude": [13.4050]},
            "routing_type": "car_peak",
            "travel_cost": {"max_traveltime": 30, "steps": 10},
        },
    },
    # 2. Multiisochrone for car
    "multi_point_car": {
        "summary": "Isochrone for multiple starting points using car",
        "value": {
            "starting_points": {
                "latitude": [52.5200, 52.5250, 52.5300],
                "longitude": [13.4050, 13.4150, 13.4250],
            },
            "routing_type": "car_peak",
            "travel_cost": {"max_traveltime": 30, "steps": 10},
        },
    },
}
