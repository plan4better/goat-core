from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field
from src.schemas.layer import ToolType
from src.schemas.toolbox_base import (
    IsochroneStartingPointsBase,
    PTSupportedDay,
    input_layer_type_polygon,
    input_layer_type_point,
    check_starting_points,
)
from src.schemas.active_mobility import RoutingActiveMobilityType
from src.schemas.colors import ColorRangeType


class IsochroneStartingPointsMotorizedMobility(IsochroneStartingPointsBase):
    """Model for the active mobility isochrone starting points."""

    # Check that the starting points for motorized mobility is 1
    check_starting_points = check_starting_points(1)


class RoutingPTMode(str, Enum):
    """Routing public transport mode schema."""

    bus = "bus"
    tram = "tram"
    rail = "rail"
    subway = "subway"
    ferry = "ferry"
    cable_car = "cable_car"
    gondola = "gondola"
    funicular = "funicular"


class RoutingPTEgressMode(str, Enum):
    walk = "walk"
    bicycle = "bicycle"


class RoutingPTAccessMode(str, Enum):
    """Routing public transport access mode schema."""

    walk = "walk"
    bicycle = "bicycle"
    car = "car"


class RoutingPTType(BaseModel):
    """Routing public transport type schema."""

    mode: List[RoutingPTMode] = Field(
        ...,
        title="Mode",
        description="The mode of the public transport.",
    )
    egress_mode: RoutingPTEgressMode = Field(
        ...,
        title="Egress Mode",
        description="The egress mode of the public transport.",
    )
    access_mode: RoutingPTAccessMode = Field(
        ...,
        title="Access Mode",
        description="The access mode of the public transport.",
    )


class TravelTimeCostMotorizedMobility(BaseModel):
    """Travel time cost schema."""

    max_traveltime: int = Field(
        ...,
        title="Max Travel Time",
        description="The maximum travel time in minutes.",
        ge=1,
        le=60,
    )
    traveltime_step: int = Field(
        ...,
        title="Travel Time Step",
        description="The travel time step in minutes.",
    )

class TravelTimeCostMotorizedMobilityNew(BaseModel):
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


class PTTimeWindow(BaseModel):
    weekday: PTSupportedDay = Field(
        title="Weekday",
        description="The weekday of the isochrone. There are three options: weekday, saturday, sunday.",
    )
    from_time: int = Field(
        25200,
        gt=0,
        lt=86400,
        description="(PT) From time. Number of seconds since midnight",
    )
    to_time: int = Field(
        39600,
        gt=0,
        lt=86400,
        description="(PT) To time . Number of seconds since midnight",
    )

    @property
    def weekday_integer(self):
        mapping = {
            "weekday": 1,
            "saturday": 2,
            "sunday": 3,
        }
        return mapping[PTSupportedDay(self.weekday).value]

    @property
    def weekday_date(self):
        mapping = {
            "weekday": "2023-06-12",
            "saturday": "2023-06-17",
            "sunday": "2023-06-18",
        }
        return mapping[PTSupportedDay(self.weekday).value]


class IsochroneType(str, Enum):
    """Isochrone type schema for public transport."""

    polygon = "polygon"
    rectangular_grid = "rectangular_grid"


class IsochroneDecayFunctionType(Enum):
    LOGISTIC = "logistic"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    STEP = "step"


class IsochroneDecayFunction(BaseModel):
    type: Optional[IsochroneDecayFunctionType] = Field(
        IsochroneDecayFunctionType.LOGISTIC, description="Decay function type"
    )
    standard_deviation_minutes: Optional[int] = Field(
        12, description="Standard deviation in minutes"
    )
    width_minutes: Optional[int] = Field(10, description="Width in minutes")


class IIsochronePT(BaseModel):
    """Model for the public transport isochrone"""

    starting_points: IsochroneStartingPointsMotorizedMobility = Field(
        ...,
        title="Starting Points",
        description="The starting points of the isochrone.",
    )
    routing_type: RoutingPTType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the isochrone.",
    )
    travel_cost: TravelTimeCostMotorizedMobility = Field(
        ...,
        title="Travel Cost",
        description="The travel cost of the isochrone.",
    )
    time_window: PTTimeWindow = Field(
        ...,
        title="Time Window",
        description="The time window of the isochrone.",
    )
    isochrone_type: IsochroneType = Field(
        ...,
        title="Return Type",
        description="The return type of the isochrone.",
    )

    decay_function: IsochroneDecayFunction = Field(
        IsochroneDecayFunction(),
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
            "color_range_type": ColorRangeType.sequential,
            "color_field": {"name": "travel_cost", "type": "number"},
            "color_scale": "quantile",
            "breaks": self.travel_cost.steps,
        }



class IIsochronePTNew(BaseModel):
    """Model for the public transport isochrone"""

    starting_points: IsochroneStartingPointsMotorizedMobility = Field(
        ...,
        title="Starting Points",
        description="The starting points of the isochrone.",
    )
    routing_type: RoutingPTType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the isochrone.",
    )
    travel_cost: TravelTimeCostMotorizedMobilityNew = Field(
        ...,
        title="Travel Cost",
        description="The travel cost of the isochrone.",
    )
    time_window: PTTimeWindow = Field(
        ...,
        title="Time Window",
        description="The time window of the isochrone.",
    )
    isochrone_type: IsochroneType = Field(
        ...,
        title="Return Type",
        description="The return type of the isochrone.",
    )

    decay_function: IsochroneDecayFunction = Field(
        IsochroneDecayFunction(),
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


class RoutingCarType(str, Enum):
    """Routing car type schema."""

    car_peak = "car_peak"


class IIsochroneCar(BaseModel):
    """Model for the car isochrone"""

    starting_points: IsochroneStartingPointsMotorizedMobility = Field(
        ...,
        title="Starting Points",
        description="The starting points of the isochrone.",
    )
    routing_type: RoutingCarType = Field(
        ...,
        title="Routing Type",
        description="The routing type of the isochrone.",
    )
    travel_cost: TravelTimeCostMotorizedMobility = Field(
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
            "color_range_type": ColorRangeType.sequential,
            "color_field": {"name": "travel_cost", "type": "number"},
            "color_scale": "quantile",
            "breaks": self.travel_cost.steps,
        }


class CountLimitPerTool(int, Enum):
    oev_gueteklasse = 1000
    isochrone_active_mobility = 1000
    isochrone_motorized_mobility = 20


class AreaLimitPerTool(int, Enum):
    oev_gueteklasse = 50  # in degree


class StationConfig(BaseModel):
    groups: dict = Field(
        ...,
        title="Groups",
        description="The groups of the station config.",
    )
    time_frequency: List[int] = Field(
        ...,
        title="Time Frequency",
        description="The time frequency of the station config.",
    )
    categories: List[dict] = Field(
        ...,
        title="Categories",
        description="The categories of the station config.",
    )
    classification: dict = Field(
        ...,
        title="Classification",
        description="The classification of the station config.",
    )


class IOevGueteklasse(BaseModel):
    reference_area_layer_project_id: int = Field(
        ...,
        title="The layer project serving reference Area for the calculation.",
        description="The reference area of the ÖV-Güteklasse.",
    )
    time_window: PTTimeWindow = Field(
        ...,
        title="Time Window",
        description="The time window of the ÖV-Güteklasse.",
    )
    station_config: StationConfig = Field(
        ...,
        title="Station Config",
        description="The station config of the ÖV-Güteklasse.",
    )

    @property
    def tool_type(self):
        return ToolType.oev_gueteklasse

    @property
    def input_layer_types(self):
        return {"reference_area_layer_project_id": input_layer_type_polygon}

    @property
    def geofence_table(self):
        return "basic.geofence_pt"

    @property
    def properties_base(self):
        return {
            "color_scale": "ordinal",
            "color_range_type": ColorRangeType.sequential,
            "color_field": {"name": "pt_class", "type": "string"},
        }


class ITripCountStation(BaseModel):
    """Model for the trip count."""

    reference_area_layer_project_id: int = Field(
        ...,
        title="The layer project serving reference Area for the calculation.",
        description="The reference area for the trip count.",
    )
    time_window: PTTimeWindow = Field(
        ...,
        title="Time Window",
        description="The time window for the trip count.",
    )

    @property
    def tool_type(self):
        return ToolType.trip_count_station

    @property
    def input_layer_types(self):
        return {"reference_area_layer_project_id": input_layer_type_polygon}

    @property
    def geofence_table(self):
        return "basic.geofence_pt"

    @property
    def properties_base(self):
        return {
            "color_range_type": ColorRangeType.sequential,
            "color_field": {"name": "total", "type": "number"},
            "color_scale": "quantile",
        }


class IStartingPointNearbyStation(IsochroneStartingPointsBase):
    """Model for the starting points of the nearby station endpoint."""

    check_starting_points = check_starting_points(1)


class INearbyStationAccess(BaseModel):
    """Model for the nearby station endpoint."""

    starting_points: IStartingPointNearbyStation = Field(
        ...,
        title="Starting Points",
        description="The starting point for the nearby station calculation.",
    )
    access_mode: RoutingActiveMobilityType = Field(
        ...,
        title="Access Mode",
        description="The access mode of the active mobility.",
    )
    speed: int = Field(
        ...,
        title="Speed",
        description="The speed in km/h.",
        ge=1,
        le=25,
    )
    max_traveltime: int = Field(
        ...,
        title="Max Travel Time",
        description="The maximum travel time in minutes.",
        ge=1,
        le=15,
    )
    mode: List[RoutingPTMode] = Field(
        ...,
        title="Mode",
        description="The mode of the public transport.",
    )
    time_window: PTTimeWindow = Field(
        ...,
        title="Time Window",
        description="The time window of the isochrone.",
    )

    @property
    def tool_type(self):
        return ToolType.nearby_station_access

    @property
    def input_layer_types(self):
        return {"layer_project_id": input_layer_type_point}

    @property
    def geofence_table(self):
        return "basic.geofence_pt"


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
            "travel_cost": {"max_traveltime": 40, "traveltime_step": 10},
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
            "travel_cost": {"max_traveltime": 35, "traveltime_step": 5},
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
            "travel_cost": {"max_traveltime": 30, "traveltime_step": 10},
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
            "travel_cost": {"max_traveltime": 30, "traveltime_step": 10},
        },
    },
}

request_example_nearby_station_access = {
    "single_point_nearby_station": {
        "summary": "Nearby station for a single starting point",
        "value": {
            "starting_points": {"latitude": [52.5200], "longitude": [13.4050]},
            "access_mode": "walk",
            "speed": 5,
            "max_traveltime": 10,
            "mode": ["bus", "tram", "rail", "subway"],
            "time_window": {"weekday": "weekday", "from_time": 25200, "to_time": 32400},
        },
    }
}


public_transport_types = {
    "bus": {
        3: "Bus",
        11: "Trolleybus",
        700: "Bus Service",
        702: "Express Bus Service",
        704: "Local Bus Service",
        705: "Night Bus Service",
        710: "Sightseeing Bus",
        712: "School Bus",
        715: "Demand and Response Bus Service",
        800: "Trolleybus Service",
    },
    "tram": {0: "Tram, Streetcar, Light rail", 900: "Tram Service"},
    "metro": {
        1: "Subway, Metro",
        400: "Metro Service",
        401: "Underground Service",
        402: "Urban Railway Service",
    },
    "rail": {
        2: "Rail",
        100: "Railway Service",
        101: "High Speed Rail Service",
        102: "Long Distance Trains",
        103: "Inter Regional Rail Service",
        105: "Sleeper Rail Service",
        106: "Regional Rail Service",
        107: "Tourist Railway Service",
        109: "Suburban Railway",
        202: "National Coach Service",
        403: "All Urban Railway Services",
    },
    "other": {
        4: "Ferry",
        6: "Aerial lift",
        7: "Funicular",
        1000: "Water Transport Service",
        1300: "Aerial Lift Service",
        1400: "Funicular Service",
        1500: "Taxi Service",
        1700: "Gondola, Suspended cable car",
    },
}


# Check for extended route_type defintion: https://developers.google.com/transit/gtfs/reference/extended-route-types
station_config_example = {
    "groups": {
        "0": "B",
        "1": "A",
        "2": "A",
        "3": "C",
        "7": "B",
        "100": "A",
        "101": "A",
        "102": "A",
        "103": "A",
        "104": "A",
        "105": "A",
        "106": "A",
        "107": "A",
        "108": "A",
        "109": "A",
        "110": "A",
        "111": "A",
        "112": "A",
        "114": "A",
        "116": "A",
        "117": "A",
        "200": "C",
        "201": "C",
        "202": "C",
        "204": "C",
        "400": "A",
        "401": "A",
        "402": "A",
        "403": "A",
        "405": "A",
        "700": "C",
        "701": "C",
        "702": "C",
        "704": "C",
        "705": "C",
        "712": "C",
        "715": "C",
        "800": "C",
        "900": "B",
        "901": "B",
        "902": "B",
        "903": "B",
        "904": "B",
        "905": "B",
        "906": "B",
        "1400": "B",
    },
    "time_frequency": [0, 4, 10, 19, 39, 60, 119],
    "categories": [
        {
            "A": 1,  # i.e. types of transports in category A are in transport stop category I
            "B": 1,
            "C": 2,
        },
        {"A": 1, "B": 2, "C": 3},
        {"A": 2, "B": 3, "C": 4},
        {"A": 3, "B": 4, "C": 5},
        {"A": 4, "B": 5, "C": 6},
        {"A": 5, "B": 6, "C": 7},
    ],
    "classification": {
        "1": {300: "1", 500: "1", 750: "2", 1000: "3", 1250: "4"},
        "2": {300: "1", 500: "2", 750: "3", 1000: "4", 1250: "5"},
        "3": {300: "2", 500: "3", 750: "4", 1000: "5", 1250: "6"},
        "4": {300: "3", 500: "4", 750: "5", 1000: "6", 1250: "6"},
        "5": {300: "4", 500: "5", 750: "6"},
        "6": {300: "5", 500: "6"},
        "7": {300: "6"},
    },
}

request_example_oev_gueteklasse = {
    "oev_gueteklasse_weekday": {
        "summary": "ÖV-Güteklassen Weekday",
        "value": {
            "time_window": {"weekday": "weekday", "from_time": 25200, "to_time": 32400},
            "reference_area_layer_project_id": "1",
            "station_config": station_config_example,
        },
    },
    "oev_gueteklasse_saturday": {
        "summary": "ÖV-Güteklassen Saturday",
        "value": {
            "time_window": {
                "weekday": "saturday",
                "from_time": 25200,
                "to_time": 32400,
            },
            "reference_area_layer_project_id": "1",
            "station_config": station_config_example,
        },
    },
}


oev_gueteklasse_station_config_layer_base = {
    "name": "ÖV-Güteklassen stations",
    "description": "ÖV-Güteklassen Categories on the station level",
    "type": "feature",
    "feature_layer_geometry_type": "point",
    "indicator_type": "oev_gueteklasse",
    "thumbnail_url": "https://goat-app-assets.s3.eu-central-1.amazonaws.com/logos/goat_green.png",
    "style": {},
    "data_source": "Plan4Better GmbH",
    "attribute_mapping": {
        "stop_id": "text_attr1",
        "stop_name": "text_attr2",
        "frequency": "float_attr1",
        "_class": "integer_attr1",
        "trip_cnt": "jsonb_attr1",
        "trip_ids": "jsonb_attr2",
    },
}

oev_gueteklasse_config_layer_base = {
    "name": "ÖV-Güteklassen",
    "description": "ÖV-Güteklassen with catchment areas",
    "layer_type": "feature_layer_type",
    "indicator_type": "oev_gueteklasse",
    "thumbnail_url": "https://goat-app-assets.s3.eu-central-1.amazonaws.com/logos/goat_green.png",
    "style": {},
    "data_source": "Plan4Better GmbH",
}
