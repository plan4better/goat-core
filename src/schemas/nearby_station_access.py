from typing import List

from pydantic import BaseModel, Field
from src.schemas.layer import ToolType
from src.schemas.toolbox_base import (
    IsochroneStartingPointsBase,
    PTTimeWindow,
    input_layer_type_point,
    check_starting_points,
)
from src.schemas.isochrone import IsochroneRoutingModeActiveMobility, IsochroneRoutingModePT


class IStartingPointNearbyStationAccess(IsochroneStartingPointsBase):
    """Model for the starting points of the nearby station endpoint."""

    check_starting_points = check_starting_points(1)


class INearbyStationAccess(BaseModel):
    """Model for the nearby station endpoint."""

    starting_points: IStartingPointNearbyStationAccess = Field(
        ...,
        title="Starting Points",
        description="The starting point for the nearby station calculation.",
    )
    access_mode: IsochroneRoutingModeActiveMobility = Field(
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
    mode: List[IsochroneRoutingModePT] = Field(
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


request_example_nearby_station_access = {
    "single_point_nearby_station": {
        "summary": "Nearby station for a single starting point",
        "value": {
            "starting_points": {"latitude": [52.5200], "longitude": [13.4050]},
            "access_mode": "walking",
            "speed": 5,
            "max_traveltime": 10,
            "mode": ["bus", "tram", "rail", "subway"],
            "time_window": {"weekday": "weekday", "from_time": 25200, "to_time": 32400},
        },
    }
}
