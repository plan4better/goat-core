# Standard Libraries
from enum import Enum
from typing import List
from uuid import UUID

from fastapi import BackgroundTasks, Depends, Query

# Third-party Libraries
from pydantic import BaseModel, Field, root_validator, validator
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models.layer import LayerType

# Local Packages
from src.endpoints.deps import get_db, get_user_id
from src.schemas.layer import FeatureGeometryType


class ColumnStatisticsOperation(str, Enum):
    count = "count"
    sum = "sum"
    mean = "mean"
    median = "median"
    min = "min"
    max = "max"


class ColumnStatistic(BaseModel):
    """Column statistic schema."""

    operation: ColumnStatisticsOperation
    field: str


class DefaultResultLayerName(str, Enum):
    """Default result layer name schema."""

    join = "Join"
    isochrone_starting_points = "Start - Catchment"
    isochrone_active_mobility = "Catchment Area"
    isochrone_pt = "Catchment Area"
    isochrone_car = "Catchment Area"
    oev_gueteklasse = "ÖV-Güteklassen"
    oev_gueteklasse_station = "ÖV-Güteklasse Station"
    trip_count_station = "Trip Count Station"
    nearby_station_access_starting_points = "Start - Nearby Stations"
    nearby_station_access = "Nearby Stations"
    aggregate_point = "Aggregation Point"
    aggregate_polygon = "Aggregation Polygon"
    aggregate_line = "Aggregation Line"
    buffer = "Buffer"
    origin_destination_relation = "O-D Relation"
    origin_destination_point = "O-D Point"

class MaxFeatureCnt(int, Enum):
    """Max feature count schema."""

    area_statistics = 100000
    join = 100000
    isochrone_active_mobility = 1000
    oev_gueteklasse = 10000
    aggregate_point = 1000000
    aggregate_polygon = 100000
    buffer = 10000
    trip_count_station = 10000
    origin_destination = 10000

class ToolsWithReferenceAreaCheck(str, Enum):
    """Tools with reference area check schema."""

    oev_gueteklasse = "oev_gueteklasse"


class MaxFeaturePolygonArea(int, Enum):
    """Max size reference in km2."""

    oev_gueteklasse = 500000
    aggregate_polygon = 100000
    trip_count_station = 500000


class GeofenceTable(str, Enum):
    """Geofence tool table mapping."""

    isochrone_active_mobility = "basic.geofence_active_mobility"
    isochrone_pt = "basic.geofence_pt"
    isochrone_car = "basic.geofence_car"
    oev_gueteklasse = "basic.geofence_pt"


class IsochroneType(str, Enum):
    """Isochrone type schema."""

    polygon = "polygon"
    network = "network"
    rectangular_grid = "rectangular_grid"


class IsochroneGeometryTypeMapping(str, Enum):
    """Isochrone geometry type mapping schema."""

    polygon = FeatureGeometryType.polygon.value
    network = FeatureGeometryType.line.value
    rectangular_grid = FeatureGeometryType.polygon.value


class IToolResponse(BaseModel):
    """Tool response schema."""

    job_id: UUID = Field(
        ...,
        title="Job ID",
        description="The ID of the job that is used to track the tool execution.",
    )


class IsochroneStartingPointsBase(BaseModel):
    """Base model for isochrone attributes."""

    latitude: List[float] | None = Field(
        None,
        title="Latitude",
        description="The latitude of the isochrone center.",
    )
    longitude: List[float] | None = Field(
        None,
        title="Longitude",
        description="The longitude of the isochrone center.",
    )
    layer_project_id: int | None = Field(
        None,
        title="Layer Project ID",
        description="The ID of the layer project that contains the starting points.",
    )

    @root_validator(pre=True)
    def check_either_coords_or_layer_project_id(cls, values):
        lat = values.get("latitude")
        long = values.get("longitude")
        layer_project_id = values.get("layer_project_id")

        if lat and long:
            if layer_project_id:
                raise ValueError(
                    "Either provide latitude and longitude or layer_project_id, not both."
                )
            if len(lat) != len(long):
                raise ValueError("Latitude and longitude must have the same length.")

            # Check if lat/lon are within WGS84 bounds
            for lat_val in lat:
                if lat_val < -90 or lat_val > 90:
                    raise ValueError("Latitude must be between -90 and 90.")
            for lon_val in long:
                if lon_val < -180 or lon_val > 180:
                    raise ValueError("Longitude must be between -180 and 180.")

        if not (lat and long) and not layer_project_id:
            raise ValueError(
                "Must provide either latitude and longitude or layer_project_id."
            )

        return values

def check_starting_points(max_count):
    @root_validator(pre=True, allow_reuse=True)
    def _validator(cls, values):
        lat = values.get("latitude")
        long = values.get("longitude")

        if lat and long:
            if len(lat) > max_count:
                raise ValueError(f"The maximum number of starting points is {max_count}.")
            if len(long) > max_count:
                raise ValueError(f"The maximum number of starting points is {max_count}.")
        return values
    return _validator

class PTSupportedDay(str, Enum):
    """PT supported days schema."""

    weekday = "weekday"
    saturday = "saturday"
    sunday = "sunday"


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

    @property
    def duration_minutes(self):
        return round((self.to_time - self.from_time) / 60)


class CommonToolParams:
    def __init__(
        self,
        background_tasks: BackgroundTasks,
        async_session: AsyncSession = Depends(get_db),
        user_id: UUID = Depends(get_user_id),
        project_id: str = Query(
            ...,
            title="Project ID of the project that contains the layers.",
            description="The project ID of the project that contains the layers.",
        ),
    ):
        self.background_tasks = background_tasks
        self.async_session = async_session
        self.user_id = user_id
        self.project_id = project_id


class InputLayerType(BaseModel):
    """Input layer type schema."""

    layer_types: List[LayerType] | None = Field(
        [LayerType.feature.value, LayerType.table.value],
        title="Layer Types",
        description="The layer types that are supported for the respective input layer of the tool.",
    )
    feature_layer_geometry_types: List[FeatureGeometryType] | None = Field(
        None,
        title="Feature Layer Geometry Types",
        description="The feature layer geometry types that are supported for the respective input layer of the tool.",
    )

    @validator("layer_types", each_item=True)
    def validate_layer_types(cls, layer_type):
        if layer_type not in LayerType.__members__:
            raise ValueError(f"{layer_type} is not a valid LayerType")
        return layer_type

    @root_validator(pre=True)
    def validate_feature_layer_geometry_types(cls, values):
        layer_types = values.get("layer_types")
        feature_layer_geometry_types = values.get("feature_layer_geometry_types")

        if LayerType.feature.value in layer_types:
            if feature_layer_geometry_types is None:
                raise ValueError(
                    "If layer_type is feature then feature_layer_geometry_types cannot be null."
                )
        elif LayerType.table.value in layer_types:
            if feature_layer_geometry_types is not None:
                raise ValueError(
                    "If layer_type is table then feature_layer_geometry_types must be null."
                )
        else:
            raise ValueError(
                "layer_type must be either feature or table, not both or none."
            )

        return values

input_layer_type_point = InputLayerType(
    layer_types=[LayerType.feature],
    feature_layer_geometry_types=[
        FeatureGeometryType.point,
    ],
)
input_layer_type_polygon = InputLayerType(
    layer_types=[LayerType.feature],
    feature_layer_geometry_types=[
        FeatureGeometryType.polygon,
    ],
)
input_layer_type_point_polygon = InputLayerType(
    layer_types=[LayerType.feature],
    feature_layer_geometry_types=[
        FeatureGeometryType.point,
        FeatureGeometryType.polygon,
    ],
)
input_layer_table = InputLayerType(
    layer_types=[LayerType.table],
    feature_layer_geometry_types=None,
)
input_layer_type_feature_all = InputLayerType(
    layer_types=[LayerType.feature],
    feature_layer_geometry_types=[
        FeatureGeometryType.point,
        FeatureGeometryType.polygon,
        FeatureGeometryType.line,
    ],
)
