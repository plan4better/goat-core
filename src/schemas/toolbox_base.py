# Standard Libraries
from enum import Enum
from typing import List
from uuid import UUID

# Third-party Libraries
from pydantic import BaseModel, Field, root_validator

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


class MaxFeatureCnt(int, Enum):
    """Max feature count schema."""

    area_statistics = 100000
    join = 1000000


class ResultTarget(BaseModel):
    """Define the target location of the produced layer."""

    layer_name: str = Field(
        ...,
        title="Layer Name",
        description="The name of the layer.",
    )
    folder_id: UUID | None = Field(
        None,
        title="Folder ID",
        description="The ID of the folder where the layer will be created.",
    )

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
    layer_id: UUID | None = Field(
        None,
        title="Layer ID",
        description="The ID of the layer that contains the starting points.",
    )

    @root_validator(pre=True)
    def check_either_coords_or_layer_id(cls, values):
        lat = values.get("latitude")
        long = values.get("longitude")
        layer_id = values.get("layer_id")

        if lat and long:
            if layer_id:
                raise ValueError("Either provide latitude and longitude or layer_id, not both.")
            if len(lat) != len(long):
                raise ValueError("Latitude and longitude must have the same length.")

            # Check if lat/lon are within WGS84 bounds
            for lat_val in lat:
                if lat_val < -90 or lat_val > 90:
                    raise ValueError("Latitude must be between -90 and 90.")
            for lon_val in long:
                if lon_val < -180 or lon_val > 180:
                    raise ValueError("Longitude must be between -180 and 180.")

        if not (lat and long) and not layer_id:
            raise ValueError("Must provide either latitude and longitude or layer_id.")

        return values


class PTSupportedDay(str, Enum):
    """PT supported days schema."""

    weekday = "weekday"
    saturday = "saturday"
    sunday = "sunday"