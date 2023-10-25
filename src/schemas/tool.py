from enum import Enum
from typing import List
from uuid import UUID
from pydantic import BaseModel, Field, validator
from toolbox_base import ColumnStatistic, ResultTarget

class IJoin(BaseModel):
    """Join indicator schema."""

    target_layer_id: UUID = Field(
        ...,
        title="Target Layer ID",
        description="The ID of the layer the data will be joined.",
    )
    target_field: str = Field(
        ...,
        title="Target Field",
        description="The field in the target layer that is used for the join.",
    )
    join_layer_id: UUID = Field(
        ...,
        title="Join Layer ID",
        description="The ID of the layer that contains the joined data.",
    )
    join_field: str = Field(
        ...,
        title="Join Field",
        description="The field in the join layer that is used for the join.",
    )
    column_statistics: ColumnStatistic | None = Field(
        None,
        title="Column Statistics",
        description="The column statistics to be calculated.",
    )


class AreaLayerType(str, Enum):
    """Area layer type schema."""

    feature_layer = "feature_layer"
    h3_grid = "h3_grid"


class IAggregationPoint(BaseModel):
    """Aggregation indicator schema."""

    point_layer_id: UUID = Field(
        ...,
        title="Point Layer ID",
        description="The ID of the layer that contains the points to be aggregated.",
    )
    area_type: AreaLayerType = Field(
        ...,
        title="Area Type",
        description="The type of the layer that contains the areas that are used to aggregate the points. It can be a feature layer or a H3 grid.",
    )
    area_layer_id: UUID | None = Field(
        None,
        title="Area Layer ID",
        description="The ID of the layer that contains the areas that are used to aggregate the points.",
    )
    h3_resolution: int | None = Field(
        None,
        title="H3 Resolution",
        description="The resolution of the H3 grid that is used to aggregate the points.",
    )
    column_statistics: ColumnStatistic = Field(
        ...,
        title="Column Statistics",
        description="The column statistics to be calculated.",
    )
    area_group_by_field: List[str] | None = Field(
        None,
        title="Area Group By Field",
        description="The field in the area layer that is used to group the aggregated points.",
    )
    result_target: ResultTarget = Field(
        ...,
        title="Result Target",
        description="The target location of the produced layer.",
    )

    @validator("h3_resolution", pre=True, always=True)
    def h3_grid_requires_resolution(cls, v, values):
        if values.get("area_type") == AreaLayerType.h3_grid and v is None:
            raise ValueError("If area_type is h3_grid then h3_resolution cannot be null.")
        return v

    @validator("area_layer_id", pre=True, always=True)
    def feature_layer_requires_area_layer_id(cls, v, values):
        if values.get("area_type") == AreaLayerType.feature_layer and v is None:
            raise ValueError("If area_type is feature_layer then area_layer_id cannot be null.")
        return v

    @validator("h3_resolution", "area_layer_id", pre=True, always=True)
    def no_conflicting_area_layer_and_resolution(cls, v, values, field):
        if "area_layer_id" in values and "h3_resolution" in values:
            if values["area_layer_id"] is not None and values["h3_resolution"] is not None:
                raise ValueError(
                    "Cannot specify both area_layer_id and h3_resolution at the same time."
                )
        return v
