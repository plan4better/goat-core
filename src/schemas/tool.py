from enum import Enum
from typing import List
from uuid import UUID

from pydantic import BaseModel, Field, validator

from src.schemas.toolbox_base import (
    ColumnStatistic,
    ColumnStatisticsOperation,
    ResultTarget,
)


class IJoin(BaseModel):
    """Join tool schema."""

    target_layer_project_id: int = Field(
        ...,
        title="Target Layer Project ID",
        description="The ID of the layer project the data will be joined.",
    )
    target_field: str = Field(
        ...,
        title="Target Field",
        description="The field in the target layer that is used for the join.",
    )
    join_layer_project_id: int = Field(
        ...,
        title="Join Layer ID",
        description="The ID of the layer project that contains the joined data.",
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
    result_target: ResultTarget = Field(
        ...,
        title="Result Target",
        description="The target location of the produced layer.",
    )


class AreaLayerType(str, Enum):
    """Area layer type schema."""

    feature = "feature"
    h3_grid = "h3_grid"


class IAggregationPoint(BaseModel):
    """Aggregation tool schema."""

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
            raise ValueError(
                "If area_type is h3_grid then h3_resolution cannot be null."
            )
        return v

    @validator("area_layer_id", pre=True, always=True)
    def feature_layer_requires_area_layer_id(cls, v, values):
        if values.get("area_type") == AreaLayerType.feature and v is None:
            raise ValueError(
                "If area_type is feature then area_layer_id cannot be null."
            )
        return v

    @validator("h3_resolution", "area_layer_id", pre=True, always=True)
    def no_conflicting_area_layer_and_resolution(cls, v, values, field):
        if "area_layer_id" in values and "h3_resolution" in values:
            if (
                values["area_layer_id"] is not None
                and values["h3_resolution"] is not None
            ):
                raise ValueError(
                    "Cannot specify both area_layer_id and h3_resolution at the same time."
                )
        return v


request_examples_join = {
    "join_count": {
        "summary": "Join Count",
        "value": {
            "target_layer_id": "12345678-1234-5678-1234-567812345678",
            "target_field": "target_field_example",
            "join_layer_id": "87654321-8765-4321-8765-432187654321",
            "join_field": "join_field_example",
            "column_statistics": {
                "operation": ColumnStatisticsOperation.count.value,
                "field": "field_example1",
            },
            "result_target": {
                "layer_name": "IJoin Result Layer Example",
                "folder_id": "699b6116-a8fb-457c-9954-7c9efc9f83ee",
                "project_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            },
        },
    },
    "join_mean": {
        "summary": "Join Mean",
        "value": {
            "target_layer_id": "23456789-2345-6789-2345-678923456789",
            "target_field": "target_field_example2",
            "join_layer_id": "98765432-9876-5432-9876-543298765432",
            "join_field": "join_field_example2",
            "column_statistics": {
                "operation": ColumnStatisticsOperation.mean.value,
                "field": "field_example2",
            },
            "result_target": {
                "layer_name": "IJoin Result Layer Example 2",
                "folder_id": "699b6116-a8fb-457c-9954-7c9efc9f83ee",
                "project_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            },
        },
    },
}

request_examples_aggregation = {
    "aggregation_feature_layer": {
        "summary": "Aggregation Feature Layer",
        "value": {
            "point_layer_id": "abcdef12-3456-7890-fedc-ba9876543210",
            "area_type": "feature",
            "area_layer_id": "699b6116-a8fb-457c-9954-7c9efc9f83ee",
            "column_statistics": {"operation": "sum", "field": "field_example1"},
            "area_group_by_field": ["group_by_example1"],
            "result_target": {
                "layer_name": "Aggregation Result Layer Feature Layer",
                "folder_id": "699b6116-a8fb-457c-9954-7c9efc9f83ee",
            },
        },
    },
    "aggregation_h3_grid": {
        "summary": "Aggregation H3 Grid",
        "value": {
            "point_layer_id": "fedcba98-7654-3210-0123-456789abcdef",
            "area_type": "h3_grid",
            "h3_resolution": 6,
            "column_statistics": {"operation": "mean", "field": "field_example2"},
            "area_group_by_field": ["group_by_example2"],
            "result_target": {
                "layer_name": "Aggregation Result Layer H3 Grid",
                "folder_id": "699b6116-a8fb-457c-9954-7c9efc9f83ee",
                "project_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            },
        },
    },
}

IAggregationPoint(**request_examples_aggregation["aggregation_h3_grid"]["value"])
