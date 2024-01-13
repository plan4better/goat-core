from enum import Enum
from typing import List

from pydantic import BaseModel, Field, validator, conlist

from src.schemas.active_mobility import IIsochroneActiveMobility
from src.schemas.motorized_mobility import IIsochroneCar, IIsochronePT, IOevGueteklasse
from src.schemas.toolbox_base import (
    ColumnStatistic,
    ColumnStatisticsOperation,
    InputLayerType,
)
from src.db.models.layer import LayerType
from src.schemas.layer import FeatureGeometryType
from src.db.models.layer import ToolType


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

    @property
    def input_layer_types(self):
        return {
            "target_layer_project_id": InputLayerType(
                layer_types=[LayerType.feature, LayerType.table],
                feature_layer_geometry_types=[
                    FeatureGeometryType.point,
                    FeatureGeometryType.polygon,
                    FeatureGeometryType.line,
                ],
            ),
            "join_layer_project_id": InputLayerType(
                layer_types=[LayerType.feature, LayerType.table],
                feature_layer_geometry_types=[
                    FeatureGeometryType.point,
                    FeatureGeometryType.polygon,
                    FeatureGeometryType.line,
                ],
            ),
        }

    @property
    def tool_type(self):
        return ToolType.join


class AreaLayerType(str, Enum):
    """Area layer type schema."""

    feature = "feature"
    h3_grid = "h3_grid"


class IAggregationBase(BaseModel):
    source_layer_project_id: int = Field(
        ...,
        title="Point Layer ID",
        description="The ID of the layer that contains the feature to be aggregated.",
    )
    area_type: AreaLayerType = Field(
        ...,
        title="Area Type",
        description="The type of the layer that contains the areas that are used to aggregate the source layer. It can be a feature layer or a H3 grid.",
    )
    aggregation_layer_project_id: int | None = Field(
        None,
        title="Area Layer ID",
        description="The ID of the layer that contains the areas that are used to aggregate the source layer.",
    )
    h3_resolution: int | None = Field(
        None,
        title="H3 Resolution",
        description="The resolution of the H3 grid that is used to aggregate the points.",
        ge=3,
        le=10,
    )
    column_statistics: ColumnStatistic = Field(
        ...,
        title="Column Statistics",
        description="The column statistics to be calculated.",
    )
    source_group_by_field: conlist(str, min_items=0, max_items=3) | None = Field(
        None,
        title="Source Group By Field",
        description="The field in the source layer that is used to group the aggregated points.",
    )

    @validator("h3_resolution", pre=True, always=True)
    def h3_grid_requires_resolution(cls, v, values):
        if values.get("area_type") == AreaLayerType.h3_grid and v is None:
            raise ValueError(
                "If area_type is h3_grid then h3_resolution cannot be null."
            )
        return v

    @validator("aggregation_layer_project_id", pre=True, always=True)
    def feature_layer_requires_aggregation_layer_project_id(cls, v, values):
        if values.get("area_type") == AreaLayerType.feature and v is None:
            raise ValueError(
                "If area_type is feature then aggregation_layer_project_id cannot be null."
            )
        return v

    @validator("h3_resolution", "aggregation_layer_project_id", pre=True, always=True)
    def no_conflicting_area_layer_and_resolution(cls, v, values, field):
        if "aggregation_layer_project_id" in values and "h3_resolution" in values:
            if (
                values["aggregation_layer_project_id"] is not None
                and values["h3_resolution"] is not None
            ):
                raise ValueError(
                    "Cannot specify both aggregation_layer_project_id and h3_resolution at the same time."
                )
        return v


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
input_layer_type_feature_all = InputLayerType(
    layer_types=[LayerType.feature],
    feature_layer_geometry_types=[
        FeatureGeometryType.point,
        FeatureGeometryType.polygon,
        FeatureGeometryType.line,
    ],
)


class IAggregationPoint(IAggregationBase):
    """Aggregation tool schema."""

    @property
    def input_layer_types(self):
        if self.area_type == AreaLayerType.feature:
            return {
                "source_layer_project_id": input_layer_type_point,
                "aggregation_layer_project_id": input_layer_type_polygon,
            }
        elif self.area_type == AreaLayerType.h3_grid:
            return {"source_layer_project_id": input_layer_type_point}

    @property
    def tool_type(self):
        return ToolType.aggregate_point


class IAggregationPolygon(IAggregationBase):
    weigthed_by_intersecting_area: bool | None = Field(
        False,
        title="Weighted By Intersection Area",
        description="If true, the aggregated values are weighted by the share of the intersection area between the source layer and the aggregation layer.",
    )

    @property
    def input_layer_types(self):
        if self.area_type == AreaLayerType.feature:
            return {
                "source_layer_project_id": input_layer_type_polygon,
                "aggregation_layer_project_id": input_layer_type_polygon,
            }
        elif self.area_type == AreaLayerType.h3_grid:
            return {"source_layer_project_id": input_layer_type_polygon}

    @property
    def tool_type(self):
        return ToolType.aggregate_polygon


class IBuffer(BaseModel):
    """Buffer tool schema."""

    source_layer_project_id: int = Field(
        ...,
        title="Source Layer Project ID",
        description="The ID of the layer project that conains the geometries that should be buffered.",
    )
    max_distance: int = Field(
        ...,
        title="Max Distance",
        description="The maximum distance in meters.",
        ge=1,
        le=5000000,
    )
    distance_step: int = Field(
        ...,
        title="Distance Step",
        description="The distance step in meters.",
    )
    polygon_union: bool | None = Field(
        None,
        title="Polygon Union",
        description="If true, the polygons returned will be the geometrical union of buffers with the same step.",
    )
    polygon_difference: bool | None = Field(
        None,
        title="Polygon Difference",
        description="If true, the polygons returned will be the geometrical difference of the current step and the predecessor steps.",
    )

    # Make sure that there is a maximum of 20 steps. It can be calculated by max_distance / distance_step
    @validator("distance_step", pre=True)
    def check_distance_step(cls, v, values):
        if 20 < values["max_distance"] / v:
            raise ValueError(
                "You can only have a maximum of 20 steps. The distance step is too small for the chosen max_distance."
            )
        return v

    # Make sure that polygon difference is only True if polygon union is True
    @validator("polygon_difference", pre=True)
    def check_polygon_difference(cls, v, values):
        if values["polygon_union"] is False and v is True:
            raise ValueError(
                "You can only have polygon difference if polygon union is True."
            )
        return v

    @property
    def input_layer_types(self):
        return {
            "source_layer_project_id": input_layer_type_feature_all
        }
    @property
    def tool_type(self):
        return ToolType.buffer


class IToolParam(BaseModel):
    data: object

    @validator("data", pre=True)
    def check_type(cls, v):
        allowed_types = (
            IJoin,
            IAggregationPoint,
            IIsochroneActiveMobility,
            IOevGueteklasse,
            IIsochroneCar,
            IIsochronePT,
        )
        if not isinstance(v, allowed_types):
            raise ValueError(f"Input type {type(v).__name__} not allowed")
        return v


request_examples_join = {
    "join_count": {
        "summary": "Join Count",
        "value": {
            "target_layer_project_id": 1,
            "target_field": "target_field_example",
            "join_layer_project_id": 2,
            "join_field": "join_field_example",
            "column_statistics": {
                "operation": ColumnStatisticsOperation.count.value,
                "field": "field_example1",
            },
        },
    },
    "join_mean": {
        "summary": "Join Mean",
        "value": {
            "target_layer_project_id": 1,
            "target_field": "target_field_example2",
            "join_layer_project_id": 2,
            "join_field": "join_field_example2",
            "column_statistics": {
                "operation": ColumnStatisticsOperation.mean.value,
                "field": "field_example2",
            },
        },
    },
}

request_examples_aggregation_point = {
    "aggregation_feature_layer": {
        "summary": "Aggregation Feature Layer",
        "value": {
            "source_layer_project_id": 1,
            "area_type": AreaLayerType.feature.value,
            "aggregation_layer_project_id": 2,
            "column_statistics": {"operation": "sum", "field": "field_example1"},
            "source_group_by_field": ["group_by_example1"],
        },
    },
    "aggregation_h3_grid": {
        "summary": "Aggregation H3 Grid",
        "value": {
            "source_layer_project_id": 1,
            "area_type": AreaLayerType.h3_grid.value,
            "h3_resolution": 6,
            "column_statistics": {"operation": "mean", "field": "field_example2"},
            "source_group_by_field": ["group_by_example2"],
        },
    },
}

request_examples_aggregation_polygon = {
    "aggregation_polygon_feature_layer": {
        "summary": "Aggregation Polygon Feature Layer",
        "value": {
            "source_layer_project_id": 1,
            "area_type": AreaLayerType.feature.value,
            "aggregation_layer_project_id": 2,
            "weigthed_by_intersecting_area": True,
            "column_statistics": {"operation": "sum", "field": "field_example1"},
            "source_group_by_field": ["group_by_example1"],
        },
    },
    "aggregation_polygon_h3_grid": {
        "summary": "Aggregation Polygon H3 Grid",
        "value": {
            "source_layer_project_id": 1,
            "area_type": AreaLayerType.h3_grid.value,
            "h3_resolution": 6,
            "weigthed_by_intersecting_area": False,
            "column_statistics": {"operation": "mean", "field": "field_example2"},
            "source_group_by_field": ["group_by_example2"],
        },
    },
}

request_example_buffer = {
    "buffer_union": {
        "summary": "Buffer union polygons",
        "value": {
            "source_layer_project_id": 1,
            "max_distance": 1000,
            "distance_step": 100,
            "polygon_union": True,
            "polygon_difference": False,
        },
    },
    "buffer_union_difference": {
        "summary": "Buffer union and difference polygons",
        "value": {
            "source_layer_project_id": 1,
            "max_distance": 1000,
            "distance_step": 100,
            "polygon_union": True,
            "polygon_difference": True,
        },
    },
}