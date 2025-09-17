from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field, conlist, validator

from src.db.models.layer import LayerType, ToolType
from src.schemas.catchment_area import (
    ICatchmentAreaActiveMobility,
    ICatchmentAreaCar,
    ICatchmentAreaPT,
)
from src.schemas.colors import ColorRangeType
from src.schemas.layer import FeatureGeometryType
from src.schemas.oev_gueteklasse import IOevGueteklasse
from src.schemas.toolbox_base import (
    ColumnStatistic,
    ColumnStatisticsOperation,
    DefaultResultLayerName,
    InputLayerType,
    input_layer_table,
    input_layer_type_feature_all,
    input_layer_type_point,
    input_layer_type_point_polygon,
    input_layer_type_polygon,
)


class JoinType(str, Enum):
    """Join type schema."""

    left = "left"
    right = "right"
    inner = "inner"


class DuplicateHandling(str, Enum):
    """How to handle duplicates in joins."""
    
    keep_all = "keep_all"
    keep_first = "keep_first"
    aggregate = "aggregate"


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
    column_statistics: ColumnStatistic = Field(
        ...,
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

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.join: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {
                    "name": self.column_statistics.operation.value,
                    "type": "number",
                },
                "color_scale": "quantile",
            }
        }


class IJoinClassical(BaseModel):
    """Classical join tool schema for left, right and inner join."""

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
    join_type: JoinType = Field(
        ...,
        title="Join Type",
        description="The type of join to perform (left, right, inner).",
    )
    spatial_join: bool = Field(
        False,
        title="Spatial Join",
        description="If true, the join will be based on the intersection of geometries rather than field values.",
    )
    handle_duplicates: DuplicateHandling = Field(
        DuplicateHandling.keep_all,
        title="Handle Duplicates",
        description="How to handle duplicate rows in the join result.",
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
        return ToolType.join_classical

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.join_classical: {
                "color_range_type": ColorRangeType.sequential,
                "color_scale": "quantile",
            }
        }


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
    scenario_id: UUID | None = Field(
        None,
        title="Scenario ID",
        description="The ID of the scenario that is to be applied on the input layer or base network.",
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

    @validator("column_statistics", pre=True, always=True)
    def check_column_statistics(cls, v, values):
        if v.get("operation") == ColumnStatisticsOperation.count:
            if v.get("field") is not None:
                raise ValueError("Field is not allowed for count operation.")
        else:
            if v.get("field") is None:
                raise ValueError("Field is required for all operations except count.")
        return v


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

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.aggregate_point: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {
                    "name": self.column_statistics.operation.value,
                    "type": "number",
                },
                "color_scale": "quantile",
            }
        }


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

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.aggregate_polygon: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {
                    "name": self.column_statistics.operation.value,
                    "type": "number",
                },
                "color_scale": "quantile",
            }
        }


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
        description="The number of steps.",
        ge=1,
        le=20,
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
    scenario_id: UUID | None = Field(
        None,
        title="Scenario ID",
        description="The ID of the scenario that is to be applied on the input layer or base network.",
    )

    # Make sure that the number of steps is smaller then then max distance
    @validator("distance_step", pre=True, always=True)
    def distance_step_smaller_than_max_distance(cls, v, values):
        if v > values["max_distance"]:
            raise ValueError("The distance step must be smaller than the max distance.")
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
        return {"source_layer_project_id": input_layer_type_feature_all}

    @property
    def tool_type(self):
        return ToolType.buffer

    @property
    def properties_base(self):
        breaks = (
            self.max_distance / self.distance_step
            if self.max_distance / self.distance_step < 7
            else 7
        )
        return {
            DefaultResultLayerName.buffer: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {"name": "radius_size", "type": "number"},
                "color_scale": "quantile",
                "breaks": breaks,
            }
        }


class IOriginDestination(BaseModel):
    """Origin Destination tool schema."""

    geometry_layer_project_id: int = Field(
        ...,
        title="Geometry layer Project ID",
        description="The ID of the layer project that conains the origins and destinations geometries.",
    )
    origin_destination_matrix_layer_project_id: int = Field(
        ...,
        title="Origins Destinations Matrix Layer Project ID",
        description="The ID of the layer project that conains the origins and destinations matrix.",
    )
    unique_id_column: str = Field(
        ...,
        title="Unique ID Column",
        description="The column that contains the unique IDs in geometry layer.",
    )
    origin_column: str = Field(
        ...,
        title="Origin Column",
        description="The column that contains the origins in the origin destination matrix.",
    )
    destination_column: str = Field(
        ...,
        title="Destination Column",
        description="The column that contains the destinations in the origin destination matrix.",
    )
    weight_column: str = Field(
        ...,
        title="Weight Column",
        description="The column that contains the weights in the origin destination matrix.",
    )

    @property
    def input_layer_types(self):
        return {
            "geometry_layer_project_id": input_layer_type_point_polygon,
            "origin_destination_matrix_layer_project_id": input_layer_table,
        }

    @property
    def tool_type(self):
        return ToolType.origin_destination

    @property
    def properties_base(self):
        return {
            DefaultResultLayerName.origin_destination_point: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {"name": self.weight_column, "type": "number"},
                "color_scale": "quantile",
            },
            DefaultResultLayerName.origin_destination_relation: {
                "color_range_type": ColorRangeType.sequential,
                "color_field": {"name": self.weight_column, "type": "number"},
                "color_scale": "quantile",
            },
        }


class IToolParam(BaseModel):
    data: object

    @validator("data", pre=True)
    def check_type(cls, v):
        allowed_types = (
            IJoin,
            IJoinClassical,
            IAggregationPoint,
            ICatchmentAreaActiveMobility,
            IOevGueteklasse,
            ICatchmentAreaCar,
            ICatchmentAreaPT,
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
    "join_sum": {
        "summary": "Join Sum",
        "value": {
            "target_layer_project_id": 1,
            "target_field": "target_field_example2",
            "join_layer_project_id": 2,
            "join_field": "join_field_example2",
            "column_statistics": {
                "operation": ColumnStatisticsOperation.sum.value,
                "field": "field_example2",
            },
        },
    },
}

request_examples_join_classical = {
    "left_join": {
        "summary": "Left Join",
        "value": {
            "target_layer_project_id": 1,
            "target_field": "target_field_example",
            "join_layer_project_id": 2,
            "join_field": "join_field_example",
            "join_type": JoinType.left.value,
            "handle_duplicates": DuplicateHandling.keep_all.value,
        },
    },
    "right_join": {
        "summary": "Right Join",
        "value": {
            "target_layer_project_id": 1,
            "target_field": "target_field_example",
            "join_layer_project_id": 2,
            "join_field": "join_field_example",
            "join_type": JoinType.right.value,
            "handle_duplicates": DuplicateHandling.keep_first.value,
        },
    },
    "inner_join": {
        "summary": "Inner Join",
        "value": {
            "target_layer_project_id": 1,
            "target_field": "target_field_example",
            "join_layer_project_id": 2,
            "join_field": "join_field_example",
            "join_type": JoinType.inner.value,
            "handle_duplicates": DuplicateHandling.aggregate.value,
        },
    },
    "spatial_join": {
        "summary": "Spatial Join",
        "value": {
            "target_layer_project_id": 1,
            "target_field": "target_field_example",
            "join_layer_project_id": 2,
            "join_field": "join_field_example",
            "join_type": JoinType.left.value,
            "spatial_join": True,
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
            "column_statistics": {"operation": "sum", "field": "field_example2"},
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
            "column_statistics": {"operation": "sum", "field": "field_example2"},
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
