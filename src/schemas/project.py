from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field, validator

from src.db.models._base_class import DateTimeBase
from src.db.models.layer import ContentBaseAttributes
from src.schemas.layer import (
    CQLQuery,
    IExternalImageryRead,
    IExternalVectorTileRead,
    IFeatureIndicatorRead,
    IFeatureScenarioRead,
    IFeatureStandardRead,
    ITableRead,
    FeatureLayerParameter,
    ExternalImageryParameter,
)
from src.utils import optional


################################################################################
# Project DTOs
################################################################################
class ProjectContentType(str, Enum):
    layer = "layer"
    report = "report"


class InitialViewState(BaseModel):
    latitude: float = Field(..., description="Latitude", ge=-90, le=90)
    longitude: float = Field(..., description="Longitude", ge=-180, le=180)
    zoom: int = Field(..., description="Zoom level", ge=0, le=20)
    min_zoom: int = Field(..., description="Minimum zoom level", ge=0, le=20)
    max_zoom: int = Field(..., description="Maximum zoom level", ge=0, le=20)
    bearing: int = Field(..., description="Bearing", ge=0, le=360)
    pitch: int = Field(..., description="Pitch", ge=0, le=60)

    @validator("max_zoom")
    def check_max_zoom(cls, max_zoom, values):
        min_zoom = values.get("min_zoom")
        if min_zoom is not None and max_zoom < min_zoom:
            raise ValueError("max_zoom should be greater than or equal to min_zoom")
        return max_zoom

    @validator("min_zoom")
    def check_min_zoom(cls, min_zoom, values):
        max_zoom = values.get("max_zoom")
        if max_zoom is not None and min_zoom > max_zoom:
            raise ValueError("min_zoom should be less than or equal to max_zoom")
        return min_zoom


initial_view_state_example = {
    "latitude": 48.1502132,
    "longitude": 11.5696284,
    "zoom": 12,
    "min_zoom": 0,
    "max_zoom": 20,
    "bearing": 0,
    "pitch": 0,
}


class IProjectCreate(ContentBaseAttributes):
    initial_view_state: InitialViewState = Field(
        ..., description="Initial view state of the project"
    )


class IProjectRead(ContentBaseAttributes, DateTimeBase):
    id: UUID = Field(..., description="Project ID")


@optional
class IProjectBaseUpdate(ContentBaseAttributes):
    pass


# Define layers within project
class Visibility(str, Enum):
    visible = "visible"
    none = "none"


class ProjectBaseParameter(BaseModel):
    z_index: int = Field(..., description="Z index of the layer")
    minzoom: int = Field(2, description="Minimum zoom level", ge=0, le=22)
    maxzoom: int = Field(20, description="Maximum zoom level", ge=0, le=22)

    @validator("maxzoom")
    def check_max_zoom(cls, max_zoom, values):
        min_zoom = values.get("minzoom")
        if min_zoom is not None and max_zoom < min_zoom:
            raise ValueError("max_zoom should be greater than or equal to min_zoom")
        return max_zoom

    @validator("minzoom")
    def check_min_zoom(cls, min_zoom, values):
        max_zoom = values.get("maxzoom")
        if max_zoom is not None and min_zoom > max_zoom:
            raise ValueError("min_zoom should be less than or equal to max_zoom")
        return min_zoom

class LayerProjectIds(BaseModel):
    id: int = Field(..., description="Layer Project ID")
    layer_id: UUID = Field(..., description="Layer ID")


class FeatureLayerProjectParameter(FeatureLayerParameter, ProjectBaseParameter):
    """Model for external imagery layer parameters."""

    pass


class IFeatureBaseProject(CQLQuery):
    name: str = Field(..., description="Layer name")
    group: str | None = Field(None, description="Layer group name")
    parameter: FeatureLayerProjectParameter = Field(
        ...,
        description="Layer parameter",
    )


class IFeatureBaseProjectRead(IFeatureBaseProject):
    total_count: int = Field(..., description="Total count of features in the layer")
    filtered_count: int | None = Field(
        None, description="Filtered count of features in the layer"
    )


class IFeatureStandardProjectRead(LayerProjectIds, IFeatureStandardRead, IFeatureBaseProjectRead):
    pass


class IFeatureIndicatorProjectRead(LayerProjectIds, IFeatureIndicatorRead, IFeatureBaseProjectRead):
    pass


class IFeatureScenarioProjectRead(LayerProjectIds, IFeatureScenarioRead, IFeatureBaseProjectRead):
    pass


@optional
class IFeatureStandardProjectUpdate(IFeatureBaseProject):
    pass


@optional
class IFeatureIndicatorProjectUpdate(IFeatureBaseProject):
    pass


@optional
class IFeatureScenarioProjectUpdate(IFeatureBaseProject):
    pass


class ITableProjectRead(LayerProjectIds, ITableRead, CQLQuery):

    group: str = Field(None, description="Layer group name")
    total_count: int = Field(..., description="Total count of features in the layer")
    filtered_count: int | None = Field(
        None, description="Filtered count of features in the layer"
    )

@optional
class ITableProjectUpdate(CQLQuery):
    name: str | None = Field(None, description="Layer name")
    group: str | None = Field(None, description="Layer group name")


class ExternalVectorTileProjectParameter(
    FeatureLayerProjectParameter, ProjectBaseParameter
):
    """Model for external vector tile layer parameters."""

    pass


class IExternalVectorTileProjectRead(LayerProjectIds, IExternalVectorTileRead):
    group: str = Field(None, description="Layer group name")
    parameter: ExternalVectorTileProjectParameter = Field(
        ...,
        description="Layer parameter",
    )

@optional
class IExternalVectorTileProjectUpdate(BaseModel):
    name: str | None = Field(None, description="Layer name")
    group: str | None = Field(None, description="Layer group name")
    parameter: ExternalVectorTileProjectParameter | None = Field(
        None,
        description="Layer parameter",
    )


class ExternalImageryProjectParameter(ExternalImageryParameter, ProjectBaseParameter):
    """Model for external imagery layer parameters."""

    pass


class IExternalImageryProjectRead(LayerProjectIds, IExternalImageryRead):
    group: str = Field(None, description="Layer group name")
    parameter: ExternalImageryProjectParameter = Field(
        ...,
        description="Layer parameter",
    )

@optional
class IExternalImageryProjectUpdate(BaseModel):
    name: str | None = Field(None, description="Layer name")
    group: str | None = Field(None, description="Layer group name")
    parameter: ExternalImageryProjectParameter | None = Field(
        None,
        description="Layer parameter",
    )


layer_type_mapping_read = {
    "feature_standard": IFeatureStandardProjectRead,
    "feature_indicator": IFeatureIndicatorProjectRead,
    "feature_scenario": IFeatureScenarioProjectRead,
    "table": ITableProjectRead,
    "external_vector_tile": IExternalVectorTileProjectRead,
    "external_imagery": IExternalImageryProjectRead,
}

layer_type_mapping_update = {
    "feature_standard": IFeatureStandardProjectUpdate,
    "feature_indicator": IFeatureIndicatorProjectUpdate,
    "feature_scenario": IFeatureScenarioProjectUpdate,
    "table": ITableProjectUpdate,
    "external_vector_tile": IExternalVectorTileProjectUpdate,
    "external_imagery": IExternalImageryProjectUpdate,
}

request_examples = {
    "get": {
        "ids": [
            "39e16c27-2b03-498e-8ccc-68e798c64b8d",
            "e7dcaae4-1750-49b7-89a5-9510bf2761ad",
        ],
    },
    "create": {
        "folder_id": "39e16c27-2b03-498e-8ccc-68e798c64b8d",
        "name": "Project 1",
        "description": "Project 1 description",
        "tags": ["tag1", "tag2"],
        "thumbnail_url": "https://goat-app-assets.s3.eu-central-1.amazonaws.com/logos/goat_green.png",
        "initial_view_state": initial_view_state_example,
    },
    "update": {
        "folder_id": "39e16c27-2b03-498e-8ccc-68e798c64b8d",
        "name": "Project 2",
        "description": "Project 2 description",
        "tags": ["tag1", "tag2"],
        "thumbnail_url": "https://goat-app-assets.s3.eu-central-1.amazonaws.com/logos/goat_green.png",
    },
    "initial_view_state": initial_view_state_example,
    "update_layer": {
        "feature_standard": {
            "summary": "Feature Layer Standard",
            "value": {
                "name": "Feature Layer Standard",
                "group": "Group 1",
                "query": {"op": "=", "args": [{"property": "category"}, "bus_stop"]},
                "parameter": {
                    "type": "circle",
                    "paint": {
                        "circle-radius": 5,
                        "circle-color": "#ff0000",
                    },
                    "z_index": 0,
                    "visibility": "visible",
                    "minzoom": 0,
                    "maxzoom": 22,
                },
            },
        },
        "feature_indicator": {
            "summary": "Feature Layer Indicator",
            "value": {
                "name": "Feature Layer Indicator",
                "group": "Group 1",
                "parameter": {
                    "type": "circle",
                    "paint": {
                        "circle-radius": 5,
                        "circle-color": "#ff0000",
                    },
                    "z_index": 0,
                    "visibility": "visible",
                    "minzoom": 0,
                    "maxzoom": 22,
                },
            },
        },
        "feature_scenario": {
            "summary": "Feature Layer Scenario",
            "value": {
                "name": "Feature Layer Scenario",
                "group": "Group 1",
                "style": {
                    "type": "circle",
                    "paint": {
                        "circle-radius": 5,
                        "circle-color": "#ff0000",
                    },
                    "z_index": 0,
                    "visibility": "visible",
                    "minzoom": 0,
                    "maxzoom": 22,
                },
            },
        },
        "table": {
            "summary": "Table Layer",
            "value": {
                "name": "Table Layer",
                "group": "Group 1",
            },
        },
        "external_vector_tile": {
            "summary": "VectorVectorTile Layer",
            "value": {
                "name": "VectorVectorTile Layer",
                "group": "Group 1",
            },
        },
        "external_imagery": {
            "summary": "Imagery Layer",
            "value": {
                "name": "Imagery Layer",
                "group": "Group 1",
            },
        },
    },
}
