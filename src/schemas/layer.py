from enum import Enum
from typing import List
from uuid import UUID

from pydantic import BaseModel, Field, ValidationError

from src.db.models._base_class import DateTimeBase, content_base_example
from src.db.models.layer import (
    FeatureLayerType,
    GeospatialAttributes,
    ImageryLayerDataType,
    IndicatorType,
    LayerBase,
    LayerType,
    ScenarioType,
    TileLayerDataType,
    layer_base_example,
)


class AnalysisType(str, Enum):
    """Analysis types."""

    intersects = "intersects"


# It was decided against using MIME types here because for e.g. gpkg they are commonly just generic application/octet-stream
class FileUploadType(str, Enum):
    """File upload types."""

    geojson = "geojson"
    csv = "csv"
    xlsx = "xlsx"
    gpkg = "gpkg"
    kml = "kml"
    zip = "zip"  # Commonly used for shapefiles


class MaxFileSizeType(str, Enum):
    """Max file size types in bytes."""

    geojson = 300000000
    csv = 100000000
    xlsx = 100000000
    gpkg = 300000000
    kml = 300000000
    zip = 300000000


class TableDataType(str, Enum):
    """Table data types."""

    csv = "csv"
    xlsx = "xlsx"


class SupportedOgrGeomType(Enum):
    Point = "point"
    Multi_Point = "point"
    Line_String = "line"
    Multi_Line_String = "line"
    Polygon = "polygon"
    Multi_Polygon = "polygon"


class OgrPostgresType(str, Enum):
    Integer = "integer"
    Integer64 = "bigint"
    Real = "float"
    String = "text"
    Date = "timestamp"
    Time = "timestamp"
    DateTime = "timestamp"


class OgrDriverType(str, Enum):
    """OGR driver types."""

    geojson = "GeoJSON"
    csv = "XLSX"  # Using XLSX driver for CSV files as the file is converted to XLSX to keep data types
    xlsx = "XLSX"
    gpkg = "GPKG"
    kml = "KML"
    zip = "ESRI Shapefile"


class NumberColumnsPerType(int, Enum):
    """Number of columns per type."""

    integer = 15
    bigint = 5
    float = 10
    text = 20
    timestamp = 3
    arrfloat = 3
    arrint = 3
    arrtext = 3
    jsonb = 3


class LayerReadBaseAttributes(BaseModel):
    id: UUID = Field(..., description="Content ID of the layer", alias="id")
    user_id: UUID = Field(..., description="User ID of the owner")
    type: LayerType = Field(..., description="Layer type")


class LayerCreateBaseAttributes(BaseModel):
    type: LayerType = Field(..., description="Layer type")


################################################################################
# LayerBase
################################################################################


class FeatureLayerBase(LayerBase, GeospatialAttributes):
    """Base model for feature layers."""

    data_store_id: UUID = Field(..., description="Data store ID of the layer")
    feature_layer_type: "FeatureLayerType" = Field(
        ..., description="Feature layer type"
    )
    size: int = Field(..., description="Size of the layer in bytes")
    style: dict = Field(..., description="Style of the layer")


################################################################################
# Feature Layer DTOs
################################################################################
# Base models


class FeatureLayerCreateBase(
    LayerCreateBaseAttributes, LayerBase, GeospatialAttributes
):
    """Base model for feature layer creates."""

    feature_layer_type: "FeatureLayerType" = Field(
        ..., description="Feature layer type"
    )


class FeatureLayerReadBaseAttributes(
    LayerReadBaseAttributes, LayerBase, GeospatialAttributes
):
    """Base model for feature layer reads."""

    feature_layer_type: "FeatureLayerType" = Field(
        ..., description="Feature layer type"
    )
    size: int = Field(..., description="Size of the layer in bytes")
    style: dict = Field(..., description="Style of the layer")


class FeatureLayerUpdateBase(LayerBase, GeospatialAttributes):
    """Base model for feature layer updates."""

    style: dict | None = Field(None, description="Style ID of the layer")


feature_layer_update_base_example = {
    "style": [
        "match",
        ["get", "category"],
        ["forest"],
        "hsl(137, 37%, 30%)",
        ["park"],
        "hsl(135, 100%, 100%)",
        "#000000",
    ],
    "size": 1000,
}


class LayerProjectAttributesBase(BaseModel):
    """Base model for the additional attributes of layers in a project."""

    active: bool = Field(
        ...,
        description="Layer is active or not in the project",
    )
    data_source: str = Field(
        ...,
        description="Data source name",
    )
    data_reference_year: int | None


# Feature Layer Standard
class IFeatureLayerStandardCreate(FeatureLayerCreateBase):
    pass


class IFeatureLayerStandardRead(FeatureLayerReadBaseAttributes, DateTimeBase):
    pass


class IFeatureLayerStandardUpdate(FeatureLayerUpdateBase):
    pass


# Feature Layer Indicator
class FeatureLayerIndicatorAttributesBase(BaseModel):
    """Base model for additional attributes feature layer indicator."""

    indicator_type: IndicatorType = Field(..., description="Indicator type")


feature_layer_indicator_attributes_example = {
    "indicator_type": "isochrone",
}


class IFeatureLayerIndicatorCreate(
    FeatureLayerCreateBase, FeatureLayerIndicatorAttributesBase
):
    """Model to create feature layer indicator."""

    pass


class IFeatureLayerIndicatorRead(
    FeatureLayerReadBaseAttributes, FeatureLayerIndicatorAttributesBase, DateTimeBase
):
    """Model to read a feature layer indicator."""

    pass


class IFeatureLayerIndicatorUpdate(FeatureLayerUpdateBase):
    """Model to update a feature layer indicator."""

    pass


# Feature Layer Scenario
class FeatureLayerScenarioAttributesBase(BaseModel):
    """Base model for additional attributes feature layer scenario."""

    scenario_id: UUID = Field(..., description="Scenario ID of the scenario layer.")
    scenario_type: ScenarioType = Field(..., description="Scenario type")


feature_layer_scenario_attributes_example = {
    "scenario_id": "60a42459-11c8-4cd7-91f1-091d0e05a4a3",
    "scenario_type": "point",
}


class IFeatureLayerScenarioCreate(
    FeatureLayerCreateBase, FeatureLayerScenarioAttributesBase
):
    """Model to create feature layer scenario."""

    pass


class IFeatureLayerScenarioRead(
    FeatureLayerReadBaseAttributes, FeatureLayerScenarioAttributesBase, DateTimeBase
):
    """Model to read a feature layer scenario."""

    pass


class IFeatureLayerScenarioUpdate(FeatureLayerUpdateBase):
    """Model to update a feature layer scenario."""

    pass


################################################################################
# Imagery Layer DTOs
################################################################################


class ImageryLayerAttributesBase(BaseModel):
    """Base model for additional attributes imagery layer."""

    url: str = Field(..., description="Layer URL")
    data_type: ImageryLayerDataType = Field(..., description="Content data type")
    legend_urls: List[str] = Field(..., description="Layer legend URLs")


imagery_layer_attributes_example = {
    "url": "https://geodata.nationaalgeoregister.nl/luchtfoto/rgb/wms?request=GetCapabilities&service=WMS",
    "data_type": "wms",
    "legend_urls": [
        "https://geodata.nationaalgeoregister.nl/luchtfoto/rgb/wms?request=GetLegendGraphic&service=WMS&layer=Actueel_ortho25&format=image/png&width=20&height=20",
        "https://geodata.nationaalgeoregister.nl/luchtfoto/rgb/wms?request=GetLegendGraphic&service=WMS&layer=Actueel_ortho25&format=image/png&width=20&height=20",
    ],
}


class IImageryLayerCreate(
    LayerCreateBaseAttributes,
    LayerBase,
    GeospatialAttributes,
    ImageryLayerAttributesBase,
):
    """Model to create a imagery layer."""

    pass


class IImageryLayerRead(
    LayerReadBaseAttributes,
    LayerBase,
    GeospatialAttributes,
    ImageryLayerAttributesBase,
    DateTimeBase,
):
    """Model to read a imagery layer."""

    pass


class IImageryLayerUpdate(LayerBase, GeospatialAttributes):
    """Model to"""

    url: str | None = Field(None, description="Layer URL")
    legend_urls: List[str] | None = Field(None, description="Layer legend URLs")


imagery_layer_update_base_example = {
    "url": "https://geodata.nationaalgeoregister.nl/luchtfoto/rgb/wms?request=GetCapabilities&service=WMS",
    "legend_urls": [
        "https://geodata.nationaalgeoregister.nl/luchtfoto/rgb/wms?request=GetLegendGraphic&service=WMS&layer=Actueel_ortho25&format=image/png&width=20&height=20",
        "https://geodata.nationaalgeoregister.nl/luchtfoto/rgb/wms?request=GetLegendGraphic&service=WMS&layer=Actueel_ortho25&format=image/png&width=20&height=20",
    ],
}

################################################################################
# Tile Layer DTOs
################################################################################


class TileLayerAttributesBase(BaseModel):
    """Base model for additional attributes tile layer."""

    url: str = Field(..., description="Layer URL")
    data_type: TileLayerDataType = Field(..., description="Content data type")


tile_layer_attributes_example = {
    "url": "https://goat.plan4better.de/api/v1/layers/tiles/accidents_pedestrians/12/2179/1420.pbf",
    "data_type": "mvt",
}


class ITileLayerCreate(
    LayerCreateBaseAttributes, LayerBase, GeospatialAttributes, TileLayerAttributesBase
):
    """Model to create a tile layer."""

    pass


class ITileLayerRead(
    LayerReadBaseAttributes,
    LayerBase,
    GeospatialAttributes,
    TileLayerAttributesBase,
    DateTimeBase,
):
    """Model to read a tile layer."""

    pass


class ITileLayerUpdate(LayerBase, GeospatialAttributes):
    """Model to update a tile layer."""

    url: str | None = Field(None, description="Layer URL")


tile_layer_update_example = {
    "url": "https://geodata.nationaalgeoregister.nl/luchtfoto/rgb/wmts?request=GetCapabilities&service=WMTS"
}

################################################################################
# Table Layer DTOs
################################################################################


class ITableLayerCreate(LayerCreateBaseAttributes, LayerBase):
    pass


class ITableLayerRead(LayerBase, LayerReadBaseAttributes, DateTimeBase):
    pass


class ITableLayerUpdate(LayerBase):
    pass


def get_layer_class(class_type: str, **kwargs):
    layer_creator_class = {
        "table": ITableLayerCreate,
        "tile_layer": ITileLayerCreate,
        "imagery_layer": IImageryLayerCreate,
        "feature_layer": {
            "standard": IFeatureLayerStandardCreate,
            "indicator": IFeatureLayerIndicatorCreate,
            "scenario": IFeatureLayerScenarioCreate,
        },
    }
    try:
        layer_type = kwargs["type"]
    except KeyError:
        raise ValidationError("Layer type is required")

    layer_class = layer_creator_class[layer_type]
    if layer_type == "feature_layer":
        try:
            feature_layer_type = kwargs["feature_layer_type"]
        except KeyError:
            raise ValidationError("Feature layer type is required")

        layer_class = layer_class[feature_layer_type]

    layer_class_name = layer_class.__name__
    if class_type == "read":
        layer_class_name = layer_class_name.replace("Create", "Read")
    elif class_type == "update":
        layer_class_name = layer_class_name.replace("Create", "Update")
    elif class_type == "create":
        pass
    else:
        raise ValueError(f"Layer class type ({class_type}) is invalid")

    return globals()[layer_class_name]


class ILayerCreate(BaseModel):
    def __new__(cls, *args, **kwargs):
        layer_create_class = get_layer_class("create", **kwargs)
        return layer_create_class(**kwargs)


class ILayerRead(BaseModel):
    def __new__(cls, *args, **kwargs):
        layer_read_class = get_layer_class("read", **kwargs)
        return layer_read_class(**kwargs)


class ILayerUpdate(BaseModel):
    def __new__(cls, *args, **kwargs):
        layer_update_class = get_layer_class("update", **kwargs)
        return layer_update_class(**kwargs)


request_examples = {
    "get": {
        "ids": [
            "e7dcaae4-1750-49b7-89a5-9510bf2761ad",
            "e7dcaae4-1750-49b7-89a5-9510bf2761ad",
        ],
    },
    "create": {
        "table_layer": {
            "summary": "Table Layer",
            "value": {
                **content_base_example,
                **layer_base_example,
                "type": "table",
            },
        },
        "feature_layer_standard": {
            "summary": "Layer Standard",
            "value": {
                **content_base_example,
                **layer_base_example,
                "type": "feature_layer",
                "feature_layer_type": "standard",
            },
        },
        "imagery_layer": {
            "summary": "Imagery Layer",
            "value": {
                **content_base_example,
                **layer_base_example,
                **imagery_layer_attributes_example,
                "type": "imagery_layer",
                "extent": "MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))",
            },
        },
        "tile_layer": {
            "summary": "Tile Layer",
            "value": {
                **content_base_example,
                **layer_base_example,
                **tile_layer_attributes_example,
                "type": "tile_layer",
                "extent": "MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))",
            },
        },
    },
    "update": {
        "table_layer": {
            "summary": "Table Layer",
            "value": {
                **content_base_example,
                **layer_base_example,
            },
        },
    },
}
