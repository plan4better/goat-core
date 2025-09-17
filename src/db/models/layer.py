from enum import Enum
from typing import TYPE_CHECKING, List, Optional, Union
from uuid import UUID

import pycountry
from geoalchemy2 import Geometry, WKBElement
from geoalchemy2.shape import to_shape
from pydantic import BaseModel, EmailStr, HttpUrl, validator
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    Boolean,
    Column,
    Field,
    ForeignKey,
    Integer,
    Relationship,
    SQLModel,
    Text,
    UniqueConstraint,
)

from src.core.config import settings
from src.db.models._base_class import ContentBaseAttributes, DateTimeBase

if TYPE_CHECKING:
    from src.db.models.folder import Folder

    from ._link_model import LayerOrganizationLink, LayerProjectLink, LayerTeamLink
    from .data_store import DataStore


class ToolType(str, Enum):
    """Indicator types."""

    catchment_area_active_mobility = "catchment_area_active_mobility"
    catchment_area_pt = "catchment_area_pt"
    catchment_area_car = "catchment_area_car"
    catchment_area_nearby_station_access = "catchment_area_nearby_station_access"
    oev_gueteklasse = "oev_gueteklasse"
    trip_count_station = "trip_count_station"
    join = "join"
    join_classical = "join_classical"
    aggregate_point = "aggregate_point"
    aggregate_polygon = "aggregate_polygon"
    aggregate_line = "aggregate_line"
    intersect = "intersect"
    buffer = "buffer"
    origin_destination = "origin_destination"
    nearby_station_access = "nearby_station_access"
    heatmap_gravity_active_mobility = "heatmap_gravity_active_mobility"
    heatmap_gravity_motorized_mobility = "heatmap_gravity_motorized_mobility"
    heatmap_closest_average_active_mobility = "heatmap_closest_average_active_mobility"
    heatmap_closest_average_motorized_mobility = (
        "heatmap_closest_average_motorized_mobility"
    )
    heatmap_connectivity_active_mobility = "heatmap_connectivity_active_mobility"
    heatmap_connectivity_motorized_mobility = "heatmap_connectivity_motorized_mobility"


class FeatureType(str, Enum):
    """Feature layer types."""

    standard = "standard"
    tool = "tool"
    street_network = "street_network"


class TableUploadType(str, Enum):
    """Table data types."""

    csv = "csv"
    xlsx = "xlsx"


# It was decided against using MIME types here because for e.g. gpkg they are commonly just generic application/octet-stream
class FeatureUploadType(str, Enum):
    """File upload types."""

    geojson = "geojson"
    gpkg = "gpkg"
    kml = "kml"
    zip = "zip"  # Commonly used for shapefiles


class FileUploadType(str, Enum):
    """All allowed file upload types"""

    csv = "csv"
    xlsx = "xlsx"
    geojson = "geojson"
    gpkg = "gpkg"
    kml = "kml"
    zip = "zip"  # Commonly used for shapefiles


class FeatureLayerExportType(str, Enum):
    """Feature Layer export types."""

    geojson = "geojson"
    shp = "shp"
    gpkg = "gpkg"
    csv = "csv"
    xlsx = "xlsx"
    kml = "kml"


class TableLayerExportType(str, Enum):
    """Table Layer export types."""

    csv = "csv"
    xlsx = "xlsx"


class FeatureServeType(str, Enum):
    mvt = "mvt"
    wfs = "wfs"
    binary = "binary"


class RasterDataType(str, Enum):
    """Imagery layer data types."""

    wms = "wms"
    xyz = "xyz"
    wmts = "wmts"


class LayerType(str, Enum):
    """Layer types that are supported."""

    feature = "feature"
    raster = "raster"
    table = "table"


class FeatureDataType(str, Enum):
    """Data types for feature layers."""

    mvt = "mvt"
    wfs = "wfs"
    # NULL / None is used for feature layers not fetched from an external service


class FeatureGeometryType(str, Enum):
    """Feature layer geometry types."""

    point = "point"
    line = "line"
    polygon = "polygon"


class DataLicense(str, Enum):
    DDN2 = "DDN2"
    DDZ2 = "DDZ2"
    CC_BY = "CC_BY"
    CC_BY_SA = "CC_BY_SA"
    CC_BY_ND = "CC_BY_ND"
    CC_BY_NC = "CC_BY_NC"
    CC_BY_NC_SA = "CC_BY_NC_SA"
    CC_BY_NC_ND = "CC_BY_NC_ND"
    CC_ZERO = "CC_ZERO"
    ODC_BY = "ODC_BY"
    ODC_ODbL = "ODC_ODbL"
    OTHER = "OTHER"


class DataCategory(str, Enum):
    basemap = "basemap"
    imagery = "imagery"
    boundary = "boundary"
    people = "people"
    transportation = "transportation"
    environment = "environment"
    landuse = "landuse"
    places = "places"


class GeospatialAttributes(SQLModel):
    """Some general geospatial attributes."""

    extent: str | None = Field(
        sa_column=Column(
            Geometry(geometry_type="MultiPolygon", srid="4326", spatial_index=True),
            nullable=True,
        ),
        description="Geographical Extent of the layer",
    )

    @validator("extent", pre=True)
    def wkt_to_geojson(cls, v):
        if v and isinstance(v, WKBElement):
            return to_shape(v).wkt
        else:
            return v


def validate_language_code(v):
    if v:
        try:
            pycountry.languages.get(alpha_2=v)
        except KeyError:
            raise ValueError(f"The passed language {v} is not valid.")
    return v


def validate_geographical_code(v):
    continents = [
        "Africa",
        "Antarctica",
        "Asia",
        "Europe",
        "North America",
        "Oceania",
        "South America",
        "World",
    ]

    if v:
        # Try if country code if not try if any of the continent codes
        try:
            pycountry.countries.get(alpha_2=v)
        except KeyError:
            if v not in continents:
                raise ValueError(f"The passed country {v} is not valid.")
    return v


class LayerBase(ContentBaseAttributes):
    """Base model for layers."""

    # Data Quality Information
    lineage: str | None = Field(
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Descriptive information about the source of the data and its derivation",
    )
    positional_accuracy: str | None = Field(
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Quantitative value indicating positional accuracy",
    )
    attribute_accuracy: str | None = Field(
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Quantitative value indicating the accuracy of attribute data",
    )
    completeness: str | None = Field(
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Quantitative value indicating the completeness of the data",
    )

    # Distribution and Geographical Information
    geographical_code: str | None = Field(
        sa_column=Column(Text, nullable=True),
        max_length=13,  # ISO 3166-1 alpha-2 country codes are 2 letters
        description="Tag indicating the primary geographical area it is following the ISO 3166-1 alpha-2 standard for country codes and for continents the following values are used: Africa, Antarctica, Asia, Europe, North America, Oceania, South America, World",
    )
    language_code: str | None = Field(
        sa_column=Column(Text, nullable=True),
        max_length=2,  # ISO 639-1 language codes are 2 letters
        description="Language of the data",
    )
    distributor_name: str | None = Field(
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Name of the entity distributing the data",
    )
    distributor_email: EmailStr | None = Field(
        sa_column=Column(Text, nullable=True),
        description="Contact information for the distributor",
    )
    distribution_url: HttpUrl | None = Field(
        sa_column=Column(Text, nullable=True),
        description="URL to the data distribution",
    )
    license: DataLicense | None = Field(
        sa_column=Column(Text, nullable=True),
        description="License of the data",
    )
    attribution: str | None = Field(
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Data source of the layer",
    )
    data_reference_year: int | None = Field(
        sa_column=Column(Integer, nullable=True),
        description="Data reference year of the layer",
    )
    data_category: DataCategory | None = Field(
        sa_column=Column(Text, nullable=True),
        description="Data category of the layer",
    )
    in_catalog: bool | None = Field(
        sa_column=Column(Boolean, nullable=False, server_default="False"),
        description="If the layer should be added in the catalog",
    )
    thumbnail_url: HttpUrl | None = Field(
        sa_column=Column(Text, nullable=True),
        description="Layer thumbnail URL",
        default=settings.DEFAULT_LAYER_THUMBNAIL,
    )

    # Check if language and geographical_tag valid according to pycountry
    @validator("language_code", pre=True, check_fields=False)
    def language_code_valid(cls, v):
        return validate_language_code(v)

    @validator("geographical_code", pre=True, check_fields=False)
    def geographical_code_valid(cls, v):
        return validate_geographical_code(v)


layer_base_example = {
    "lineage": "Derived from web research and ground surveys conducted in 2021 by trained professionals.",
    "positional_accuracy": "High accuracy with an error margin of ±2 meters.",
    "attribute_accuracy": "Attribute data verified with 90% confidence level.",
    "completeness": "Data is 98% complete, missing data in remote areas.",
    "upload_reference_system": 4326,
    "upload_file_type": "geojson",
    "geographical_code": "de",  # ISO Alpha-2 code for Germany
    "language_code": "de",  # ISO Alpha-2 code for German
    "distributor_name": "Plan4Better GmbH",
    "distributor_email": "info@plan4better.de",
    "distribution_url": "https://plan4better.de/data/samples/sample_data.geojson",
    "license": "ODC_ODbL",  # Assuming this is a value from the DataLicense Enum
    "attribution": "Dataset provided by Plan4Better GmbH.",
    "data_reference_year": 2021,
    "data_category": "transportation",  # Assuming this is a value from the DataCategory Enum
}


def internal_layer_table_name(values: SQLModel | BaseModel):
    """Get the table name for the internal layer."""

    # Get table name
    if values.type == LayerType.feature.value:
        # If of type enum return value
        if isinstance(values.feature_layer_geometry_type, Enum):
            feature_layer_geometry_type = values.feature_layer_geometry_type.value
        else:
            feature_layer_geometry_type = values.feature_layer_geometry_type
    elif values.type == LayerType.table.value:
        feature_layer_geometry_type = "no_geometry"
    else:
        raise ValueError(f"The passed layer type {values.type} is not supported.")

    return f"{settings.USER_DATA_SCHEMA}.{feature_layer_geometry_type}_{str(values.user_id).replace('-', '')}"


class Layer(LayerBase, GeospatialAttributes, DateTimeBase, table=True):
    """Layer model."""

    __tablename__ = "layer"
    __table_args__ = {"schema": settings.CUSTOMER_SCHEMA}

    id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
        description="Layer ID",
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Layer owner ID",
    )
    folder_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.folder.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Layer folder ID",
    )
    type: LayerType = Field(
        sa_column=Column(Text, nullable=False), description="Layer type"
    )
    data_store_id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.data_store.id"),
        ),
        description="Data store ID of the layer",
    )
    extent: str | None = Field(
        sa_column=Column(
            Geometry(geometry_type="MultiPolygon", srid="4326", spatial_index=False),
            nullable=True,
        ),
        description="Geographical Extent of the layer",
    )
    properties: dict | None = Field(
        sa_column=Column(JSONB, nullable=True),
        description="Properties of the layer",
    )
    other_properties: dict | None = Field(
        sa_column=Column(JSONB, nullable=True),
        description="Other properties of the layer",
    )
    url: HttpUrl | None = Field(
        sa_column=Column(Text, nullable=True),
        description="Layer URL for vector and imagery layers",
    )
    data_type: Optional[Union["RasterDataType", "FeatureDataType"]] = Field(
        sa_column=Column(Text, nullable=True),
        description="Data type to store the source of the layer",
    )
    tool_type: Optional[ToolType] = Field(
        sa_column=Column(Text, nullable=True),
        description="If it is an tool layer, the tool type",
    )
    job_id: UUID | None = Field(
        sa_column=Column(UUID_PG(as_uuid=True), nullable=True),
        description="Job ID if the layer is a tool layer",
    )
    feature_layer_type: Optional["FeatureType"] = Field(
        sa_column=Column(Text, nullable=True), description="Feature layer type"
    )
    feature_layer_geometry_type: FeatureGeometryType | None = Field(
        sa_column=Column(Text, nullable=True),
        description="Geometry type for feature layers",
    )
    size: int | None = Field(
        sa_column=Column(Integer, nullable=True),
        description="Size of the layer in bytes",
    )
    attribute_mapping: dict | None = Field(
        sa_column=Column(JSONB, nullable=True),
        description="Attribute mapping for feature layers",
    )
    upload_reference_system: int | None = Field(
        sa_column=Column(Integer, nullable=True),
        description="Description of the spatial reference systems",
    )
    upload_file_type: FileUploadType | None = Field(
        sa_column=Column(Text, nullable=True),
        description="Description of the upload file type",
    )

    # Relationships
    data_store: "DataStore" = Relationship(back_populates="layers")
    layer_projects: List["LayerProjectLink"] = Relationship(
        back_populates="layer", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    folder: "Folder" = Relationship(back_populates="layers")
    organization_links: List["LayerOrganizationLink"] = Relationship(
        back_populates="layer", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    team_links: List["LayerTeamLink"] = Relationship(
        back_populates="layer", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )

    @validator("extent", pre=True)
    def wkt_to_geojson(cls, v):
        if v and isinstance(v, WKBElement):
            return to_shape(v).wkt
        else:
            return v

    @property
    def table_name(self):
        return internal_layer_table_name(self)

    @property
    def layer_id(self):
        return self.id


# Constraints
UniqueConstraint(Layer.__table__.c.folder_id, Layer.__table__.c.name)
Layer.update_forward_refs()
