import io
import json
from typing import Dict, List, Union

from pymgl import Map
from shapely import from_wkt

from src.core.config import settings
from src.db.models.layer import Layer
from src.utils import async_get_with_retry


def rgb_to_hex(rgb: tuple) -> str:
    return "#{:02x}{:02x}{:02x}".format(rgb[0], rgb[1], rgb[2])


def get_mapbox_style_color(data: Dict, type: str) -> Union[str, List]:
    colors = data.get(f"{type}_range", {}).get("color")
    field_name = data.get(f"{type}_field", {}).get("name")
    if (
        not field_name
        or not colors
        or len(data.get(f"{type}_scale_breaks", {}).get("breaks", []))
        != len(colors) - 1
    ):
        return (
            rgb_to_hex(data["properties"].get(type))
            if data["properties"].get(type)
            else "#000000"
        )

    color_steps = []
    for index, color in enumerate(colors):
        if index == len(colors) - 1:
            color_steps.append([colors[index]])
        else:
            color_steps.append(
                [
                    color,
                    data.get(f"{type}_scale_breaks", {}).get("breaks", [])[index] or 0,
                ]
            )

    config = ["step", ["get", field_name]] + [
        item for sublist in color_steps for item in sublist
    ]
    return config


def transform_to_mapbox_layer_style_spec(data: Dict) -> Dict:
    type = data.get("feature_layer_geometry_type")
    if type == "point":
        point_properties = data.get("properties")
        return {
            "type": "circle",
            "paint": {
                "circle-color": get_mapbox_style_color(data, "color"),
                "circle-opacity": point_properties.get("filled", False)
                * point_properties.get("opacity", 0),
                "circle-radius": point_properties.get("radius", 5),
                "circle-stroke-color": get_mapbox_style_color(data, "stroke_color"),
                "circle-stroke-width": point_properties.get("stroked", False)
                * point_properties.get("stroke_width", 1),
            },
        }
    elif type == "polygon":
        polygon_properties = data.get("properties")
        return {
            "type": "fill",
            "paint": {
                "fill-color": get_mapbox_style_color(data, "color"),
                "fill-opacity": polygon_properties.get("filled", False)
                * polygon_properties.get("opacity", 0),
                "fill-outline-color": get_mapbox_style_color(data, "stroke_color"),
                "fill-antialias": polygon_properties.get("stroked", False),
            },
        }
    elif type == "line":
        line_properties = data.get("properties")
        return {
            "type": "line",
            "paint": {
                "line-color": get_mapbox_style_color(data, "stroke_color"),
                "line-opacity": line_properties.get("opacity", 0),
                "line-width": line_properties.get("stroke_width", 1),
            },
        }
    else:
        raise ValueError(f"Invalid type: {type}")


class Print:
    def __init__(self):
        self.thumbnail_zoom = 13
        self.thumbnail_height = 140 * 2
        self.thumbnail_width = 337 * 2

    async def create_layer_thumnnail(self, layer: Layer, file_name: str):
        map = Map(
            "mapbox://styles/mapbox/light-v11",
            provider="mapbox",
            token=settings.MAPBOX_TOKEN,
        )
        map.load()

        # Load wkt extent using shapely and pass centroid
        geom_shape = from_wkt(layer.extent)
        map.setCenter(geom_shape.centroid.x, geom_shape.centroid.y)

        # Set zoom and size
        map.setZoom(self.thumbnail_zoom)
        map.setSize(self.thumbnail_width, self.thumbnail_height)

        # Transform layer to mapbox style
        style = transform_to_mapbox_layer_style_spec(layer.dict())

        # Get collection id
        # Check if layer is of type Layer then use id else use layer_id
        if isinstance(layer, Layer):
            layer_id = layer.id
        else:
            layer_id = layer.layer_id

        collection_id = "user_data." + str(layer_id).replace("-", "")

        # Request in recursive loop if layer was already added in geoapi if it does not fail the layer was added
        header = {"Content-Type": "application/json"}
        await async_get_with_retry(
            url="https://geoapi.goat.dev.plan4better.de/collections/" + collection_id,
            headers=header,
            num_retries=10,
            retry_delay=1,
        )

        # Add layer source
        tile_url = (
            "https://geoapi.goat.dev.plan4better.de/collections/"
            + collection_id
            + "/tiles/{z}/{x}/{y}"
        )
        map.addSource(
            layer.name,
            json.dumps(
                {
                    "type": "vector",
                    "tiles": [tile_url],
                }
            ),
        )
        # Add layer
        map.addLayer(
            json.dumps(
                {
                    "id": layer.name,
                    "type": style["type"],
                    "source": layer.name,
                    "source-layer": "default",
                    "paint": style["paint"],
                }
            )
        )

        img_bytes = map.renderPNG()
        image = io.BytesIO(img_bytes)

        # Save image to s3 bucket using s3 client from settings
        dir = settings.THUMBNAIL_DIR_LAYER + "/" + file_name
        url = settings.ASSETS_URL + "/" + dir

        # Save to s3
        settings.S3_CLIENT.upload_fileobj(
            Fileobj=image,
            Bucket=settings.AWS_S3_ASSETS_BUCKET,
            Key=dir,
            ExtraArgs={"ContentType": "image/png"},
            Callback=None,
            Config=None,
        )
        return url
