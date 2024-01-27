import io
import json
from typing import Dict, List, Union

import matplotlib.pyplot as plt
import pandas as pd
from pymgl import Map
from shapely import from_wkt
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.db.models.layer import Layer, LayerType
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


class PrintMap:
    def __init__(self, async_session: AsyncSession):
        self.thumbnail_zoom = 13
        self.thumbnail_height = 280
        self.thumbnail_width = 674
        self.async_session = async_session

    async def create_layer_thumbnail(self, layer: Layer, file_name: str) -> str:
        """Create layer thumbnail."""

        # Check layer type
        if layer.type == LayerType.table:
            image = await self.create_table_thumbnail(layer, file_name)
        elif layer.type == LayerType.feature:
            image = await self.create_feature_layer_thumbnail(layer, file_name)
        else:
            raise ValueError("Invalid layer type.")

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

    async def create_feature_layer_thumbnail(self, layer: Layer) -> io.BytesIO:
        """Create feature layer thumbnail."""

        map = Map(
            "mapbox://styles/mapbox/light-v11",
            provider="mapbox",
            token=settings.MAPBOX_TOKEN,
        )
        map.load()

        # Load wkt extent using shapely and pass centroid
        geom_shape = from_wkt(layer.extent)
        map.setBounds(
            xmin=geom_shape.bounds[0],
            ymin=geom_shape.bounds[1],
            xmax=geom_shape.bounds[2],
            ymax=geom_shape.bounds[3],
        )
        # Set zoom and size
        map.setSize(self.thumbnail_width, self.thumbnail_height)

        # Transform layer to mapbox style
        style = transform_to_mapbox_layer_style_spec(layer.dict())

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

        return image

    async def create_table_thumbnail(self, layer: Layer, file_name: str):
        """Create table thumbnail."""

        # Get the first 4 four columns of the attribute mapping.
        columns = []
        columns_mapped = []
        for index, (key, value) in enumerate(layer.attribute_mapping.items()):
            if index < 5:
                columns.append(key)
                # Limit columns name to 5 chars and add ...
                if len(value) > 6:
                    value = value[:6] + "..."
                columns_mapped.append(value)

        # Read four rows of the table and create a DataFrame
        data = await self.async_session.execute(
            f"""
            SELECT {', '.join(columns[:4])}
            FROM {layer.table_name}
            LIMIT 4
            """
        )
        data = data.all()

        # Create a DataFrame
        df = pd.DataFrame(data, columns=columns_mapped[:4])

        # If the len of the columns exceed 4 then add a column with ...
        if len(columns_mapped) > 4:
            df["..."] = "..."

        thumbnail_height = 280
        thumbnail_width = 674

        # Create a figure and an axes
        fig, ax = plt.subplots(
            figsize=(thumbnail_width / 80, thumbnail_height / 80)
        )  # Convert pixels to inches

        # Remove the axes
        ax.axis("off")

        # Create a table and add it to the axes
        table = plt.table(
            cellText=df.values,
            colLabels=df.columns,
            loc="center",
            cellLoc="center",
            colWidths=[1] * len(df.columns),  # Make columns of equal size
            bbox=[0, 0, 1, 1],  # Full height and width with a small padding
        )
        table.auto_set_font_size(False)
        table.set_fontsize(13)

        # Set the color, font weight, and font color of the header cells
        table_props = table.properties()
        table_cells = table_props["children"]
        for cell in table_cells:
            if cell.get_text().get_text() in df.columns:
                cell.set_facecolor("#99ADC6")
                cell.get_text().set_weight("bold")  # Make the text bold
                cell.get_text().set_color("white")  # Set the font color to white

        # Save the file as bytes and return it
        image = io.BytesIO()
        fig.savefig(image, bbox_inches="tight", pad_inches=0)
        image.seek(0)
        return image


# import time
