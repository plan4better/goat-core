import random
from src.db.models.layer import FeatureGeometryType
from src.schemas.colors import diverging_colors, color_ranges, ColorRangeType
from src.utils import hex_to_rgb
from src.schemas.tool import ToolType
from src.core.config import settings

# TODO: Add Basic pydantic validation
def get_base_style(feature_geometry_type: FeatureGeometryType):
    """Return the base style for the given feature geometry type and tool type."""

    color = hex_to_rgb(random.choice(diverging_colors["Spectral"][-1]["colors"]))
    if feature_geometry_type == FeatureGeometryType.point:
        return {
            "color": color,
            "filled": True,
            "radius": 5,
            "opacity": 1,
            "stroked": False,
            "max_zoom": 22,
            "min_zoom": 1,
            "visibility": True,
            "fixed_radius": False,
            "radius_range": [0, 10],
            "radius_scale": "linear",
        }
    elif feature_geometry_type == FeatureGeometryType.line:
        return {
            "color": color,
            "filled": True,
            "opacity": 1,
            "stroked": True,
            "max_zoom": 22,
            "min_zoom": 1,
            "visibility": True,
            "stroke_color": color,
            "stroke_width": 10,
            "stroke_width_range": [0, 10],
            "stroke_width_scale": "linear",
        }
    elif feature_geometry_type == FeatureGeometryType.polygon:
        return {
            "color": color,
            "filled": True,
            "opacity": 0.8,
            "stroked": False,
            "max_zoom": 22,
            "min_zoom": 1,
            "visibility": True,
            "stroke_color": [217, 25, 85],
            "stroke_width": 3,
            "stroke_width_range": [0, 10],
            "stroke_width_scale": "linear",
        }


def get_tool_style(
    feature_geometry_type: FeatureGeometryType,
    color_field: dict,
    color_scale_breaks: dict,
    color_range_type: ColorRangeType,
):
    """Return the style for the given feature geometry type and property settings."""

    index_color_range = len(color_scale_breaks["breaks"]) - 2
    random_color_range_key = random.choice(
        list(color_ranges.get(color_range_type).keys())
    )
    random_color_range = color_ranges[color_range_type][random_color_range_key][
        index_color_range
    ]
    color = hex_to_rgb(random.choice(random_color_range["colors"]))
    
    if feature_geometry_type == FeatureGeometryType.point:
        return {
            "color": color,
            "filled": True,
            "radius": 5,
            "opacity": 1,
            "stroked": False,
            "max_zoom": 22,
            "min_zoom": 1,
            "visibility": True,
            "color_field": color_field,
            "color_range": random_color_range,
            "color_scale": "quantile",
            "fixed_radius": False,
            "radius_range": [0, 10],
            "radius_scale": "linear",
        }
    elif feature_geometry_type == FeatureGeometryType.polygon:
        return {
            "color": color,
            "filled": True,
            "opacity": 0.8,
            "stroked": False,
            "max_zoom": 22,
            "min_zoom": 1,
            "visibility": True,
            "color_field": color_field,
            "color_range": random_color_range,
            "color_scale": "quantile",
            "stroke_color": [217, 25, 85],
            "stroke_width": 3,
            "color_scale_breaks": color_scale_breaks,
            "stroke_color_range": random_color_range,
            "stroke_color_scale": "quantile",
            "stroke_width_range": [0, 10],
            "stroke_width_scale": "linear",
        }
    elif feature_geometry_type == FeatureGeometryType.line:
        return {
            "color": color,
            "filled": True,
            "opacity": 1,
            "stroked": True,
            "max_zoom": 22,
            "min_zoom": 1,
            "visibility": True,
            "color_field": color_field,
            "color_range": random_color_range,
            "color_scale": "quantile",
            "stroke_color": color,
            "stroke_width": 10,
            "stroke_color_range": random_color_range,
            "stroke_color_scale": "quantile",
            "stroke_width_range": [0, 10],
            "stroke_width_scale": "linear",
        }

base_properties = {
    "tool": {
        "join": {
            "point": {
                "type": "circle",
                "paint": {"circle-color": "#00ffff", "circle-radius": 5},
            },
            "line": {
                "type": "line",
                "paint": {"line-color": "#00ffff", "line-width": 2},
            },
            "polygon": {"type": "fill", "paint": {"fill-color": "#00ffff"}},
        }
    },
}
