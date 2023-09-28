from typing import List, Optional
from pydantic import BaseModel, validator
from src.resources.enums import ReturnType
from enum import Enum
from uuid import UUID


class CountLimitPerTool(int, Enum):
    oev_gueteklasse = 1000


class AreaLimitPerTool(int, Enum):
    oev_gueteklasse = 50  # in degree


class StationConfig(BaseModel):
    groups: dict
    time_frequency: List[int]
    categories: List[dict]
    classification: dict


class CalculateOevGueteklassenParameters(BaseModel):
    folder_id: UUID
    project_id: UUID = None
    start_time: int = 25200
    end_time: int = 32400
    weekday: int = 1
    reference_area: UUID  # UUID of layers
    station_config: StationConfig

    @validator("start_time", "end_time")
    def seconds_validator(cls, v):
        if v < 0 or v > 86400:
            raise ValueError("Should be between or equal to 0 and 86400")
        return v

    @validator("weekday")
    def weekday_validator(cls, v):
        if v < 1 or v > 7:
            raise ValueError("weekday should be between or equal to 1 and 7")
        return v


# {
#         "0": "B",  # tram
#         "1": "A",  # metro
#         "2": "A",  # train
#         "3": "A",  # bus
#         "100": "A",  # rail
#         "101": "A",  # highspeed train
#         "102": "A",  # long distance train
#         "103": "A",  # interregional train
#         "105": "A",  # car sleeper train
#         "106": "A",  # regional train
#         "109": "A",  # suburban train
#         "400": "A",  # urban rail
#         "402": "A",  # underground service
#         "700": "C",  # bus service
#         "704": "C",  # local bus service
#         "715": "C",  # demand and response bus service
#         "900": "B",  # tram
#     }


station_config_example = {
    "groups": {
        "0": "B",
        "1": "A",
        "2": "A",
        "3": "C",
        "100": "A",
        "101": "A",
        "102": "A",
        "103": "A",
        "104": "A",
        "105": "A",
        "106": "A",
        "107": "A",
        "108": "A",
        "109": "A",
        "110": "A",
        "111": "A",
        "112": "A",
        "114": "A",
        "116": "A",
        "117": "A",
        "200": "C",
        "201": "C",
        "202": "C",
        "204": "C",
        "400": "A",
        "401": "A",
        "402": "A",
        "403": "A",
        "405": "A",
        "700": "C",
        "701": "C",
        "702": "C",
        "704": "C",
        "715": "C",
        "800": "C",
        "900": "B",
        "901": "B",
        "902": "B",
        "903": "B",
        "904": "B",
        "905": "B",
        "906": "B",
    },
    "time_frequency": [0, 4, 10, 19, 39, 60, 119],
    "categories": [
        {
            "A": 1,  # i.e. types of transports in category A are in transport stop category I
            "B": 1,
            "C": 2,
        },
        {"A": 1, "B": 2, "C": 3},
        {"A": 2, "B": 3, "C": 4},
        {"A": 3, "B": 4, "C": 5},
        {"A": 4, "B": 5, "C": 6},
        {"A": 5, "B": 6, "C": 7},
    ],
    "classification": {
        "1": {300: "1", 500: "1", 750: "2", 1000: "3", 1250: "4"},
        "2": {300: "1", 500: "2", 750: "3", 1000: "4", 1250: "5"},
        "3": {300: "2", 500: "3", 750: "4", 1000: "5", 1250: "6"},
        "4": {300: "3", 500: "4", 750: "5", 1000: "6", 1250: "6"},
        "5": {300: "4", 500: "5", 750: "6"},
        "6": {300: "5", 500: "6"},
        "7": {300: "6"},
    },
}

oev_gueteklasse_config_example = {
    "folder_id": "732fc631-e1a4-44c5-b7ef-27c7d49e65f7",
    "start_time": 21600,
    "end_time": 72000,
    "weekday": 1,
    "reference_area": "99261caf-bb4a-42ef-8212-423a3dd6d613",
    "station_config": station_config_example,
}

oev_gueteklasse_station_config_layer_base = {
    "name": "ÖV-Güteklassen stations",
    "description": "ÖV-Güteklassen Categories on the station level",
    "type": "feature_layer",
    "feature_layer_geometry_type": "point",
    "indicator_type": "oev_gueteklasse",
    "thumbnail_url": "https://goat-app-assets.s3.eu-central-1.amazonaws.com/logos/goat_green.png",
    "style": {},
    "data_source": "Plan4Better GmbH",
    "attribute_mapping": {
        "stop_id": "text_attr1",
        "stop_name": "text_attr2",
        "frequency": "float_attr1",
        "_class": "integer_attr1",
        "trip_cnt": "jsonb_attr1",
        "trip_ids": "jsonb_attr2",
    },
}

oev_gueteklasse_config_layer_base = {
    "name": "ÖV-Güteklassen",
    "description": "ÖV-Güteklassen with catchment areas",
    "layer_type": "feature_layer_type",
    "indicator_type": "oev_gueteklasse",
    "thumbnail_url": "https://goat-app-assets.s3.eu-central-1.amazonaws.com/logos/goat_green.png",
    "style": {},
    "data_source": "Plan4Better GmbH",
}
