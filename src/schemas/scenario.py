from pydantic import BaseModel

from src.db.models.scenario_feature import (
    ScenarioFeatureEditType,
    UserData,
)


class IScenarioCreate(BaseModel):
    name: str


class IScenarioUpdate(BaseModel):
    name: str


class IScenarioFeatureCreate(UserData):
    layer_id: str
    edit_type: ScenarioFeatureEditType


request_examples = {
    "create": {
        "name": "Scenario test",
    },
    "update": {
        "name": "Scenario test updated",
    },
    "create_scenario_features": [
        {
            "geom": "POINT (35.5 47.8)",
            "layer_id": "4f7924a8-9699-41ef-8998-8d33a98e1604",
            "edit_type": "n",
        }
    ],
}
