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
    project_layer_id: str
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
            "id": 127,
            "edit_type": "n",
        }
    ],
}
