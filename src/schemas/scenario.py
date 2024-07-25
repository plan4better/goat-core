from uuid import UUID

from pydantic import BaseModel, Extra

from src.db.models.scenario_feature import (
    ScenarioFeatureEditType,
)
from src.utils import optional


class IScenarioCreate(BaseModel):
    name: str


class IScenarioUpdate(BaseModel):
    name: str


class IScenarioFeatureCreate(BaseModel):
    layer_project_id: int
    edit_type: ScenarioFeatureEditType
    geom: str

    class Config:
        extra = Extra.allow


@optional
class IScenarioFeatureUpdate(BaseModel):
    id: UUID
    feature_id: UUID
    edit_type: ScenarioFeatureEditType
    layer_project_id: int
    h3_3: int
    geom: str

    class Config:
        extra = Extra.allow


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
