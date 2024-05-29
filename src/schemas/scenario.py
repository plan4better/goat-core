from pydantic import BaseModel


class IScenarioCreate(BaseModel):
    name: str


class IScenarioUpdate(BaseModel):
    name: str


request_examples = {
    "create": {
        "name": "Scenario test",
    },
    "update": {
        "name": "Scenario test updated",
    },
}
