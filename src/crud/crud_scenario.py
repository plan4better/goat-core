from src.crud.base import CRUDBase
from src.db.models.scenario import Scenario
from src.schemas.scenario import IScenarioCreate, IScenarioUpdate


class CRUDScenario(CRUDBase[Scenario, IScenarioCreate, IScenarioUpdate]):
    pass


scenario = CRUDScenario(Scenario)
