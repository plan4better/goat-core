from src.crud.base import CRUDBase
from src.db.models.scenario import Scenario


class CRUDScenario(CRUDBase):
    pass


scenario = CRUDScenario(Scenario)
