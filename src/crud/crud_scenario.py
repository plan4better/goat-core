from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.crud.base import CRUDBase
from src.db.models._link_model import ScenarioScenarioFeatureLink
from src.db.models.scenario import Scenario
from src.db.models.scenario_feature import ScenarioFeature
from src.schemas.scenario import IScenarioCreate, IScenarioUpdate


class CRUDScenario(CRUDBase[Scenario, IScenarioCreate, IScenarioUpdate]):
    async def get_features(
        self,
        async_session: AsyncSession,
        scenario_id: UUID,
    ):
        """Get all features of a scenario."""

        query = select(ScenarioFeature).where(
            ScenarioScenarioFeatureLink.scenario_id == scenario_id,
            ScenarioFeature.id == ScenarioScenarioFeatureLink.scenario_feature_id,
        )

        result = await async_session.execute(query)
        features = result.scalars().all()
        return features


scenario = CRUDScenario(Scenario)
