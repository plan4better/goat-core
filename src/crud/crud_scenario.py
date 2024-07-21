from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, load_only

from src.core.layer import get_user_table
from src.crud.base import CRUDBase
from src.db.models._link_model import LayerProjectLink, ScenarioScenarioFeatureLink
from src.db.models.layer import Layer
from src.db.models.scenario import Scenario
from src.db.models.scenario_feature import ScenarioFeature, ScenarioFeatureEditType
from src.schemas.scenario import (
    IScenarioCreate,
    IScenarioFeatureCreate,
    IScenarioFeatureUpdate,
    IScenarioUpdate,
)


class CRUDScenario(CRUDBase[Scenario, IScenarioCreate, IScenarioUpdate]):
    async def _get_origin_features(
        self,
        async_session: AsyncSession,
        layer_project: LayerProjectLink,
        feature_id: UUID,
    ):
        """Get all features from the origin table."""

        user_table = get_user_table(layer_project.layer.dict())
        origin_feature_result = await async_session.execute(
            text(f"""SELECT * FROM {user_table} WHERE id = :id"""),
            {"id": feature_id},
        )
        origin_feature_obj = origin_feature_result.fetchone()
        return origin_feature_obj

    def _get_rev_attr_mapping(self, layer_project):
        """Get attribute mapping for a project layer."""

        attribute_mapping = layer_project.layer.attribute_mapping
        if attribute_mapping:
            reversed_attribute_mapping = {v: k for k, v in attribute_mapping.items()}
            attribute_mapping = reversed_attribute_mapping

        return attribute_mapping

    async def get_features(
        self,
        async_session: AsyncSession,
        scenario_id: UUID,
    ):
        """Get all features of a scenario."""

        query = (
            select(ScenarioFeature)
            .where(
                ScenarioScenarioFeatureLink.scenario_id == scenario_id,
                ScenarioFeature.id == ScenarioScenarioFeatureLink.scenario_feature_id,
            )
            .options(
                joinedload(ScenarioFeature.layer_project).options(
                    load_only(LayerProjectLink.id),
                    joinedload(LayerProjectLink.layer).options(
                        load_only(Layer.attribute_mapping, Layer.id)
                    ),
                )
            )
        )

        result = await async_session.execute(query)
        features = result.scalars().all()

        transformed_features = []
        for feature in features:
            attribute_mapping = feature.layer_project.layer.attribute_mapping
            transformed_feature = {
                "id": feature.id,
                "geom": feature.geom,
                "feature_id": feature.feature_id,
                "layer_project_id": feature.layer_project_id,
                "edit_type": feature.edit_type,
                "updated_at": feature.updated_at,
                "created_at": feature.created_at,
            }
            for key, value in feature.dict().items():
                if key in attribute_mapping:
                    transformed_feature[attribute_mapping[key]] = value

            transformed_features.append(transformed_feature)

        return transformed_features

    async def create_feature(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        layer_project: LayerProjectLink,
        scenario: Scenario,
        feature: IScenarioFeatureCreate,
    ):
        """Create a feature in a scenario."""

        feature_id = feature.get("id")
        if feature_id:
            feature_db = await CRUDBase(ScenarioFeature).get(
                db=async_session, id=feature_id
            )
            if feature_db:
                return feature_db

        user_table = get_user_table(layer_project.layer.dict())
        origin_feature_result = await async_session.execute(
            text(f"""SELECT * FROM {user_table} WHERE id = :id"""),
            {"id": feature["feature_id"]},
        )
        origin_feature_obj = origin_feature_result.fetchone()
        if origin_feature_obj:
            scenario_feature_dict = {
                **origin_feature_obj,
                "id": None,
                "feature_id": feature["feature_id"],
                "layer_project_id": layer_project.id,
                "edit_type": feature["edit_type"],
            }
            scenario_feature_obj = ScenarioFeature(**scenario_feature_dict)
            scenario_scenario_feature_link = ScenarioScenarioFeatureLink(
                scenario=scenario, scenario_feature=scenario_feature_obj
            )
            async_session.add(scenario_scenario_feature_link)
            await async_session.commit()
            return scenario_feature_obj

        raise ValueError("Feature does not exist")

    async def update_feature(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        layer_project: LayerProjectLink,
        scenario: Scenario,
        feature: IScenarioFeatureUpdate,
    ):
        """Update a feature in a scenario."""

        attribute_mapping = self._get_rev_attr_mapping(layer_project)

        feature_db = None
        # Check if feature exists in the scenario_feature table
        if isinstance(feature.id, UUID):
            feature_db = await CRUDBase(ScenarioFeature).get(
                db=async_session, id=feature.id
            )
            for key, value in feature.dict().items():
                if value is not None and key in attribute_mapping:
                    setattr(feature_db, attribute_mapping[key], value)
                if key == "geom" and value is not None:
                    setattr(feature_db, key, value)
            async_session.add(feature_db)
            await async_session.commit()
            return feature_db

        # New modified feature. Create a new feature in the scenario_feature table
        elif isinstance(feature.id, int):
            origin_feature_obj = await self._get_origin_features(
                async_session, layer_project, feature.id
            )
            if origin_feature_obj:
                scenario_feature_dict = {
                    **origin_feature_obj,
                    "id": None,
                    "feature_id": feature.id,
                    "layer_project_id": layer_project.id,
                    "edit_type": ScenarioFeatureEditType.modified,
                }
                for key, value in feature.dict().items():
                    if value is not None and key in attribute_mapping:
                        scenario_feature_dict[attribute_mapping[key]] = value

                scenario_feature_obj = ScenarioFeature(**scenario_feature_dict)
                scenario_scenario_feature_link = ScenarioScenarioFeatureLink(
                    scenario=scenario, scenario_feature=scenario_feature_obj
                )
                async_session.add(scenario_scenario_feature_link)
                await async_session.commit()
                return scenario_feature_obj

        raise ValueError("Cannot update feature")

    async def delete_feature(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        layer_project: LayerProjectLink,
        scenario: Scenario,
        feature_id: UUID | int,
    ):
        """Delete a feature from a scenario."""

        # Check if feature exists in the scenario_feature table
        feature_db = None
        if isinstance(feature_id, UUID):
            feature_db = await CRUDBase(ScenarioFeature).get(
                db=async_session, id=feature_id
            )
            if feature_db:
                return await CRUDBase(ScenarioFeature).remove(
                    db=async_session, id=feature_db.id
                )

        elif isinstance(feature_id, int):
            origin_feature_obj = await self._get_origin_features(
                async_session, layer_project, feature_id
            )
            if origin_feature_obj:
                scenario_feature_dict = {
                    **origin_feature_obj,
                    "id": None,
                    "feature_id": feature_id,
                    "layer_project_id": layer_project.id,
                    "edit_type": ScenarioFeatureEditType.deleted,
                }
                scenario_feature_obj = ScenarioFeature(**scenario_feature_dict)
                scenario_scenario_feature_link = ScenarioScenarioFeatureLink(
                    scenario=scenario, scenario_feature=scenario_feature_obj
                )
                async_session.add(scenario_scenario_feature_link)
                await async_session.commit()
                return scenario_feature_obj

        # Throw error if feature does not exist
        raise ValueError("Feature does not exist")


scenario = CRUDScenario(Scenario)
