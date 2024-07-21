from typing import TYPE_CHECKING, List
from uuid import UUID

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    Column,
    Field,
    ForeignKey,
    Integer,
    Relationship,
    Text,
    UniqueConstraint,
)

from src.db.models._base_class import DateTimeBase

if TYPE_CHECKING:
    from .layer import Layer
    from .project import Project
    from .scenario import Scenario
    from .scenario_feature import ScenarioFeature


class LayerProjectLink(DateTimeBase, table=True):
    __tablename__ = "layer_project"
    __table_args__ = {"schema": "customer"}

    id: int | None = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    group: str | None = Field(
        sa_column=Column(Text, nullable=True),
        description="Layer group name",
        max_length=255,
    )
    layer_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True), ForeignKey("customer.layer.id", ondelete="CASCADE")
        ),
        description="Layer ID",
    )
    project_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True), ForeignKey("customer.project.id", ondelete="CASCADE")
        ),
        description="Project ID",
    )
    name: str = Field(
        sa_column=Column(Text, nullable=False),
        description="Layer name within the project",
        max_length=255,
    )
    properties: dict | None = Field(
        sa_column=Column(JSONB, nullable=True), description="Layer properties"
    )
    other_properties: dict | None = Field(
        sa_column=Column(JSONB, nullable=True), description="Layer other properties"
    )
    query: dict | None = Field(
        sa_column=Column(JSONB, nullable=True),
        description="CQL2-JSON filter to query the layer",
    )
    charts: dict | None = Field(
        sa_column=Column(JSONB, nullable=True), description="Chart configuration"
    )

    # Relationships
    project: "Project" = Relationship(back_populates="layer_projects")
    layer: "Layer" = Relationship(back_populates="layer_projects")

    scenario_features: List["ScenarioFeature"] = Relationship(
        back_populates="layer_project",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


class ScenarioScenarioFeatureLink(DateTimeBase, table=True):
    __tablename__ = "scenario_scenario_feature"
    __table_args__ = {"schema": "customer"}

    id: int | None = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    scenario_id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey("customer.scenario.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        description="Scenario ID",
    )
    scenario_feature_id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey("customer.scenario_feature.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        description="Scenario Feature ID",
    )

    scenario: "Scenario" = Relationship(back_populates="scenario_features_links")
    scenario_feature: "ScenarioFeature" = Relationship(back_populates="scenarios_links")


class UserProjectLink(DateTimeBase, table=True):
    __tablename__ = "user_project"
    __table_args__ = {"schema": "customer"}

    id: int | None = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True), ForeignKey("customer.user.id", ondelete="CASCADE")
        ),
        description="User ID",
    )
    project_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True), ForeignKey("customer.project.id", ondelete="CASCADE")
        ),
        description="Project ID",
    )
    initial_view_state: dict = Field(
        sa_column=Column(JSONB, nullable=False),
        description="Initial view state of the project",
    )

    # Relationships
    project: "Project" = Relationship(back_populates="user_projects")


UniqueConstraint(
    UserProjectLink.project_id, UserProjectLink.user_id, name="unique_user_project"
)
