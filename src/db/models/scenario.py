from typing import TYPE_CHECKING, List
from uuid import UUID

from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    Column,
    Field,
    ForeignKey,
    Relationship,
    Text,
    text,
)

from ._base_class import DateTimeBase
from ._link_model import ScenarioScenarioFeatureLink

if TYPE_CHECKING:
    from .scenario_feature import ScenarioFeature
    from .user import User


class Scenario(DateTimeBase, table=True):
    __tablename__ = "scenario"
    __table_args__ = {"schema": "customer"}

    id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        )
    )
    name: str = Field(sa_column=Column(Text, nullable=False), max_length=255)
    user_id: UUID = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey("customer.user.id", ondelete="CASCADE"),
            nullable=False,
        ),
    )

    user: "User" = Relationship(back_populates="scenarios")

    scenario_features: List["ScenarioFeature"] = Relationship(
        back_populates="scenarios", link_model=ScenarioScenarioFeatureLink
    )
