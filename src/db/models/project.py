from typing import TYPE_CHECKING, List
from uuid import UUID

from pydantic import HttpUrl
from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlalchemy.sql import text
from sqlmodel import (
    ARRAY,
    Column,
    Field,
    ForeignKey,
    Integer,
    Relationship,
    UniqueConstraint,
)

from src.core.config import settings
from src.db.models._base_class import DateTimeBase
from src.db.models.layer import ContentBaseAttributes

if TYPE_CHECKING:
    from _link_model import LayerProjectLink, UserProjectLink
    from .report import Report
    from .scenario import Scenario


class Project(ContentBaseAttributes, DateTimeBase, table=True):
    __tablename__ = "project"
    __table_args__ = {"schema": "customer"}

    id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
        description="Layer ID",
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey("customer.user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Project owner ID",
    )
    folder_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey("customer.folder.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Project folder ID",
    )
    active_scenario_id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            nullable=True,
        ),
        description="Active scenario ID",
    )
    layer_order: List[int] | None = Field(
        sa_column=Column(
            ARRAY(Integer),
            nullable=True,
        ),
        description="Layer order in project",
    )
    thumbnail_url: HttpUrl | None = Field(
        sa_column=Column(Text, nullable=True),
        description="Project thumbnail URL",
        default=settings.DEFAULT_PROJECT_THUMBNAIL,
    )
    # Relationships
    reports: List["Report"] = Relationship(
        back_populates="project",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )
    user_projects: List["UserProjectLink"] = Relationship(
        back_populates="project",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )
    layer_projects: List["LayerProjectLink"] = Relationship(
        back_populates="project",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )
    scenarios: List["Scenario"] = Relationship(
        back_populates="project",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


UniqueConstraint(Project.__table__.c.folder_id, Project.__table__.c.name)
