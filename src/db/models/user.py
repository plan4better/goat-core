from typing import TYPE_CHECKING, List
from uuid import UUID

from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    Column,
    Field,
    Relationship,
    SQLModel,
)

if TYPE_CHECKING:
    from .folder import Folder
    from .job import Job
    from .scenario import Scenario
    from .system_setting import SystemSetting
    #from ._link_model import UserTeamLink


class User(SQLModel, table=True):
    __tablename__ = "user"
    __table_args__ = {"schema": "customer"}

    id: UUID = Field(
        sa_column=Column(UUID_PG(as_uuid=True), primary_key=True, nullable=False)
    )
    # Relationships
    scenarios: List["Scenario"] = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    folders: List["Folder"] = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    system_setting: "SystemSetting" = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    jobs: List["Job"] = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    # team_links: List["UserTeamLink"] = Relationship(
    #     back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    # )
