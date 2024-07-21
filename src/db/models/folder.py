from uuid import UUID
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    Column,
    Field,
    Relationship,
    Text,
    text,
    UniqueConstraint
)
from typing import List
from src.db.models._base_class import DateTimeBase
from src.db.models.user import User
from src.db.models.layer import Layer

class Folder(DateTimeBase, table=True):
    __tablename__ = "folder"
    __table_args__ = {"schema": "customer"}

    id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        )
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey("customer.user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Folder owner ID",
    )
    name: str = Field(sa_column=Column(Text, nullable=False), description="Folder name", max_length=255)

    # Relationships
    user: "User" = Relationship(back_populates="folders")
    layers: List["Layer"] = Relationship(back_populates="folder", sa_relationship_kwargs={"cascade": "all, delete-orphan"})

# Constraints
UniqueConstraint(Folder.__table__.c.user_id, Folder.__table__.c.name)