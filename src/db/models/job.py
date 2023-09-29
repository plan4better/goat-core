from typing import List, TYPE_CHECKING
from uuid import UUID

from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, Field, Text, text, ARRAY, Boolean, ForeignKey, Relationship
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from ._base_class import DateTimeBase
from src.schemas.job import JobType

if TYPE_CHECKING:
    from .user import User
    from .project import Project


class Job(DateTimeBase, table=True):
    """Analysis Request model."""

    __tablename__ = "job"
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
        description="User ID of the user who created the job",
    )
    project_id: UUID | None = Field(
        sa_column=Column(UUID_PG(as_uuid=True), ForeignKey("customer.project.id"), nullable=True),
        description="Project ID of the project the job belongs to",
    )
    type: JobType = Field(sa_column=Column(Text, nullable=False), description="Type of the job")
    layer_ids: List[UUID] | None = Field(
        sa_column=Column(
            ARRAY(UUID_PG()),
            nullable=True,
            index=True,
        ),
        description="Layer IDs that are produced by the job",
    )
    payload: dict = Field(
        sa_column=Column(JSONB, nullable=False), description="Payload of the request"
    )
    status: dict = Field(sa_column=Column(JSONB, nullable=False), description="Status of the job")
    read: bool | None = Field(
        sa_column=Column(Boolean, nullable=False, server_default="False"),
        description="Whether the user has marked the job as read",
    )

    # Relationships
    user: "User" = Relationship(back_populates="jobs")
    project: "Project" = Relationship(back_populates="jobs")
