from typing import List
from uuid import UUID

from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, Field, Text, text, ARRAY
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from ._base_class import DateTimeBase
from src.schemas.job import JobType


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
    # TODO: Add other job types
    status: dict = Field(
        sa_column=Column(JSONB, nullable=False), description="Status of the job"
    )
