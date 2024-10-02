from datetime import datetime, timezone
from typing import Any, List, Optional
from uuid import UUID

from sqlalchemy import ARRAY, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlmodel import Column, DateTime, Field, SQLModel, text
from src.core.config import settings


class DateTimeBase(SQLModel):
    """Base class for models with created_at and updated_at fields."""

    updated_at: Optional[datetime] = Field(
        sa_column=Column(
            DateTime(timezone=True), nullable=False, onupdate=datetime.utcnow
        ),
        default_factory=lambda: datetime.now(timezone.utc),
    )
    created_at: Optional[datetime] = Field(
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text(
                """to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SSOF')::timestamptz"""
            ),
        )
    )


class ContentBaseAttributes(SQLModel):
    """Base model for content attributes."""

    folder_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.folder.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Layer folder ID",
    )
    name: str = Field(
        sa_column=Column(Text, nullable=False), description="Layer name", max_length=255
    )
    description: str | None = Field(
        sa_column=Column(Text), description="Layer description", max_length=2000
    )
    tags: List[str] | None = Field(
        sa_column=Column(ARRAY(Text()), nullable=True), description="Layer tags"
    )


content_base_example = {
    "folder_id": "c97b577f-7f8b-4713-8250-1518e189e822",
    "name": "Layer name",
    "description": "Layer description",
    "tags": ["tag1", "tag2"],
}


@as_declarative()
class Base:
    id: Any
    __name__: str

    # Generate __tablename__ automatically
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()
