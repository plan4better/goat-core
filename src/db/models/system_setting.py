from enum import Enum
from uuid import UUID

from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Column, Field, SQLModel, Text

from src.db.models._base_class import DateTimeBase


class ClientThemeType(str, Enum):
    """Layer types that are supported."""

    dark = "dark"
    light = "light"


class LanguageType(str, Enum):
    """Layer types that are supported."""

    en = "en"
    de = "de"


class UnitType(str, Enum):
    """Layer types that are supported."""

    metric = "metric"
    imperial = "imperial"


class SystemSettingBase(SQLModel):
    client_theme: ClientThemeType = Field(sa_column=Column(Text, nullable=False))
    preferred_language: LanguageType = Field(sa_column=Column(Text, nullable=False))
    unit: UnitType = Field(sa_column=Column(Text, nullable=False))


class SystemSetting(SystemSettingBase, DateTimeBase, table=True):
    __tablename__ = "system_setting"
    __table_args__ = {"schema": "customer"}

    id: UUID | None = Field(
        sa_column=Column(UUID_PG(as_uuid=True), primary_key=True, nullable=False)
    )
    user_id: UUID = Field(sa_column=Column(UUID_PG(as_uuid=True), nullable=False))
