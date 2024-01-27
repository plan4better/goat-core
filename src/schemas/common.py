from enum import Enum
from typing import List

from pydantic import UUID4, BaseModel, Field, validator, ValidationError
from pygeofilter.parsers.cql2_json import parse as cql2_json_parser


class OrderEnum(str, Enum):
    ascendent = "ascendent"
    descendent = "descendent"


class ContentIdList(BaseModel):
    ids: List[UUID4]

class CQLQuery(BaseModel):
    """Model for CQL query."""

    query: dict | None = Field(None, description="CQL query")

    # Validate using cql2_json_parser(query)
    @validator("query")
    def validate_query(cls, v):
        if v is None:
            return v
        try:
            cql2_json_parser(v)
        except Exception as e:
            raise ValidationError(f"Invalid CQL query: {e}")
        return v
