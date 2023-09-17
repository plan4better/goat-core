from enum import Enum
from pydantic import BaseModel
from datetime import datetime

# TODO: Add other job types

class JobType(str, Enum):
    """Job types."""
    layer_upload = "layer_upload"

class JobStatusType(str, Enum):
    """Status types."""

    pending = "pending"
    running = "running"
    finished = "finished"
    failed = "failed"

class MsgType(str, Enum):
    """Message types."""

    info = "info"
    warning = "warning"
    error = "error"

class Msg(BaseModel):
    """Message attribute types."""

    type: MsgType
    text: str

class JobStep(BaseModel):
    """Job step attribute types."""

    status: JobStatusType = JobStatusType.pending.value
    timestamp_start: datetime | None
    timestamp_end: datetime | None
    msg: Msg | None

class JobStatusLayerUpload(BaseModel):
    """Job layer upload attribute types."""

    validation: JobStep = {}
    upload: JobStep = {}
    migration: JobStep = {}

job_mapping = {
    JobType.layer_upload: JobStatusLayerUpload
}