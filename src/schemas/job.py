from enum import Enum
from pydantic import BaseModel
from datetime import datetime

# TODO: Add other job types

class JobType(str, Enum):
    """Job types."""
    file_validate = "file_validate"
    file_import = "file_import"

class JobStatusType(str, Enum):
    """Status types."""

    pending = "pending"
    running = "running"
    finished = "finished"
    failed = "failed"
    timeout = "timeout"
    killed = "killed"

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


class JobStatusFileImport(BaseModel):
    """Job layer upload attribute types."""

    upload: JobStep = {}
    migration: JobStep = {}

job_mapping = {
    JobType.file_import: JobStatusFileImport
}