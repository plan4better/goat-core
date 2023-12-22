from enum import Enum
from pydantic import BaseModel
from datetime import datetime

# TODO: Add other job types


class JobType(str, Enum):
    """Job types."""

    file_import = "file_import"
    join = "join"
    isochrone_active_mobility = "isochrone_active_mobility"
    oev_gueteklasse = "oev_gueteklasse"


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

    type: MsgType = MsgType.info
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

class JobStatusJoin(BaseModel):
    """Job layer upload attribute types."""

    join: JobStep = {}

class JobStatusOevGueteklasse(BaseModel):
    """Job layer upload attribute types."""

    station_category: JobStep = {}
    station_buffer: JobStep = {}


# Only add jobs here that are consisting of multiple steps
job_mapping = {JobType.file_import: JobStatusFileImport, JobType.join: JobStatusJoin, JobType.oev_gueteklasse: JobStatusOevGueteklasse}
