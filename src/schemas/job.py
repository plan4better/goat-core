from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class JobType(str, Enum):
    """Job types."""

    file_import = "file_import"
    join = "join"
    isochrone_active_mobility = "isochrone_active_mobility"
    isochrone_pt = "isochrone_pt"
    isochrone_car = "isochrone_car"
    oev_gueteklasse = "oev_gueteklasse"
    aggregate_point = "aggregate_point"
    aggregate_polygon = "aggregate_polygon"
    buffer = "buffer"
    trip_count_station = "trip_count_station"
    origin_destination = "origin_destination"


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
    upload: JobStep = {}
    migration: JobStep = {}


class JobStatusJoin(BaseModel):
    join: JobStep = {}


class JobStatusOevGueteklasse(BaseModel):
    station_category: JobStep = {}
    station_buffer: JobStep = {}


class JobStatusAggregationPoint(BaseModel):
    aggregation: JobStep = {}


class JobStatusAggregationPolygon(BaseModel):
    aggregation: JobStep = {}


class JobStatusIsochroneBase(BaseModel):
    isochrone: JobStep = {}


class JobStatusIsochroneActiveMobility(JobStatusIsochroneBase):
    pass


class JobStatusIsochronePT(JobStatusIsochroneBase):
    pass


class JobStatusIsochroneCar(JobStatusIsochroneBase):
    pass


class JobStatusBuffer(BaseModel):
    buffer: JobStep = {}


class JobStatusTripCountStation(BaseModel):
    trip_count_station: JobStep = {}

class OriginDestination(BaseModel):
    origin_destination: JobStep = {}


# Only add jobs here that are consisting of multiple steps
job_mapping = {
    JobType.file_import: JobStatusFileImport,
    JobType.join: JobStatusJoin,
    JobType.oev_gueteklasse: JobStatusOevGueteklasse,
    JobType.aggregate_point: JobStatusAggregationPoint,
    JobType.aggregate_polygon: JobStatusAggregationPolygon,
    JobType.isochrone_active_mobility: JobStatusIsochroneActiveMobility,
    JobType.isochrone_pt: JobStatusIsochronePT,
    JobType.isochrone_car: JobStatusIsochroneCar,
    JobType.buffer: JobStatusBuffer,
    JobType.trip_count_station: JobStatusTripCountStation,
    JobType.origin_destination: OriginDestination,
}
