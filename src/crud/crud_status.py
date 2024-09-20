from src.crud.base import CRUDBase
from src.db.models.status import Status


class CRUDStatus(CRUDBase):
    pass


status = CRUDStatus(Status)
