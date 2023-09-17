from src.crud.base import CRUDBase
from src.db.models.job import Job


class CRUDJob(CRUDBase):
    pass

job = CRUDJob(Job)
