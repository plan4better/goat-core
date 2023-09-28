from src.db.models.folder import Folder
from src.schemas.folder import FolderCreate, FolderUpdate

from .base import CRUDBase


class CRUDFolder(CRUDBase[Folder, FolderCreate, FolderUpdate]):
    pass


folder = CRUDFolder(Folder)
