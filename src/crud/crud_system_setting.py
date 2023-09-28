from src.db.models.system_setting import SystemSetting
from src.schemas.system_setting import (
    SystemSettingsCreate,
    SystemSettingsRead,
    SystemSettingsUpdate,
)

from .base import CRUDBase


class CRUDSystemSetting(CRUDBase[SystemSettingsRead, SystemSettingsCreate, SystemSettingsUpdate]):
    pass


system_setting = CRUDSystemSetting(SystemSetting)
