from fastapi import APIRouter

from . import folder, job, layer, project, report, system, user, active_mobility, motorized_mobility, tool

router = APIRouter()

router.include_router(user.router, prefix="/user", tags=["User"])
router.include_router(folder.router, prefix="/folder", tags=["Folder"])
router.include_router(layer.router, prefix="/layer", tags=["Layer"])
router.include_router(project.router, prefix="/project", tags=["Project"])
router.include_router(report.router, prefix="/report", tags=["Report"])
router.include_router(job.router, prefix="/job", tags=["Job"])
router.include_router(system.router, prefix="/system", tags=["System Settings"])
router.include_router(
    active_mobility.router, prefix="/active-mobility", tags=["Active Mobility Indicators"]
)
router.include_router(motorized_mobility.router, prefix="/motorized-mobility", tags=["Motorized Mobility Indicators"])
router.include_router(
    tool.router, prefix="/tool", tags=["Toolbox"]
)
