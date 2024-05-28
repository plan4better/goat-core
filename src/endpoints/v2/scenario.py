from fastapi import APIRouter

router = APIRouter()


@router.get(
    "",
    summary="Get all scenarios",
    status_code=201,
)
async def get_scenarios():
    """Get all scenarios."""
    return "get_scenarios"


@router.get(
    "/{scenario_id}",
    summary="Get scenario by id",
    status_code=201,
)
async def get_scenario_by_id(scenario_id: int):
    """Get scenario by id."""
    return "get_scenario_by_id"


@router.post(
    "",
    summary="Create scenario",
    status_code=201,
)
async def create_scenario():
    """Create scenario."""
    return "create_scenario"


@router.put(
    "/{scenario_id}",
    summary="Update scenario",
    status_code=201,
)
async def update_scenario(scenario_id: int):
    """Update scenario."""
    return "update_scenario"
