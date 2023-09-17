from fastapi import APIRouter, Depends, Path
from src.crud.crud_job import job as crud_job
from src.db.models.job import Job
from sqlalchemy.ext.asyncio import AsyncSession
from src.endpoints.deps import get_db
from uuid import UUID

router = APIRouter()


@router.get(
    "/{id}",
    response_model=Job,
    response_model_exclude_none=True,
    status_code=201,
)
async def get_job(
    async_session: AsyncSession = Depends(get_db),
    id: UUID = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
):
    """Retrieve a job by its ID."""
    job = await crud_job.get(db=async_session, id=id)
    return job

#TODO: Get jobs by user_id and project_id