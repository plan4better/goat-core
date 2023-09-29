from src.crud.base import CRUDBase
from src.db.models.job import Job
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import and_, select
from uuid import UUID
from datetime import datetime
from fastapi_pagination import Params as PaginationParams
from src.schemas.common import OrderEnum
from fastapi import HTTPException, status
from src.schemas.job import JobType


class CRUDJob(CRUDBase):
    async def get_by_date(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        page_params: PaginationParams,
        project_id: UUID,
        job_type: JobType,
        start_data: datetime,
        end_data: datetime,
        read: bool,
        order_by: str,
        order: OrderEnum,
    ):
        """Get all jobs by date."""

        and_conditions = [Job.user_id == user_id]
        # User start and end date to filter jobs if available else get all jobs by user_id.
        if start_data and end_data:
            # Check if start date is before end date.
            if start_data > end_data:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Start date must be before end date.",
                )
            and_conditions.extend(
                [
                    Job.created_at >= start_data,
                    Job.created_at <= end_data,
                ]
            )
        if job_type:
            and_conditions.append(Job.type.in_(job_type))

        if read:
            and_conditions.append(Job.read == read)

        if project_id:
            and_conditions.append(Job.project_id == project_id)

        query = select(Job).where(and_(*and_conditions))
        jobs = await self.get_multi(
            async_session,
            query=query,
            page_params=page_params,
            order_by=order_by,
            order=order,
        )
        return jobs


job = CRUDJob(Job)
