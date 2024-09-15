from uuid import UUID

from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.content import (
    create_query_shared_content,
    update_content_by_id,
    build_shared_with_object,
)
from src.crud.base import CRUDBase
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.crud.crud_user_project import user_project as crud_user_project
from src.db.models import (
    Organization,
    Project,
    ProjectOrganizationLink,
    ProjectTeamLink,
    Role,
    Team,
)
from src.db.models._link_model import UserProjectLink
from src.schemas.common import OrderEnum
from src.schemas.project import (
    InitialViewState,
    IProjectBaseUpdate,
    IProjectCreate,
    IProjectRead,
)


class CRUDProject(CRUDBase):
    async def create(
        self,
        async_session: AsyncSession,
        project_in: IProjectCreate,
        initial_view_state: InitialViewState,
    ) -> IProjectRead:
        """Create project"""

        # Create project
        project = await CRUDBase(Project).create(
            db=async_session,
            obj_in=project_in,
        )
        # Default initial view state
        initial_view_state = {
            "zoom": 5,
            "pitch": 0,
            "bearing": 0,
            "latitude": 51.01364693631891,
            "max_zoom": 20,
            "min_zoom": 0,
            "longitude": 9.576740589534126,
        }

        # Create link between user and project for initial view state
        await crud_user_project.create(
            async_session,
            obj_in=UserProjectLink(
                user_id=project.user_id,
                project_id=project.id,
                initial_view_state=initial_view_state,
            ),
        )
        # If not in testing environment add default layers to project
        if not settings.TESTING:
            # Add network layer to project
            await crud_layer_project.create(
                async_session=async_session,
                project_id=project.id,
                layer_ids=[settings.BASE_STREET_NETWORK],
            )
        # Doing unneeded type conversion to make sure the relations of project are not loaded
        return IProjectRead(**project.dict())

    async def get_projects(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        page_params: PaginationParams,
        folder_id: UUID = None,
        search: str = None,
        order_by: str = None,
        order: OrderEnum = None,
        ids: list = None,
        team_id: UUID = None,
        organization_id: UUID = None,
    ) -> Page[IProjectRead]:
        """Get projects for a user and folder"""

        # If ids are provided apply filter by ids, otherwise apply filter by folder_id and user_id
        if team_id or organization_id:
            filters = []
        elif folder_id:
            filters = [
                Project.user_id == user_id,
                Project.folder_id == folder_id,
            ]
        else:
            filters = [Project.user_id == user_id]

        if ids:
            query = select(Project).where(Project.id.in_(ids))
        else:
            query = create_query_shared_content(
                Project,
                ProjectTeamLink,
                ProjectOrganizationLink,
                Team,
                Organization,
                Role,
                filters,
                team_id=team_id,
                organization_id=organization_id,
            )

        # Get roles
        roles = await CRUDBase(Role).get_all(
            async_session,
        )
        role_mapping = {role.id: role.name for role in roles}

        # Get projects
        projects = await self.get_multi(
            async_session,
            query=query,
            page_params=page_params,
            search_text={"name": search} if search else {},
            order_by=order_by,
            order=order,
        )
        projects.items = build_shared_with_object(
            projects.items,
            role_mapping,
            team_key="team_links",
            org_key="organization_links",
            model_name="project",
            team_id=team_id,
            organization_id=organization_id,
        )
        return projects

    async def update_base(
        self, async_session: AsyncSession, id: UUID, project: IProjectBaseUpdate
    ) -> IProjectRead:
        """Update project base"""

        # Update project
        project = await update_content_by_id(
            async_session=async_session,
            id=id,
            model=Project,
            crud_content=self,
            content_in=project,
        )

        return IProjectRead(**project.dict())


project = CRUDProject(Project)
