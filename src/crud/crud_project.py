from uuid import UUID
import pytz
from fastapi_pagination import Page, Params as PaginationParams
from sqlalchemy import and_, select, update, text
from sqlalchemy.ext.asyncio import AsyncSession
from src.core.content import update_content_by_id
from src.db.models.project import Project
from src.schemas.common import OrderEnum
from src.schemas.project import (
    IProjectBaseUpdate,
    IProjectRead,
    IProjectCreate,
    InitialViewState,
)
from src.crud.crud_user_project import user_project as crud_user_project
from src.crud.crud_layer_project import layer_project as crud_layer_project
from src.crud.base import CRUDBase
from datetime import datetime, timedelta
from src.core.config import settings
from src.db.models._link_model import LayerProjectLink, UserProjectLink
# Import PrintMap only if not in test mode
if settings.TEST_MODE is False:
    from src.core.print import PrintMap

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
        # Create link between user and project for initial view state
        user_project = await crud_user_project.create(
            async_session,
            obj_in=UserProjectLink(
                user_id=project.user_id,
                project_id=project.id,
                initial_view_state=initial_view_state,
            ),
        )
        # Create thumbnail
        print_map = PrintMap(async_session)
        thumbnail_url = await print_map.create_project_thumbnail(
            project=project,
            initial_view_state=user_project.initial_view_state,
            layers_project=[],
            file_name=str(project.id)
            + project.updated_at.strftime("_%Y-%m-%d_%H-%M-%S-%f")[:-3]
            + ".png",
        )

        # Update project with thumbnail url
        project = await self.update(
            async_session,
            db_obj=project,
            obj_in={"thumbnail_url": thumbnail_url},
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
    ) -> Page[IProjectRead]:
        """Get projects for a user and folder"""

        # If ids are provided apply filter by ids, otherwise apply filter by folder_id and user_id
        if ids:
            query = select(Project).where(Project.id.in_(ids))
        else:
            if not folder_id:
                query = select(Project).where(Project.user_id == user_id)
            else:
                query = select(Project).where(
                    and_(
                        Project.user_id == user_id,
                        Project.folder_id == folder_id,
                    )
                )

        projects = await self.get_multi(
            async_session,
            query=query,
            page_params=page_params,
            search_text={"name": search} if search else {},
            order_by=order_by,
            order=order,
        )

        # Check for projects that have the thumbnail older then updated at
        cnt = 0
        for project in projects.items:
            old_thumbnail_url = project.thumbnail_url
            thumbnail_updated_at = old_thumbnail_url.replace(
                settings.ASSETS_URL
                + "/"
                + settings.THUMBNAIL_DIR_PROJECT
                + "/"
                + str(project.id)
                + "_",
                "",
            ).replace(".png", "")
            try:
                date_to_check = datetime.strptime(
                    thumbnail_updated_at, "%Y-%m-%d_%H-%M-%S-%f"
                )
            except ValueError:
                print("Thumbnail name is not in correct format")
                continue

            if settings.TEST_MODE is False:
                if date_to_check < project.updated_at.astimezone(pytz.UTC).replace(
                    tzinfo=None
                ):
                    user_project = await crud_user_project.get_by_multi_keys(
                        async_session,
                        keys={"user_id": user_id, "project_id": project.id},
                    )
                    layers_project = await crud_layer_project.get_layers(
                        async_session=async_session, project_id=project.id
                    )
                    if user_project != [] and layers_project != []:
                        # Create thumbnail
                        print_map = PrintMap(async_session)
                        thumbnail_url = await print_map.create_project_thumbnail(
                            project=project,
                            initial_view_state=user_project[0].initial_view_state,
                            layers_project=layers_project,
                            file_name=str(project.id)
                            + project.updated_at.strftime("_%Y-%m-%d_%H-%M-%S-%f")
                            + ".png",
                        )
                        # Update project with thumbnail url by passing the model to avoid the table to get a new updated at
                        await async_session.execute(
                            text(
                                """UPDATE customer.project
                                SET thumbnail_url = :thumbnail_url WHERE id = :id""",
                            ),
                            {"thumbnail_url": thumbnail_url, "id": project.id},
                        )
                        await async_session.commit()

                        # Update returned project
                        projects.items[cnt].thumbnail_url = thumbnail_url

                        # Delete old thumbnail url
                        settings.S3_CLIENT.delete_object(
                            Bucket=settings.AWS_S3_ASSETS_BUCKET,
                            Key=old_thumbnail_url.replace(
                                settings.ASSETS_URL + "/", ""
                            ),
                        )
            cnt += 1
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
