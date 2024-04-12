import asyncio
from uuid import uuid4
from src.crud.base import CRUDBase
from src.core.print import PrintMap
from sqlalchemy import select, text
from src.core.config import settings
from src.db.models.layer import Layer
from src.schemas.layer import LayerType
from src.db.models.project import Project
from src.db.session import session_manager
from sqlalchemy.ext.asyncio import AsyncSession
from src.db.models.system_task import SystemTask
from datetime import datetime, timedelta, timezone
from fastapi_pagination import Params as PaginationParams
from src.crud.crud_user_project import user_project as crud_user_project
from src.crud.crud_layer_project import layer_project as crud_layer_project


async def fetch_last_run_timestamp(async_session: AsyncSession) -> datetime:
    """Fetch the last run timestamp of the thumbnail update task."""

    crud_system_task = CRUDBase(SystemTask)
    result = await crud_system_task.get(async_session, "thumbnail_update")
    return result.last_run if result is not None else datetime.now(timezone.utc) - timedelta(minutes=5)


async def update_last_run_timestamp(async_session: AsyncSession, last_run: datetime):
    """Update the last run timestamp of the thumbnail update task."""

    await async_session.execute(
        text(
            """UPDATE customer.system_task
            SET last_run = :last_run WHERE id = 'thumbnail_update'""",
        ),
        {"last_run": last_run},
    )
    await async_session.commit()


async def fetch_projects_to_update(async_session: AsyncSession, last_run: datetime, page: int):
    """Fetch all projects which require a thumbnail update."""

    query = select(Project).where(Project.updated_at > last_run)
    return await CRUDBase(Project).get_multi(
        async_session,
        query=query,
        page_params=PaginationParams(page=page),
    )


async def fetch_layers_to_update(async_session: AsyncSession, last_run: datetime, page: int):
    """Fetch all layers which require a thumbnail update."""

    query = select(Layer).where(Layer.updated_at > last_run)
    return await CRUDBase(Layer).get_multi(
        async_session,
        query=query,
        page_params=PaginationParams(page=page),
    )


async def process_projects(async_session: AsyncSession, last_run: datetime):
    """Update thumbnails of projects."""

    # Process projects page-by-page
    page = 1
    projects = (await fetch_projects_to_update(async_session, last_run, page)).items
    while len(projects) > 0:
        for project in projects:
            user_project = await crud_user_project.get_by_multi_keys(
                async_session,
                keys={"user_id": project.user_id, "project_id": project.id},
            )
            layers_project = await crud_layer_project.get_layers(
                async_session=async_session, project_id=project.id
            )
            if user_project != [] and layers_project != []:
                try:
                    old_thumbnail_url = project.thumbnail_url

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

                    # Update project with thumbnail url bypassing the model to avoid the table getting a new updated at
                    await async_session.execute(
                        text(
                            """UPDATE customer.project
                            SET thumbnail_url = :thumbnail_url WHERE id = :id""",
                        ),
                        {"thumbnail_url": thumbnail_url, "id": project.id},
                    )
                    await async_session.commit()

                    # Delete old thumbnail from s3 if the thumbnail is not the default thumbnail
                    if (old_thumbnail_url and settings.THUMBNAIL_DIR_PROJECT in old_thumbnail_url):
                        settings.S3_CLIENT.delete_object(
                            Bucket=settings.AWS_S3_ASSETS_BUCKET,
                            Key=old_thumbnail_url.replace(
                                settings.ASSETS_URL + "/", ""
                            ),
                        )
                except Exception as e:
                    print(f"Error updating project thumbnail: {e}")

        # Fetch next page of projects to process
        page += 1
        projects = (await fetch_projects_to_update(async_session, last_run, page)).items


async def process_layers(async_session: AsyncSession, last_run: datetime):
    """Update thumbnails of layers."""

    # Process projects page-by-page
    page = 1
    layers = (await fetch_layers_to_update(async_session, last_run, page)).items
    while len(layers) > 0:
        for layer in layers:
            if (layer.type in (LayerType.feature, LayerType.table)):
                try:
                    old_thumbnail_url = layer.thumbnail_url

                    # Create thumbnail
                    print_map = PrintMap(async_session)
                    thumbnail_url = await print_map.create_layer_thumbnail(
                        layer=layer,
                        file_name=str(layer.id) + "_" + str(uuid4()) + ".png",
                    )

                    # Update layer with thumbnail url bypassing the model to avoid the table getting a new updated at
                    await async_session.execute(
                        text(
                            """UPDATE customer.layer
                            SET thumbnail_url = :thumbnail_url WHERE id = :id""",
                        ),
                        {"thumbnail_url": thumbnail_url, "id": layer.id},
                    )
                    await async_session.commit()

                    # Delete old thumbnail from s3 if the thumbnail is not the default thumbnail
                    if (old_thumbnail_url and settings.THUMBNAIL_DIR_LAYER in old_thumbnail_url):
                        settings.S3_CLIENT.delete_object(
                            Bucket=settings.AWS_S3_ASSETS_BUCKET,
                            Key=old_thumbnail_url.replace(settings.ASSETS_URL + "/", ""),
                        )
                except Exception as e:
                    print(f"Error updating layer thumbnail: {e}")

        # Fetch next page of projects to process
        page += 1
        layers = (await fetch_layers_to_update(async_session, last_run, page)).items


async def main():
    session_manager.init(settings.ASYNC_SQLALCHEMY_DATABASE_URI)
    async with session_manager.session() as async_session:
        # Get timestamp of last thumbnail update script run
        last_run = await fetch_last_run_timestamp(async_session)
        current_run = datetime.now(timezone.utc)

        # Update project thumbnails
        await process_projects(async_session, last_run)

        # Update layer thumbnails
        await process_layers(async_session, last_run)

        # Set last run timestamp to current time
        await update_last_run_timestamp(async_session, current_run)


if __name__ == "__main__":
    asyncio.run(main())
