from typing import Callable, List, Type
from fastapi import Depends, HTTPException, status
from fastapi_pagination import Params as PaginationParams
from pydantic import UUID4
from sqlalchemy import select
from sqlalchemy.sql import Select
from sqlmodel import SQLModel
from src.db.session import AsyncSession
from src.schemas.common import ContentIdList
from sqlalchemy import select, and_
from sqlalchemy.orm import contains_eager, selectinload


### Generic helper functions for content
async def create_content(
    async_session: AsyncSession,
    *,
    model: Type[SQLModel],
    crud_content: Callable,
    content_in: SQLModel,
    other_params: dict = {},
) -> SQLModel:
    """Create a new content."""
    content_in = model(**content_in.dict(exclude_none=True), **other_params)
    content = await crud_content.create(async_session, obj_in=content_in)
    return content


async def read_content_by_id(
    async_session: AsyncSession,
    id: UUID4,
    model: Type[SQLModel],
    crud_content: Callable,
    extra_fields: List = [],
) -> SQLModel:
    """Read a content by its ID."""
    content = await crud_content.get(async_session, id=id, extra_fields=extra_fields)

    if content is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"{model.__name__} not found"
        )

    return content


async def read_contents_by_ids(
    async_session: AsyncSession,
    ids: ContentIdList,
    model: Type[SQLModel],
    crud_content: Callable,
    page_params: PaginationParams = Depends(),
) -> Select:
    """Read contents by their IDs."""
    # Read contents by IDs
    query = select(model).where(model.id.in_(ids.ids))
    contents = await crud_content.get_multi(
        async_session, query=query, page_params=page_params
    )

    # Check if all contents were found
    if len(contents.items) != len(ids.ids):
        not_found_contents = [
            content_id
            for content_id in ids.ids
            if content_id not in [content.id for content in contents.items]
        ]
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{model.__name__} with {not_found_contents} not found",
        )

    return contents


async def update_content_by_id(
    async_session: AsyncSession,
    id: UUID4,
    model: Type[SQLModel],
    crud_content: Callable,
    content_in: SQLModel,
) -> SQLModel:
    """Update a content by its ID."""
    db_obj = await crud_content.get(async_session, id=id)
    if db_obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"{model.__name__} not found"
        )
    content = await crud_content.update(async_session, db_obj=db_obj, obj_in=content_in)
    return content


async def delete_content_by_id(
    async_session: AsyncSession,
    id: UUID4,
    model: Type[SQLModel],
    crud_content: Callable,
) -> None:
    """Delete a content by its ID."""
    db_obj = await crud_content.get(async_session, id=id)
    if db_obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"{model.__name__} not found"
        )
    await crud_content.remove(async_session, id=id)
    return


def create_query_shared_content(
    model,
    team_link_model,
    organization_link_model,
    team_model,
    organization_model,
    role_model,
    filters,
    team_id=None,
    organization_id=None,
):
    """
    Creates a dynamic query for a given model (Layer or Project) and its associated team and organization links.

    :param model: The main model (Layer or Project)
    :param team_link_model: The model linking the main model with teams (LayerTeamLink or ProjectTeamLink)
    :param organization_link_model: The model linking the main model with organizations (LayerOrganizationLink or ProjectOrganizationLink)
    :param team_model: The Team model
    :param organization_model: The Organization model
    :param role_model: The Role model
    :param filters: Additional filters to apply
    :param team_id: ID of the team (optional)
    :param organization_id: ID of the organization (optional)
    :return: A SQLAlchemy query object
    """

    # Determine the link field based on the model
    link_field = f"{model.__tablename__}_id"

    if team_id:
        query = (
            select(
                model,
                role_model.name,
                team_model.name,
                team_model.id,
                team_model.avatar,
            )
            .join(
                team_link_model, getattr(team_link_model, link_field) == model.id
            )  # Dynamically replace `layer_id` or `project_id`
            .join(role_model, team_link_model.role_id == role_model.id)
            .join(team_model, team_link_model.team_id == team_model.id)
            .where(
                and_(
                    team_link_model.team_id == team_id,
                    *filters,
                )
            )
            .options(
                contains_eager(getattr(model, "team_links"))
            )  # Adjust field as needed for relationships
        )
    elif organization_id:
        query = (
            select(
                model,
                role_model.name,
                organization_model.name,
                organization_model.id,
                organization_model.avatar,
            )
            .join(
                organization_link_model,
                getattr(organization_link_model, link_field) == model.id,
            )  # Dynamically replace `layer_id` or `project_id`
            .join(role_model, organization_link_model.role_id == role_model.id)
            .join(
                organization_model,
                organization_link_model.organization_id == organization_model.id,
            )
            .where(
                and_(
                    organization_link_model.organization_id == organization_id,
                    *filters,
                )
            )
            .options(
                contains_eager(getattr(model, "organization_links"))
            )  # Adjust field as needed for relationships
        )
    else:
        query = (
            select(model)
            .outerjoin(
                team_link_model, getattr(team_link_model, link_field) == model.id
            )  # Dynamically replace `layer_id` or `project_id`
            .outerjoin(
                organization_link_model,
                getattr(organization_link_model, link_field) == model.id,
            )  # Dynamically replace `layer_id` or `project_id`
            .where(and_(*filters))
            .options(
                selectinload(getattr(model, "team_links")).selectinload(
                    getattr(team_link_model, "team")
                ),  # Adjust fields as needed
                selectinload(getattr(model, "organization_links")).selectinload(
                    getattr(organization_link_model, "organization")
                ),  # Adjust fields as needed
            )
        )

    return query


def build_shared_with_object(
    items,
    role_mapping,
    team_key="team_links",
    org_key="organization_links",
    model_name="layer",
    team_id=None,
    organization_id=None,
):
    """
    Builds the shared_with object for both Layer and Project models.

    :param items: The list of Layer or Project items
    :param role_mapping: The mapping of role IDs to role names
    :param team_key: The attribute name for team links (default is "team_links")
    :param org_key: The attribute name for organization links (default is "organization_links")
    :param model_name: The name of the model ("layer" or "project")
    :param team_id: Optional ID for team-specific sharing
    :param organization_id: Optional ID for organization-specific sharing
    :return: A list of dictionaries containing the model and the shared_with data
    """
    result_arr = []

    # Determine shared_with key
    if team_id:
        shared_with_key = "teams"
    elif organization_id:
        shared_with_key = "organizations"

    # Case where shared_with is for a specific team or organization
    if team_id or organization_id:
        for item in items:
            shared_with = {shared_with_key: []}
            shared_with[shared_with_key].append(
                {
                    "role": item[1],  # Role name
                    "id": item[3],  # Team or Organization ID
                    "name": item[2],  # Team or Organization name
                    "avatar": item[4],  # Team or Organization avatar
                }
            )
            result_arr.append({**item[0].dict(), "shared_with": shared_with})
    else:
        # Case where shared_with includes both teams and organizations
        for item in items:
            shared_with = {"teams": [], "organizations": []}

            # Process team links
            team_links = getattr(item, team_key)
            for team_link in team_links:
                shared_with["teams"].append(
                    {
                        "role": role_mapping[
                            team_link.role_id
                        ],  # Role based on role_mapping
                        "id": team_link.team.id,
                        "name": team_link.team.name,
                        "avatar": team_link.team.avatar,
                    }
                )

            # Process organization links
            organization_links = getattr(item, org_key)
            for organization_link in organization_links:
                shared_with["organizations"].append(
                    {
                        "role": role_mapping[
                            organization_link.role_id
                        ],  # Role based on role_mapping
                        "id": organization_link.organization.id,
                        "name": organization_link.organization.name,
                        "avatar": organization_link.organization.avatar,
                    }
                )

            result_arr.append({**item.dict(), "shared_with": shared_with})

    return result_arr
