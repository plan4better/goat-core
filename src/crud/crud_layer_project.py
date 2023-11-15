# Standard library imports
from uuid import UUID

# Third party imports
from fastapi import HTTPException, status
from pydantic import ValidationError, parse_obj_as
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.crud.crud_layer import layer as crud_layer
from src.db.models._link_model import LayerProjectLink
from src.db.models.layer import Layer
from src.schemas.layer import LayerType
from src.schemas.project import (
    layer_type_mapping_read,
    layer_type_mapping_update,
)

# Local application imports
from .base import CRUDBase


class CRUDLayerProject(CRUDBase):
    async def layer_projects_to_schemas(
        self, async_session: AsyncSession, layers_project
    ):
        """Convert layer projects to schemas."""
        layer_projects_schemas = []

        # Loop through layer and layer projects
        for layer_project_tuple in layers_project:
            layer = layer_project_tuple[0]
            layer_project = layer_project_tuple[1]

            # Get layer type
            if layer.feature_layer_type is not None:
                layer_type = layer.type + "_" + layer.feature_layer_type
            else:
                layer_type = layer.type

            # Convert to dicts and update layer
            layer = layer.dict()
            layer_project = layer_project.dict()
            # Delete id from layer
            del layer["id"]

            # Update layer
            layer.update(layer_project)

            # Get feature cnt for all feature layers and tables
            if layer["type"] in [LayerType.feature.value, LayerType.table.value]:
                feature_cnt = await crud_layer.get_feature_cnt(
                    async_session=async_session, layer_project=layer
                )
            else:
                feature_cnt = {}

            # Write into correct schema
            layer_projects_schemas.append(
                layer_type_mapping_read[layer_type](**layer, **feature_cnt)
            )

        return layer_projects_schemas

    async def get_layers(
        self,
        async_session: AsyncSession,
        project_id: UUID,
    ):
        """Get all layers from a project"""

        # Get all layers from project
        query = select([Layer, LayerProjectLink]).where(
            LayerProjectLink.project_id == project_id,
            Layer.id == LayerProjectLink.layer_id,
        )

        # Get all layers from project
        layers_project = await self.get_multi(
            async_session,
            query=query,
        )
        layer_projects_to_schemas = await self.layer_projects_to_schemas(
            async_session, layers_project
        )
        return layer_projects_to_schemas

    async def get_by_ids(self, async_session: AsyncSession, ids: [int]):
        """Get all layer projects links by the ids"""

        # Get all layers from project by id
        query = select([Layer, LayerProjectLink]).where(
            LayerProjectLink.id.in_(ids),
            Layer.id == LayerProjectLink.layer_id,
        )

        # Get all layers from project
        layer_projects = await self.get_multi(
            async_session,
            query=query,
        )
        layer_projects = await self.layer_projects_to_schemas(
            async_session, layer_projects
        )
        return layer_projects

    async def create(
        self,
        async_session: AsyncSession,
        project_id: UUID,
        layer_ids: [UUID],
    ):
        """Create a link between a project and a layer"""

        # Get number of layers in project
        layer_projects = await self.get_multi(
            async_session,
            query=select(LayerProjectLink).where(
                LayerProjectLink.project_id == project_id
            ),
        )

        # Check if maximum number of layers in project is reached. In case layer_project is empty just go on.
        if layer_projects != []:
            if len(layer_projects) + len(layer_ids) >= 300:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Maximum number of layers in project reached",
                )
            z_index = (
                max([layer_project[0].z_index for layer_project in layer_projects]) + 1
            )
        else:
            z_index = 0

        # Get layer from catalog
        layers = await crud_layer.get_multi(
            async_session,
            query=select(Layer).where(Layer.id.in_(layer_ids)),
        )

        if len(layers) != len(layer_ids):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="One or several Layers were not found",
            )

        # Define array for layer project ids
        layer_project_ids = []

        # Create link between project and layer
        for layer in layers:
            layer = layer[0]

            # Check if layer with same name and ID already exists in project. Then the layer should be duplicated with a new name.
            if layer_projects != []:
                if layer.name in [
                    layer_project[0].name for layer_project in layer_projects
                ]:
                    layer.name = "Copy from " + layer.name

            # Create layer project link
            layer_project = LayerProjectLink(
                project_id=project_id,
                layer_id=layer.id,
                name=layer.name,
                properties=layer.properties,
                other_properties=layer.other_properties,
                z_index=z_index,
            )

            # Add to database
            layer_project = await CRUDBase(LayerProjectLink).create(
                async_session,
                obj_in=layer_project,
            )
            layer_project_ids.append(layer_project.id)

            # Increase z-index
            z_index += 1

        layers = await self.get_by_ids(async_session, ids=layer_project_ids)
        return layers

    async def update(
        self,
        async_session: AsyncSession,
        id: int,
        layer_in: dict,
    ):
        """Update a link between a project and a layer"""

        # Get layer project
        layer_project_old = await self.get(
            async_session,
            id=id,
        )
        layer_id = layer_project_old.layer_id

        # Get base layer object
        layer = await crud_layer.get(async_session, id=layer_id)
        layer_dict = layer.dict()

        # Get right schema for respective layer type
        if layer.feature_layer_type is not None:
            model_type_update = layer_type_mapping_update.get(
                layer.type + "_" + layer.feature_layer_type
            )
            model_type_read = layer_type_mapping_read.get(
                layer.type + "_" + layer.feature_layer_type
            )
        else:
            model_type_update = layer_type_mapping_update.get(layer.type)
            model_type_read = layer_type_mapping_read.get(layer.type)

        # Parse and validate the data against the model
        try:
            layer_in = parse_obj_as(model_type_update, layer_in)
        except ValidationError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(e),
            )

        if layer_project_old is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Layer project not found"
            )

        # Update layer project
        layer_project = await CRUDBase(LayerProjectLink).update(
            async_session,
            db_obj=layer_project_old,
            obj_in=layer_in,
        )
        layer_project_dict = layer_project.dict()
        del layer_project_dict["id"]
        # Update layer
        layer_dict.update(layer_project_dict)

        # Get feature cnt
        feature_cnt = await crud_layer.get_feature_cnt(
            async_session, layer_project=layer_dict
        )
        return model_type_read(**layer_dict, **feature_cnt)


layer_project = CRUDLayerProject(LayerProjectLink)
