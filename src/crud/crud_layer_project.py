# Standard library imports
import re
from uuid import UUID

# Third party imports
from fastapi import HTTPException, status
from pydantic import ValidationError, parse_obj_as
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.crud.crud_layer import layer as crud_layer
from src.db.models._link_model import LayerProjectLink
from src.db.models.layer import Layer
from src.schemas.layer import LayerType, FeatureGeometryType
from src.schemas.project import (
    layer_type_mapping_read,
    layer_type_mapping_update,
)
from typing import Union

# Local application imports
from .base import CRUDBase
from .crud_project import project as crud_project
from src.schemas.error import UnsupportedLayerTypeError, LayerNotFoundError


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

            layer_dict = layer.dict()
            # Delete id from layer
            del layer_dict["id"]
            # Update layer with layer project
            layer_dict.update(layer_project.dict())
            layer_project = layer_type_mapping_read[layer_type](**layer_dict)

            # Get feature cnt for all feature layers and tables
            if layer_project.type in [LayerType.feature.value, LayerType.table.value]:
                feature_cnt = await crud_layer.get_feature_cnt(
                    async_session=async_session, layer_project=layer_project
                )
                layer_project.total_count = feature_cnt["total_count"]
                layer_project.filtered_count = feature_cnt.get("filtered_count")

            # Write into correct schema
            layer_projects_schemas.append(layer_project)

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

    async def get_internal(
        self,
        async_session: AsyncSession,
        id: int,
        project_id: UUID,
        expected_layer_types: [Union[LayerType.feature, LayerType.table]] = [
            LayerType.feature
        ],
        expected_geometry_types: [FeatureGeometryType] = None,
    ):
        """Get internal layer from layer project"""

        # Get layer project
        query = select([Layer, LayerProjectLink]).where(
            LayerProjectLink.id == id,
            Layer.id == LayerProjectLink.layer_id,
            LayerProjectLink.project_id == project_id,
        )
        layer_project = await self.get_multi(
            db=async_session,
            query=query,
        )
        layer_project = await self.layer_projects_to_schemas(
            async_session, layer_project
        )

        # Make sure layer project exists
        if layer_project == []:
            raise LayerNotFoundError("Layer project not found")
        layer_project = layer_project[0]
        # Check if one of the expected layer types is given
        if layer_project.type not in expected_layer_types:
            raise UnsupportedLayerTypeError(
                f"Layer {layer_project.name} is not a {[layer_type.value for layer_type in expected_layer_types]} layer"
            )

        # Check if geometry type is correct
        if layer_project.type == LayerType.feature.value:
            if expected_geometry_types is not None:
                if layer_project.feature_layer_geometry_type not in expected_geometry_types:
                    raise UnsupportedLayerTypeError(
                        f"Layer {layer_project.name} is not a {[geom_type.value for geom_type in expected_geometry_types]} layer"
                    )

        return layer_project

    async def check_and_alter_layer_name(
        self, async_session: AsyncSession, project_id: UUID, layer_name: str
    ) -> str:
        """Check if layer name already exists in project and alter it like layer (n+1) if necessary"""

        # Regular expression to find layer names with a number
        pattern = re.compile(rf"^{re.escape(layer_name)} \((\d+)\)$")

        # Modify the query to select only the name attribute of layers that start with the given layer_name
        query = select(LayerProjectLink.name).where(
            LayerProjectLink.project_id == project_id,
            LayerProjectLink.name.like(f"{layer_name}%"),
        )

        # Execute the query
        result = await async_session.execute(query)
        layer_names = [row[0] for row in result.fetchall()]

        # Find the highest number (n) among the layer names using list comprehension
        numbers = [
            int(match.group(1))
            for name in layer_names
            if (match := pattern.match(name))
        ]
        highest_num = max(numbers, default=0)

        # Check if the base layer name exists
        base_name_exists = layer_name in layer_names

        # Construct the new layer name
        if base_name_exists or highest_num > 0:
            new_layer_name = f"{layer_name} ({highest_num + 1})"
        else:
            new_layer_name = layer_name

        return new_layer_name

    async def create(
        self,
        async_session: AsyncSession,
        project_id: UUID,
        layer_ids: [UUID],
    ):
        """Create a link between a project and a layer"""

        # Remove duplicate layer_ids
        layer_ids = list(set(layer_ids))

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
            )

            # Add to database
            layer_project = await CRUDBase(LayerProjectLink).create(
                async_session,
                obj_in=layer_project,
            )
            layer_project_ids.append(layer_project.id)

        # Get project to update layer order
        project = await crud_project.get(async_session, id=project_id)
        layer_order = project.layer_order
        # Add layer ids to the beginning of the list
        if layer_order is None:
            layer_order = layer_project_ids
        else:
            layer_order = layer_project_ids + layer_order

        # Update project layer order
        project = await crud_project.update(
            async_session,
            db_obj=project,
            obj_in={"layer_order": layer_order},
        )
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
        layer_project = model_type_read(**layer_dict)

        # Get feature cnt
        feature_cnt = await crud_layer.get_feature_cnt(
            async_session, layer_project=layer_project
        )
        layer_project.total_count = feature_cnt.get("total_count")
        layer_project.filtered_count = feature_cnt.get("filtered_count")
        return layer_project


layer_project = CRUDLayerProject(LayerProjectLink)
