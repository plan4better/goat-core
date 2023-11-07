from .base import CRUDBase
from src.db.models._link_model import LayerProjectLink
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from src.crud.crud_layer import layer as crud_layer
from sqlalchemy import select
from src.db.models.layer import Layer
from fastapi import HTTPException, status
from src.schemas.project import (
    ITileLayerProjectUpdate,
    IFeatureLayerStandardProjectUpdate,
    IFeatureLayerIndicatorProjectUpdate,
    IFeatureLayerScenarioProjectUpdate,
    IImageryLayerProjectUpdate,
    ITableLayerProjectUpdate,
    layer_type_mapping_read,
    layer_type_mapping_update,
)
from src.schemas.layer import LayerType, UserDataGeomType
from pydantic import parse_obj_as, ValidationError
from pygeofilter.parsers.cql2_json import parse as cql2_json_parser

class CRUDLayerProject(CRUDBase):
    async def layer_projects_to_schemas(self, async_session: AsyncSession, layers_project):
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

            # Delete id from layer project
            del layer_project["id"]

            # Update layer
            layer.update(layer_project)

            # Get feature cnt for all feature layers and tables
            feature_cnt = await crud_layer.get_feature_cnt(
                async_session=async_session, layer=layer
            )
            # Write into correct schema
            layer_projects_schemas.append(layer_type_mapping_read[layer_type](**layer, **feature_cnt))

        return layer_projects_schemas

    async def get_layers(
        self,
        async_session: AsyncSession,
        project_id: UUID,
    ):
        """Get all layers from a project"""

        # Get all layers from project
        query = select([Layer, LayerProjectLink]).where(
            LayerProjectLink.project_id == project_id, Layer.id == LayerProjectLink.layer_id
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

    async def get_by_ids(
        self,
        async_session: AsyncSession,
        project_id: UUID,
        layer_ids: [UUID],
    ):
        # Get all layers from project by id
        query = select([Layer, LayerProjectLink]).where(
            LayerProjectLink.project_id == project_id,
            Layer.id == LayerProjectLink.layer_id,
            Layer.id.in_(layer_ids),
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
            query=select(LayerProjectLink).where(LayerProjectLink.project_id == project_id),
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

        # Create link between project and layer
        for layer in layers:
            layer = layer[0]
            layer_project = LayerProjectLink(
                project_id=project_id, layer_id=layer.id, name=layer.name, query={}
            )
            # Add style if exists
            if layer.style is not None:
                layer_project.style = layer.style

            # Add to database
            await CRUDBase(LayerProjectLink).create(
                async_session,
                obj_in=layer_project,
            )

        layers = await self.get_by_ids(async_session, project_id, layer_ids)
        return layers

    async def update(
        self,
        async_session: AsyncSession,
        project_id: UUID,
        layer_id: UUID,
        layer_in: dict,
    ):
        """Update a link between a project and a layer"""

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
        # Validate query
        if layer_in.query:
            try:
                cql2_json_parser(layer_in.query)
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="CQL filter is not valid",
                )

        # Get layer project
        layer_project_old = await self.get_by_multi_keys(
            async_session,
            keys={"project_id": project_id, "layer_id": layer_id},
        )
        if layer_project_old == []:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Layer project not found"
            )

        # Update layer project
        layer_project = await CRUDBase(LayerProjectLink).update(
            async_session,
            db_obj=layer_project_old[0],
            obj_in=layer_in,
        )
        layer_project_dict = layer_project.dict()
        del layer_project_dict["id"]
        # Update layer
        layer_dict.update(layer_project_dict)

        # Get feature cnt
        feature_cnt = await crud_layer.get_feature_cnt(async_session, layer=layer_dict)
        return model_type_read(**layer_dict, **feature_cnt)


layer_project = CRUDLayerProject(LayerProjectLink)
