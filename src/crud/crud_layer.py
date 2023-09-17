import os
from uuid import UUID, uuid4
from fastapi import UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from src.core.layer import OGRFileUpload
from src.crud.base import CRUDBase
from src.crud.crud_job import job as crud_job
from src.db.models.layer import Layer
from src.schemas.job import JobStatusType
from src.schemas.layer import (
    FeatureLayerType,
    FileUploadType,
    ILayerCreate,
    LayerType,
    SupportedOgrGeomType,
)
from src.schemas.style import base_styles
from asyncio import sleep as async_sleep

class CRUDLayer(CRUDBase):
    async def create_layer_metadata(
        self, async_session: AsyncSession, user_id: UUID, layer_in: ILayerCreate
    ):
        layer_in = Layer(**layer_in.dict(exclude_none=True), **{"user_id": user_id})
        layer = await self.create(async_session, obj_in=layer_in)
        return layer

    async def create_table_or_feature_layer(
        self,
        async_session: AsyncSession,
        job_id: UUID,
        file: UploadFile,
        user_id: UUID,
        layer_in: ILayerCreate,
        payload: dict,
    ):
        """Create a table or feature layer."""

        if layer_in.type == LayerType.table:
            pass
        elif layer_in.type == LayerType.feature_layer:
            if layer_in.feature_layer_type != FeatureLayerType.standard:
                return {
                    "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY,
                    "detail": f"Feature layer type not allowed for file upload. Allowed feature layer type is: {FeatureLayerType.standard.value}",
                }
        else:
            return {
                "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY,
                "detail": f"Layer type not allowed for file upload. Allowed layer types are: {', '.join(LayerType.table, LayerType.feature_layer)}",
            }

        file_ending = os.path.splitext(file.filename)[-1][1:]
        if not any(file_ending == item.value for item in FileUploadType):
            return {
                "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY,
                "detail": f"File type not allowed. Allowed file types are: {', '.join(FileUploadType.__members__)}",
            }

        # Initialize OGRFileUpload
        ogr_file_upload = OGRFileUpload(
            async_session=async_session, user_id=user_id, file=file, layer_in=layer_in
        )

        # Validate file before uploading
        validation_result = await ogr_file_upload.validate(job_id=job_id)
        if validation_result["status"] == "error":
            return {
                "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY,
                "detail": validation_result["msg"].text,
            }

        # Get file size in bytes
        original_position = file.file.tell()
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(original_position)

        # Create attribute mapping out of valid attributes
        attribute_mapping = {}
        for field_type, field_names in validation_result["data_types"]["valid"].items():
            cnt = 1
            for field_name in field_names:
                attribute_mapping[field_name] = field_type + "_attr" + str(cnt)
                cnt += 1

        # Populate Layer Object with file metadata
        additional_attributes = {
            "size": file_size,
            "user_id": user_id,
            "attribute_mapping": attribute_mapping,
        }
        if layer_in.type == "feature_layer":
            geom_type = SupportedOgrGeomType[
                validation_result["data_types"]["geometry"]["type"]
            ].value
            additional_attributes["feature_layer_geometry_type"] = geom_type
            additional_attributes["extent"] = validation_result["data_types"]["geometry"]["extent"]
            additional_attributes["style"] = base_styles["feature_layer"]["standard"][geom_type]

        # Create layer metadata
        layer = Layer(
            **layer_in.dict(exclude_none=True),
            **additional_attributes,
        )
        layer = await self.create(async_session, obj_in=layer)
        layer_id = layer.id
        # Upload file to temporary table
        temp_table_name = f'user_data."{str(uuid4()).replace("-", "")}"'

        result = await ogr_file_upload.upload_ogr2ogr(
            validation_result=validation_result,
            temp_table_name=temp_table_name,
            job_id=job_id,
        )
        if result["status"] == JobStatusType.failed.value:
            # Remove layer metadata
            await self.delete(db=async_session, id=layer_id)
            return {
                "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY,
                "detail": result["msg"].text,
            }

        # Migrate temporary table to target table
        result = await ogr_file_upload.migrate_target_table(
            validation_result=validation_result,
            attribute_mapping=attribute_mapping,
            temp_table_name=temp_table_name,
            layer_id=layer_id,
            job_id=job_id,
        )
        if result["status"] == JobStatusType.failed.value:
            # Remove layer metadata
            await self.delete(db=async_session, id=layer_id)
            return {
                "status_code": status.HTTP_422_UNPROCESSABLE_ENTITY,
                "detail": result["msg"].text,
            }

        # Add Layer ID to job
        job = await crud_job.get(db=async_session, id=job_id)
        await crud_job.update(
            db=async_session,
            db_obj=job,
            obj_in={"layer_ids": [str(layer_id)], "payload": payload},
        )


layer = CRUDLayer(Layer)


# Run create_table_or_feature_layer in isolation for development
async def main():
    from src.db.session import async_session
    from src.endpoints.deps import get_user_id
    from src.schemas.layer import ILayerCreate, request_examples

    layer_in = ILayerCreate(**request_examples["create"]["feature_layer_standard"]["value"])
    file = UploadFile(
        filename="valid_feature_layer.gpkg",
        file=open("/app/tests/data/valid_feature_layer.gpkg", "rb"),
    )
    user_id = get_user_id(authorization=None)

    await layer.create_table_or_feature_layer(
        async_session=async_session(),
        file=file,
        user_id=user_id,
        layer_in=layer_in,
        payload={},
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
