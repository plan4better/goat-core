import os
from uuid import UUID, uuid4
from fastapi import UploadFile, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from src.core.layer import FileUpload, OGRFileHandling
from src.crud.base import CRUDBase
from src.crud.crud_job import job as crud_job
from src.db.models.layer import Layer
from src.db.models.job import Job
from src.schemas.job import JobStatusType
from src.schemas.layer import (
    ILayerCreate,
    LayerType,
    SupportedOgrGeomType,
)
from src.schemas.style import base_styles
from src.core.config import settings
from src.core.job import job_init, run_background_or_immediately


class CRUDLayer(CRUDBase):
    async def create_layer_metadata(
        self, async_session: AsyncSession, user_id: UUID, layer_in: ILayerCreate
    ):
        layer_in = Layer(**layer_in.dict(exclude_none=True), **{"user_id": user_id})
        layer = await self.create(async_session, obj_in=layer_in)
        return layer

    @run_background_or_immediately(settings)
    @job_init()
    async def validate_file(
        self,
        background_tasks: BackgroundTasks,
        async_session: AsyncSession,
        user_id: UUID,
        job_id: UUID,
        layer_type: LayerType,
        file: UploadFile,
    ):
        # Initialize OGRFileUpload
        file_upload = FileUpload(
            async_session=async_session, user_id=user_id, job_id=job_id, file=file
        )

        # Save file
        result = await file_upload.save_file(file=file, job_id=job_id)
        if result["status"] in [JobStatusType.failed.value, JobStatusType.timeout.value, JobStatusType.killed.value]:
            return result

        # Build folder path
        file_path = os.path.join(
            settings.DATA_DIR, str(job_id), "file." + os.path.splitext(file.filename)[-1][1:]
        )

        # Initialize OGRFileHandling
        ogr_file_handling = OGRFileHandling(
            async_session=async_session, user_id=user_id, job_id=job_id, file_path=file_path
        )
        # Validate file before uploading
        validation_result = await ogr_file_handling.validate(job_id=job_id)
        if validation_result["status"] in [JobStatusType.failed.value, JobStatusType.timeout.value, JobStatusType.killed.value]:
            return validation_result

        # Get file size in bytes
        original_position = file.file.tell()
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(original_position)

        # Add layer_type and file_size to validation_result
        response = {}
        response["data_types"] = validation_result["data_types"]
        response["layer_type"] = layer_type
        response["file_ending"] = os.path.splitext(file.filename)[-1][1:]
        response["file_size"] = file_size
        response["file_path"] = validation_result["file_path"]

        # Update job with validation result
        job = await crud_job.get(db=async_session, id=job_id)
        await crud_job.update(db=async_session, db_obj=job, obj_in={"response": response})

        return validation_result

    @run_background_or_immediately(settings)
    @job_init()
    async def import_file(
        self,
        background_tasks: BackgroundTasks,
        async_session: AsyncSession,
        job_id: UUID,
        upload_job: Job,
        user_id: UUID,
        layer_id: UUID,
        file_path: str,
    ):
        """Import file using ogr2ogr."""

        # Initialize OGRFileHandling
        ogr_file_upload = OGRFileHandling(
            async_session=async_session,
            user_id=user_id,
            job_id=job_id,
            file_path=file_path,
        )

        # Create attribute mapping out of valid attributes
        attribute_mapping = {}
        for field_type, field_names in upload_job.response["data_types"]["valid"].items():
            cnt = 1
            for field_name in field_names:
                attribute_mapping[field_name] = field_type + "_attr" + str(cnt)
                cnt += 1

        # Populate Layer Object with file metadata
        additional_attributes = {
            "size": upload_job.response,
            "user_id": user_id,
            "attribute_mapping": attribute_mapping,
        }
        if upload_job.response["layer_type"] == "feature_layer":
            geom_type = SupportedOgrGeomType[
                upload_job.response["data_types"]["geometry"]["type"]
            ].value
            additional_attributes["feature_layer_geometry_type"] = geom_type
            additional_attributes["extent"] = upload_job.response["data_types"]["geometry"][
                "extent"
            ]
            additional_attributes["style"] = base_styles["feature_layer"]["standard"][geom_type]

        # Upload file to temporary table
        temp_table_name = f'{settings.USER_DATA_SCHEMA}."{str(uuid4()).replace("-", "")}"'

        result = await ogr_file_upload.upload_ogr2ogr(
            temp_table_name=temp_table_name,
            job_id=job_id,
        )
        if result["status"] in [JobStatusType.failed.value, JobStatusType.timeout.value, JobStatusType.killed.value]:
            return result

        # Migrate temporary table to target table
        result = await ogr_file_upload.migrate_target_table(
            validation_result=upload_job.response,
            attribute_mapping=attribute_mapping,
            temp_table_name=temp_table_name,
            layer_id=layer_id,
            job_id=job_id,
        )
        if result["status"] in [JobStatusType.failed.value, JobStatusType.timeout.value, JobStatusType.killed.value]:
            await async_session.rollback()
            return result

        # Add Layer ID to job
        job = await crud_job.get(db=async_session, id=job_id)
        await crud_job.update(
            db=async_session,
            db_obj=job,
            obj_in={"layer_ids": [str(layer_id)]},
        )
        return result

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
