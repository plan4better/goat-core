import os
from uuid import UUID, uuid4

from fastapi import BackgroundTasks, UploadFile, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.job import job_init, run_background_or_immediately
from src.core.layer import FileUpload, OGRFileHandling
from src.crud.base import CRUDBase
from src.crud.crud_job import job as crud_job
from src.db.models.job import Job
from src.db.models.layer import Layer
from src.schemas.job import JobStatusType
from src.schemas.layer import (
    LayerType,
    SupportedOgrGeomType,
    FeatureLayerType,
    IFeatureLayerStandardCreateAdditionalAttributes,
    ITableLayerCreateAdditionalAttributes,
)
from src.schemas.style import base_styles
from src.schemas.job import JobType
from sqlalchemy import text

class CRUDLayer(CRUDBase):
    """CRUD class for Layer."""

    async def create_internal(self, async_session: AsyncSession, user_id: UUID, layer_in):
        # Get import job
        import_job = await crud_job.get_by_multi_keys(
            db=async_session,
            keys={
                "id": layer_in.import_job_id,
                "user_id": user_id,
                "type": JobType.file_import.value,
                "status_simple": JobStatusType.finished.value,
            },
        )
        # Check if import job exists, is finished and owned by the user
        if import_job == []:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Import job not found or not finished.",
            )
        import_job = import_job[0]

        # Get validate job
        validate_job = await crud_job.get_by_multi_keys(
            db=async_session,
            keys={
                "id": import_job.response["validate_job_id"],
                "user_id": user_id,
                "type": JobType.file_validate.value,
                "status_simple": JobStatusType.finished.value,
            },
        )
        # Check if validate job exists, is finished and owned by the user
        if validate_job == []:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Validate job not found or not finished.",
            )
        validate_job = validate_job[0]
        layer_attributes = validate_job.response

        additional_attributes = {}
        # Get layer_id and size from import job
        additional_attributes["id"] = import_job.layer_ids[0]
        additional_attributes["user_id"] = user_id
        additional_attributes["size"] = layer_attributes["file_size"]

        # Create attribute mapping
        attribute_mapping = {}
        for data_type in layer_attributes["data_types"]["valid"]:
            cnt = 0
            for column in layer_attributes["data_types"]["valid"][data_type]:
                cnt += 1
                attribute_mapping[data_type + "_" + str(cnt)] = column

        additional_attributes["attribute_mapping"] = attribute_mapping

        # Get default style if feature layer
        if layer_attributes["data_types"].get("geometry"):
            geom_type = SupportedOgrGeomType[
                layer_attributes["data_types"]["geometry"]["type"]
            ].value
            additional_attributes["style"] = base_styles["feature_layer"]["standard"][geom_type]
            additional_attributes["type"] = LayerType.feature_layer
            additional_attributes["feature_layer_type"] = FeatureLayerType.standard
            additional_attributes["feature_layer_geometry_type"] = geom_type
            additional_attributes["extent"] = layer_attributes["data_types"]["geometry"]["extent"]
            additional_attributes = IFeatureLayerStandardCreateAdditionalAttributes(
                **additional_attributes
            ).dict()
        else:
            additional_attributes["type"] = LayerType.table
            additional_attributes = ITableLayerCreateAdditionalAttributes(
                **additional_attributes
            ).dict()

        # Populate layer_in with additional attributes
        layer_in = Layer(
            **layer_in.dict(exclude_none=True),
            **additional_attributes,
        )
        layer = await self.create(
            db=async_session,
            obj_in=layer_in,
        )
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
        if result["status"] in [
            JobStatusType.failed.value,
            JobStatusType.timeout.value,
            JobStatusType.killed.value,
        ]:
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
        if validation_result["status"] in [
            JobStatusType.failed.value,
            JobStatusType.timeout.value,
            JobStatusType.killed.value,
        ]:
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
        validate_job: Job,
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
        for field_type, field_names in validate_job.response["data_types"]["valid"].items():
            cnt = 1
            for field_name in field_names:
                attribute_mapping[field_name] = field_type + "_attr" + str(cnt)
                cnt += 1

        # Populate Layer Object with file metadata
        additional_attributes = {
            "size": validate_job.response,
            "user_id": user_id,
            "attribute_mapping": attribute_mapping,
        }
        if validate_job.response["layer_type"] == "feature_layer":
            geom_type = SupportedOgrGeomType[
                validate_job.response["data_types"]["geometry"]["type"]
            ].value
            additional_attributes["feature_layer_geometry_type"] = geom_type
            additional_attributes["extent"] = validate_job.response["data_types"]["geometry"][
                "extent"
            ]
            additional_attributes["style"] = base_styles["feature_layer"]["standard"][geom_type]

        # Upload file to temporary table
        temp_table_name = f'{settings.USER_DATA_SCHEMA}."{str(job_id).replace("-", "")}"'

        result = await ogr_file_upload.upload_ogr2ogr(
            temp_table_name=temp_table_name,
            job_id=job_id,
        )
        if result["status"] in [
            JobStatusType.failed.value,
            JobStatusType.timeout.value,
            JobStatusType.killed.value,
        ]:
            return result

        # Migrate temporary table to target table
        result = await ogr_file_upload.migrate_target_table(
            validation_result=validate_job.response,
            attribute_mapping=attribute_mapping,
            temp_table_name=temp_table_name,
            layer_id=layer_id,
            job_id=job_id,
        )
        if result["status"] in [
            JobStatusType.failed.value,
            JobStatusType.timeout.value,
            JobStatusType.killed.value,
        ]:
            await async_session.rollback()
            return result

        # Add Layer ID to job
        job = await crud_job.get(db=async_session, id=job_id)
        await crud_job.update(
            db=async_session,
            db_obj=job,
            obj_in={
                "layer_ids": [str(layer_id)],
                "response": {"validate_job_id": str(validate_job.id)},
            },
        )
        return result

    async def delete_layer_data(self, async_session: AsyncSession, layer_id: UUID, table_name: str):
        """Delete layer data which is in the user data tables."""

        # Delete layer data
        await async_session.execute(
            text(f"DELETE FROM {table_name} WHERE layer_id = '{layer_id}'")
        )
        await async_session.commit()


layer = CRUDLayer(Layer)
