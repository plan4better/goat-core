# Standard library imports
import json
import os
from uuid import UUID

# Third party imports
from fastapi import BackgroundTasks, HTTPException, UploadFile, status
from fastapi_pagination import Params as PaginationParams
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Local application imports
from src.core.config import settings
from src.core.job import job_init, run_background_or_immediately
from src.core.layer import FileUpload, OGRFileHandling
from src.crud.base import CRUDBase
from src.crud.crud_job import job as crud_job
from src.db.models.job import Job
from src.db.models.layer import Layer
from src.schemas.job import JobStatusType, JobType
from src.schemas.layer import (
    ColumnStatisticsOperation,
    CQLQuery,
    FeatureType,
    IFeatureStandardCreateAdditionalAttributes,
    ITableCreateAdditionalAttributes,
    LayerType,
    SupportedOgrGeomType,
)
from src.schemas.style import base_properties
from src.utils import build_where, get_user_table, search_value


class CRUDLayer(CRUDBase):
    """CRUD class for Layer."""

    async def create_internal(
        self, async_session: AsyncSession, user_id: UUID, layer_in
    ):
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
                attribute_mapping[data_type + "_attr" + str(cnt)] = column

        additional_attributes["attribute_mapping"] = attribute_mapping

        # Get default style if feature layer
        if layer_attributes["data_types"].get("geometry"):
            geom_type = SupportedOgrGeomType[
                layer_attributes["data_types"]["geometry"]["type"]
            ].value
            additional_attributes["properties"] = base_properties["feature"]["standard"][
                geom_type
            ]
            additional_attributes["type"] = LayerType.feature
            additional_attributes["feature_layer_type"] = FeatureType.standard
            additional_attributes["feature_layer_geometry_type"] = geom_type
            additional_attributes["extent"] = layer_attributes["data_types"][
                "geometry"
            ]["extent"]
            additional_attributes = IFeatureStandardCreateAdditionalAttributes(
                **additional_attributes
            ).dict()
        else:
            additional_attributes["type"] = LayerType.table
            additional_attributes = ITableCreateAdditionalAttributes(
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
            settings.DATA_DIR,
            str(job_id),
            "file." + os.path.splitext(file.filename)[-1][1:],
        )

        # Initialize OGRFileHandling
        ogr_file_handling = OGRFileHandling(
            async_session=async_session,
            user_id=user_id,
            job_id=job_id,
            file_path=file_path,
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
        await crud_job.update(
            db=async_session, db_obj=job, obj_in={"response": response}
        )

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
        for field_type, field_names in validate_job.response["data_types"][
            "valid"
        ].items():
            cnt = 1
            for field_name in field_names:
                if field_name == "id":
                    continue
                attribute_mapping[field_name] = field_type + "_attr" + str(cnt)
                cnt += 1

        temp_table_name = (
            f'{settings.USER_DATA_SCHEMA}."{str(job_id).replace("-", "")}"'
        )

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

    async def delete_layer_data(self, async_session: AsyncSession, layer):
        """Delete layer data which is in the user data tables."""

        # Delete layer data
        table_name = get_user_table(layer)
        await async_session.execute(
            text(f"DELETE FROM {table_name} WHERE layer_id = '{layer.id}'")
        )
        await async_session.commit()

    async def get_feature_cnt(
        self,
        async_session: AsyncSession,
        layer_project: dict,
    ):
        """Get feature count for a layer project."""

        # Get table name
        table_name = get_user_table(layer_project)

        # Get feature count total
        feature_cnt = {}
        sql_query = f"SELECT COUNT(*) FROM {table_name} WHERE layer_id = '{str(layer_project['layer_id'])}'"
        result = await async_session.execute(text(sql_query))
        feature_cnt["total_count"] = result.scalar_one()

        # Get feature count filtered
        if layer_project.get("query", None):
            where = build_where(
                query=layer_project["query"],
                attribute_mapping=layer_project["attribute_mapping"],
            )
            sql_query += f" AND {where}"
            result = await async_session.execute(text(sql_query))
            feature_cnt["filtered_count"] = result.scalar_one()
        return feature_cnt

    async def check_layer_suitable_for_stats(
        self, async_session: AsyncSession, id: UUID, column_name: str, query: str
    ):
        # Get layer
        layer = await self.get(async_session, id=id)
        if layer is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Layer not found"
            )

        # Check if layer_type is feature or table
        if layer.type not in [LayerType.feature, LayerType.table]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Layer is not a feature layer or table layer. Unique values can only be requested for internal layers.",
            )

        column_mapped = search_value(layer.attribute_mapping, column_name)
        if column_mapped is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Column not found"
            )

        # Check if internal or external layer. And get table name if external.
        table_name = get_user_table(layer)

        # Build query
        if query:
            # Validate query
            query = json.loads(query)
            query_obj = CQLQuery(query=query)
            where = "AND " + build_where(
                query=query_obj.query, attribute_mapping=layer.attribute_mapping
            )
        else:
            where = ""

        return {
            "layer": layer,
            "column_mapped": column_mapped,
            "table_name": table_name,
            "where": where,
        }

    async def get_unique_values(
        self,
        async_session: AsyncSession,
        id: UUID,
        column_name: str,
        order: str,
        query: str,
        page_params: PaginationParams,
    ):
        # Check if layer is suitable for stats
        res_check = await self.check_layer_suitable_for_stats(
            async_session=async_session, id=id, column_name=column_name, query=query
        )
        layer = res_check["layer"]
        column_mapped = res_check["column_mapped"]
        table_name = res_check["table_name"]
        where = res_check["where"]
        # Map order
        order_mapped = {"descendent": "DESC", "ascendent": "ASC"}[order]
        # Build query
        sql_query = f"""
        WITH cnt AS (
            SELECT {column_mapped} AS {column_name}, COUNT(*) AS count
            FROM {table_name}
            WHERE layer_id = '{layer.id}'
            {where}
            AND {column_mapped} IS NOT NULL
            GROUP BY {column_mapped}
            ORDER BY COUNT(*)
            {order_mapped}
            LIMIT {page_params.size}
            OFFSET {(page_params.page - 1) * page_params.size}
        )
        SELECT JSONB_OBJECT_AGG({column_name}, count) FROM cnt
        """

        # Execute query
        result = await async_session.execute(text(sql_query))
        result = result.fetchall()
        return result[0][0]

    async def get_class_breaks(
        self,
        async_session: AsyncSession,
        id: UUID,
        operation: ColumnStatisticsOperation,
        query: str,
        column_name: str,
        stripe_zeros: bool = None,
        breaks: int = None,
    ):
        # Check if layer is suitable for stats
        column_mapped, table_name, where = await self.check_layer_suitable_for_stats(
            async_session=async_session, id=id, column_name=column_name, query=query
        )

        # Extend where clause
        if stripe_zeros:
            where += f" AND {column_mapped} != 0"

        # Define arguments
        args = {"table_name": table_name, "column_name": column_mapped, "where": where}
        if breaks:
            args["breaks"] = breaks

        if operation == ColumnStatisticsOperation.quantile:
            sql_query = """SELECT * FROM basic.quantile_breaks(:table_name, :column_name, :where, :breaks)"""
        elif operation == ColumnStatisticsOperation.equal_interval:
            sql_query = """SELECT * FROM basic.equal_interval_breaks(:table_name, :column_name, :where, :breaks)"""
        elif operation == ColumnStatisticsOperation.standard_deviation:
            sql_query = """SELECT * FROM basic.std_deviation_breaks(:table_name, :column_name, :where)"""
        elif operation == ColumnStatisticsOperation.heads_and_tails:
            sql_query = """SELECT * FROM basic.heads_and_tails_breaks(:table_name, :column_name, :where, :breaks)"""
        else:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Operation not supported",
            )

        res = await async_session.execute(sql_query, args)
        res = res.fetchall()
        return res[0][0]


layer = CRUDLayer(Layer)
