# Standard library imports
import asyncio
import json
import os
from datetime import datetime
from uuid import UUID, uuid4

# Third party imports
from fastapi import HTTPException, UploadFile, status
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import SQLModel

# Local application imports
from src.core.config import settings
from src.core.job import CRUDFailedJob, job_init, run_background_or_immediately
from src.core.layer import FileUpload, OGRFileHandling, delete_old_files
from src.core.print import PrintMap
from src.crud.base import CRUDBase
from src.db.models.layer import Layer
from src.schemas.error import LayerNotFoundError, NoCRSError, ThumbnailComputeError
from src.schemas.job import JobStatusType
from src.schemas.layer import (
    ColumnStatisticsOperation,
    FeatureType,
    IFeatureStandardCreateAdditionalAttributes,
    IFileUploadMetadata,
    IInternalLayerCreate,
    IInternalLayerExport,
    ITableCreateAdditionalAttributes,
    IUniqueValue,
    LayerType,
    OgrDriverType,
    SupportedOgrGeomType,
    UserDataGeomType,
    get_layer_schema,
    layer_update_class,
)
from src.schemas.style import get_base_style
from src.schemas.toolbox_base import MaxFeatureCnt
from src.utils import (
    async_delete_dir,
    async_zip_directory,
    build_where,
    build_where_clause,
    sanitize_error_message,
)


class CRUDLayer(CRUDBase):
    """CRUD class for Layer."""

    async def create_internal(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        layer_in: IInternalLayerCreate,
        file_metadata: dict,
        attribute_mapping: dict,
        job_id: UUID,
    ):
        additional_attributes = {}
        # Get layer_id and size from import job
        additional_attributes["user_id"] = user_id
        # Create attribute mapping
        additional_attributes["attribute_mapping"] = attribute_mapping
        # Map original file type
        additional_attributes["original_file_type"] = file_metadata["file_ending"]

        # Get default style if feature layer
        if file_metadata["data_types"].get("geometry"):
            geom_type = SupportedOgrGeomType[
                file_metadata["data_types"]["geometry"]["type"]
            ].value
            additional_attributes["properties"] = get_base_style(
                feature_geometry_type=geom_type
            )
            additional_attributes["type"] = LayerType.feature
            additional_attributes["feature_layer_type"] = FeatureType.standard
            additional_attributes["feature_layer_geometry_type"] = geom_type
            additional_attributes["extent"] = file_metadata["data_types"]["geometry"][
                "extent"
            ]
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
            job_id=job_id,
        )

        # Update size
        layer_in.size = await self.get_feature_layer_size(
            async_session=async_session, layer=layer_in
        )
        layer = await self.create(
            db=async_session,
            obj_in=layer_in,
        )

        # Create thumbnail using print class
        file_name = str(layer.id) + "_" + str(uuid4()) + ".png"
        if layer.type in (LayerType.feature, LayerType.table) and settings.TEST_MODE is False:
            try:
                thumbnail_url = await PrintMap(
                    async_session=async_session
                ).create_layer_thumbnail(layer=layer_in, file_name=file_name)
            except Exception as e:
                raise ThumbnailComputeError(sanitize_error_message(str(e)))

            # Update thumbnail_url
            layer = await self.update(
                db=async_session,
                db_obj=layer,
                obj_in={"thumbnail_url": thumbnail_url},
            )

        return layer

    async def get_internal(self, async_session: AsyncSession, id: UUID):
        """Gets a layer and make sure it is a internal layer."""

        layer = await self.get(async_session, id=id)
        if layer is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Layer not found"
            )
        if layer.type not in [LayerType.feature, LayerType.table]:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Layer is not a feature layer or table layer. The requested operation cannot be performed on this layer.",
            )
        return layer

    async def update(
        self,
        async_session: AsyncSession,
        id: UUID,
        layer_in: dict,
    ):
        # Get layer
        layer = await self.get(async_session, id=id)
        if layer is None:
            raise LayerNotFoundError(f"{Layer.__name__} not found")
        old_thumbnail_url = layer.thumbnail_url

        # Get the right Layer model for update
        schema = get_layer_schema(
            class_mapping=layer_update_class,
            layer_type=layer.type,
            feature_layer_type=layer.feature_layer_type,
        )

        # Populate layer schema
        layer_in = schema(**layer_in)

        layer = await CRUDBase(Layer).update(
            async_session, db_obj=layer, obj_in=layer_in
        )

        # Update thumbnail. Only run outside of tests as the print class depends on geoapi as external service.
        if layer.type in (LayerType.feature, LayerType.table) and settings.TEST_MODE is False:
            file_name = str(layer.id) + "_" + str(uuid4()) + ".png"
            try:
                thumbnail_url = await PrintMap(async_session=async_session).create_layer_thumbnail(
                    layer=layer, file_name=file_name
                )
            except Exception as e:
                raise ThumbnailComputeError(sanitize_error_message(str(e)))

            # Update thumbnail_url
            layer = await CRUDBase(Layer).update(
                db=async_session,
                db_obj=layer,
                obj_in={"thumbnail_url": thumbnail_url},
            )
        # Delete old thumbnail from s3 if the thumbnail is not a base thumbnail.
        if (
            old_thumbnail_url
            and settings.THUMBNAIL_DIR_LAYER in old_thumbnail_url
            and settings.TEST_MODE is False
        ):
            settings.S3_CLIENT.delete_object(
                Bucket=settings.AWS_S3_ASSETS_BUCKET,
                Key=old_thumbnail_url.replace(settings.ASSETS_URL + "/", ""),
            )
        return layer

    async def delete(
        self,
        async_session: AsyncSession,
        id: UUID,
    ):
        layer = await CRUDBase(Layer).get(async_session, id=id)
        if layer is None:
            raise LayerNotFoundError(f"{Layer.__name__} not found")

        # Check if internal or external layer
        if layer.type in [LayerType.table.value, LayerType.feature.value]:
            # Delete layer data
            await self.delete_layer_data(async_session=async_session, layer=layer)

        # Delete layer metadata
        await CRUDBase(Layer).delete(
            db=async_session,
            id=id,
        )

        # Delete layer thumbnail
        if (
            layer.thumbnail_url
            and settings.THUMBNAIL_DIR_LAYER in layer.thumbnail_url
            and settings.TEST_MODE is False
        ):
            settings.S3_CLIENT.delete_object(
                Bucket=settings.AWS_S3_ASSETS_BUCKET,
                Key=layer.thumbnail_url.replace(settings.ASSETS_URL + "/", ""),
            )

    async def upload_file(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        layer_type: LayerType,
        file: UploadFile,
    ):
        """Validate file using ogr2ogr."""

        dataset_id = uuid4()
        # Initialize OGRFileUpload
        file_upload = FileUpload(
            async_session=async_session,
            user_id=user_id,
            dataset_id=dataset_id,
            file=file,
        )

        # Save file
        timeout = 120
        try:
            file_path = await asyncio.wait_for(
                file_upload.save_file(file=file),
                timeout,
            )
        except asyncio.TimeoutError:
            # Handle the timeout here. For example, you can raise a custom exception or log it.
            await file_upload.save_file_fail()
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail=f"File upload timed out after {timeout} seconds.",
            )
        except Exception as e:
            # Run failure function if exists
            await file_upload.save_file_fail()
            # Update job status simple to failed
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e),
            )

        # Initialize OGRFileHandling
        ogr_file_handling = OGRFileHandling(
            async_session=async_session,
            user_id=user_id,
            file_path=file_path,
        )

        # Validate file before uploading
        try:
            validation_result = await asyncio.wait_for(
                ogr_file_handling.validate(),
                timeout,
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail=f"File validation timed out after {timeout} seconds.",
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e),
            )

        if validation_result.get("status") == "failed":
            # Run failure function if exists
            await file_upload.save_file_fail()
            # Update job status simple to failed
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=validation_result["msg"],
            )

        # Get file size in bytes
        original_position = file.file.tell()
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(original_position)

        # Define metadata object
        metadata = IFileUploadMetadata(
            **validation_result,
            dataset_id=dataset_id,
            file_ending=os.path.splitext(file.filename)[-1][1:],
            file_size=file_size,
            layer_type=layer_type,
        )

        # Save metadata into user folder as json
        metadata_path = os.path.join(
            os.path.dirname(metadata.file_path), "metadata.json"
        )
        with open(metadata_path, "w") as f:
            # Convert dict to json
            json.dump(metadata.json(), f)

        # Add layer_type and file_size to validation_result
        return metadata

    async def delete_layer_data(self, async_session: AsyncSession, layer):
        """Delete layer data which is in the user data tables."""

        # Delete layer data
        await async_session.execute(
            text(f"DELETE FROM {layer.table_name} WHERE layer_id = '{layer.id}'")
        )
        await async_session.commit()

    async def get_feature_layer_size(
        self, async_session: AsyncSession, layer: BaseModel | SQLModel
    ):
        """Get size of feature layer."""

        # Get size
        sql_query = f"""
            SELECT SUM(pg_column_size(p.*))
            FROM {layer.table_name} AS p
            WHERE layer_id = '{str(layer.id)}'
        """
        result = await async_session.execute(text(sql_query))
        result = result.fetchall()
        return result[0][0]

    async def get_feature_layer_extent(
        self, async_session: AsyncSession, layer: BaseModel | SQLModel
    ):
        """Get extent of feature layer."""

        # Get extent
        sql_query = f"""
            SELECT CASE WHEN ST_MULTI(ST_ENVELOPE(ST_Extent(geom))) <> 'ST_MultiPolygon'
            THEN ST_MULTI(ST_ENVELOPE(ST_Extent(ST_BUFFER(geom, 0.00001))))
            ELSE ST_MULTI(ST_ENVELOPE(ST_Extent(geom))) END AS extent
            FROM {layer.table_name}
            WHERE layer_id = '{str(layer.id)}'
        """
        result = await async_session.execute(text(sql_query))
        result = result.fetchall()
        return result[0][0]

    async def get_feature_cnt(
        self,
        async_session: AsyncSession,
        layer_project: SQLModel | BaseModel,
        where_query: str = None,
    ):
        """Get feature count for a layer or a layer project."""

        # Get feature count total
        feature_cnt = {}
        table_name = layer_project.table_name
        sql_query = f"SELECT COUNT(*) FROM {table_name} WHERE layer_id = '{str(layer_project.layer_id)}'"
        result = await async_session.execute(text(sql_query))
        feature_cnt["total_count"] = result.scalar_one()

        # Get feature count filtered
        if not where_query:
            where_query = build_where_clause([layer_project.where_query])
        else:
            where_query = build_where_clause([where_query])
        if where_query:
            sql_query = f"SELECT COUNT(*) FROM {table_name} {where_query}"
            result = await async_session.execute(text(sql_query))
            feature_cnt["filtered_count"] = result.scalar_one()
        return feature_cnt

    async def check_exceed_feature_cnt(
        self,
        async_session: AsyncSession,
        max_feature_cnt: int,
        layer,
        where_query: str,
    ):
        """Check if feature count is exceeding the defined limit."""
        feature_cnt = await self.get_feature_cnt(
            async_session=async_session, layer_project=layer, where_query=where_query
        )

        if feature_cnt.get("filtered_count") is not None:
            cnt_to_check = feature_cnt["filtered_count"]
        else:
            cnt_to_check = feature_cnt["total_count"]

        if cnt_to_check > max_feature_cnt:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Operation not supported. The layer contains more than {max_feature_cnt} features. Please apply a filter to reduce the number of features.",
            )
        return feature_cnt

    async def check_if_column_suitable_for_stats(
        self, async_session: AsyncSession, id: UUID, column_name: str, query: str
    ):
        # Check if layer is internal layer
        layer = await self.get_internal(async_session, id=id)
        column_mapped = next(
            (
                key
                for key, value in layer.attribute_mapping.items()
                if value == column_name
            ),
            None,
        )

        if column_mapped is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Column not found"
            )

        return {
            "layer": layer,
            "column_mapped": column_mapped,
            "where_query": build_where(
                id=layer.id,
                table_name=layer.table_name,
                query=query,
                attribute_mapping=layer.attribute_mapping,
            ),
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
        res_check = await self.check_if_column_suitable_for_stats(
            async_session=async_session, id=id, column_name=column_name, query=query
        )
        layer = res_check["layer"]
        column_mapped = res_check["column_mapped"]
        where_query = res_check["where_query"]
        # Map order
        order_mapped = {"descendent": "DESC", "ascendent": "ASC"}[order]

        # Build count query
        count_query = f"""
            SELECT COUNT(*) AS total_count
            FROM (
                SELECT {column_mapped}
                FROM {layer.table_name}
                WHERE {where_query}
                AND {column_mapped} IS NOT NULL
                GROUP BY {column_mapped}
            ) AS subquery
        """

        # Execute count query
        count_result = await async_session.execute(text(count_query))
        total_results = count_result.scalar_one()

        # Build data query
        data_query = f"""
        SELECT *
        FROM (

            SELECT JSONB_BUILD_OBJECT(
                'value', {column_mapped}, 'count', COUNT(*)
            )
            FROM {layer.table_name}
            WHERE {where_query}
            AND {column_mapped} IS NOT NULL
            GROUP BY {column_mapped}
            ORDER BY COUNT(*) {order_mapped}, {column_mapped}
        ) AS subquery
        LIMIT {page_params.size}
        OFFSET {(page_params.page - 1) * page_params.size}
        """

        # Execute data query
        data_result = await async_session.execute(text(data_query))
        result = data_result.fetchall()
        result = [IUniqueValue(**res[0]) for res in result]

        # Create Page object
        page = Page(
            items=result,
            total=total_results,
            page=page_params.page,
            size=page_params.size,
        )

        return page

    async def get_area_statistics(
        self,
        async_session: AsyncSession,
        id: UUID,
        operation: ColumnStatisticsOperation,
        query: str,
    ):
        # Check if layer is internal layer
        layer = await self.get_internal(async_session, id=id)

        # Where query
        where_query = build_where(
            id=layer.id,
            table_name=layer.table_name,
            query=query,
            attribute_mapping=layer.attribute_mapping,
        )

        # Check if layer has polygon geoms
        if layer.feature_layer_geometry_type != UserDataGeomType.polygon.value:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Operation not supported. The layer does not contain polygon geometries. Pick a layer with polygon geometries.",
            )

        # Check if feature count is exceeding the defined limit
        await self.check_exceed_feature_cnt(
            async_session=async_session,
            max_feature_cnt=MaxFeatureCnt.area_statistics.value,
            layer=layer,
            where_query=where_query,
        )
        where_query = "WHERE " + where_query
        # Call SQL function
        sql_query = f"""
            SELECT * FROM basic.area_statistics('{operation.value}', '{layer.table_name}', '{where_query.replace("'", "''")}')
        """
        res = await async_session.execute(
            sql_query,
        )
        res = res.fetchall()
        return res[0][0] if res else None

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
        res = await self.check_if_column_suitable_for_stats(
            async_session=async_session, id=id, column_name=column_name, query=query
        )

        args = res
        where_clause = res["where_query"]
        args["table_name"] = args["layer"].table_name
        # del layer from args
        del args["layer"]

        # Extend where clause
        column_mapped = res["column_mapped"]
        if stripe_zeros:
            where_extension = (
                f" AND {column_mapped} != 0"
                if where_clause
                else f"{column_mapped} != 0"
            )
            args["where"] = where_clause + where_extension

        # Define additional arguments
        if breaks:
            args["breaks"] = breaks

        # Choose the SQL query based on operation
        if operation == ColumnStatisticsOperation.quantile:
            sql_query = "SELECT * FROM basic.quantile_breaks(:table_name, :column_mapped, :where, :breaks)"
        elif operation == ColumnStatisticsOperation.equal_interval:
            sql_query = "SELECT * FROM basic.equal_interval_breaks(:table_name, :column_mapped, :where, :breaks)"
        elif operation == ColumnStatisticsOperation.standard_deviation:
            sql_query = "SELECT * FROM basic.standard_deviation_breaks(:table_name, :column_mapped, :where)"
        elif operation == ColumnStatisticsOperation.heads_and_tails:
            sql_query = "SELECT * FROM basic.heads_and_tails_breaks(:table_name, :column_mapped, :where, :breaks)"
        else:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Operation not supported",
            )

        # Execute the query
        res = await async_session.execute(sql_query, args)
        res = res.fetchall()
        return res[0][0] if res else None

    async def get_last_data_updated_at(
        self, async_session: AsyncSession, id: UUID, query: str
    ) -> datetime:
        """Get last updated at timestamp."""

        # Check if layer is internal layer
        layer = await self.get_internal(async_session, id=id)
        where_query = build_where(
            id=layer.id,
            table_name=layer.table_name,
            query=query,
            attribute_mapping=layer.attribute_mapping,
        )

        # Get last updated at timestamp
        sql_query = f"""
            SELECT MAX(updated_at)
            FROM {layer.table_name}
            WHERE {where_query}
        """
        result = await async_session.execute(text(sql_query))
        result = result.fetchall()
        return result[0][0]


layer = CRUDLayer(Layer)


class CRUDLayerImport(CRUDFailedJob):
    """CRUD class for Layer import."""

    def __init__(self, job_id, background_tasks, async_session, user_id):
        super().__init__(job_id, background_tasks, async_session, user_id)

    @run_background_or_immediately(settings)
    @job_init()
    async def import_file(
        self,
        file_metadata: dict,
        layer_in: IInternalLayerCreate,
    ):
        """Import file using ogr2ogr."""

        # Initialize OGRFileHandling
        ogr_file_upload = OGRFileHandling(
            async_session=self.async_session,
            user_id=self.user_id,
            file_path=file_metadata["file_path"],
        )

        # Create attribute mapping out of valid attributes
        attribute_mapping = {}
        for field_type, field_names in file_metadata["data_types"]["valid"].items():
            cnt = 1
            for field_name in field_names:
                if field_name == "id":
                    continue
                attribute_mapping[field_name] = field_type + "_attr" + str(cnt)
                cnt += 1

        temp_table_name = (
            f'{settings.USER_DATA_SCHEMA}."{str(self.job_id).replace("-", "")}"'
        )

        result = await ogr_file_upload.upload_ogr2ogr(
            temp_table_name=temp_table_name,
            job_id=self.job_id,
        )
        if result["status"] in [
            JobStatusType.failed.value,
            JobStatusType.timeout.value,
            JobStatusType.killed.value,
        ]:
            return result

        # Migrate temporary table to target table
        result = await ogr_file_upload.migrate_target_table(
            validation_result=file_metadata,
            attribute_mapping=attribute_mapping,
            temp_table_name=temp_table_name,
            layer_id=layer_in.id,
            job_id=self.job_id,
        )
        if result["status"] in [
            JobStatusType.failed.value,
            JobStatusType.timeout.value,
            JobStatusType.killed.value,
        ]:
            await self.async_session.rollback()
            return result

        # Create layer
        attribute_mapping = {value: key for key, value in attribute_mapping.items()}
        await CRUDLayer(Layer).create_internal(
            async_session=self.async_session,
            user_id=self.user_id,
            layer_in=layer_in,
            file_metadata=file_metadata,
            attribute_mapping=attribute_mapping,
            job_id=self.job_id,
        )
        return result


class CRUDLayerExport:
    """CRUD class for Layer import."""

    def __init__(self, id, async_session, user_id):
        self.id = id
        self.user_id = user_id
        self.async_session = async_session
        self.folder_path = os.path.join(
            settings.DATA_DIR, str(self.user_id), str(self.id)
        )

    async def create_metadata_file(self, layer: Layer, layer_in: IInternalLayerExport):
        last_data_updated_at = await CRUDLayer(Layer).get_last_data_updated_at(
            async_session=self.async_session, id=self.id, query=layer_in.query
        )
        # Write metadata to metadata.txt file
        with open(
            os.path.join(self.folder_path, layer_in.file_name, "metadata.txt"), "w"
        ) as f:
            # Write some heading
            f.write("############################################################\n")
            f.write(f"Metadata for layer {layer.name}\n")
            f.write("############################################################\n")
            f.write(
                f"Exported Coordinate Reference System: EPSG {layer.upload_reference_system}\n"
            )
            f.write(
                f"Exported File Type: {OgrDriverType[layer_in.file_type.value].value}\n"
            )
            f.write("############################################################\n")
            f.write(f"Last data update: {last_data_updated_at}\n")
            f.write(f"Last metadata update: {layer.updated_at}\n")
            f.write(f"Created at: {layer.created_at}\n")
            f.write(f"Exported at: {datetime.now()}\n")
            f.write("############################################################\n")
            f.write(f"Name: {layer.name}\n")
            f.write(f"Description: {layer.description}\n")
            f.write(f"Tags: {', '.join(layer.tags)}\n")
            f.write(f"Lineage: {layer.lineage}\n")
            f.write(f"Positional Accuracy: {layer.positional_accuracy}\n")
            f.write(f"Attribute Accuracy: {layer.attribute_accuracy}\n")
            f.write(f"Completeness: {layer.completeness}\n")
            f.write(f"Upload Reference System: {layer.upload_reference_system}\n")
            f.write(f"Upload File Type: {layer.upload_file_type}\n")
            f.write(f"Geographical Code: {layer.geographical_code}\n")
            f.write(f"Language Code: {layer.language_code}\n")
            f.write(f"Distributor Name: {layer.distributor_name}\n")
            f.write(f"Distributor Email: {layer.distributor_email}\n")
            f.write(f"Distribution URL: {layer.distribution_url}\n")
            f.write(f"License: {layer.license}\n")
            f.write(f"Attribution: {layer.attribution}\n")
            f.write(f"Data Reference Year: {layer.data_reference_year}\n")
            f.write(f"Data Category: {layer.data_category}\n")
            f.write("############################################################")

    async def export_file(
        self,
        layer_in: IInternalLayerExport,
    ):
        """Export file using ogr2ogr."""

        # Get layer
        layer = await CRUDLayer(Layer).get_internal(
            async_session=self.async_session, id=self.id
        )

        # Make sure that feature layer have CRS set
        if layer.type == LayerType.feature:
            if layer_in.crs is None:
                raise NoCRSError(
                    "CRS is required for feature layers. Please provide a CRS."
                )

        # Build SQL query for export
        # Build select query based on attribute mapping
        select_query = ""
        for key, value in layer.attribute_mapping.items():
            select_query += f"{key} AS {value}, "

        # Add id and geom
        if layer.type == LayerType.feature:
            select_query = "id, " + select_query + "geom"
        else:
            select_query = "id, " + select_query
            select_query = select_query[:-2]

        # Build where query
        where_query = build_where(
            layer.id, layer.table_name, layer_in.query, layer.attribute_mapping
        )
        query = build_where_clause([where_query])
        sql_query = f"""
            SELECT {select_query}
            FROM {layer.table_name}
            {query}
        """
        # Build filepath
        file_path = os.path.join(
            self.folder_path,
            layer_in.file_name,
            f"{layer_in.file_name}." + layer_in.file_type,
        )

        # Delete files that are older then one hour
        await delete_old_files(3600)

        # Initialize OGRFileHandling
        ogr_file_handling = OGRFileHandling(
            async_session=self.async_session,
            user_id=self.user_id,
            file_path=file_path,
        )
        file_path = await ogr_file_handling.export_ogr2ogr(
            layer=layer,
            file_type=layer_in.file_type,
            file_name=layer_in.file_name,
            sql_query=sql_query,
            crs=layer_in.crs,
        )

        # Write data into metadata.txt file
        await self.create_metadata_file(layer=layer, layer_in=layer_in)

        # Zip result folder
        result_dir = os.path.join(
            settings.DATA_DIR, str(self.user_id), str(layer_in.file_name) + ".zip"
        )
        await async_zip_directory(
            result_dir, os.path.join(self.folder_path, layer_in.file_name)
        )

        # Delete folder
        await async_delete_dir(self.folder_path)

        return result_dir

    async def export_file_run(self, layer_in: IInternalLayerExport):
        return await self.export_file(layer_in=layer_in)
