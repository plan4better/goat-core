from fastapi import HTTPException, status, UploadFile
from openpyxl import load_workbook
from src.schemas.layer import (
    FileUploadType,
    NumberColumnsPerType,
    OgrPostgresType,
    OgrDriverType,
    ILayerCreate,
    SupportedOgrGeomType,
)
from src.core.config import settings
import csv
import zipfile
import os
from osgeo import ogr, osr
import pandas as pd
from uuid import uuid4, UUID
import subprocess
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from src.utils import create_dir, delete_dir
from src.schemas.job import JobStatusType, MsgType, Msg
from src.core.job import job_log


class OGRFileUpload:
    def __init__(self, async_session: AsyncSession, user_id: UUID, file: UploadFile, layer_in: ILayerCreate):
        self.async_session = async_session
        self.user_id = user_id
        self.file = file
        self.file_ending = os.path.splitext(self.file.filename)[-1][1:]
        self.folder_name = str(uuid4())
        self.file_name = self.folder_name + "." + self.file_ending
        self.folder_path = os.path.join("/tmp", self.folder_name)
        self.file_path = os.path.join(self.folder_path, self.file_name)
        self.method_match = {
            FileUploadType.csv: self.validate_csv,
            FileUploadType.xlsx: self.validate_xlsx,
            FileUploadType.zip: self.validate_shapefile,
            FileUploadType.gpkg: self.validate_gpkg,
            FileUploadType.geojson: self.validate_geojson,
            FileUploadType.kml: self.validate_kml,
        }
        self.driver_name = OgrDriverType[self.file_ending].value
        self.layer_in = layer_in

    def validate_ogr(self, file_path: str):
        """Validate using ogr and get valid attributes."""

        # Get driver
        driver = ogr.GetDriverByName(self.driver_name)

        # Open the file using OGR
        data_source = driver.Open(file_path)
        if data_source is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Could not open the file",
            )

        # Count the number of layers
        layer_count = data_source.GetLayerCount()

        # Validate that there is only one layer
        if layer_count != 1:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="File must contain exactly one layer.",
            )
        # Get Layer and check types
        layer = data_source.GetLayer(0)

        # Check if CRS is give other no conversion can be done later
        spatial_ref = layer.GetSpatialRef()

        if spatial_ref is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Could not determine Coordinate Reference System (CRS).",
            )

        data_types = self.check_field_types(layer)

        # Close the datasource
        data_source = None

        return {
            "file_path": file_path,
            "data_types": data_types,
        }

    def get_layer_extent(self, layer) -> str:
        """Get layer extent in EPSG:4326."""
        # Get the original extent
        minX, maxX, minY, maxY = layer.GetExtent()

        # Define the source SRS
        source_srs = layer.GetSpatialRef()

        # Define the target SRS (EPSG:4326)
        target_srs = osr.SpatialReference()
        target_srs.ImportFromEPSG(4326)

        # Create a coordinate transformation
        transform = osr.CoordinateTransformation(source_srs, target_srs)

        # Transform the coordinates
        min_point = ogr.Geometry(ogr.wkbPoint)
        min_point.AddPoint(minX, minY)
        min_point.Transform(transform)

        max_point = ogr.Geometry(ogr.wkbPoint)
        max_point.AddPoint(maxX, maxY)
        max_point.Transform(transform)

        # Get the transformed coordinates
        minX_transformed, minY_transformed, _ = min_point.GetPoint()
        maxX_transformed, maxY_transformed, _ = max_point.GetPoint()

        # Create a Multipolygon from the extent
        multipolygon_wkt = f"MULTIPOLYGON((({minX_transformed} {minY_transformed}, {minX_transformed} {maxY_transformed}, {maxX_transformed} {maxY_transformed}, {maxX_transformed} {minY_transformed}, {minX_transformed} {minY_transformed})))"
        return multipolygon_wkt

    def check_field_types(self, layer):
        """Check if field types are valid and label if too many columns where specified."""

        field_types = {"valid": {}, "unvalid": {}, "overflow": {}, "geometry": {}}
        layer_def = layer.GetLayerDefn()

        # Get geometry type of layer to upload to specify target table
        geometry_type = ogr.GeometryTypeToName(layer_def.GetGeomType()).replace(" ", "_")
        if geometry_type not in SupportedOgrGeomType.__members__:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Geometry type {geometry_type} not supported.",
            )
        # Save geometry type and geometry column name
        field_types["geometry"]["column_name"] = layer_def.GetGeomFieldDefn(0).GetName()
        field_types["geometry"]["type"] = geometry_type
        field_types["geometry"]["extent"] = self.get_layer_extent(layer)

        for i in range(layer_def.GetFieldCount()):
            field_def = layer_def.GetFieldDefn(i)
            field_name = field_def.GetName()
            field_type_code = field_def.GetType()
            field_type = field_def.GetFieldTypeName(field_type_code)

            # Get field type from OgrPostgresType enum if exists
            field_type_pg = (
                OgrPostgresType[field_type].value
                if field_type in OgrPostgresType.__members__
                else None
            )

            # Check if field type is defined
            if field_type_pg is None:
                field_types["unvalid"][field_name] = field_type
                continue
            # Create array for field names of respective type if not already existing
            if field_type_pg not in field_types["valid"].keys():
                field_types["valid"][field_type_pg] = []

            # Check if number of specified field excesses the maximum specified number
            if (
                NumberColumnsPerType[field_type_pg].value
                > len(field_types["valid"][field_type_pg])
                and field_name not in field_types["valid"][field_type_pg]
            ):
                field_types["valid"][field_type_pg].append(field_name)

            # Place fields that are exceeding the maximum number of columns or if the column name was already specified.
            elif (
                NumberColumnsPerType[field_type_pg] <= len(field_types["valid"][field_type_pg])
                or field_name in field_types["valid"][field_type_pg]
            ):
                field_types["overflow"][field_type_pg] = field_name

        return field_types

    def validate_csv(self):
        """Validate if CSV."""

        # Read from file_path and check if CSV is well-formed
        with open(self.file_path, "rb") as f:
            contents = f.readlines()
            csv_reader = csv.reader([line.decode() for line in contents])
            header = next(csv_reader)

            if not header:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="CSV is not well-formed: Missing header.",
                )

            if any(not col for col in header):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="CSV is not well-formed: Header contains empty values.",
                )

        # Load in df to get data types
        df = pd.read_csv(self.file_path)
        # Save in XLSX to keep data types
        df.to_excel(self.file_path + ".xlsx", index=False)
        return self.validate_ogr(self.file_path + ".xlsx")

    def validate_xlsx(self):
        """Validate if XLSX is well-formed."""
        # Load workbook
        wb = load_workbook(filename=self.file_path, read_only=True)

        # Check if only one sheet is present
        if len(wb.sheetnames) != 1:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="XLSX is not well-formed: More than one sheet is present.",
            )

        sheet = wb.active
        # Check header
        header = [cell.value for cell in sheet[1]]
        if any(value is None for value in header):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="XLSX is not well-formed: Header contains empty values.",
            )

        return self.validate_ogr(self.file_path)

    def validate_shapefile(self):
        """Validate if ZIP contains a valid shapefile."""
        with zipfile.ZipFile(self.file_path) as zip_ref:
            # List all file names in the zip file
            file_names = zip_ref.namelist()
            # Remove directories from the list of file names
            file_names = [f for f in file_names if not f.endswith("/")]

            # Check for required shapefile components
            extensions = [".shp", ".shx", ".dbf", ".prj"]
            valid_files = []
            for ext in extensions:
                matching_files = [f for f in file_names if f.endswith(ext)]
                if len(matching_files) != 1:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=f"ZIP must contain exactly one {ext} file.",
                    )
                valid_files.append(matching_files[0])

            # Check if the main shapefile components share the same base name
            base_name = os.path.splitext(valid_files[0])[0]
            if any(f"{base_name}{ext}" not in valid_files for ext in extensions):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="All main shapefile components (.shp, .shx, .dbf, .prj) must share the same base name.",
                )

        # Unzip file in temporary directory
        zip_dir = os.path.join(
            os.path.dirname(self.file_path), os.path.basename(self.file_path).split(".")[0]
        )

        # Extra zip file to zip_dir
        with zipfile.ZipFile(self.file_path) as zip_ref:
            zip_ref.extractall(zip_dir)

        return self.validate_ogr(os.path.join(zip_dir, base_name + ".shp"))

    def validate_gpkg(self):
        """Validate geopackage."""
        return self.validate_ogr(self.file_path)

    def validate_geojson(self):
        """Validate geojson."""
        return self.validate_ogr(self.file_path)

    def validate_kml(self):
        """Validate kml."""
        return self.validate_ogr(self.file_path)

    @job_log(job_step_name='validation')
    async def validate(self, job_id: UUID):
        # Save file to disk inside /tmp folder
        try:
            # Create folder
            create_dir(self.folder_path)
            # Save file
            with open(self.file_path, "wb") as buffer:
                buffer.write(self.file.file.read())
            # Run validation
            result = self.method_match[self.file_ending]()

            # Build object for job step status
            msg_text = ""
            if result["data_types"]["unvalid"] == {} and result["data_types"]["overflow"] == {}:
                msg_type = MsgType.info.value
                msg_text = "File is valid."
            else:
                msg_type = MsgType.warning.value
                if result["data_types"]["unvalid"]:
                    msg_text = f"The following attributes are not saved as they could not be mapped to a valid data type: {', '.join(result['data_types']['unvalid'].keys())}"
                if result["data_types"]["overflow"]:
                    msg_text = msg_text + f"The following attributes are not saved as they exceed the maximum number of columns per data type: {', '.join(result['data_types']['overflow'].keys())}"

            result["msg"] = Msg(type=msg_type, text=msg_text)
            result["status"] = JobStatusType.finished.value
            return result

        except Exception as e:
            # Clean up temporary files
            delete_dir(self.folder_path)

            # Build object for job step status
            msg = Msg(type=MsgType.error, text=str(e))
            return {
                "msg": msg,
                "status": JobStatusType.failed.value,
            }

    @job_log(job_step_name='upload')
    async def upload_ogr2ogr(
        self, validation_result: dict, temp_table_name: str, job_id: UUID
    ):
        """Upload file to database."""

        try:
            file_path = validation_result["file_path"]
            # Initialize OGR
            ogr.RegisterAll()

            # Setup the input GeoJSON data source
            driver = ogr.GetDriverByName(self.driver_name)
            data_source = driver.Open(file_path, 0)
            layer = data_source.GetLayer(0)
            layer.GetLayerDefn()

            # Prepare the ogr2ogr command
            if self.file_ending == FileUploadType.gpkg.value:
                layer_name = layer.GetName()
            else:
                layer_name = None

            # Build CMD command
            cmd = f'ogr2ogr -f "PostgreSQL" "PG:host={settings.POSTGRES_SERVER} dbname={settings.POSTGRES_DB} user={settings.POSTGRES_USER} password={settings.POSTGRES_PASSWORD} port={settings.POSTGRES_PORT}" {file_path} {layer_name} -nln {temp_table_name} -t_srs "EPSG:4326" -progress'

            # Execute the command command using subprocess
            subprocess.run(cmd, shell=True, check=True)

            # Close data source
            data_source = None

            # Build object for job step status
            msg = Msg(type=MsgType.info, text="File uploaded.")
            return {
                "msg": msg,
                "status": JobStatusType.finished.value,
            }
        except Exception as e:
            # Clean up temporary files
            delete_dir(self.folder_path)
            await self.async_session.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            await self.async_session.commit()

            # Build object for job step status
            msg = Msg(type=MsgType.error, text=str(e).replace("'", "''"))
            return {
                "msg": msg,
                "status": JobStatusType.failed.value,
            }

    @job_log(job_step_name='migration')
    async def migrate_target_table(
        self, validation_result: dict, attribute_mapping: dict, temp_table_name: str, layer_id: UUID, job_id: UUID
    ):
        """Migrate data from temporary table to target table."""

        try:
            data_types = validation_result["data_types"]
            geom_column = data_types["geometry"]["column_name"]
            geometry_type = data_types["geometry"]["type"]
            target_table = f"user_data.{SupportedOgrGeomType[geometry_type].value}_{str(self.user_id).replace('-', '')}"

            select_statement = ""
            insert_statement = ""
            for i in attribute_mapping:
                select_statement += f"{i} as {attribute_mapping[i]}, "
                insert_statement += f"{attribute_mapping[i]}, "
            select_statement = f"""SELECT {select_statement} {geom_column} AS geom, '{str(layer_id)}' FROM {temp_table_name}"""

            # Insert data in target table
            await self.async_session.execute(
                text(
                    f"INSERT INTO {target_table}({insert_statement} geom, layer_id) {select_statement}"
                )
            )
            await self.async_session.commit()
            return {
                "msg": Msg(type=MsgType.info, text="Data migrated."),
                "status": JobStatusType.finished.value,
            }

        except Exception as e:
            await self.async_session.rollback()
            # Clean up temporary files
            delete_dir(self.folder_path)
            await self.async_session.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
            await self.async_session.commit()

            # Delete data from user table if already inserted
            await self.async_session.execute(text(f"DELETE FROM {target_table} WHERE layer_id = '{str(layer_id)}'"))
            await self.async_session.commit()

            # Build object for job step status
            msg = Msg(type=MsgType.error, text=str(e).replace("'", "''"))
            return {
                "msg": msg,
                "status": JobStatusType.failed.value,
            }
