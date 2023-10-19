from src.db.models.user import User
from .base import CRUDBase
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from src.utils import table_exists
from src.schemas.layer import NumberColumnsPerType
from src.core.config import settings

class CRUDUser(CRUDBase):
    async def create_user_data_tables(self, async_session: AsyncSession, user_id: UUID):
        """Create the user data tables."""

        for table_type in ["point", "line", "polygon", "no_geometry"]:
            table_name = f"{table_type}_{str(user_id).replace('-', '')}"

            # Check if table exists
            if not await table_exists(async_session, settings.USER_DATA_SCHEMA, table_name):
                # Create table
                if table_type == "no_geometry":
                    geom_column = ""
                else:
                    geom_column = "geom GEOMETRY,"

                sql_create_table = f"""
                CREATE TABLE {settings.USER_DATA_SCHEMA}."{table_name}" (
                    id SERIAL PRIMARY KEY,
                    layer_id UUID NOT NULL,
                    {geom_column}
                    {', '.join([f'integer_attr{i+1} INTEGER' for i in range(NumberColumnsPerType.integer.value)])},
                    {', '.join([f'bigint_attr{i+1} BIGINT' for i in range(NumberColumnsPerType.bigint.value)])},
                    {', '.join([f'float_attr{i+1} FLOAT' for i in range(NumberColumnsPerType.float.value)])},
                    {', '.join([f'text_attr{i+1} TEXT' for i in range(NumberColumnsPerType.text.value)])},
                    {', '.join([f'jsonb_attr{i+1} jsonb' for i in range(NumberColumnsPerType.jsonb.value)])},
                    {', '.join([f'arrint_attr{i+1} INTEGER[]' for i in range(NumberColumnsPerType.arrint.value)])},
                    {', '.join([f'arrfloat_attr{i+1} FLOAT[]' for i in range(NumberColumnsPerType.arrfloat.value)])},
                    {', '.join([f'arrtext_attr{i+1} TEXT[]' for i in range(NumberColumnsPerType.arrtext.value)])},
                    {', '.join([f'timestamp_attr{i+1} TIMESTAMP' for i in range(NumberColumnsPerType.timestamp.value)])},
                    {', '.join([f'boolean_attr{i+1} BOOLEAN' for i in range(NumberColumnsPerType.boolean.value)])}
                );
                """
                await async_session.execute(text(sql_create_table))
                # Create GIST Index
                if table_type != "no_geometry":
                    await async_session.execute(text(f"""CREATE INDEX ON {settings.USER_DATA_SCHEMA}."{table_name}" USING GIST(geom);"""))
                # Create Index on ID
                await async_session.execute(text(f"""CREATE INDEX ON {settings.USER_DATA_SCHEMA}."{table_name}" (id);"""))

            else:
                print(f"Table '{table_name}' already exists.")

        # Commit changes
        await async_session.commit()

    async def delete_user_data_tables(self, async_session: AsyncSession, user_id: UUID):
        """Delete the user data tables."""

        for table_type in ["point", "line", "polygon", "no_geometry"]:
            table_name = f"{table_type}_{str(user_id).replace('-', '')}"

            # Check if table exists
            if await table_exists(async_session, settings.USER_DATA_SCHEMA, table_name):
                sql_delete_table = f"""
                DROP TABLE IF EXISTS {settings.USER_DATA_SCHEMA}."{table_name}";
                """
                await async_session.execute(text(sql_delete_table))
            else:
                print(f"Table '{table_name}' does not exist.")
        await async_session.commit()


user = CRUDUser(User)
