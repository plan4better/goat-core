import contextlib
from typing import AsyncIterator
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    AsyncConnection,
    create_async_engine,
)
from sqlalchemy import event
import asyncpg


async def set_type_codec(
    conn,
    typenames,
    encode=lambda a: a,
    decode=lambda a: a,
    schema="pg_catalog",
    format="text",
):
    conn._check_open()
    for typename in typenames:
        typeinfo = await conn.fetchrow(
            asyncpg.introspection.TYPE_BY_NAME, typename, schema
        )
        if not typeinfo:
            raise ValueError(f"unknown type: {schema}.{typename}")

        oid = typeinfo["oid"]
        conn._protocol.get_settings().add_python_codec(
            oid, typename, schema, "scalar", encode, decode, format
        )

    # Statement cache is no longer valid due to codec changes.
    conn._drop_local_statement_cache()


async def setup(conn):
    # Register geometry type
    await conn.set_type_codec(
        "geometry", encoder=str, decoder=str, schema="public", format="text"
    )


    # Register h3 index type
    await conn.set_type_codec(
        "h3index", encoder=str, decoder=str, schema="public", format="text"
    )

    # Register integer array type
    await set_type_codec(
        conn,
        ["_int4"],
        encode=lambda a: "{"
        + ",".join(map(str, a))
        + "}",  # Convert list to PostgreSQL array literal
        decode=lambda a: (
            list(map(int, a.strip("{}").split(",")))
            if a is not None and a != "{}"
            else []
        ),  # Convert PostgreSQL array literal to list, handling None and empty array
        schema="pg_catalog",
        format="text",
    )
    # Register biginteger array type
    await set_type_codec(
        conn,
        ["_int8"],
        encode=lambda a: "{"
        + ",".join(map(str, a))
        + "}",  # Convert list to PostgreSQL array literal
        decode=lambda a: (
            list(map(int, a.strip("{}").split(",")))
            if a is not None and a != "{}"
            else []
        ),  # Convert PostgreSQL array literal to list
        schema="pg_catalog",
        format="text",
    )

    # # Register float array type
    await set_type_codec(
        conn,
        ["_float8"],
        encode=lambda a: "{"
        + ",".join(map(str, a))
        + "}",  # Convert list to PostgreSQL array literal
        decode=lambda a: (
            list(map(float, a.strip("{}").split(",")))
            if a is not None and a != "{}"
            else []
        ),  # Convert PostgreSQL array literal to list
        schema="pg_catalog",
        format="text",
    )

    # Register UUID array type
    await set_type_codec(
        conn,
        ["_uuid"],
        encode=lambda a: "{" + ",".join(a) + "}",  # Directly join UUID strings
        decode=lambda a: (
            a.strip("{}").split(",")
            if a is not None and a != "{}"
            else []
        ),  # Split string into UUID strings
        schema="pg_catalog",
        format="text",
    )


class DatabaseSessionManager:
    def __init__(self):
        self._engine: AsyncEngine | None = None
        self._session_maker: sessionmaker | None = None

    def init(self, host: str):
        self._engine = create_async_engine(
            host,
            isolation_level="AUTOCOMMIT",
            connect_args={"server_settings": {"application_name": "GOAT Core"}},
        )
        self._session_maker = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
            class_=AsyncSession,
        )
        self.register_event_listeners()

    def register_event_listeners(self):
        @event.listens_for(self._engine.sync_engine, "connect")
        def register_custom_types(dbapi_connection, connection_record):
            dbapi_connection.run_async(setup)

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        """
        Connect to the auth database and return a connection object.
        """
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise
            finally:
                await connection.close()

    async def close(self):
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")
        await self._engine.dispose()
        self._engine = None
        self._session_maker = None

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._session_maker is None:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self._session_maker()
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def create_all(self, connection: AsyncConnection):
        await connection.run_sync(SQLModel.metadata.create_all)

    async def drop_all(self, connection: AsyncConnection):
        await connection.run_sync(SQLModel.metadata.drop_all)


session_manager = DatabaseSessionManager()
