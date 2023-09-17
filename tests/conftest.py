import asyncio
import os
import subprocess
from datetime import datetime
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text

from src.core.config import settings
from src.endpoints.deps import get_db, session_manager
from src.main import app

now = datetime.now().strftime("%Y%m%d%H%M%S")
schema_name = f"test_{now}"


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def session_fixture(event_loop):
    session_manager.init(settings.ASYNC_SQLALCHEMY_DATABASE_URI)
    session_manager._engine.update_execution_options(
        schema_translate_map={"customer": schema_name}
    )
    async with session_manager.connect() as connection:
        await connection.execute(text(f"""CREATE SCHEMA IF NOT EXISTS {schema_name}"""))
        await session_manager.drop_all(connection)
        await session_manager.create_all(connection)
        # triggers_file = os.path.join(
        #     Path(__file__).resolve().parent.parent, "src/db/triggers.sql"
        # )
        # with open(triggers_file, "r") as f:
        #     content = f.read()
        # content = content.replace("customer", schema_name)
        # triggers_test_path = f"/tmp/{schema_name}_triggers.sql"
        # with open(triggers_test_path, "w") as f:
        #     f.write(content)

        # command = f'psql "postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_SERVER}/accounts" -f {triggers_test_path}'
        # process = subprocess.Popen(
        #     command,
        #     stdout=subprocess.PIPE,
        #     stderr=subprocess.PIPE,
        #     shell=True,
        #     universal_newlines=True,
        # )
        # output, error = process.communicate()
        # if process.returncode != 0:
        #     raise Exception(f"Error running psql command: {error}")
        # print("Triggers created")
        # os.remove(triggers_test_path)

    yield
    async with session_manager.connect() as connection:
        pass
        await connection.execute(
            text(f"""DROP SCHEMA IF EXISTS {schema_name} CASCADE""")
        )
    await session_manager.close()


@pytest_asyncio.fixture(autouse=True)
async def session_override(session_fixture):
    async def get_db_override():
        async with session_manager.session() as session:
            yield session

    app.dependency_overrides[get_db] = get_db_override


@pytest_asyncio.fixture
async def db_session():
    async with session_manager.session() as session:
        yield session

