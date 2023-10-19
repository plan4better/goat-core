import asyncio
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text
from src.core.config import settings
from src.endpoints.deps import get_db, session_manager
from src.main import app
from src.schemas.project import request_examples as project_request_examples
from tests.utils import validate_valid_files, validate_invalid_file

settings.RUN_AS_BACKGROUND_TASK = True
settings.USER_DATA_SCHEMA = "test_user_data"
schema_customer = "test_customer"
schema_user_data = "test_user_data"

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
        schema_translate_map={"customer": schema_customer}
    )
    async with session_manager.connect() as connection:
        await connection.execute(text(f"""CREATE SCHEMA IF NOT EXISTS {schema_customer}"""))
        await connection.execute(text(f"""CREATE SCHEMA IF NOT EXISTS {schema_user_data}"""))
        await session_manager.drop_all(connection)
        await session_manager.create_all(connection)
    yield
    async with session_manager.connect() as connection:
        pass
        await connection.execute(text(f"""DROP SCHEMA IF EXISTS {schema_customer} CASCADE"""))
        await connection.execute(text(f"""DROP SCHEMA IF EXISTS {schema_user_data} CASCADE"""))
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


@pytest.fixture
async def create_user(client: AsyncClient):
    # Setup: Create the user
    response = await client.post(f"{settings.API_V2_STR}/user")
    user = response.json()
    yield user
    # Teardown: Delete the user after the test
    await client.delete(f"{settings.API_V2_STR}/user")


@pytest.fixture
async def create_folder(client: AsyncClient, create_user):
    # Setup: Create the folder
    response = await client.post(f"{settings.API_V2_STR}/folder", json={"name": "test"})
    folder = response.json()
    yield folder
    # Teardown: Delete the folder after the test
    await client.delete(f"{settings.API_V2_STR}/folder/{folder['id']}")


@pytest.fixture
async def create_folders(client: AsyncClient, create_user):
    folder_names = ["test1", "test2", "test3"]
    created_folders = []

    # Setup: Create multiple folders
    for name in folder_names:
        response = await client.post(f"{settings.API_V2_STR}/folder", json={"name": name})
        folder = response.json()
        created_folders.append(folder)

    yield created_folders

    # Teardown: Delete the folders after the test
    for folder in created_folders:
        await client.delete(f"{settings.API_V2_STR}/folder/{folder['id']}")


@pytest.fixture
async def create_project(client: AsyncClient, create_user, create_folder):
    # Assuming create_folder yields a folder object
    folder = create_folder

    # Setup: Create the project within the folder
    example = project_request_examples["create"]
    example["folder_id"] = folder["id"]
    response = await client.post(f"{settings.API_V2_STR}/project", json=example)
    project = response.json()

    yield project

    # Teardown: Delete the project after the test
    # Note: Folder deletion will be handled by the create_folder fixture's teardown
    await client.delete(f"{settings.API_V2_STR}/project/{project['id']}")

@pytest.fixture
async def create_projects(client: AsyncClient, create_user, create_folder):
    project_names = ["test1", "test2", "test3"]

    # Assuming create_folder yields a folder object
    folder = create_folder

    # Setup: Create the project within the folder
    example = project_request_examples["create"]
    example["folder_id"] = folder["id"]
    created_projects = []
    for i in project_names:
        example["name"] = i
        response = await client.post(f"{settings.API_V2_STR}/project", json=example)
        project = response.json()
        created_projects.append(project)

    yield created_projects

    # Teardown: Delete the project after the test
    # Note: Folder deletion will be handled by the create_folder fixture's teardown
    for project in created_projects:
        await client.delete(f"{settings.API_V2_STR}/project/{project['id']}")

@pytest.fixture(autouse=True)
def set_testing_config():
    settings.TESTING = True
    yield
    settings.TESTING = False

file_types = ["point", "line", "polygon", "no_geometry"]
@pytest.fixture(params=file_types)
async def validate_files(client: AsyncClient, create_user, request):
    return await validate_valid_files(client, request.param)

files = ["invalid_wrong_file_extension.gpkg", "invalid_bad_formed.xlsx", "invalid_no_header.csv", "invalid_missing_file.zip"]
@pytest.fixture(params=files)
async def validate_file_invalid(client: AsyncClient, create_user, request):
    return await validate_invalid_file(client, request.param)