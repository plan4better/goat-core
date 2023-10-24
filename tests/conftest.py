import asyncio

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text
from src.core.config import settings
from src.endpoints.deps import get_db, session_manager
from src.main import app
from src.schemas.layer import request_examples as layer_request_examples
from src.schemas.project import request_examples as project_request_examples, initial_view_state_example
from tests.utils import (
    upload_file,
    validate_invalid_file,
    validate_valid_file,
    validate_valid_files,
)
from src.utils import get_user_table

settings.RUN_AS_BACKGROUND_TASK = True
settings.USER_DATA_SCHEMA = "test_user_data"
schema_customer = "test_customer"
schema_user_data = "test_user_data"
settings.MAX_FOLDER_COUNT = 10


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
async def fixture_create_user(client: AsyncClient):
    # Setup: Create the user
    response = await client.post(f"{settings.API_V2_STR}/user")
    user = response.json()
    yield user
    # Teardown: Delete the user after the test
    await client.delete(f"{settings.API_V2_STR}/user")


@pytest.fixture
async def fixture_create_folder(client: AsyncClient, fixture_create_user):
    # Setup: Create the folder
    response = await client.post(f"{settings.API_V2_STR}/folder", json={"name": "test"})
    folder = response.json()
    yield folder
    # Teardown: Delete the folder after the test
    await client.delete(f"{settings.API_V2_STR}/folder/{folder['id']}")


@pytest.fixture
async def fixture_get_home_folder(client: AsyncClient):
    response = await client.get(
        f"{settings.API_V2_STR}/folder?search=home&order=descendent&page=1&size=1",
    )
    assert response.status_code == 200
    return response.json()[0]


@pytest.fixture
async def fixture_create_exceed_folders(client: AsyncClient, fixture_create_user):
    max_folder_cnt = settings.MAX_FOLDER_COUNT
    folder_names = [f"test{i}" for i in range(1, max_folder_cnt + 1)]

    # Setup: Create multiple folders
    cnt = 0
    for name in folder_names:
        cnt += 1
        # Request to create a folder
        response = await client.post(f"{settings.API_V2_STR}/folder", json={"name": name})
        if cnt >= max_folder_cnt:
            assert response.status_code == 429  # Too Many Requests
        else:
            assert response.status_code == 201


@pytest.fixture
async def fixture_create_folders(client: AsyncClient, fixture_create_user):
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
async def fixture_create_project(client: AsyncClient, fixture_create_user, fixture_create_folder):
    # Assuming fixture_create_folder yields a folder object
    folder = fixture_create_folder

    # Setup: Create the project within the folder
    example = project_request_examples["create"]
    example["folder_id"] = folder["id"]
    response = await client.post(f"{settings.API_V2_STR}/project", json=example)
    project = response.json()

    yield project

    # Teardown: Delete the project after the test
    # Note: Folder deletion will be handled by the fixture_create_folder fixture's teardown
    await client.delete(f"{settings.API_V2_STR}/project/{project['id']}")


@pytest.fixture
async def fixture_create_projects(client: AsyncClient, fixture_create_user, fixture_create_folder):
    project_names = ["test1", "test2", "test3"]

    # Assuming fixture_create_folder yields a folder object
    folder = fixture_create_folder

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
    # Note: Folder deletion will be handled by the fixture_create_folder fixture's teardown
    for project in created_projects:
        await client.delete(f"{settings.API_V2_STR}/project/{project['id']}")


@pytest.fixture
async def fixture_create_layer_project(
    client: AsyncClient,
    fixture_create_project,
    fixture_create_internal_layer,
):
    project_id = fixture_create_project["id"]
    layer_id = fixture_create_internal_layer["id"]

    # Add layers to project
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer", json={"layer_ids": [layer_id]}
    )
    assert response.status_code == 200
    return response.json()




@pytest.fixture(autouse=True)
def set_testing_config():
    settings.TESTING = True
    yield
    settings.TESTING = False


@pytest.fixture()
async def fixture_validate_file_point(client: AsyncClient, fixture_create_user):
    return await validate_valid_file(client, "point")


@pytest.fixture()
async def fixture_validate_file_table(client: AsyncClient, fixture_create_user):
    return await validate_valid_file(client, "table")


file_types = ["point", "line", "polygon", "no_geometry"]


@pytest.fixture(params=file_types)
async def fixture_validate_files(client: AsyncClient, fixture_create_user, request):
    return await validate_valid_files(client, request.param)


files = [
    "invalid_wrong_file_extension.gpkg",
    "invalid_bad_formed.xlsx",
    "invalid_no_header.csv",
    "invalid_missing_file.zip",
]


@pytest.fixture(params=files)
async def fixture_validate_file_invalid(client: AsyncClient, fixture_create_user, request):
    return await validate_invalid_file(client, request.param)


async def create_internal_layer(
    client: AsyncClient, validate_job_id, fixture_get_home_folder, layer_type
):
    # Hit endpoint to upload file
    job_id = await upload_file(client, validate_job_id)

    # Get feature layer dict and add layer ID
    feature_layer_dict = layer_request_examples["create_internal"][layer_type]["value"]
    feature_layer_dict["import_job_id"] = job_id
    feature_layer_dict["folder_id"] = fixture_get_home_folder["id"]
    # Hit endpoint to create internal layer
    response = await client.post(f"{settings.API_V2_STR}/layer/internal", json=feature_layer_dict)
    assert response.status_code == 201
    return response.json()


internal_layers = ["feature_layer_standard", "table_layer"]


@pytest.fixture(params=internal_layers)
async def fixture_create_internal_layers(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder, request
):
    if request.param == "feature_layer_standard":
        validate_job_id = await validate_valid_file(client, "point")
        layer = await create_internal_layer(
            client, validate_job_id, fixture_get_home_folder, request.param
        )
    elif request.param == "table_layer":
        validate_job_id = await validate_valid_file(client, "no_geometry")
        layer = await create_internal_layer(
            client, validate_job_id, fixture_get_home_folder, request.param
        )
    return layer


async def create_external_layer(client: AsyncClient, fixture_get_home_folder, layer_type):
    # Get table layer dict and add layer ID
    external_layer_dict = layer_request_examples["create_external"][layer_type]["value"]
    external_layer_dict["folder_id"] = fixture_get_home_folder["id"]
    # Hit endpoint to create external layer
    response = await client.post(f"{settings.API_V2_STR}/layer/external", json=external_layer_dict)
    assert response.status_code == 201
    return response.json()


external_layers = ["tile_layer", "imagery_layer"]


@pytest.fixture(params=external_layers)
async def fixture_create_external_layers(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder, request
):
    return await create_external_layer(client, fixture_get_home_folder, request.param)


@pytest.fixture
async def fixture_create_external_layer(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    return await create_external_layer(client, fixture_get_home_folder, "tile_layer")


@pytest.fixture
async def fixture_create_internal_layer(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    validate_job_id = await validate_valid_file(client, "point")
    return await create_internal_layer(
        client, validate_job_id, fixture_get_home_folder, "feature_layer_standard"
    )


@pytest.fixture
async def fixture_delete_internal_layers(client: AsyncClient, fixture_create_internal_layers):
    layer = fixture_create_internal_layers
    layer_id = layer["id"]
    response = await client.delete(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 204

    # Check if layer is deleted
    response = await client.get(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 404  # Not Found

    # Check if user data table has not data
    if layer["type"] == "feature_layer":
        table_name = get_user_table(layer["user_id"], layer["feature_layer_geometry_type"])
    else:
        table_name = get_user_table(layer["user_id"], "no_geometry")

    # Check if there is data for the layer_id
    async with session_manager.session() as session:
        result = await session.execute(
            text(
                f"""SELECT COUNT(*) FROM {table_name} WHERE layer_id = :layer_id LIMIT 1""",
            ),
            {"layer_id": layer_id},
        )
        assert result.scalar() == 0


@pytest.fixture
async def fixture_delete_external_layers(client: AsyncClient, fixture_create_external_layers):
    layer = fixture_create_external_layers
    layer_id = layer["id"]
    response = await client.delete(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 204

    response = await client.get(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 404  # Not Found
