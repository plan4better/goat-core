# Standard library imports
import asyncio
import os

# Third party imports
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text

# Local application imports
from src.core.config import settings
from src.endpoints.deps import get_db, session_manager
from src.main import app
from src.schemas.active_mobility import (
    request_examples as active_mobility_request_examples,
)
from src.schemas.layer import LayerType
from src.schemas.layer import request_examples as layer_request_examples
from src.schemas.motorized_mobility import (
    request_examples_isochrone_car,
    request_examples_isochrone_pt,
)
from src.schemas.project import (
    request_examples as project_request_examples,
)
from src.utils import get_user_table
from tests.utils import (
    check_job_status,
    generate_random_string,
    upload_file,
    upload_invalid_file,
    upload_valid_file,
    upload_valid_files,
)

settings.RUN_AS_BACKGROUND_TASK = True
settings.USER_DATA_SCHEMA = "test_user_data"
settings.CUSTOMER_SCHEMA = "test_customer"
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
        schema_translate_map={"customer": settings.CUSTOMER_SCHEMA}
    )
    async with session_manager.connect() as connection:
        await connection.execute(
            text(f"""CREATE SCHEMA IF NOT EXISTS {settings.CUSTOMER_SCHEMA}""")
        )
        await connection.execute(
            text(f"""CREATE SCHEMA IF NOT EXISTS {settings.USER_DATA_SCHEMA}""")
        )
        await session_manager.drop_all(connection)
        await session_manager.create_all(connection)
    yield
    async with session_manager.connect() as connection:
        pass
        await connection.execute(
            text(f"""DROP SCHEMA IF EXISTS {settings.CUSTOMER_SCHEMA} CASCADE""")
        )
        await connection.execute(
            text(f"""DROP SCHEMA IF EXISTS {settings.USER_DATA_SCHEMA} CASCADE""")
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
        response = await client.post(
            f"{settings.API_V2_STR}/folder", json={"name": name}
        )
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
        response = await client.post(
            f"{settings.API_V2_STR}/folder", json={"name": name}
        )
        folder = response.json()
        created_folders.append(folder)

    yield created_folders

    # Teardown: Delete the folders after the test
    for folder in created_folders:
        await client.delete(f"{settings.API_V2_STR}/folder/{folder['id']}")


@pytest.fixture
async def fixture_create_project(
    client: AsyncClient, fixture_create_user, fixture_create_folder
):
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
async def fixture_create_projects(
    client: AsyncClient, fixture_create_user, fixture_create_folder
):
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
    fixture_create_internal_and_external_layer,
):
    project_id = fixture_create_project["id"]
    internal_layer, external_layer = fixture_create_internal_and_external_layer
    internal_layer_id = internal_layer["id"]
    external_layer_id = external_layer["id"]
    # Add layers to project
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={internal_layer_id}&layer_ids={external_layer_id}"
    )
    assert response.status_code == 200
    layer_project = response.json()

    # Get layer project ids
    layer_project_ids = []
    for layer in response.json():
        layer_project_ids.append(layer["id"])

    # Get Project
    response = await client.get(f"{settings.API_V2_STR}/project/{project_id}")
    assert response.status_code == 200

    # Check if layers are in layer order at right position
    layer_order = response.json()["layer_order"]
    assert layer_order[0] == layer_project_ids[0]
    assert layer_order[1] == layer_project_ids[1]

    return {"layer_project": layer_project, "project_id": project_id}


@pytest.fixture(autouse=True)
def set_testing_config():
    settings.TESTING = True
    yield
    settings.TESTING = False


@pytest.fixture()
async def fixture_upload_file_point(client: AsyncClient, fixture_create_user):
    return await upload_valid_file(client, "point")


@pytest.fixture()
async def fixture_upload_file_table(client: AsyncClient, fixture_create_user):
    return await upload_valid_file(client, "table")


file_types = ["point", "line", "polygon", "no_geometry"]


@pytest.fixture(params=file_types)
async def fixture_upload_files(client: AsyncClient, fixture_create_user, request):
    return await upload_valid_files(client, request.param)


files = [
    "invalid_wrong_file_extension.gpkg",
    "invalid_bad_formed.xlsx",
    "invalid_no_header.csv",
    "invalid_missing_file.zip",
]


@pytest.fixture(params=files)
async def fixture_upload_file_invalid(
    client: AsyncClient, fixture_create_user, request
):
    return await upload_invalid_file(client, request.param)


async def create_internal_layer(
    client: AsyncClient, dataset_id, fixture_get_home_folder, layer_type
):
    # Get feature layer dict and add layer ID
    feature_layer_dict = layer_request_examples["create_internal"][layer_type]["value"]
    feature_layer_dict["name"] = generate_random_string(10)
    feature_layer_dict["dataset_id"] = dataset_id
    feature_layer_dict["folder_id"] = fixture_get_home_folder["id"]
    # Hit endpoint to create internal layer
    response = await client.post(
        f"{settings.API_V2_STR}/layer/internal", json=feature_layer_dict
    )
    assert response.status_code == 201

    # Get job id
    job_id = response.json()["job_id"]

    # Check if job is finished
    job = await check_job_status(client, job_id)
    assert job["status_simple"] == "finished"

    # Get layer
    response = await client.get(f"{settings.API_V2_STR}/layer/{job['layer_ids'][0]}")
    assert response.status_code == 200

    return {**response.json(), "job_id": job_id}


internal_layers = ["feature_layer_standard", "table"]


async def upload_and_create_layer(client, file_dir, home_folder, layer_type):
    dataset = await upload_file(client, file_dir)
    layer = await create_internal_layer(
        client,
        dataset["dataset_id"],
        home_folder,
        layer_type,
    )
    return layer


@pytest.fixture(params=internal_layers)
async def fixture_create_internal_layers(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder, request
):
    if request.param == "feature_layer_standard":
        metadata = await upload_valid_file(client, "point")
        layer = await create_internal_layer(
            client, metadata["dataset_id"], fixture_get_home_folder, request.param
        )
    elif request.param == "table":
        metadata = await upload_valid_file(client, "no_geometry")
        layer = await create_internal_layer(
            client, metadata["dataset_id"], fixture_get_home_folder, request.param
        )
    return layer


@pytest.fixture
async def fixture_create_polygon_layer(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    dir_gpkg = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "zipcode_polygon.gpkg"
    )
    return await upload_and_create_layer(
        client, dir_gpkg, fixture_get_home_folder, "feature_layer_standard"
    )


@pytest.fixture
async def fixture_add_polygon_layer_to_project(
    client: AsyncClient, fixture_create_project, fixture_create_polygon_layer
):
    layer_gpkg = fixture_create_polygon_layer
    project_id = fixture_create_project["id"]
    # Add layers to project
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_gpkg['id']}"
    )
    assert response.status_code == 200
    layers_project = response.json()
    layer_project_id = layers_project[0]["id"]

    return {
        "layer_project_id": layer_project_id,
        "project_id": project_id,
    }


@pytest.fixture
async def fixture_create_large_polygon_layer(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    dir_gpkg = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "large_polygon.gpkg"
    )
    return await upload_and_create_layer(
        client, dir_gpkg, fixture_get_home_folder, "feature_layer_standard"
    )


@pytest.fixture
async def fixture_add_large_polygon_layer_to_project(
    client: AsyncClient, fixture_create_project, fixture_create_large_polygon_layer
):
    layer_gpkg = fixture_create_large_polygon_layer
    project_id = fixture_create_project["id"]
    # Add layers to project
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_gpkg['id']}"
    )
    assert response.status_code == 200
    layers_project = response.json()
    layer_project_id = layers_project[0]["id"]

    return {
        "layer_project_id": layer_project_id,
        "project_id": project_id,
    }


@pytest.fixture
async def fixture_create_join_layers(client: AsyncClient, fixture_get_home_folder):
    dir_csv = os.path.join(settings.TEST_DATA_DIR, "layers", "tool", "events.csv")
    dir_gpkg = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "zipcode_polygon.gpkg"
    )
    layer_csv = await upload_and_create_layer(
        client, dir_csv, fixture_get_home_folder, "table"
    )
    layer_gpkg = await upload_and_create_layer(
        client, dir_gpkg, fixture_get_home_folder, "feature_layer_standard"
    )
    return {
        "home_folder": fixture_get_home_folder,
        "layer_csv": layer_csv,
        "layer_gpkg": layer_gpkg,
    }


@pytest.fixture
async def fixture_create_aggregate_point_layers(
    client: AsyncClient, fixture_get_home_folder
):
    dir_points = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "points_with_category.gpkg"
    )
    dir_polygons = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "zipcode_polygon.gpkg"
    )
    layer_points = await upload_and_create_layer(
        client, dir_points, fixture_get_home_folder, "feature_layer_standard"
    )
    layer_polygons = await upload_and_create_layer(
        client, dir_polygons, fixture_get_home_folder, "feature_layer_standard"
    )
    return {
        "home_folder": fixture_get_home_folder,
        "layer_points": layer_points,
        "layer_polygons": layer_polygons,
    }


@pytest.fixture
async def fixture_create_aggregate_point_layer(
    client: AsyncClient, fixture_get_home_folder
):
    dir_points = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "points_with_category.gpkg"
    )
    layer_points = await upload_and_create_layer(
        client, dir_points, fixture_get_home_folder, "feature_layer_standard"
    )
    return {"home_folder": fixture_get_home_folder, "layer_points": layer_points}


@pytest.fixture
async def fixture_add_join_layers_to_project(
    client: AsyncClient, fixture_create_project, fixture_create_join_layers
):
    layer_csv = fixture_create_join_layers["layer_csv"]
    layer_gpkg = fixture_create_join_layers["layer_gpkg"]
    project_id = fixture_create_project["id"]
    # Add layers to project
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_csv['id']}&layer_ids={layer_gpkg['id']}"
    )
    assert response.status_code == 200
    layers_project = response.json()
    for layer in layers_project:
        if layer["type"] == LayerType.table.value:
            layer_id_table = layer["id"]
        elif layer["type"] == LayerType.feature.value:
            layer_id_gpkg = layer["id"]

    return {
        "layer_id_table": layer_id_table,
        "layer_id_gpkg": layer_id_gpkg,
        "project_id": project_id,
    }


@pytest.fixture
async def fixture_add_aggregate_point_layers_to_project(
    client: AsyncClient, fixture_create_project, fixture_create_aggregate_point_layers
):
    layer_points = fixture_create_aggregate_point_layers["layer_points"]
    layer_polygons = fixture_create_aggregate_point_layers["layer_polygons"]
    project_id = fixture_create_project["id"]

    # Add layers to project one by one and return layer_project_id
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_points['id']}"
    )
    assert response.status_code == 200
    layer_project_points = response.json()
    source_layer_project_id = layer_project_points[0]["id"]

    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_polygons['id']}"
    )
    assert response.status_code == 200
    layer_project_polygons = response.json()
    aggregation_layer_project_id = layer_project_polygons[0]["id"]

    return {
        "source_layer_project_id": source_layer_project_id,
        "aggregation_layer_project_id": aggregation_layer_project_id,
        "project_id": project_id,
    }


@pytest.fixture
async def fixture_add_aggregate_point_layer_to_project(
    client: AsyncClient, fixture_create_project, fixture_create_aggregate_point_layer
):
    layer_points = fixture_create_aggregate_point_layer["layer_points"]
    project_id = fixture_create_project["id"]

    # Add layers to project one by one and return layer_project_id
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_points['id']}"
    )
    assert response.status_code == 200
    layer_project_points = response.json()
    source_layer_project_id = layer_project_points[0]["id"]

    return {
        "source_layer_project_id": source_layer_project_id,
        "project_id": project_id,
    }


@pytest.fixture
async def fixture_create_aggregate_polygon_layers(
    client: AsyncClient, fixture_get_home_folder
):
    dir_source_layer = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "green_areas.gpkg"
    )
    dir_aggregation_layer = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "zipcode_polygon.gpkg"
    )
    layer_source = await upload_and_create_layer(
        client, dir_source_layer, fixture_get_home_folder, "feature_layer_standard"
    )
    layer_aggregation = await upload_and_create_layer(
        client, dir_aggregation_layer, fixture_get_home_folder, "feature_layer_standard"
    )
    return {
        "home_folder": fixture_get_home_folder,
        "layer_source": layer_source,
        "layer_aggregation": layer_aggregation,
    }


@pytest.fixture
async def fixture_create_aggregate_polygon_layer(
    client: AsyncClient, fixture_get_home_folder
):
    dir_source_layer = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "green_areas.gpkg"
    )
    layer_source = await upload_and_create_layer(
        client, dir_source_layer, fixture_get_home_folder, "feature_layer_standard"
    )
    return {
        "home_folder": fixture_get_home_folder,
        "layer_source": layer_source,
    }


@pytest.fixture
async def fixture_add_aggregate_polygon_layers_to_project(
    client: AsyncClient, fixture_create_project, fixture_create_aggregate_polygon_layers
):
    layer_source = fixture_create_aggregate_polygon_layers["layer_source"]
    layer_aggregation = fixture_create_aggregate_polygon_layers["layer_aggregation"]
    project_id = fixture_create_project["id"]

    # Add layers to project one by one and return layer_project_id
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_source['id']}"
    )
    assert response.status_code == 200
    layer_project_source = response.json()
    source_layer_project_id = layer_project_source[0]["id"]

    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_aggregation['id']}"
    )
    assert response.status_code == 200
    layer_project_aggregation = response.json()
    aggregation_layer_project_id = layer_project_aggregation[0]["id"]

    return {
        "source_layer_project_id": source_layer_project_id,
        "aggregation_layer_project_id": aggregation_layer_project_id,
        "project_id": project_id,
    }


@pytest.fixture
async def fixture_add_aggregate_polygon_layer_to_project(
    client: AsyncClient, fixture_create_project, fixture_create_aggregate_polygon_layer
):
    layer_source = fixture_create_aggregate_polygon_layer["layer_source"]
    project_id = fixture_create_project["id"]

    # Add layers to project one by one and return layer_project_id
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_source['id']}"
    )
    assert response.status_code == 200
    layer_project_source = response.json()
    source_layer_project_id = layer_project_source[0]["id"]

    return {
        "source_layer_project_id": source_layer_project_id,
        "project_id": project_id,
    }


layer_types = ["point", "line", "polygon"]


@pytest.fixture(params=layer_types)
async def fixture_create_basic_layer(
    request, client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    layer_type = request.param
    dir_gpkg = os.path.join(
        settings.TEST_DATA_DIR, "layers", "valid", layer_type, "valid.gpkg"
    )
    layer = await upload_and_create_layer(
        client, dir_gpkg, fixture_get_home_folder, "feature_layer_standard"
    )
    return {
        "home_folder": fixture_get_home_folder,
        "layer": layer,
        "layer_type": layer_type,
    }


@pytest.fixture
async def fixture_add_basic_layer_to_project(
    client: AsyncClient, fixture_create_project, fixture_create_basic_layer
):
    layer = fixture_create_basic_layer["layer"]
    project_id = fixture_create_project["id"]
    # Add layers to project
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer['id']}"
    )
    assert response.status_code == 200
    layers_project = response.json()
    layer_project_id = layers_project[0]["id"]

    return {
        "layer_project_id": layer_project_id,
        "project_id": project_id,
        "layer_type": fixture_create_basic_layer["layer_type"],
    }


geometry_layers = ["zipcode_polygon.gpkg", "zipcode_point.gpkg"]


@pytest.fixture(params=geometry_layers)
async def fixture_add_origin_destination_layers_to_project(
    request,
    client: AsyncClient,
    fixture_create_project,
    fixture_get_home_folder,
):
    # Create layers
    dir_origin_destination_matrix = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", "origin_destination_matrix.csv"
    )
    dir_geometry_layer = os.path.join(
        settings.TEST_DATA_DIR, "layers", "tool", request.param
    )
    layer_origin_destination_matrix = await upload_and_create_layer(
        client, dir_origin_destination_matrix, fixture_get_home_folder, "table"
    )
    layer_geometry_layer = await upload_and_create_layer(
        client, dir_geometry_layer, fixture_get_home_folder, "feature_layer_standard"
    )
    project_id = fixture_create_project["id"]

    # Add layers to project one by one and return layer_project_id
    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_origin_destination_matrix['id']}"
    )
    assert response.status_code == 200
    layer_project_origin_destination_matrix = response.json()
    origin_destination_matrix_layer_project_id = (
        layer_project_origin_destination_matrix[0]["id"]
    )

    response = await client.post(
        f"{settings.API_V2_STR}/project/{project_id}/layer?layer_ids={layer_geometry_layer['id']}"
    )
    assert response.status_code == 200
    layer_project_geometry_layer = response.json()
    geometry_layer_project_id = layer_project_geometry_layer[0]["id"]

    return {
        "origin_destination_matrix_layer_project_id": origin_destination_matrix_layer_project_id,
        "geometry_layer_project_id": geometry_layer_project_id,
        "project_id": project_id,
    }


async def create_external_layer(client: AsyncClient, home_folder, layer_type):
    # Get table layer dict and add layer ID
    external_layer_dict = layer_request_examples["create_external"][layer_type]["value"]
    external_layer_dict["folder_id"] = home_folder["id"]

    # Give layer a random name
    external_layer_dict["name"] = generate_random_string(10)
    # Hit endpoint to create external layer
    response = await client.post(
        f"{settings.API_V2_STR}/layer/external", json=external_layer_dict
    )
    assert response.status_code == 201
    return response.json()


external_layers = ["external_vector_tile", "external_imagery"]


@pytest.fixture(params=external_layers)
async def fixture_create_external_layers(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder, request
):
    return await create_external_layer(client, fixture_get_home_folder, request.param)


@pytest.fixture
async def fixture_create_external_layer(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    return await create_external_layer(
        client, fixture_get_home_folder, "external_vector_tile"
    )


@pytest.fixture
async def fixture_create_internal_feature_layer(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    metadata = await upload_valid_file(client, "point")
    return await create_internal_layer(
        client,
        metadata["dataset_id"],
        fixture_get_home_folder,
        "feature_layer_standard",
    )


@pytest.fixture
async def fixture_create_internal_feature_polygon_layer(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    metadata = await upload_valid_file(client, "polygon")
    return await create_internal_layer(
        client,
        metadata["dataset_id"],
        fixture_get_home_folder,
        "feature_layer_standard",
    )


@pytest.fixture
async def fixture_create_internal_and_external_layer(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    metadata = await upload_valid_file(client, "point")
    internal_layer = await create_internal_layer(
        client,
        metadata["dataset_id"],
        fixture_get_home_folder,
        "feature_layer_standard",
    )
    external_layer = await create_external_layer(
        client, fixture_get_home_folder, "external_vector_tile"
    )
    return internal_layer, external_layer


@pytest.fixture
async def fixture_create_internal_table_layer(
    client: AsyncClient, fixture_create_user, fixture_get_home_folder
):
    metadata = await upload_valid_file(client, "no_geometry")
    return await create_internal_layer(
        client, metadata["dataset_id"], fixture_get_home_folder, "table"
    )


@pytest.fixture
async def fixture_delete_internal_layers(
    client: AsyncClient, fixture_create_internal_layers
):
    layer = fixture_create_internal_layers
    layer_id = layer["id"]
    response = await client.delete(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 204

    # Check if layer is deleted
    response = await client.get(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 404  # Not Found

    # Get table name
    table_name = get_user_table(layer)

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
async def fixture_delete_external_layers(
    client: AsyncClient, fixture_create_external_layers
):
    layer = fixture_create_external_layers
    layer_id = layer["id"]
    response = await client.delete(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 204

    response = await client.get(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 404  # Not Found


def get_payload_types(request_examples: dict) -> list:
    return request_examples


def create_generic_toolbox_fixture(endpoint: str, request_examples: dict):
    @pytest.fixture(params=get_payload_types(request_examples))
    async def generic_post_fixture(
        client: AsyncClient, fixture_create_project, request
    ):
        payload = request_examples[request.param]["value"]
        project_id = fixture_create_project["id"]
        response = await client.post(
            f"{settings.API_V2_STR}{endpoint}?project_id={project_id}", json=payload
        )
        assert response.status_code == 201
        return response.json()

    return generic_post_fixture


fixture_isochrone_active_mobility = create_generic_toolbox_fixture(
    "/active-mobility/isochrone",
    active_mobility_request_examples["isochrone_active_mobility"],
)

fixture_isochrone_pt = create_generic_toolbox_fixture(
    "/motorized-mobility/pt/isochrone",
    request_examples_isochrone_pt,
)

fixture_isochrone_car = create_generic_toolbox_fixture(
    "/motorized-mobility/car/isochrone",
    request_examples_isochrone_car,
)
