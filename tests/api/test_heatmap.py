import pytest
from typing import List
from httpx import AsyncClient
from src.core.config import settings
from tests.utils import check_job_status
from src.schemas.heatmap import (
    ImpedanceFunctionType,
    ActiveRoutingHeatmapType,
    MotorizedRoutingHeatmapType,
)

# TODO: Upload larger heatmap-specific input/opportunity layers to test functionality in a more robust way

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "routing_type,impedance_function,opportunities",
    [
        (ActiveRoutingHeatmapType.walking, ImpedanceFunctionType.gaussian, [{"max_traveltime": 15, "sensitivity": 150000}]),
        (ActiveRoutingHeatmapType.walking, ImpedanceFunctionType.linear, [{"max_traveltime": 20, "sensitivity": 200000}, {"max_traveltime": 30, "sensitivity": 300000}]),
        (ActiveRoutingHeatmapType.bicycle, ImpedanceFunctionType.exponential, [{"max_traveltime": 15, "sensitivity": 150000}]),
        (ActiveRoutingHeatmapType.bicycle, ImpedanceFunctionType.power, [{"max_traveltime": 20, "sensitivity": 200000}, {"max_traveltime": 30, "sensitivity": 300000}]),
        (ActiveRoutingHeatmapType.pedelec, ImpedanceFunctionType.gaussian, [{"max_traveltime": 15, "sensitivity": 150000}]),
        (ActiveRoutingHeatmapType.pedelec, ImpedanceFunctionType.linear, [{"max_traveltime": 20, "sensitivity": 200000}, {"max_traveltime": 30, "sensitivity": 300000}]),
        (MotorizedRoutingHeatmapType.car, ImpedanceFunctionType.exponential, [{"max_traveltime": 15, "sensitivity": 150000}]),
        (MotorizedRoutingHeatmapType.car, ImpedanceFunctionType.power, [{"max_traveltime": 30, "sensitivity": 300000}, {"max_traveltime": 60, "sensitivity": 600000}]),
    ]
)
async def test_heatmap_gravity(
    client: AsyncClient,
    fixture_add_aggregate_point_layer_to_project,
    routing_type: ActiveRoutingHeatmapType | MotorizedRoutingHeatmapType,
    impedance_function: ImpedanceFunctionType,
    opportunities: List[dict],
):
    # Generate sample layers for conducting the test
    project_id = fixture_add_aggregate_point_layer_to_project["project_id"]
    layer_project_id = fixture_add_aggregate_point_layer_to_project["source_layer_project_id"]

    # Produce request payload
    params = {
        "routing_type": routing_type.value,
        "impedance_function": impedance_function.value,
        "opportunities": [{
            "opportunity_layer_project_id": layer_project_id,
            "max_traveltime": item["max_traveltime"],
            "sensitivity": item["sensitivity"],
            "destination_potential_column": None, # TODO: Add dynamic destination potential column
        } for item in opportunities]
    }

    # Call endpoint
    endpoint_type = "active-mobility" if type(routing_type) == ActiveRoutingHeatmapType else "motorized-mobility"
    response = await client.post(
        f"{settings.API_V2_STR}/{endpoint_type}/heatmap-gravity?project_id={project_id}",
        json=params,
    )
    assert response.status_code == 201

    # Check if job is finished
    job = await check_job_status(client, response.json()["job_id"])
    assert job["status_simple"] == "finished"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "routing_type,opportunities",
    [
        (ActiveRoutingHeatmapType.walking, [{"max_traveltime": 15, "number_of_destinations": 3}]),
        (ActiveRoutingHeatmapType.walking, [{"max_traveltime": 20, "number_of_destinations": 5}, {"max_traveltime": 30, "number_of_destinations": 10}]),
        (ActiveRoutingHeatmapType.bicycle, [{"max_traveltime": 15, "number_of_destinations": 3}]),
        (ActiveRoutingHeatmapType.bicycle, [{"max_traveltime": 20, "number_of_destinations": 5}, {"max_traveltime": 30, "number_of_destinations": 10}]),
        (ActiveRoutingHeatmapType.pedelec, [{"max_traveltime": 15, "number_of_destinations": 3}]),
        (ActiveRoutingHeatmapType.pedelec, [{"max_traveltime": 20, "number_of_destinations": 5}, {"max_traveltime": 30, "number_of_destinations": 10}]),
        (MotorizedRoutingHeatmapType.car, [{"max_traveltime": 15, "number_of_destinations": 3}]),
        (MotorizedRoutingHeatmapType.car, [{"max_traveltime": 30, "number_of_destinations": 5}, {"max_traveltime": 60, "number_of_destinations": 10}]),
    ]
)
async def test_heatmap_closest_average(
    client: AsyncClient,
    fixture_add_aggregate_point_layer_to_project,
    routing_type: ActiveRoutingHeatmapType | MotorizedRoutingHeatmapType,
    opportunities: List[dict],
):
    # Generate sample layers for conducting the test
    project_id = fixture_add_aggregate_point_layer_to_project["project_id"]
    layer_project_id = fixture_add_aggregate_point_layer_to_project["source_layer_project_id"]

    # Produce heatmap request payload
    params = {
        "routing_type": routing_type.value,
        "opportunities": [{
            "opportunity_layer_project_id": layer_project_id,
            "max_traveltime": item["max_traveltime"],
            "number_of_destinations": item["number_of_destinations"],
        } for item in opportunities]
    }

    # Call endpoint
    endpoint_type = "active-mobility" if type(routing_type) == ActiveRoutingHeatmapType else "motorized-mobility"
    response = await client.post(
        f"{settings.API_V2_STR}/{endpoint_type}/heatmap-closest-average?project_id={project_id}",
        json=params,
    )
    assert response.status_code == 201

    # Check if job is finished
    job = await check_job_status(client, response.json()["job_id"])
    assert job["status_simple"] == "finished"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "routing_type,max_traveltime",
    [
        (ActiveRoutingHeatmapType.walking, 30),
        (ActiveRoutingHeatmapType.bicycle, 30),
        (ActiveRoutingHeatmapType.pedelec, 30),
        (MotorizedRoutingHeatmapType.car, 60),
    ]
)
async def test_heatmap_connectivity(
    client: AsyncClient,
    fixture_add_aggregate_polygon_layer_to_project,
    routing_type: ActiveRoutingHeatmapType | MotorizedRoutingHeatmapType,
    max_traveltime: int,
):
    # Generate sample layers for conducting the test
    project_id = fixture_add_aggregate_polygon_layer_to_project["project_id"]
    layer_project_id = fixture_add_aggregate_polygon_layer_to_project["source_layer_project_id"]

    # Produce heatmap request payload
    params = {
        "routing_type": routing_type.value,
        "reference_area_layer_project_id": layer_project_id,
        "max_traveltime": max_traveltime,
    }

    # Call endpoint
    endpoint_type = "active-mobility" if type(routing_type) == ActiveRoutingHeatmapType else "motorized-mobility"
    response = await client.post(
        f"{settings.API_V2_STR}/{endpoint_type}/heatmap-connectivity?project_id={project_id}",
        json=params,
    )
    assert response.status_code == 201

    # Check if job is finished
    job = await check_job_status(client, response.json()["job_id"])
    assert job["status_simple"] == "finished"
