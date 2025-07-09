import pytest
from unittest.mock import MagicMock
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig
import pydantic

@pytest.fixture
def mock_integration():
    """Shared mock integration object"""
    integration = MagicMock()
    integration.id = "test-integration-id"
    integration.base_url = None
    integration.configurations = []
    return integration


@pytest.fixture
def mock_auth_config():
    """Shared mock auth config"""
    auth_config = MagicMock(spec=AuthenticateConfig)
    auth_config.username = "test_user"
    auth_config.password = pydantic.SecretStr("test_password")
    return auth_config


@pytest.fixture
def mock_pull_config():
    """Shared mock pull config"""
    pull_config = MagicMock(spec=PullObservationsConfig)
    pull_config.look_back_window_hours = 4
    pull_config.gmt_offset = -5
    return pull_config


@pytest.fixture
def sample_galooli_dataset():
    """Sample Galooli dataset for testing"""
    return [
        ["sensor1", "Vehicle1", "Model1", "Org1", "extra", "2023-01-01 10:00:00", "Moving", 40.7128, -74.0060, 100, 50, 1.2, 100, 90, "Test vehicle"],
        ["sensor2", "Vehicle2", "Model2", "Org2", "extra", "2023-01-01 11:00:00", "Moving", 40.7589, -73.9851, 200, 60, 1.5, 150, 180, "Test vehicle 2"],
        ["sensor3", "Vehicle3", "Model3", "Org3", "extra", "2023-01-01 12:00:00", "Stopped", 40.7505, -73.9934, 300, 0, 2.0, 200, 270, "Test vehicle 3"]
    ]


@pytest.fixture
def sample_er_observation():
    """Sample ER observation for testing"""
    return {
        'manufacturer_id': 'sensor1',
        'subject_name': 'Vehicle1',
        'subject_subtype': "security_vehicle",
        'recorded_at': '2023-01-01T10:00:00-05:00',
        'location': {
            'lat': 40.7128,
            'lon': -74.0060
        },
        'additional': {
            'sensor_id': 'sensor1',
            'asset_model': 'Model1',
            'org_name': 'Org1',
            'status': 'Moving',
            'distance': 100,
            'speed': 50,
            'hdop': 1.2,
            'altitude': 100,
            'heading': 90,
            'description': 'Test vehicle',
        }
    } 