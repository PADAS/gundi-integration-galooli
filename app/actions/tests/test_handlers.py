import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch
from io import StringIO
import pytz
import pydantic

from app.actions.handlers import action_auth, action_pull_observations
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig
from app.actions.client import (
    GalooliInvalidUserCredentialsException,
    GalooliGeneralErrorException,
    GalooliTooManyRequestsException
)


class TestActionAuth:
    """Test cases for action_auth function"""

    @pytest.fixture
    def mock_integration(self):
        """Mock integration object"""
        integration = MagicMock()
        integration.id = "test-integration-id"
        integration.base_url = None
        return integration

    @pytest.fixture
    def mock_action_config(self):
        """Mock AuthenticateConfig"""
        config = MagicMock(spec=AuthenticateConfig)
        config.username = "test_user"
        config.password = pydantic.SecretStr("test_password")
        return config


    @pytest.mark.asyncio
    async def test_action_auth_success(self, mock_integration, mock_action_config):
        """Test successful authentication"""
        with patch('app.actions.handlers.client.get_observations') as mock_get_obs:
            mock_get_obs.return_value = [["data1"], ["data2"]]
            
            result = await action_auth(mock_integration, mock_action_config)
            
            assert result == {"valid_credentials": True}
            mock_get_obs.assert_called_once_with(
                "https://sdk.galooli-systems.com/galooliSDKService.svc/json/Assets_Report",
                username="test_user",
                password="test_password",
                look_back_window_hours=1
            )

    @pytest.mark.asyncio
    async def test_action_auth_with_custom_base_url(self, mock_integration, mock_action_config):
        """Test authentication with custom base URL"""
        a_custom_url = "https://something-special.com/api"
        mock_integration.base_url = a_custom_url
        
        with patch('app.actions.handlers.client.get_observations') as mock_get_obs:
            mock_get_obs.return_value = [["data1"]]
            
            result = await action_auth(mock_integration, mock_action_config)
            
            assert result == {"valid_credentials": True}
            mock_get_obs.assert_called_once_with(
                a_custom_url,
                username="test_user",
                password="test_password",
                look_back_window_hours=1
            )

    @pytest.mark.asyncio
    async def test_action_auth_invalid_credentials(self, mock_integration, mock_action_config):
        """Test authentication with invalid credentials"""
        with patch('app.actions.handlers.client.get_observations') as mock_get_obs:
            mock_get_obs.side_effect = GalooliInvalidUserCredentialsException(
                Exception(), "Invalid credentials", 1000
            )
            
            result = await action_auth(mock_integration, mock_action_config)
            
            assert result == {
                "valid_credentials": False,
                "message": "Invalid credentials",
                "code": 1000
            }

    @pytest.mark.asyncio
    async def test_action_auth_general_error(self, mock_integration, mock_action_config):
        """Test authentication with general error"""
        with patch('app.actions.handlers.client.get_observations') as mock_get_obs:
            mock_get_obs.side_effect = GalooliGeneralErrorException(
                Exception(), "General error", -1
            )
            
            result = await action_auth(mock_integration, mock_action_config)
            
            assert result == {
                "valid_credentials": False,
                "message": "General error",
                "code": -1
            }

    @pytest.mark.asyncio
    async def test_action_auth_too_many_requests(self, mock_integration, mock_action_config):
        """Test authentication with too many requests error"""
        with patch('app.actions.handlers.client.get_observations') as mock_get_obs:
            mock_get_obs.side_effect = GalooliTooManyRequestsException(
                Exception(), "Too many requests", 1101
            )
            
            result = await action_auth(mock_integration, mock_action_config)
            
            assert result == {
                "valid_credentials": False,
                "message": "Too many requests",
                "code": 1101
            }

    @pytest.mark.asyncio
    async def test_action_auth_http_error(self, mock_integration, mock_action_config):
        """Test authentication with HTTP error"""
        with patch('app.actions.handlers.client.get_observations') as mock_get_obs:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_get_obs.side_effect = httpx.HTTPStatusError(
                "Server error", request=MagicMock(), response=mock_response
            )
            
            result = await action_auth(mock_integration, mock_action_config)
            
            assert result == {"error": True, "status_code": 500}


@patch('app.services.activity_logger.publish_event', AsyncMock())
class TestActionPullObservations:
    """Test cases for action_pull_observations function"""

    @pytest.fixture
    def mock_integration(self):
        """Mock integration object"""
        integration = MagicMock()
        integration.id = "test-integration-id"
        integration.base_url = None
        return integration

    @pytest.fixture
    def mock_action_config(self):
        """Mock PullObservationsConfig"""
        config = MagicMock(spec=PullObservationsConfig)
        config.look_back_window_hours = 4
        config.gmt_offset = -5
        return config

    @pytest.fixture
    def mock_auth_config(self):
        """Mock auth config"""
        auth_config = MagicMock()
        auth_config.username = "test_user"
        auth_config.password.get_secret_value.return_value = "test_password"
        return auth_config
    
    @pytest.mark.asyncio
    async def test_action_pull_observations_success(self, mock_integration, mock_action_config, mock_auth_config):
        """Test successful pull observations"""
        # Mock dataset with valid observation data
        mock_dataset = [
            ["sensor1", "Vehicle1", "Model1", "Org1", "extra", "2023-01-01 10:00:00", "Moving", 40.7128, -74.0060, 100, 50, 1.2, 100, 90, "Test vehicle"],
            ["sensor2", "Vehicle2", "Model2", "Org2", "extra", "2023-01-01 11:00:00", "Moving", 40.7589, -73.9851, 200, 60, 1.5, 150, 180, "Test vehicle 2"]
        ]
        
        with patch('app.actions.handlers.get_auth_config', return_value=mock_auth_config), \
             patch('app.actions.handlers.client.get_observations', return_value=mock_dataset), \
             patch('app.actions.handlers.send_observations_to_gundi', return_value=["obs1", "obs2"]) as mock_send:
            
            result = await action_pull_observations(mock_integration, mock_action_config)
            
            assert result == {"observations_extracted": 2}
            mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_action_pull_observations_no_dataset(self, mock_integration, mock_action_config, mock_auth_config):
        """Test pull observations with no dataset returned"""
        with patch('app.actions.handlers.get_auth_config', return_value=mock_auth_config), \
             patch('app.actions.handlers.client.get_observations', return_value=None):
            
            result = await action_pull_observations(mock_integration, mock_action_config)
            
            assert result == {"observations_extracted": 0}

    @pytest.mark.asyncio
    async def test_action_pull_observations_no_valid_observations(self, mock_integration, mock_action_config, mock_auth_config):
        """Test pull observations with dataset but no valid observations after processing"""
        # Mock dataset with invalid observation data (non-moving status)
        mock_dataset = [
            ["sensor1", "Vehicle1", "Model1", "Org1", "extra", "2023-01-01 10:00:00", "Stopped", 40.7128, -74.0060, 100, 50, 1.2, 100, 90, "Test vehicle"]
        ]
        
        with patch('app.actions.handlers.get_auth_config', return_value=mock_auth_config), \
             patch('app.actions.handlers.client.get_observations', return_value=mock_dataset):
            
            result = await action_pull_observations(mock_integration, mock_action_config)
            
            assert result == {"observations_extracted": 0}

    @pytest.mark.asyncio
    async def test_action_pull_observations_with_custom_base_url(self, mock_integration, mock_action_config, mock_auth_config):
        """Test pull observations with custom base URL"""
        mock_integration.base_url = "https://custom.galooli.com/api"
        mock_dataset = [["sensor1", "Vehicle1", "Model1", "Org1", "extra", "2023-01-01 10:00:00", "Moving", 40.7128, -74.0060, 100, 50, 1.2, 100, 90, "Test vehicle"]]
        
        with patch('app.actions.handlers.get_auth_config', return_value=mock_auth_config), \
             patch('app.actions.handlers.client.get_observations', return_value=mock_dataset) as mock_get_obs, \
             patch('app.actions.handlers.send_observations_to_gundi', return_value=["obs1"]):
            
            result = await action_pull_observations(mock_integration, mock_action_config)
            
            assert result == {"observations_extracted": 1}
            mock_get_obs.assert_called_once_with(
                "https://custom.galooli.com/api",
                username="test_user",
                password="test_password",
                look_back_window_hours=4
            )

    @pytest.mark.asyncio
    async def test_action_pull_observations_client_exception(self, mock_integration, mock_action_config, mock_auth_config):
        """Test pull observations with client exception"""
        with patch('app.actions.handlers.get_auth_config', return_value=mock_auth_config), \
             patch('app.actions.handlers.client.get_observations') as mock_get_obs:
            mock_get_obs.side_effect = GalooliInvalidUserCredentialsException(
                Exception(), "Invalid credentials", 1000
            )
            
            with pytest.raises(GalooliInvalidUserCredentialsException):
                await action_pull_observations(mock_integration, mock_action_config)

    @pytest.mark.asyncio
    async def test_action_pull_observations_http_error(self, mock_integration, mock_action_config, mock_auth_config):
        """Test pull observations with HTTP error"""
        with patch('app.actions.handlers.get_auth_config', return_value=mock_auth_config), \
             patch('app.actions.handlers.client.get_observations') as mock_get_obs:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_get_obs.side_effect = httpx.HTTPStatusError(
                "Server error", request=MagicMock(), response=mock_response
            )
            
            with pytest.raises(httpx.HTTPStatusError):
                await action_pull_observations(mock_integration, mock_action_config)

    @pytest.mark.asyncio
    async def test_action_pull_observations_batch_processing(self, mock_integration, mock_action_config, mock_auth_config):
        """Test pull observations with batch processing"""
        # Create a large dataset to test batching
        mock_dataset = [
            ["sensor1", "Vehicle1", "Model1", "Org1", "extra", "2023-01-01 10:00:00", "Moving", 40.7128, -74.0060, 100, 50, 1.2, 100, 90, "Test vehicle"]
        ] * 250  # 250 observations to test batching
        
        with patch('app.actions.handlers.get_auth_config', return_value=mock_auth_config), \
             patch('app.actions.handlers.client.get_observations', return_value=mock_dataset), \
             patch('app.actions.handlers.send_observations_to_gundi', return_value=["obs"] * 200) as mock_send:
            
            result = await action_pull_observations(mock_integration, mock_action_config)
            
            # Should have 2 batches: 200 + 50 observations
            assert mock_send.call_count == 2
            assert result == {"observations_extracted": 400}  # 200 * 2 batches

@patch('app.services.activity_logger.publish_event', AsyncMock())
class TestHandlersIntegration:
    """Integration tests for handlers"""

    @pytest.mark.asyncio
    async def test_action_auth_logging(self, caplog):
        """Test that action_auth logs appropriately"""
        mock_integration = MagicMock()
        mock_integration.id = "test-id"
        mock_integration.base_url = None
        
        mock_config = MagicMock(spec=AuthenticateConfig)
        mock_config.username = "test_user"
        mock_config.password = pydantic.SecretStr("test_password")
        
        with patch('app.actions.handlers.client.get_observations', return_value=[["data"]]):
            await action_auth(mock_integration, mock_config)
            
            assert "Executing 'auth' action with integration ID test-id" in caplog.text

    @pytest.mark.asyncio
    async def test_action_pull_observations_logging(self, caplog):
        """Test that action_pull_observations logs appropriately"""
        mock_integration = MagicMock()
        mock_integration.id = "test-id"
        mock_integration.base_url = None
        
        mock_action_config = MagicMock(spec=PullObservationsConfig)
        mock_action_config.look_back_window_hours = 4
        mock_action_config.gmt_offset = -5
        
        mock_auth_config = MagicMock()
        mock_auth_config.username = "test_user"
        mock_auth_config.password.get_secret_value.return_value = "test_password"
        
        mock_dataset = [["sensor1", "Vehicle1", "Model1", "Org1", "extra", "2023-01-01 10:00:00", "Moving", 40.7128, -74.0060, 100, 50, 1.2, 100, 90, "Test vehicle"]]
        
        with patch('app.actions.handlers.get_auth_config', return_value=mock_auth_config), \
             patch('app.actions.handlers.client.get_observations', return_value=mock_dataset), \
             patch('app.actions.handlers.send_observations_to_gundi', return_value=["obs1"]):
            
            await action_pull_observations(mock_integration, mock_action_config)
            
            assert "Executing 'pull_observations' action with integration ID test-id" in caplog.text
            assert "Getting observations for Username: test_user" in caplog.text 