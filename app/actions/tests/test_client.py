import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, timedelta

from app.actions.client import (
    get_observations,
    GalooliInvalidUserCredentialsException,
    GalooliGeneralErrorException,
    GalooliTooManyRequestsException
)


class TestGetObservations:
    """Test cases for get_observations function"""

    @pytest.fixture
    def mock_request_params(self):
        """Mock request parameters for get_observations"""
        return {
            'url': "https://test.galooli.com/api",
            'username': "test_user",
            'password': "test_password",
            'start': datetime(2024, 6, 27, 12, 0, 0, tzinfo=timezone.utc)
        }

    @pytest.fixture
    def mock_response_success(self):
        """Mock successful API response"""
        response = MagicMock()
        response.is_error = False
        response.json.return_value = {
            'MaxGmtUpdateTime': '2025-06-27 01:56:02',
            'CommonResult': {
                'ResultCode': 0,
                'DataSet': [
                    ['sensor1', 'Vehicle1', 'Model1', 'Org1', 'extra', '2023-01-01 10:00:00', 'Moving', 40.7128, -74.0060, 100, 50, 1.2, 100, 90, 'Test vehicle'],
                    ['sensor2', 'Vehicle2', 'Model2', 'Org2', 'extra', '2023-01-01 11:00:00', 'Moving', 40.7589, -73.9851, 200, 60, 1.5, 150, 180, 'Test vehicle 2']
                ]
            }
        }
        return response

    @pytest.fixture
    def mock_response_error(self):
        """Mock error API response"""
        response = MagicMock()
        response.is_error = True
        response.text = "Error response"
        return response

    @pytest.mark.asyncio
    async def test_get_observations_success(self, mock_request_params, mock_response_success):
        """Test successful observation retrieval"""
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response_success

            response = await get_observations(**mock_request_params)

            result = response['CommonResult']['DataSet']
            
            assert result == [
                ['sensor1', 'Vehicle1', 'Model1', 'Org1', 'extra', '2023-01-01 10:00:00', 'Moving', 40.7128, -74.0060, 100, 50, 1.2, 100, 90, 'Test vehicle'],
                ['sensor2', 'Vehicle2', 'Model2', 'Org2', 'extra', '2023-01-01 11:00:00', 'Moving', 40.7589, -73.9851, 200, 60, 1.5, 150, 180, 'Test vehicle 2']
            ]
            
            # Verify the request parameters
            mock_client.get.assert_called_once()
            call_args = mock_client.get.call_args
            assert call_args[0][0] == "https://test.galooli.com/api"
            assert call_args[1]['params']['userName'] == "test_user"
            assert call_args[1]['params']['password'] == "test_password"

    @pytest.mark.asyncio
    async def test_get_observations_invalid_credentials(self, mock_request_params):
        """Test observation retrieval with invalid credentials"""
        mock_response = MagicMock()
        mock_response.is_error = False
        mock_response.json.return_value = {
            'CommonResult': {
                'ResultCode': 1000,
                'ResultDescription': 'Invalid credentials'
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response
            
            with pytest.raises(GalooliInvalidUserCredentialsException) as exc_info:
                await get_observations(**mock_request_params)
            
            assert exc_info.value.code == 1000
            assert "Invalid credentials" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_observations_too_many_requests(self, mock_request_params):
        """Test observation retrieval with too many requests error"""
        mock_response = MagicMock()
        mock_response.is_error = False
        mock_response.json.return_value = {
            'CommonResult': {
                'ResultCode': 1101,
                'ResultDescription': 'Too many requests'
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response
            
            with pytest.raises(GalooliTooManyRequestsException) as exc_info:
                await get_observations(**mock_request_params)
            
            assert exc_info.value.code == 1101
            assert "Too many requests" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_observations_general_error(self, mock_request_params):
        """Test observation retrieval with general error"""
        mock_response = MagicMock()
        mock_response.is_error = False
        mock_response.json.return_value = {
            'CommonResult': {
                'ResultCode': 999,
                'ResultDescription': 'General error'
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response
            
            with pytest.raises(GalooliGeneralErrorException) as exc_info:
                await get_observations(**mock_request_params)
            
            assert exc_info.value.code == -1
            assert "General error occurred" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_observations_http_error_403(self, mock_request_params):
        """Test observation retrieval with HTTP 403 error"""
        mock_response = MagicMock()
        mock_response.status_code = 403
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.side_effect = httpx.HTTPStatusError(
                "Forbidden", request=MagicMock(), response=mock_response
            )
            
            with pytest.raises(GalooliInvalidUserCredentialsException) as exc_info:
                await get_observations(**mock_request_params)
            
            assert exc_info.value.code == 403

    @pytest.mark.asyncio
    async def test_get_observations_http_error_404(self, mock_request_params):
        """Test observation retrieval with HTTP 404 error"""
        mock_response = MagicMock()
        mock_response.status_code = 404
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.side_effect = httpx.HTTPStatusError(
                "Not Found", request=MagicMock(), response=mock_response
            )
            
            with pytest.raises(GalooliGeneralErrorException) as exc_info:
                await get_observations(**mock_request_params)
            
            assert exc_info.value.code == 404

    @pytest.mark.asyncio
    async def test_get_observations_http_error_other(self, mock_request_params):
        """Test observation retrieval with other HTTP error"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.side_effect = httpx.HTTPStatusError(
                "Internal Server Error", request=MagicMock(), response=mock_response
            )
            
            with pytest.raises(httpx.HTTPStatusError):
                await get_observations(**mock_request_params)

    @pytest.mark.asyncio
    async def test_get_observations_empty_response(self, mock_request_params, mock_response_success):
        """Test observation retrieval with empty response"""
        mock_response_success.json.return_value = None
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response_success
            
            result = await get_observations(**mock_request_params)
            
            assert result is None

    @pytest.mark.asyncio
    async def test_get_observations_request_parameters(self, mock_request_params, mock_response_success):
        """Test that request parameters are correctly set"""
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response_success
            
            await get_observations(**mock_request_params)
            
            # Verify the request was made with correct parameters
            mock_client.get.assert_called_once()
            call_args = mock_client.get.call_args
            
            # Check URL
            assert call_args[0][0] == "https://test.galooli.com/api"
            
            # Check parameters
            params = call_args[1]['params']
            assert params['userName'] == "test_user"
            assert params['password'] == "test_password"
            assert 'requestedPropertiesStr' in params
            assert 'lastGMTUpdateTime' in params
            
            # Check that follow_redirects is True
            assert call_args[1]['follow_redirects'] is True

    @pytest.mark.skip("Needs refactor")
    @pytest.mark.asyncio
    async def test_get_observations_time_window_calculation(self, mock_request_params, mock_response_success):
        """Test that the time window is calculated correctly"""
        with patch('httpx.AsyncClient') as mock_client_class, \
             patch('app.actions.client.datetime') as mock_datetime:
            
            # Mock current time
            mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = mock_now
            mock_datetime.strftime = datetime.strftime
            
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            mock_client.get.return_value = mock_response_success
            
            await get_observations(**mock_request_params)
            
            # Verify the time window calculation
            expected_start_time = mock_now - timedelta(hours=3)
            expected_time_str = expected_start_time.strftime('%Y-%m-%d %H:%M:%S')
            
            call_args = mock_client.get.call_args
            params = call_args[1]['params']
            assert params['lastGMTUpdateTime'] == expected_time_str
