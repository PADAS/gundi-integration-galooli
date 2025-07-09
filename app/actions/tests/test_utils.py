import pytest
import pytz
from app.actions.utils import convert_to_gundi_observation


class TestConvertToGundiObservation:
    """Test cases for convert_to_gundi_observation function"""

    @pytest.fixture
    def reports_timezone(self):
        """Mock timezone for testing"""
        return pytz.FixedOffset(-300)  # EST timezone

    def test_convert_to_gundi_observation_success(self, reports_timezone):
        """Test successful conversion of Galooli record to Gundi observation"""
        galooli_record = [
            "sensor1", "Vehicle1", "Org1",  
            "2023-01-01 10:00:00", "Moving", 40.7128, -74.0060, 
            100, 50
        ]

        result = convert_to_gundi_observation(galooli_record, reports_timezone=reports_timezone, subject_type="vehicle")

        assert result is not None
        assert result['source'] == "sensor1"
        assert result['source_name'] == "Vehicle1"
        assert result['subject_type'] == 'vehicle'
        assert result['type'] == "tracking-device"
        assert result['location']['lat'] == 40.7128
        assert result['location']['lon'] == -74.0060
        assert result['additional']['sensor_id'] == "sensor1"
        assert result['additional']['org_name'] == "Org1"
        assert result['additional']['status'] == "Moving"
        assert result['additional']['distance'] == 100
        assert result['additional']['speed'] == 50

    def test_convert_to_er_observation_stopped_vehicle_returns_valid_observation(self, reports_timezone):
        """Test conversion of stopped vehicle (should return None)"""
        galooli_record = [
            "sensor1", "Vehicle1", "Org1", 
            "2023-01-01 10:00:00", "Stopped", 40.7128, -74.0060, 
            100, 0
        ]
        result = convert_to_gundi_observation(galooli_record, reports_timezone=reports_timezone)
        assert result is not None

    def test_convert_to_er_observation_missing_coordinates(self, reports_timezone):
        """Test conversion with missing coordinates (should return None)"""
        galooli_record = [
            "sensor1", "Vehicle1", "Org1", 
            "2023-01-01 10:00:00", "Moving", None, None, 
            100, 50
        ]
        result = convert_to_gundi_observation(galooli_record, reports_timezone=reports_timezone)
        assert result is None

    def test_convert_to_er_observation_missing_time(self, reports_timezone):
        """Test conversion with missing time (should return None)"""
        galooli_record = [
            "sensor1", "Vehicle1", "Org1", 
            None, "Moving", 40.7128, -74.0060, 
            100, 50
        ]
        result = convert_to_gundi_observation(galooli_record, reports_timezone=reports_timezone)
        assert result is None

    def test_convert_to_er_observation_missing_sensor_id(self, reports_timezone):
        """Test conversion with missing sensor ID (should return None)"""
        galooli_record = [
            None, "Vehicle1", "Org1", 
            "2023-01-01 10:00:00", "Moving", 40.7128, -74.0060, 
            100, 50
        ]
        result = convert_to_gundi_observation(galooli_record, reports_timezone=reports_timezone)
        assert result is None

    def test_convert_to_er_observation_invalid_record_length(self, reports_timezone):
        """Test conversion with invalid record length (should raise ValueError)"""
        galooli_record = ["sensor1", "Vehicle1", "Model1"]  # Too short
        
        with pytest.raises(ValueError):
            convert_to_gundi_observation(galooli_record, reports_timezone=reports_timezone)

    def test_convert_to_er_observation_zero_coordinates(self, reports_timezone):
        """Test conversion with zero coordinates (should return None)"""
        galooli_record = [
            "sensor1", "Vehicle1", "Org1",
            "2023-01-01 10:00:00", "Moving", 0, 0, 
            100, 50
        ]
        result = convert_to_gundi_observation(galooli_record, reports_timezone=reports_timezone)
        assert result is None

    def test_convert_to_er_observation_empty_string_coordinates(self, reports_timezone):
        """Test conversion with empty string coordinates (should return None)"""
        galooli_record = [
            "sensor1", "Vehicle1", "Org1", 
            "2023-01-01 10:00:00", "Moving", "", "", 
            100, 50
        ]
        result = convert_to_gundi_observation(galooli_record, reports_timezone=reports_timezone)
        assert result is None

    def test_convert_to_er_observation_different_status_values_returns_valid_observations(self, reports_timezone):
        """Test conversion with different status values"""
        # Test with "Idle" status
        galooli_record_idle = [
            "sensor1", "Vehicle1", "Org1",
            "2023-01-01 10:00:00", "Idle", 40.7128, -74.0060, 
            100, 0
        ]
        result_idle = convert_to_gundi_observation(galooli_record_idle, reports_timezone=reports_timezone)

        assert result_idle is not None
        
        # Test with "Parked" status
        galooli_record_parked = [
            "sensor1", "Vehicle1", "Org1",
            "2023-01-01 10:00:00", "Parked", 40.7128, -74.0060, 
            100, 0
        ]
        result_parked = convert_to_gundi_observation(galooli_record_parked, reports_timezone=reports_timezone)
        assert result_parked is not None

    def test_convert_to_er_observation_timezone_handling(self):
        """Test timezone handling in conversion"""
        # Test with UTC timezone
        utc_timezone = pytz.FixedOffset(0)
        galooli_record = [
            "sensor1", "Vehicle1", "Org1",
            "2023-01-01 10:00:00", "Moving", 40.7128, -74.0060, 
            100, 50
        ]
        result = convert_to_gundi_observation(galooli_record, reports_timezone=utc_timezone)

        assert result is not None
        assert "T10:00:00+00:00" in result['recorded_at']

    def test_convert_to_er_observation_numeric_values(self, reports_timezone):
        """Test conversion with various numeric values"""
        galooli_record = [
            "sensor1", "Vehicle1", "Org1",
            "2023-01-01 10:00:00", "Moving", 40.7128, -74.0060, 
            123.45, 67.89
        ]
        result = convert_to_gundi_observation(galooli_record, reports_timezone=reports_timezone)

        assert result is not None
        assert result['additional']['distance'] == 123.45
        assert result['additional']['speed'] == 67.89
