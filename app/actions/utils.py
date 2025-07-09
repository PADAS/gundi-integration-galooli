import logging
import dateparser
import pytz

from app.services.state import IntegrationStateManager

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


def convert_to_gundi_observation(galooli_record, *, reports_timezone:pytz.FixedOffset, subject_type:str="vehicle"):
    try:
        # Unpack the galooli record into its components
        (sensor_id, subject_name, org_name, fixtime, status, latitude, longitude,
         distance, speed) = galooli_record
    except ValueError as e:
        logger.exception(f"Failed to unpack Galooli record: {galooli_record}")
        raise e

    if latitude and longitude and fixtime and sensor_id:
        localized_gps_time = reports_timezone.localize(dateparser.parse(fixtime))
        obs = {
            'source': sensor_id,
            'source_name': subject_name,
            'subject_type': subject_type,
            'type': 'tracking-device',
            'recorded_at': localized_gps_time.isoformat(),
            'location': {
                'lat': latitude,
                'lon': longitude
            },
            'additional': {
                'sensor_id': sensor_id,
                'org_name': org_name,
                'status': status,
                'distance': distance,
                'speed': speed
            }
        }
        return obs
    else:
        logger.error(f'Got bad data from Galooli: mfg_id {sensor_id}, time {fixtime}, lat {latitude} long {longitude}, record: ({galooli_record})')


async def filter_observations_by_device_status(integration_id:str, observations:list[dict]):
    """
    Filters observations based on the device status.
    """
    if not observations:
        return []

    filtered_observations = []

    for obs in observations:

        # If status is "Off", we check whether we've seen the record within the last 10 minutes.
        status = obs['additional'].get('status', '').lower()

        if status == "off":

            recorded_at = obs['recorded_at']
            cache_key = f"quiet_period:{status}"

            sensor_id = obs['source']
            if device_state := await state_manager.get_state(
                integration_id=integration_id,
                action_id=cache_key,
                source_id=sensor_id
            ):
                
                # If the recorded_at has changed, then we want to send this observation.
                if device_state.get('recorded_at') != recorded_at:
                    filtered_observations.append(obs)
            else:
                filtered_observations.append(obs) # first time for this device+off status

            # Set TTL on "Off" status 
            await state_manager.set_state(integration_id=integration_id, action_id=cache_key, 
                                        source_id=sensor_id, state={"recorded_at": recorded_at}, ex=600)

        else:
            filtered_observations.append(obs)

    return filtered_observations
