import logging
import dateparser

from app.services.state import IntegrationStateManager

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


def convert_to_er_observation(galooli_record, reports_timezone):
    try:
        # Unpack the galooli record into its components
        (sensor_id, subject_name, org_name, time, status, latitude, longitude,
         distance, speed) = galooli_record
    except ValueError as e:
        logger.exception(f"Failed to unpack Galooli record: {galooli_record}")
        raise e

    if latitude and longitude and time and sensor_id:
        localized_gps_time = reports_timezone.localize(dateparser.parse(time))
        obs = {
            'source': sensor_id,
            'source_name': subject_name,
            'subject_type': "security_vehicle",
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
                'speed': speed,
                'subject_groups': ['Vehicles', ]
            }
        }
        return obs
    else:
        logger.error(f'Got bad data from Galooli: mfg_id {sensor_id}, time {time}, lat {latitude} long {longitude}')
        logger.error(f'{galooli_record}')
        return None

async def filter_observations_by_device_status(integration_id, observations):
    """
    Filters observations based on the device status.
    """
    async def save_state_and_append_observation():
        state = {"status": status}
        await state_manager.set_state(
            integration_id=integration_id,
            action_id="pull_observations",
            source_id=sensor_id,
            state=state
        )
        filtered_observations.append(obs)

    if not observations:
        return []

    filtered_observations = []

    for obs in observations:
        sensor_id = obs['source']
        subject_name = obs['source_name']
        time = obs['recorded_at']
        status = obs['additional'].get('status')

        device_state = await state_manager.get_state(
            integration_id=str(integration_id),
            action_id="pull_observations",
            source_id=sensor_id
        )

        if device_state:
            # If the device state exists, we check if the status has changed
            if device_state.get('status') == status:
                logger.info(f'Ignoring observation for {sensor_id} {subject_name} recorded at {time} status: {status} (status has not changed)')
            else:
                await save_state_and_append_observation()
        else:
            # No device_state (new sensor), so we create a new observation
            await save_state_and_append_observation()

    return filtered_observations
