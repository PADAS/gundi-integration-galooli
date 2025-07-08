import logging
import dateparser

from app.services.state import IntegrationStateManager

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


async def convert_to_er_observation(integration_id, galooli_record, reports_timezone):
    async def save_device_state_and_build_observation():
        # Save current device status
        state = {"status": status}

        await state_manager.set_state(
            integration_id=integration_id,
            action_id="pull_observations",
            source_id=sensor_id,
            state=state
        )

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
    try:
        # Unpack the galooli record into its components
        (sensor_id, subject_name, org_name, time, status, latitude, longitude,
         distance, speed) = galooli_record
    except ValueError as e:
        logger.exception(f"Failed to unpack Galooli record: {galooli_record}")
        raise e

    if latitude and longitude and time and sensor_id:
        device_state = await state_manager.get_state(
            integration_id=str(integration_id),
            action_id="pull_observations",
            source_id=sensor_id
        )

        if device_state:
            # If the device state exists, we check if the status has changed
            if device_state.get('status') == status:
                logger.info(f'Ignoring observation for {sensor_id} {subject_name} recorded at {time} status: {status} (status has not changed)')
                return None
            else:
                # Status has changed, so we save the new state and build the observation
                return await save_device_state_and_build_observation()
        else:
            # No device_state (new sensor), so we create a new observation
            return await save_device_state_and_build_observation()
    else:
        logger.error(f'Got bad data from Galooli: mfg_id {sensor_id}, time {time}, lat {latitude} long {longitude}')
        logger.error(f'{galooli_record}')
        return None 