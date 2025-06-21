import logging
import dateparser

logger = logging.getLogger(__name__)


def convert_to_er_observation(galooli_record, reports_timezone):

    try:
        # Unpack the galooli record into its components
        (sensor_id, subject_name, org_name, time, status, latitude, longitude,
         distance, speed) = galooli_record
    except ValueError as e:
        logger.exception(f"Failed to unpack Galooli record: {galooli_record}")
        raise e

    if latitude and longitude and time and sensor_id:
        # TODO: Add support for other statuses
        if status == 'Moving':
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
            logger.debug(f'Ignoring observation for {sensor_id} {subject_name} recorded at {time} status: {status}')
            return None
    else:
        logger.error(f'Got bad data from Galooli: mfg_id {sensor_id}, time {time}, lat {latitude} long {longitude}')
        logger.error(f'{galooli_record}')
        return None 