import csv
import logging
import httpx
import stamina
import dateparser
import pytz

from io import StringIO
from functional import seq
from datetime import datetime, timezone, timedelta
from app.services.state import IntegrationStateManager


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

REQUESTED_PROPERTIES = 'u.unit_id,u.unit_name,a.model,o.name,ac.gps_time,ac.status,ac.latitude,ac.longitude,ac.current_odometer_reading,' \
                           'ac.speed,ac.hdop,ac.altitude,ac.heading,u.description'


class GalooliGeneralErrorException(Exception):
    def __init__(self, error: Exception, message: str, code=-1):
        self.code = code
        self.message = message
        self.error = error
        super().__init__(f"'{self.code}: {self.message}, Error: {self.error}'")


class GalooliInvalidUserCredentialsException(Exception):
    def __init__(self, error: Exception, message: str, code=1000):
        self.code = code
        self.message = message
        self.error = error
        super().__init__(f"'{self.code}: {self.message}, Error: {self.error}'")


class GalooliTooManyRequestsException(Exception):
    def __init__(self, error: Exception, message: str, code=1101):
        self.code = code
        self.message = message
        self.error = error
        super().__init__(f"'{self.code}: {self.message}, Error: {self.error}'")


def convert_to_er_observation(r, reports_timezone):
    sensor_id = r[0]
    subject_name = r[1]
    asset_model = r[2]
    org_name = r[3]
    time = r[5]
    status = r[6]
    latitude = r[7]
    longitude = r[8]
    distance = r[9]
    speed = r[10]
    hdop = r[11]
    altitude = r[12]
    heading = r[13]
    description = r[14]

    if latitude and longitude and time and sensor_id:
        if status == 'Moving':
            localized_gps_time = reports_timezone.localize(dateparser.parse(time))
            obs = {
                'manufacturer_id': sensor_id,
                'subject_name': subject_name,
                'subject_groups': ['Vehicles', ],
                'subject_subtype': "security_vehicle",
                'recorded_at': localized_gps_time.isoformat(),
                'location': {
                    'lat': latitude,
                    'lon': longitude
                },
                'additional': {
                    'sensor_id': sensor_id,
                    'asset_model': asset_model,
                    'org_name': org_name,
                    'status': status,
                    'distance': distance,
                    'speed': speed,
                    'hdop': hdop,
                    'altitude': altitude,
                    'heading': heading,
                    'description': description,
                }
            }
            return obs
        else:
            logger.debug(f'Ignoring observation for {sensor_id} {subject_name} recorded at {time} status: {status}')
            return None
    else:
        logger.error(f'Got bad data from Galooli: mfg_id {sensor_id}, time {time}, lat {latitude} long {longitude}')
        logger.error(f'{r}')
        return None


@stamina.retry(on=GalooliTooManyRequestsException, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_observations(integration, url, request):
    async with httpx.AsyncClient(timeout=120) as session:
        logger.info(f"-- Getting observations for integration ID: {integration.id} Username: {request['username']} --")

        current_time = datetime.now(timezone.utc)
        window_start_time = current_time - timedelta(hours=request["look_back_window_hours"])

        params = {
            'requestedPropertiesStr': REQUESTED_PROPERTIES,
            'lastGMTUpdateTime': datetime.strftime(window_start_time, '%Y-%m-%d %H:%M:%S'),
            'userName': request['username'],
            'password': request['password']
        }

        try:
            response = await session.get(url, params=params, follow_redirects=True)
            if response.is_error:
                logger.error(f"Error 'get_observations'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                result_code = parsed_response['CommonResult']['ResultCode']
                if result_code != 0:
                    result_description = parsed_response['CommonResult'].get('ResultDescription', parsed_response['CommonResult'].get('RejectReason'))
                    if result_code == 1000:
                        raise GalooliInvalidUserCredentialsException(
                            Exception(),
                            result_description
                        )
                    if result_code == 1101:
                        raise GalooliTooManyRequestsException(
                            Exception(),
                            result_description
                        )
                    raise GalooliGeneralErrorException(
                        Exception(),
                        f"General error occurred. Result code: {result_code}"
                    )

                offset_minutes = request["gmt_offset"] * 60
                reports_timezone = pytz.FixedOffset(offset_minutes)
                dataset = parsed_response['CommonResult']['DataSet']
                logger.info('%s records received from Galooli', len(dataset))

                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerows(dataset)
                csv_buffer.seek(0)

                observations = seq.csv(csv_buffer) \
                    .map(lambda r: convert_to_er_observation(r, reports_timezone)) \
                    .filter(lambda x: x is not None) \
                    .to_list()

                logger.info(f"Galooli observations: {observations}")
                return observations
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise GalooliInvalidUserCredentialsException(e, "Unauthorized access", code=403)
            if e.response.status_code == 404:
                raise GalooliGeneralErrorException(e, "Not found", code=404)
            raise e
