import logging
import httpx
import stamina

from datetime import datetime, timezone, timedelta


logger = logging.getLogger(__name__)

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


@stamina.retry(on=GalooliTooManyRequestsException, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_observations(url, *, username, password, look_back_window_hours):
    async with httpx.AsyncClient(timeout=120) as session:
        current_time = datetime.now(timezone.utc)
        window_start_time = current_time - timedelta(hours=look_back_window_hours)

        params = {
            'requestedPropertiesStr': REQUESTED_PROPERTIES,
            'lastGMTUpdateTime': datetime.strftime(window_start_time, '%Y-%m-%d %H:%M:%S'),
            'userName': username,
            'password': password
        }

        try:
            response = await session.get(url, params=params, follow_redirects=True)
            if response.is_error:
                logger.error(f"Error 'get_observations'. Response body: {response.text}")
            response.raise_for_status()

            if parsed_response := response.json():
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

                dataset = parsed_response['CommonResult']['DataSet']
                logger.info('%s records received from Galooli', len(dataset))
                return dataset
            else:
                logger.info(f"Galooli response: {response.text}")
                return None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise GalooliInvalidUserCredentialsException(e, "Unauthorized access", code=403)
            if e.response.status_code == 404:
                raise GalooliGeneralErrorException(e, "Not found", code=404)
            raise e
