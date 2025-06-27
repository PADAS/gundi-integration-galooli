import logging
import httpx
import stamina

from datetime import datetime, timezone, timedelta


logger = logging.getLogger(__name__)

REQUESTED_PROPERTIES = 'unit_id,unit_name,organization_name,real_time_GPS_Time,real_time_status,real_time_Latitude,real_time_Longitude,real_time_Distance,real_time_Speed'



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
async def get_observations(url, *, username, password, start):
    async with httpx.AsyncClient(timeout=120) as session:
        params = {
            'requestedPropertiesStr': REQUESTED_PROPERTIES,
            'lastGMTUpdateTime': start,
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

                return parsed_response
            else:
                logger.info(f"Galooli response: {response.text}")

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise GalooliInvalidUserCredentialsException(e, "Unauthorized access", code=403)
            if e.response.status_code == 404:
                raise GalooliGeneralErrorException(e, "Not found", code=404)
            raise e
