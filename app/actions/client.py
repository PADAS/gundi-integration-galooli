import logging
import httpx
import stamina

from datetime import datetime, timedelta

from app.services.state import IntegrationStateManager


state_manager = IntegrationStateManager()
logger = logging.getLogger(__name__)

REQUESTED_PROPERTIES = 'unit_id,unit_name,organization_name,real_time_GPS_Time,real_time_status,real_time_Latitude,real_time_Longitude,real_time_Distance,real_time_Speed'
QUIET_PERIOD_MINS = 15




class GalooliException(Exception):
    def __init__(self, message: str, code: int = -1):
        self.code = code
        self.message = message
        text = f"{self.code}: {self.message}"
        super().__init__(text)


class GalooliGeneralErrorException(GalooliException):
    def __init__(self, message: str, code: int = -1):
        super().__init__(message, code=code)


class GalooliInvalidUserCredentialsException(GalooliException):
    def __init__(self, message: str, code: int = 1000):
        super().__init__(message, code=code)


class GalooliTooManyRequestsException(GalooliException):
    def __init__(self, message: str, code: int = 1101):
        super().__init__(message, code=code)


@stamina.retry(on=GalooliTooManyRequestsException, attempts=3, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_observations(url, *, integration_id: str, username: str, password: str, start: datetime):
    async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=30.0, write=15.0, pool=5.0)) as session:
        params = {
            'requestedPropertiesStr': REQUESTED_PROPERTIES,
            'lastGMTUpdateTime': start.strftime("%Y-%m-%d %H:%M:%S"),
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
                    if result_code == 1101:
                        raise GalooliTooManyRequestsException(message=result_description)
                    elif result_code == 1000:
                        logger.warning(f"Galooli returned '{result_code}:{result_description}' error for user {username}. Setting {QUIET_PERIOD_MINS} mins of quiet period...")
                        await state_manager.set_quiet_period(
                            integration_id=integration_id,
                            action_id="pull_observations",
                            quiet_period=timedelta(minutes=QUIET_PERIOD_MINS)
                        )
                        raise GalooliInvalidUserCredentialsException(message=result_description)
                    raise GalooliGeneralErrorException(message=f"General error occurred. Result code: {result_code}")

                return parsed_response
            else:
                logger.info(f"Galooli response: {response.text}")
                return None

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                logger.warning(f"Galooli returned HTTP 403 error for user {username}. Setting {QUIET_PERIOD_MINS} mins of quiet period...")
                await state_manager.set_quiet_period(
                    integration_id=integration_id,
                    action_id="pull_observations",
                    quiet_period=timedelta(minutes=QUIET_PERIOD_MINS)
                )
                raise GalooliInvalidUserCredentialsException("Unauthorized access", code=403)
            if e.response.status_code == 404:
                raise GalooliGeneralErrorException("Not found", code=404)
            raise e
