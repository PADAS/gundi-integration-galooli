import httpx
import logging
import csv
import app.actions.client as client
from dateparser import parse as dp
from functional import seq
from datetime import datetime, timedelta, timezone
import pytz

from io import StringIO
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, get_auth_config
from app.actions.utils import convert_to_er_observation, filter_observations_by_device_status
from app.services.activity_logger import activity_logger
from app.services.action_scheduler import crontab_schedule
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

GALOOLI_BASE_URL = "https://sdk.galooli-systems.com/galooliSDKService.svc/json/Assets_Report"


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing 'auth' action with integration ID {integration.id} and action_config {action_config}...")

    url = integration.base_url or GALOOLI_BASE_URL

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=1) # Use small window, since we're just checking credentials

    try:
        await client.get_observations(
            url,
            username=action_config.username,
            password=action_config.password.get_secret_value(),
            start=start
        )
        
        return {"valid_credentials": True}
    except (client.GalooliInvalidUserCredentialsException, client.GalooliGeneralErrorException, client.GalooliTooManyRequestsException) as e:
        return {"valid_credentials": False, "message": e.message, "code": e.code}
    except httpx.HTTPStatusError as e:
        return {"error": True, "status_code": e.response.status_code}

@crontab_schedule("*/5 * * * *")
@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    url = integration.base_url or GALOOLI_BASE_URL
    auth_config = get_auth_config(integration)

    observations_extracted = 0

    last_updated_time = await state_manager.get_state(
        integration_id=integration.id,
        action_id="pull_observations"
    )

    if not last_updated_time:
        now = datetime.now(timezone.utc)
        logger.info(f"Setting initial lookback hours to {action_config.look_back_window_hours} hrs from now")
        start = now - timedelta(hours=action_config.look_back_window_hours)
    else:
        start = dp(last_updated_time.get("last_updated_time")).replace(tzinfo=timezone.utc)

    try:
        logger.info(f"-- Getting observations for Username: {auth_config.username} from {start} --")
        
        if get_observations_response := await client.get_observations(
            url,
            username=auth_config.username,
            password=auth_config.password.get_secret_value(),
            start=start
        ):

            dataset = get_observations_response['CommonResult']['DataSet']
            logger.info('%s records received from Galooli', len(dataset))

            # TODO: Figure out why we're converting to CSV first and then posting to Gundi.
            # Convert dataset to CSV format
            csv_buffer = StringIO()
            csv_writer = csv.writer(csv_buffer)
            csv_writer.writerows(dataset)
            csv_buffer.seek(0)
            
            reports_timezone = pytz.FixedOffset(action_config.gmt_offset * 60)
            # Apply map and filter operations
            observations = seq.csv(csv_buffer) \
                .map(lambda r: convert_to_er_observation(r, reports_timezone)) \
                .filter(lambda x: x is not None) \
                .to_list()
            
            if observations:
                logger.info(f"Extracted {len(observations)} observations for username {auth_config.username}")

                filtered_observations = await filter_observations_by_device_status(str(integration.id), observations)

                for i, batch in enumerate(generate_batches(filtered_observations, 200)):
                    logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Username: {auth_config.username}')
                    response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                    observations_extracted += len(response)

            else:
                logger.warning(f"No valid observations found for Username: {auth_config.username}")

            # Save latest execution time to state
            latest_time = get_observations_response["MaxGmtUpdateTime"]
            state = {"last_updated_time": latest_time}

            await state_manager.set_state(
                integration_id=integration.id,
                action_id="pull_observations",
                state=state
            )
            logger.info(f"State updated for integration {integration.id} with last_updated_time: {latest_time}")

            return {"observations_extracted": observations_extracted}
        else:
            logger.warning(f"No observations found for Username: {auth_config.username}")
            return {"observations_extracted": 0}
    except (client.GalooliInvalidUserCredentialsException, client.GalooliGeneralErrorException, client.GalooliTooManyRequestsException) as e:
        logger.error(f"Galooli API returned error for integration {integration.id}. Exception: {e}")
        raise
    except httpx.HTTPStatusError as e:
        message = f"Error while executing 'pull_observations' for integration {integration.id}. Exception: {e}"
        logger.exception(message)
        raise
