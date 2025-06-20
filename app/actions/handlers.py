import httpx
import logging
import csv
import app.actions.client as client
from functional import seq
import pytz

from io import StringIO
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, get_pull_config, get_auth_config
from app.actions.utils import convert_to_er_observation
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

GALOOLI_BASE_URL = "https://sdk.galooli-systems.com/galooliSDKService.svc/json/Assets_Report"


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing 'auth' action with integration ID {integration.id} and action_config {action_config}...")

    url = integration.base_url or GALOOLI_BASE_URL

    try:
        await client.get_observations(
            url,
            username=action_config.username,
            password=action_config.password.get_secret_value(),
            look_back_window_hours=1 # Use small window, since we're just checking credentials
        )
        
        return {"valid_credentials": True}
    except (client.GalooliInvalidUserCredentialsException, client.GalooliGeneralErrorException, client.GalooliTooManyRequestsException) as e:
        return {"valid_credentials": False, "message": e.message, "code": e.code}
    except httpx.HTTPStatusError as e:
        return {"error": True, "status_code": e.response.status_code}


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    url = integration.base_url or GALOOLI_BASE_URL
    auth_config = get_auth_config(integration)

    observations_extracted = 0

    try:
        logger.info(f"-- Getting observations for Username: {auth_config.username} --")
        dataset = await client.get_observations(
            url,
            username=auth_config.username,
            password=auth_config.password.get_secret_value(),
            look_back_window_hours=int(action_config.look_back_window_hours)
        )
        
        if dataset:

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

                for i, batch in enumerate(generate_batches(observations, 200)):
                    logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Username: {auth_config.username}')
                    response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                    observations_extracted += len(response)

                return {"observations_extracted": observations_extracted}
            else:
                logger.warning(f"No valid observations found for Username: {auth_config.username}")
                return {"observations_extracted": 0}
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
