import httpx
import logging
import app.actions.client as client

from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, get_pull_config, get_auth_config
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
    pull_config = get_pull_config(integration)

    try:
        obs = await client.get_observations(integration, url, action_config, pull_config)
        if not obs:
            logger.error(f"Failed to authenticate with integration {integration.id} using {action_config}")
            return {"valid_credentials": False, "message": "Bad credentials"}
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
        observations = await client.get_observations(integration, url, auth_config, action_config)
        if observations:
            logger.info(f"Extracted {len(observations)} observations for username {auth_config.username}")

            for i, batch in enumerate(generate_batches(observations, 200)):
                logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Username: {auth_config.username}')
                response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                observations_extracted += len(response)

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
