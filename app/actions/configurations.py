import pydantic

from app.actions.core import PullActionConfiguration, AuthActionConfiguration, ExecutableActionMixin
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, UIOptions, find_config_for_action
from app.services.errors import ConfigurationNotFound


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    username: str
    password: pydantic.SecretStr = pydantic.Field(..., format="password")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "username",
            "password",
        ],
    )


class PullObservationsConfig(PullActionConfiguration):
    look_back_window_hours: int = FieldWithUIOptions(
        4,
        le=24,
        ge=1,
        title="Look Back Window Hours",
        description="Interval in hours to look back for data",
        ui_options=UIOptions(
            widget="range",
        )
    )
    gmt_offset: int = FieldWithUIOptions(
        0,
        le=12,
        ge=-12,
        title="GMT Offset",
        description="Offset from GMT in hours (e.g., -5 for EST, +1 for CET). This is used to adjust the timestamps of the observations.",
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "look_back_window_hours",
            "gmt_offset",
        ],
    )

def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"Pull observations settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)
