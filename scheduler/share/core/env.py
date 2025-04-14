import os

# from core.errors import MissingEnvironmentPropertyException
from core.logger import Logger
from dotenv import load_dotenv

load_dotenv()


def get_env_key(key_name, default_value=None):
    """Get the environment variable.
    In case environment variable is not set:
       If default_value is specified, it will be returned
       Otherwise it will raise MissingEnvironmentPropertyException"""
    if os.getenv(key_name) is None:
        if default_value is None:
            # raise MissingEnvironmentPropertyException(key_name)
            Logger("logs").error(f"Missing environment property: {key_name}")
        return default_value

    return os.getenv(key_name)


# *************** DATABASE ***************
CONNECTION_STRING = get_env_key("CONNECTION_STRING")
DB_NAME = get_env_key("DB_NAME")

# *************** HITIT ***************
HITIT_FILES_PATH = get_env_key("HITIT_FILES_PATH")
ATPCO_FILES_PATH = get_env_key("ATPCO_FILES_PATH")
HITIT_AUTHORIZATION_STORE_TO = get_env_key("HITIT_AUTHORIZATION_STORE_TO")
HITIT_USERNAME = get_env_key("HITIT_USERNAME")
HITIT_UPDATE_INVENTORY_DAY_COUNT = get_env_key("HITIT_UPDATE_INVENTORY_DAY_COUNT")
