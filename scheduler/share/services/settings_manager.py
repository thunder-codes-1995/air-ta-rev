from core.db import DB
from core.errors import InvalidParameterException
from models.configuration import Configuration


class SettingsManager:
    """Class is responsible for reading configuration for a given customer from the database
    It should take care of caching and reloading configuration (if it changes in the DB while app is running(TODO)"""
    DEFAULT_CONFIG_CODE = "DEFAULT"

    def __init__(self):
        self.db = DB()

    def _find_configuration(self, customer_code: str) -> dict:
        """Loads entire configuration for a given customer code from the database"""
        rec = self.db.configuration.find_one({'customer': customer_code})
        if rec is None:
            raise InvalidParameterException(f"Missing configuration records for customer:{customer_code}")
        return rec

    @staticmethod
    def _add_default_entries(customer_config: dict, default_config: dict):
        """Copy missing configuration entries from default config to customer config and returns copy of customer
        config"""
        customer_config_copy = customer_config.copy()
        customer_entries = customer_config_copy['configurationEntries']
        default_entries = default_config['configurationEntries']

        def is_key_in_customer_entries(key: str) -> bool:
            for customer_entry in customer_entries:
                if customer_entry['key'] == key:
                    return True
            return False

        for default_entry in default_entries:
            if not is_key_in_customer_entries(default_entry['key']):
                customer_entries.append(default_entry)
        return customer_config_copy

    def _load_customer_configuration(self, customer_code: str) -> Configuration:
        """ load customer configuration from DB, inject default values and return instance of Configuration"""
        host_configuration = self._find_configuration(customer_code)
        default_configuration = self._find_configuration(SettingsManager.DEFAULT_CONFIG_CODE)
        # combine host with defaults
        host_with_defaults = SettingsManager._add_default_entries(host_configuration,
                                                                  default_configuration)
        config = Configuration(host_with_defaults)
        return config

    def get_customer_configuration(self, customer_code: str) -> Configuration:
        # TODO - add caching (redis, with relatively short TTL)
        return self._load_customer_configuration(customer_code)
