from typing import List

from flask import request

from base.service import BaseService
from configurations.forms import GetCustomerMarketsForm
from configurations.repository import ConfigurationRepository

config_repo = ConfigurationRepository()


class ConfigurationService(BaseService):

    def get_by_key(self, key: str):
        value = config_repo.get_by_key(key, request.user.carrier)
        return {"value": value}

    def get_customers(self):
        return config_repo.get_customers()

    def get_customer_markets(self, form: GetCustomerMarketsForm):
        customer: str = form.customer.data
        markets = config_repo.get_customer_markets(customer)["markets"]
        table_labels = [
            {"orig": "Origin"},
            {"dest": "Destination"},
            {"dir_curr": "Direction, Currency"},
        ]
        table_data = []
        for market in markets:
            table_data.append(
                {
                    "orig": market["orig"],
                    "dest": market["dest"],
                    "dir_curr": market["direction"] + ", " + market["currency"],
                }
            )
        return {"table": {"data": table_data, "labels": table_labels}}
