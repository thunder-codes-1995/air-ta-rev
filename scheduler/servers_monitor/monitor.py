import logging
import os
import sys

import boto3
import requests
from dotenv import load_dotenv
from jobs.lib.utils.logger import setup_logging
from lib.utils.logger import setup_logging

load_dotenv()

setup_logging()
logger = logging.getLogger("monitor")


class ServerMonitor:
    def check_status(self):
        # taking the validity percentage from terminal
        try:
            valid_percentage = int(sys.argv[1])
        except IndexError:
            raise Exception("Please enter the validity percentage")

        invalid_markets = self.__get_invalid_markets(valid_percentage)

        if invalid_markets:
            self.send_alert(invalid_markets)

    @staticmethod
    def __get_invalid_markets(valid_percentage):
        """
        This method will return true if all markets are up-to-date based on the valid_percentage parameter
        it will check every market and return False if one of the market is below the valid_percentage
        """
        url = "https://msd.prod.atarev.com/api/msdv2/fare-structure/scraper-health"
        headers = {"Authorization": "Bearer " + os.getenv("AUTH_TOKEN")}
        raw_response = requests.get(url=url, headers=headers)
        response = raw_response.json()

        invalid_markets = [market for market in response if market["validPercentage"] < valid_percentage]
        return invalid_markets

    @staticmethod
    def send_alert(markets: list):
        message = "ALERT!! \nThe following markets are outdated: \n"
        for market in markets:
            message += (
                f"{market['carrierCode']} {market['marketOrigin']} - {market['marketDestination']}: "
                f"{round(market['validPercentage'], 2)}%\n"
            )

        sns_client = boto3.client(
            "sns",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name="eu-central-1",
        )

        try:
            response = sns_client.publish(TopicArn=os.getenv("TOPIC_ARN"), Message=message)
            logger.info(f"Sent alert, response: {response}")
        except:
            logger.exception("Couldn't publish a message")
            raise


if __name__ == "__main__":
    ServerMonitor().check_status()
