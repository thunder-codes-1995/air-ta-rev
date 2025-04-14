import datetime
import unittest

from jobs.base_scraped_fares_processor import TripDirection
from jobs.scraped_fares_etl_job import ScrapedFaresProcessorFilterArguments


class TestFaresEtlJob(unittest.TestCase):

    def test_params(self):
        ScrapedFaresProcessorFilterArguments(host_carrier_code="AA", origin="JFK", destination="LAX", departure_date=datetime.date(2021, 1, 1),
                                             scraped_after=datetime.datetime(2021, 1, 1, 0, 0, 0), direction=TripDirection.OneWay)
        ScrapedFaresProcessorFilterArguments(host_carrier_code="AA", origin="JFK", destination="LAX", departure_date=datetime.date(2021, 1, 1),
                                             scraped_after=datetime.datetime(2021, 1, 1, 0, 0, 0), direction=TripDirection.RoundTrip,
                                             stay_duration=7)

        with self.assertRaises(TypeError):
            # in case of RT flights, stay_duration is required and cannot be 0
            ScrapedFaresProcessorFilterArguments(host_carrier_code="AA", origin="JFK", destination="LAX", departure_date=datetime.date(2021, 1, 1),
                                                 scraped_after=datetime.datetime(2021, 1, 1, 0, 0, 0),
                                                 direction=TripDirection.RoundTrip)
            ScrapedFaresProcessorFilterArguments(host_carrier_code="AA", origin="JFK", destination="LAX", departure_date=datetime.date(2021, 1, 1),
                                                 scraped_after=datetime.datetime(2021, 1, 1, 0, 0, 0),
                                                 direction=TripDirection.RoundTrip, stay_duration=0)

        with self.assertRaises(TypeError):
            # missing origin
            ScrapedFaresProcessorFilterArguments(host_carrier_code="AA", origin="", destination="LAX", departure_date=datetime.date(2021, 1, 1),
                                                 scraped_after=datetime.datetime(2021, 1, 1, 0, 0, 0),
                                                 direction=TripDirection.RoundTrip, stay_duration=7)

        with self.assertRaises(TypeError):
            # missing departure_date
            ScrapedFaresProcessorFilterArguments(host_carrier_code="AA", origin="JFK", destination="LAX",
                                                 scraped_after=datetime.datetime(2021, 1, 1, 0, 0, 0),
                                                 direction=TripDirection.RoundTrip, stay_duration=7)

        with self.assertRaises(TypeError):
            # missing scraped_after
            ScrapedFaresProcessorFilterArguments(host_carrier_code="AA", origin="JFK", destination="LAX", departure_date=datetime.date(2021, 1, 1),
                                                 direction=TripDirection.OneWay)


if __name__ == "__main__":
    unittest.main()
