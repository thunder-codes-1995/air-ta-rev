import datetime
import unittest

from lfa_queue_jobs.schedule_processor import ScrapeFrequency, ScraperMarketSchedule, ScraperMarket, Direction


class TestScheduleProcessor(unittest.TestCase):

    def test_frequency_is_day_of_week_enabled(self):
        all_days_of_week = ScrapeFrequency("0123456")
        self.assertTrue(all_days_of_week.is_day_of_week_enabled(0))
        self.assertTrue(all_days_of_week.is_day_of_week_enabled(1))
        self.assertTrue(all_days_of_week.is_day_of_week_enabled(2))
        self.assertTrue(all_days_of_week.is_day_of_week_enabled(3))
        self.assertTrue(all_days_of_week.is_day_of_week_enabled(4))
        self.assertTrue(all_days_of_week.is_day_of_week_enabled(5))
        self.assertTrue(all_days_of_week.is_day_of_week_enabled(6))
        self.assertFalse(all_days_of_week.is_day_of_week_enabled(-1))
        self.assertFalse(all_days_of_week.is_day_of_week_enabled(7))

        monday_only = ScrapeFrequency("1")
        self.assertTrue(monday_only.is_day_of_week_enabled(1))
        self.assertFalse(monday_only.is_day_of_week_enabled(0))
        self.assertFalse(monday_only.is_day_of_week_enabled(2))

    def test_frequency_validation(self):
        # all below should be valid
        for i in range(7):
            ScrapeFrequency(f"{i}")
        ScrapeFrequency("0123456")
        ScrapeFrequency("0000000000")
        ScrapeFrequency("0101010101")

        # all below should be invalid
        with self.assertRaises(TypeError):
            ScrapeFrequency("-1")
        with self.assertRaises(TypeError):
            ScrapeFrequency("-")
        with self.assertRaises(TypeError):
            ScrapeFrequency("7")
        with self.assertRaises(TypeError):
            ScrapeFrequency("")

    def test_scraper_market_schedule(self):
        ScraperMarketSchedule(effective_from=datetime.date(2022, 1, 1), effective_to=datetime.date(2022, 2, 28),
                              frequency=ScrapeFrequency("0123456"), scrapers=["itasoftware"])

        # all below should be invalid
        with self.assertRaises(TypeError):
            ScraperMarketSchedule(effective_from=datetime.date(2022, 1, 1), effective_to=datetime.date(2018, 1, 1),
                                  frequency=ScrapeFrequency("0123456"), scrapers=["itasoftware"])
        with self.assertRaises(TypeError):
            ScraperMarketSchedule(effective_from=datetime.date(2022, 1, 1), effective_to=None,
                                  frequency=ScrapeFrequency("0123456"), scrapers=["itasoftware"])
        with self.assertRaises(TypeError):
            ScraperMarketSchedule(effective_from=None, effective_to=datetime.date(2018, 1, 1),
                                  frequency=ScrapeFrequency("0123456"), scrapers=["itasoftware"])
        with self.assertRaises(TypeError):
            ScraperMarketSchedule(effective_from=datetime.date(2022, 1, 1), effective_to=datetime.date(2018, 1, 1),
                                  frequency=None, scrapers=["itasoftware"])

    def test_scraper_market(self):
        schedule_fixture = ScraperMarketSchedule(effective_from=datetime.date(2022, 1, 1),
                                                 effective_to=datetime.date(2022, 2, 28),
                                                 frequency=ScrapeFrequency("0123456"), scrapers=["itasoftware"])
        # this should be ok
        ScraperMarket(origin="PBM", destination="AMS", host_carrier="PY", start_offset=0, number_of_days_to_scrape=5,
                      direction=Direction.RoundTrip, stay_duration=7, schedule=[schedule_fixture])
        ScraperMarket(origin="PBM", destination="AMS", host_carrier="PY", start_offset=0, number_of_days_to_scrape=5,
                      direction=Direction.OneWay, stay_duration=None, schedule=[schedule_fixture])
        # all below should be invalid
        with self.assertRaises(TypeError):
            ScraperMarket(origin="PBM", destination="AMS", host_carrier="PY", start_offset=0,
                          number_of_days_to_scrape=5, direction=Direction.OneWay, stay_duration=7, schedule=[])
            ScraperMarket(origin="", destination="AMS", host_carrier="PY", start_offset=0, number_of_days_to_scrape=5,
                          direction=Direction.OneWay, stay_duration=7, schedule=[schedule_fixture])
            ScraperMarket(origin="PBM", destination="", host_carrier="PY", start_offset=0, number_of_days_to_scrape=5,
                          direction=Direction.OneWay, stay_duration=7, schedule=[schedule_fixture])
            ScraperMarket(origin="PBM", destination="AMS", host_carrier="", start_offset=0, number_of_days_to_scrape=5,
                          direction=Direction.OneWay, stay_duration=7, schedule=[schedule_fixture])

    def test_scraper_market_schedule_is_active(self):
        frequency_monday = ScrapeFrequency("0")
        frequency_sunday = ScrapeFrequency("6")
        frequency_everyday = ScrapeFrequency("0123456")
        today = datetime.date.today()
        effective_in_7_days = today + datetime.timedelta(days=7)
        effective_in_14_days = today + datetime.timedelta(days=14)

        everyday_schedule_fixture = ScraperMarketSchedule(effective_from=effective_in_7_days,
                                                          effective_to=effective_in_14_days,
                                                          frequency=frequency_everyday, scrapers=["itasoftware"])
        monday_schedule_fixture = ScraperMarketSchedule(effective_from=effective_in_7_days,
                                                        effective_to=effective_in_14_days,
                                                        frequency=frequency_monday, scrapers=["itasoftware"])
        frequency_sunday = ScraperMarketSchedule(effective_from=effective_in_7_days, effective_to=effective_in_14_days,
                                                 frequency=frequency_sunday, scrapers=["itasoftware"])

        scrape_start_offset = 2
        number_of_days_to_scrape = 15
        everyday_market = ScraperMarket(origin="PBM", destination="AMS", host_carrier="PY",
                                        start_offset=scrape_start_offset,
                                        number_of_days_to_scrape=number_of_days_to_scrape, direction=Direction.OneWay,
                                        stay_duration=7, schedule=[everyday_schedule_fixture])

        monday_only_market = ScraperMarket(origin="PBM", destination="AMS", host_carrier="PY",
                                           start_offset=scrape_start_offset,
                                           number_of_days_to_scrape=number_of_days_to_scrape,
                                           direction=Direction.OneWay, stay_duration=7,
                                           schedule=[monday_schedule_fixture])

        sunday_only_market = ScraperMarket(origin="PBM", destination="AMS", host_carrier="PY",
                                           start_offset=scrape_start_offset,
                                           number_of_days_to_scrape=number_of_days_to_scrape,
                                           direction=Direction.OneWay, stay_duration=7, schedule=[frequency_sunday])

        without_schedule = ScraperMarket(origin="PBM", destination="AMS", host_carrier="PY",
                                           start_offset=scrape_start_offset,
                                           number_of_days_to_scrape=number_of_days_to_scrape,
                                           direction=Direction.OneWay, stay_duration=7, schedule=[])

        # iterate over all days in the scrape period + 10 days
        for day in range(number_of_days_to_scrape + 10):
            day_to_check = today + datetime.timedelta(days=day)
            effective_in_7_days = today + datetime.timedelta(days=7)
            # check if a given day to check is within scraping offset (start_offset and number_of_days_to_scrape)
            self.assertEqual(everyday_market.is_within_offset(day_to_check), today + datetime.timedelta(
                days=scrape_start_offset) <= day_to_check <= today + datetime.timedelta(
                days=scrape_start_offset + number_of_days_to_scrape))

            # check if a given date should be scraped due to operating frequency
            if effective_in_7_days <= day_to_check <= effective_in_14_days:
                self.assertEqual(monday_only_market.is_date_operational(day_to_check),
                                 day_to_check.weekday() == 0)  # this should be true only on monday
                self.assertEqual(sunday_only_market.is_date_operational(day_to_check),
                                 day_to_check.weekday() == 6)  # this should be true only on sunday
                self.assertEqual(everyday_market.is_date_operational(day_to_check),
                                 0 <= day_to_check.weekday() <= 6)  # this should be true on every day
            else:
                self.assertFalse(monday_only_market.is_date_operational(day_to_check))
                self.assertFalse(sunday_only_market.is_date_operational(day_to_check))
                self.assertFalse(everyday_market.is_date_operational(day_to_check))


if __name__ == "__main__":
    unittest.main()
