import datetime
import unittest
from jobs.base_scraped_fares_processor import ScrapedFaresProcessorFilterArguments, TripDirection
from jobs.scraped_fares_etl_job import ExtractTransformedScrapedFares, SOLD_OUT


class CopyFareTest(unittest.TestCase):

    def test(self):
        args = ScrapedFaresProcessorFilterArguments(host_carrier_code="CY",
                                                    scraped_after=datetime.datetime(year=2020, month=12, day=12),
                                                    # TODO - use other field than this to get scraped_after
                                                    departure_date=datetime.datetime.strptime("2023-03-01", "%Y-%m-%d"),
                                                    origin="EVN", destination="LCA",
                                                    direction=TripDirection.OneWay,
                                                    dry_run=False, stay_duration=3)
        ExtractTransformedScrapedFares(args).process()

    def test_sold_out_functionality(self):
        args = ScrapedFaresProcessorFilterArguments(host_carrier_code="CY",
                                                    scraped_after=datetime.datetime(year=2023, month=1, day=1),
                                                    departure_date=datetime.datetime(year=2023, month=5, day=3),
                                                    origin="LCA", destination="ATH",
                                                    direction=TripDirection.OneWay,
                                                    dry_run=False, stay_duration=5)
        result = ExtractTransformedScrapedFares(args)._create_database_record({'type': 'OW', 'itineraries': [
            {'itinOriginCode': 'LCA', 'itinDestCode': 'ATH', 'itinId': 0, 'itinDuration': 145, 'itinDeptDate': 20230503,
             'itinDeptTime': 700, 'itinArrivalDate': 20230503, 'itinArrivalTime': 845, 'direction': 'OB', 'legs': [
                {'legOriginCode': 'LCA', 'legDestCode': 'ATH', 'legDeptDate': 20230503, 'legDeptTime': 700,
                 'legArrivalDate': 20230503, 'legArrivalTime': 845, 'mkAlCode': 'CY', 'mkFltNum': 310, 'opAlCode': 'CY',
                 'opFltNum': 310, 'flightDuration': 145, 'aircraft': 'A320'}]}], 'flightKey': 'CY310|20230503|LCAATH:',
                                                                               'fares': [{'travelAgency': 'CY',
                                                                                          'scrapedFrom': 'cyprus',
                                                                                          'fareAmount': 73.96,
                                                                                          'fareCurrency': 'EUR',
                                                                                          'baseFare': 38,
                                                                                          'taxAmount': 29.96,
                                                                                          'yqyrAmount': 6,
                                                                                          'scrapeTime': datetime.datetime(
                                                                                              year=2023, month=1,
                                                                                              day=15),
                                                                                          'lastUpdateDateTime': 20230411042759,
                                                                                          'cabinName': 'Economy',
                                                                                          'classCode': 'Q'}],
                                                                               'hostCode': 'CY', 'min_fares': [
                {'travelAgency': 'CY', 'scrapedFrom': 'cyprus', 'fareAmount': 53.96, 'fareCurrency': 'EUR',
                 'lastUpdateDateTime': 20230411042759, 'scrapeTime': datetime.datetime(year=2023, month=1, day=15),
                 'cabinName': 'Economy', 'classCode': 'Q', 'baseFare': 18, 'taxAmount': 29.96, 'yqyrAmount': 6}]})

        if result["state"] != SOLD_OUT:
            raise
