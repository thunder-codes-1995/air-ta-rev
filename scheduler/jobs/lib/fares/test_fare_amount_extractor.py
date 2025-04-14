import unittest

from jobs.lib.fares.fare_amount_extractor import FareAmountExtractor, HalfRoundTripFareAmountExtractorStrategy, \
    DefaultTripFareAmountExtractorStrategy


class FareAmountExtractorTest(unittest.TestCase):

    def test_half_round_trip_amount_extractor_strategy(self):
        converter = HalfRoundTripFareAmountExtractorStrategy()
        itineraries = [{"itinId": 0}]
        fare = {"fareAmount": 200,"fareCurrency": "USD"}
        (amount, currency) = converter.extract_fare_amount(itineraries, fare)
        self.assertEqual(100, amount)
        self.assertEqual('USD', currency)

    def test_default_amount_extractor_strategy(self):
        converter = DefaultTripFareAmountExtractorStrategy()
        itineraries = [{"itinId": 0}]
        fare = {"fareAmount": 200,"fareCurrency": "USD"}
        (amount, currency) = converter.extract_fare_amount(itineraries, fare)
        self.assertEqual(200, amount)
        self.assertEqual('USD', currency)

    def test_fare_amount_extractor(self):
        extractor = FareAmountExtractor()
        itineraries = [{"itinId": 0}]

        fare = {"scrapedFrom":"turkishairlines.com", "fareAmount": 200,"fareCurrency": "USD","itinCabins": [{"cabin": ["ECO"],"itinId": 0,"cabinClass": "Y"}]}
        (amount, currency) = extractor.extract_fare_amount(itineraries, fare)
        self.assertEqual(200, amount)
        self.assertEqual('USD', currency)

        # on rome2rio.com half round trip extractor should be used
        fare = {"scrapedFrom":"rome2rio.com", "fareAmount": 200,"fareCurrency": "USD","itinCabins": [{"cabin": ["ECO"],"itinId": 0,"cabinClass": "Y"}]}
        (amount, currency) = extractor.extract_fare_amount(itineraries, fare)
        self.assertEqual(100, amount)
        self.assertEqual('USD', currency)


if __name__ == '__main__':
    unittest.main()