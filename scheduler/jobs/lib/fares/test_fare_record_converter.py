import unittest
from lib.fares.fare_record_converter import FareRecordConverter

converter = FareRecordConverter()

class ConverterOneWayTest(unittest.TestCase):

    def test_convert_single_fare(self):
        itineraries = [{"itinId": 0}]
        fare = {"scrapedFrom":"turkishairlines.com", "fareAmount": 200,"fareCurrency": "USD","itinCabins": [{"cabin": ["ECONOMY"],"itinId": 0,"cabinClass": "Y"}]}
        converted = converter.convert_single_fare(itineraries, fare)
        self.assertEqual(200, converted['fareAmount'])
        self.assertEqual('USD', converted['fareCurrency'])
        self.assertEqual('ECONOMY', converted['cabinName'])
        self.assertEqual('Y', converted['classCode'])

    def test_convert_business_fares(self):
        itineraries = [{"itinId": 0},{"itinId": 1}]
        fares = [
            {"scrapedFrom":"turkishairlines.com",   "fareAmount": 500,"fareCurrency": "USD","itinCabins": [{"cabin": ["BUSINESS"],"itinId": 0,"cabinClass": "K"}]},
        ]
        for fare in fares:
            converted = converter.convert_single_fare(itineraries, fare)
            self.assertEqual('BUSINESS', converted['cabinName'])
            self.assertEqual('K', converted['classCode'])

    def test_economy_business_fares(self):
        itineraries = [{"itinId": 0},{"itinId": 1}]
        fares = [
            {"scrapedFrom":"turkishairlines.com",   "fareAmount": 200,"fareCurrency": "USD","itinCabins": [{"cabin": ["ECONOMY"],   "itinId": 0,"cabinClass": "Y"}]},
            {"scrapedFrom":"flypgs.com",            "fareAmount": 100, "fareCurrency": "USD", "itinCabins": [{"cabin": ["SUPER_ECO"], "itinId": 0, "cabinClass": "Y"}]},
            {"scrapedFrom":"flypgs.com",            "fareAmount": 200, "fareCurrency": "USD", "itinCabins": [{"cabin": ["ECONOMY"],     "itinId": 0, "cabinClass": "Y"}]}
        ]
        for fare in fares:
            converted = converter.convert_single_fare(itineraries, fare)
            self.assertEqual('ECONOMY', converted['cabinName'])
            self.assertEqual('Y', converted['classCode'])

if __name__ == '__main__':
    unittest.main()