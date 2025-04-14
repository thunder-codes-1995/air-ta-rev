
import unittest

from jobs.scraped_fares_etl_job import ExtractTransformedScrapedFares

handler = ExtractTransformedScrapedFares()

data = [  
    {   
        "itineraries" : [{"itinId": 0, 'legs' : [{},{}]},{"itinId": 1, 'legs' : [{},{}]}],
        "fares" : [
            {"scrapedFrom":"turkishairlines.com", "lastUpdateDateTime" :20200101000000,"fareAmount" : 5,"fareCurrency" : "USD","itinCabins": [{"cabin": ["ECONOMY"],   "itinId": 0,"cabinClass": "Y"}]},
            {"scrapedFrom":"turkishairlines.com", "lastUpdateDateTime" :20200101000000,"fareAmount" : 4,"fareCurrency" : "USD","itinCabins": [{"cabin": ["ECONOMY"],   "itinId": 0,"cabinClass": "Y"}]},
            {"scrapedFrom":"turkishairlines.com", "lastUpdateDateTime" :20200102000000,"fareAmount" : 7,"fareCurrency" : "USD","itinCabins": [{"cabin": ["ECONOMY"],   "itinId": 0,"cabinClass": "Y"}]},
        ]
    },
    {   
        "itineraries" : [
            {
                'legs' : [{}]
            }
        ],
        "fares" : [
            {"scrapedFrom":"turkishairlines.com", "lastUpdateDateTime" :20220602000000,"fareAmount" : 10,"fareCurrency" : "EUR", "itinCabins": [{"cabin": ["ECONOMY"],   "itinId": 0,"cabinClass": "Y"}]},
            {"scrapedFrom":"turkishairlines.com", "lastUpdateDateTime" :20200101000000,"fareAmount" : 4,"fareCurrency" : "EUR", "itinCabins": [{"cabin": ["ECONOMY"],   "itinId": 0,"cabinClass": "Y"}]},
            {"scrapedFrom":"turkishairlines.com", "lastUpdateDateTime" :20220505000000,"fareAmount" : 8,"fareCurrency" : "EUR", "itinCabins": [{"cabin": ["ECONOMY"],   "itinId": 0,"cabinClass": "Y"}]},
        ]
    }
]

class ScrapedFaresETLJobTest(unittest.TestCase):

    def test_get_latest_minimum_fare(self):
        fare1 = handler.get_latest_minimum_fare(data[0]['fares'])

        self.assertEqual(4,fare1['fareAmount'])
        self.assertEqual(20200101000000,fare1['lastUpdateDateTime'])

        fare2 = handler.get_latest_minimum_fare(data[1]['fares'])
        self.assertEqual(4, fare2['fareAmount'])
        self.assertEqual(20200101000000,fare2['lastUpdateDateTime'])

    def test_is_direct(self) :
        self.assertFalse(handler.is_direct(data[0]['itineraries']),'Result should be False')
        self.assertTrue(handler.is_direct(data[1]['itineraries']),'Result should be True')


if __name__ == '__main__':
    unittest.main()