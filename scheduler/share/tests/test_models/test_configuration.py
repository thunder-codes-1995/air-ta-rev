import unittest
from models.configuration import (
    Configuration,
    Market,
    Competitor
)


class TestValidConfiguration(unittest.TestCase):
    valid_json_config = {
        'customer': "PY",
        'configurationEntries': [
            {'key': "COMPETITORS", "description": "", "value": [
                {"origin": "PBM", 'destination': "AMS", "competitors": ["KL", 'PY']},
            ]},
            {'key': "DEFAULT_CURRENCY", "description": "", "value": "USD"},
            {"key": "MARKETS", "description": "", "value": [{
                "currency": "USD",
                "direction": "RT",
                "numberOfDays": 10,
                "startOffset": 1,
                'stayDuration': 20,
                'orig': "PBM",
                "dest": "LFA"
            }]}
        ]
    }

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.config = Configuration(self.valid_json_config)

    def test_default_currency(self):
        self.assertEqual(self.config.currency, self.default_currency)

    def test_market_for_competitor(self):
        orig, dest = self.competitors[0]['origin'], self.competitors[0]['destination']
        self.assertEqual(self.config.market_for_competitor(0), (orig, dest))

    def test_competitors_for_market_by_index(self):
        self.assertEqual(self.config.market_competitor(idx=0), self.competitors[0]['competitors'])

    def test_competitors_for_market_by_origin_destination(self):
        orig, dest = self.competitors[0]['origin'], self.competitors[0]['destination']
        self.assertEqual(self.config.market_competitor(idx=None, origin=orig, destination=dest),
                         self.competitors[0]['competitors'])
        self.assertIsNone(self.config.market_competitor(idx=None, origin="AAA", destination="BBB"),
                          self.competitors[0]['competitors'])

    def test_first_competitor_values(self):
        self.assertEqual(self.config.competitor_at(0, 'competitors'), self.competitors[0]['competitors'])

    def test_first_market_origin_destination(self):
        self.assertEqual(self.config.market_at(0, 'orig'), self.markets[0]['orig'])
        self.assertEqual(self.config.market_at(0, 'dest'), self.markets[0]['dest'])

    @property
    def competitors(self):
        return self.valid_json_config['configurationEntries'][0]['value']

    @property
    def markets(self):
        return self.valid_json_config['configurationEntries'][2]['value']

    @property
    def default_currency(self):
        return self.valid_json_config['configurationEntries'][1]['value']


class TestInvalidCompetitor(unittest.TestCase):

    def test_invalid_origin_int(self):
        try:
            Competitor(1, 'B', ['dd', 'gg'])
        except Exception as e:
            self.assertIsInstance(e, AssertionError)

    def test_invalid_dest_empty(self):
        try:
            Competitor("AA", '', ['dd', 'gg'])
        except Exception as e:
            self.assertIsInstance(e, AssertionError)

    def test_invalid_value_empty(self):
        try:
            Competitor("AA", 'BB', [])
        except Exception as e:
            self.assertIsInstance(e, AssertionError)


class TestInvalidMarket(unittest.TestCase):

    def test_invalid_currency_type(self):
        try:
            Market(1, "OW", 10, 1, 2, 'ff', 'gg')
        except Exception as e:
            self.assertIsInstance(e, AssertionError)

    def test_invalid_currency_empty(self):
        try:
            Market("", "OW", 10, 1, 2, 'ff', 'gg')
        except Exception as e:
            self.assertIsInstance(e, AssertionError)

    def test_invalid_currency_empty(self):
        try:
            Market("", "OW", 10, 1, 2, 'ff', 'gg')
        except Exception as e:
            self.assertIsInstance(e, AssertionError)

    def test_invalid_number_of_days_limit(self):
        try:
            Market("PP", "OW", 0, 1, 2, 'ff', 'gg')
        except Exception as e:
            self.assertIsInstance(e, AssertionError)

    def test_invalid_direction(self):
        try:
            Market("PP", "PP", 0, 1, 2, 'ff', 'gg')
        except Exception as e:
            self.assertIsInstance(e, AssertionError)


if __name__ == "__main__":
    unittest.main()
