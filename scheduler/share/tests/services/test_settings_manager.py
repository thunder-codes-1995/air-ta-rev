import unittest
from services.settings_manager import SettingsManager


class TestSettingsManager(unittest.TestCase):
    # sample customer configuration
    valid_json_config = {
        'customer': "CY",
        'configurationEntries': [
            {'key': "COMPETITORS", "description": "", "value": [
                {"origin": "PBM", 'destination': "AMS", "competitors": ["KL", 'PY']},
            ]},
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

    # sample default configuration
    valid_json_default_config = {
        'customer': "DEFAULT",
        'configurationEntries': [
            {'key': "DEFAULT_CURRENCY", "description": "", "value": "USD"}
        ]
    }

    def test_initialize_customer_configuration(self):
        cfg = SettingsManager()

        # Mock the find configuration method to avoid DB access
        def mocked_find_configuration(customer):
            if customer == "DEFAULT":
                return self.valid_json_default_config
            return self.valid_json_config

        cfg._find_configuration = mocked_find_configuration

        rec = cfg._load_customer_configuration('CY')
        self.assertIsNotNone(rec, "should not be none")
        self.assertEqual(self.valid_json_config['customer'], rec.customer)
        self.assertEqual('USD', rec.currency)

    def test_add_default_entries(self):
        """Keys that are missing in customer configuration should be added from default """
        customer_config_json = {
            'customer': "PY",
            'configurationEntries': [
                {'key': "k1", "value": "key1_customer_value"},
                {'key': "k2", "value": "key2_customer_value"}
            ]
        }

        default_config_json = {
            'customer': "DEFAULT",
            'configurationEntries': [
                {'key': "k1", "value": "key1_default_value"},
                {'key': "k3", "value": "key3_default_value"}
            ]
        }
        result = SettingsManager._add_default_entries(customer_config_json, default_config_json)
        expected_json = {
            'customer': 'PY',
            'configurationEntries': [{'key': 'k1', 'value': 'key1_customer_value'},
                                     {'key': 'k2', 'value': 'key2_customer_value'},
                                     {'key': 'k3', 'value': 'key3_default_value'}]}
        self.assertEqual(result, expected_json)


if __name__ == "__main__":
    unittest.main()
