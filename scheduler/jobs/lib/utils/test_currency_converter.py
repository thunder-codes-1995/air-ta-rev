import unittest

from jobs.lib.utils.currency_converter import CurrencyConverter

converter = CurrencyConverter()

rates = {
    "CHF":0.5,
    "USD":1,
    "EUR":2,
    "TRY":15,

}

converter.exchange_rates = rates
converter.db = {}   #to avoid connecting to the db
class CurrencyConverterTest(unittest.TestCase):

    def test_convert_direct(self):
        #convert direct (where target currency or from_currency = USD)
        converted = converter.convert_currency('USD','TRY',100) #1 USD --> 1500 TRY
        self.assertEqual(1500, converted)

        converted = converter.convert_currency('TRY','USD',1500) #1500 TRY --> 100 USD
        self.assertEqual(100, converted)

    def test_convert_indirect(self):
        # convert indirect (where target currency or from_currency is not USD and we need to have intermediate step)
        converted = converter.convert_currency('EUR','TRY',10) #10 EUR --> 5USD -> 75 TRY
        self.assertEqual(75, converted)

if __name__ == '__main__':
    unittest.main()