import unittest
from share.core.mongo.me_connect import MEConnect
from lfa_queue_jobs.queue_calculate_auth import AuthorizationCalculator
from share.tests.test_auth_calc.mock_data import (
    upsell_brule, flight, flight_total_bookings, brule_set_avail
)
from share.typs.hitit import FlightParams, CabinParams


class TestAuthorizationCalculation(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.db = MEConnect().create_and_connect_to_test_db()

        self.brule = upsell_brule
        self.flight = flight
        self.flight_total_bookings = flight_total_bookings
        self.brule_set_avail = brule_set_avail

        hitit = self.db['hitit']
        hitit.delete_many(filter={})
        hitit.insert_one(flight)

        bre_results = self.db['bre_rules_results']
        bre_results.delete_many(filter={})
        bre_results.insert_one(upsell_brule)

        self.flight_params = FlightParams(
            airline_code=self.flight['airline_code'],
            origin=self.flight['origin'],
            destination=self.flight['destination'],
            departure_date=self.flight['departure_date'],
        )
        self.cabin_params = CabinParams(
            leg_origin=self.brule['facts']['market']['originCityCode'],
            leg_destination=self.brule['facts']['market']['destCityCode'],
            cabin_code='Y',
        )

    def test_auth_results_inserted_to_db(self):
        self.db['authorization_results'].delete_many(filter={})
        AuthorizationCalculator(self.flight_params, False).generate_authorizations()
        count = self.db['authorization_results'].count_documents(filter={})
        self.assertEqual(count, 1) 

    def test_upsell_auth_value(self): 
        auth = self.db['authorization_results'].find_one()
        auth_value = auth['authorization_value']['authorization']
        target_auth_value = self.flight_total_bookings + self.brule_set_avail 
        self.assertEqual(auth_value, target_auth_value)

    def test_upsell_auth_class(self):
        auth = self.db['authorization_results'].find_one()
        auth_class = auth['authorization_value']['class_code']
        self.assertEqual(auth_class, 'X')

    def test_auth_cabin_params(self):
        params = self.db['authorization_results'].find_one()['cabin_params']
        cabin_params = CabinParams(
            leg_origin=params['leg_origin'],
            leg_destination=params['leg_destination'],
            cabin_code=params['cabin_code'],
        )
        self.assertEqual(cabin_params, self.cabin_params)
        flight_params = FlightParams(
            airline_code=params['airline_code'],
            origin=params['origin'],
            destination=params['destination'],
            departure_date=params['departure_date'],
        )
        self.assertEqual(flight_params, self.flight_params)
        

if __name__ == '__main__':
    unittest.main()