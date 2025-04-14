from models.schedule import Segment
import unittest


class TestSchedule(unittest.TestCase):
    OBJECT = {
        'airline_code': 'PY',
        'departure_date': 20221112,
        'flight_number_ext': '-',
        'flight_departure_date': 20221112,
        'destination': 'MIA',
        'origin': 'GEO',
        'flight_number': "421",
        'legs': [
            {
                'origin': 'PBM',
                'destination': 'GEO',
                'flight_number': "421",
                'flight_number_ext': '-',
                'arrival_date': 20221112,
                'departure_date': 20221112,
                'departure_time': 715,
                'arrival_time': 2015,
                'cabins': [
                    {
                        'code': 'Y',
                        'capacity': 120,
                        'allotment': 10,
                        'total_booking': 80,
                        'total_group_booking': 0,
                        'available_seats': 5,
                        'overbooking_level': 10,
                        'classes': [
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},

                        ],
                    }
                ],

            },
            {
                'origin': 'PBM',
                'destination': 'GEO',
                'flight_number': "421",
                'flight_number_ext': '-',
                'arrival_date': 20221112,
                'departure_date': 20221112,
                'departure_time': 715,
                'arrival_time': 2015,
                'cabins': [
                    {
                        'code': 'Y',
                        'capacity': 120,
                        'allotment': 10,
                        'total_booking': 80,
                        'total_group_booking': 0,
                        'available_seats': 5,
                        'overbooking_level': 10,
                        'classes': [
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},
                            {'code': 'D', 'seats_available': 150, 'total_booking': 10,
                                'total_group_booking': 0, "parent_booking_class": "A", "authorization": 10},

                        ]
                    }
                ],

            }
        ]
    }

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.schedule = Segment(self.OBJECT)

    def test_filter_legs(self):
        legs1 = self.schedule._legs(origin='AA', destination="BB")
        legs2 = self.schedule._legs(origin='PBM', destination="GEO")
        self.assertEqual(len(legs1), 0)
        self.assertEqual(len(legs2), 2)

    def test_has_legs(self):
        self.assertFalse(self.schedule.has_legs(origin='AA', destination="BB"))
        self.assertTrue(self.schedule.has_legs(
            origin='PBM', destination="GEO"))

    def test_has_cabins(self):
        leg = self.schedule._legs(origin='PBM', destination="GEO")[0]
        self.assertTrue(leg.has_cabins(code="Y"))
        self.assertFalse(leg.has_cabins(code="+"))

    def test_filter_cabins(self):
        leg = self.schedule._legs(origin='PBM', destination="GEO")[0]
        cabins = leg._cabins(code='Y')
        self.assertEqual(len(cabins), 1)

    def test_filter_clases(self):
        leg = self.schedule._legs(origin='PBM', destination="GEO")[0]
        cabin = leg.cabins[0]
        self.assertEqual(len(cabin._classes(seats_available=150)), 5)
        self.assertEqual(len(cabin._classes(code='D')), 5)
