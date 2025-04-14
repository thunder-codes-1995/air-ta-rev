import copy

upsell_brule = {
    "facts": {
    "hostFare": {
        "carrierCode": "PY",
        "fareAmount": 384.18,
        "fareCurrency": "EUR",
        "itinDeptTime": 1020,
        "seatsAvailable": 9,
        "classCode": None,
    },
    "mainCompetitorFare": {
        "carrierCode": "KL",
        "fareAmount": 478.55,
        "fareCurrency": "EUR",
        "itinDeptTime": 900,
        "seatsAvailable": 9,
        "classCode": "R"
    },
    "lowestFare": {
        "carrierCode": "KL",
        "fareAmount": 478.55,
        "fareCurrency": "EUR",
        "itinDeptTime": 900,
        "seatsAvailable": 9,
        "classCode": "R"
    },
    "leg": {
        "carrierCode": "PY",
        "flightNumber": 993,
        "arrivalDate": 20230409,
        "arrivalDay": 9,
        "arrivalMonth": 4,
        "arrivalTime": 1450,
        "arrivalYear": 2023,
        "dayOfWeek": 7,
        "daysToDeparture": 18,
        "deptDate": 20230409,
        "deptDay": 9,
        "deptMonth": 4,
        "deptTime": 1020,
        "deptYear": 2023,
        "destCityCode": "PBM",
        "destCode": "PBM",
        "originCityCode": "AMS",
        "originCode": "AMS"
    },
    "legForecast": {
        "cabinCode": "ECONOMY",
        "bookedLF": 0,
        "expectedLF": 0,
        "averageLF": 0,
        "classes": [
        {
            "classCode": "Y",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "S",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "B",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "M",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "L",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "H",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "K",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "Q",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "E",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "O",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "A",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "T",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "V",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "P",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "X",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "U",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "W",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "G",
            "seatsAvailable": 0,
            "seatsBooked": 0
        },
        {
            "classCode": "D",
            "seatsAvailable": 0,
            "seatsBooked": 0
        }
        ]
    },
    "market": {
        "marketGroup": "",
        "originCityCode": "AMS",
        "destCityCode": "PBM"
    },
    "cabin": {
        "cabinCode": "ECONOMY"
    },
    "fares": {
        "maf": -94,
        "laf": -94
    }
    },
    "action": {
    "type": "UPSELL",
    "params": {
        "class_rank": "CLOSEST_CLASS_1",
        "set_avail": 5,
        "ruleID": "PBM AMS - DUBLICATE - DUBLICATE",
        "priority": None
    }
    },
    "creationDate": {
    "$date": "2023-03-21T21:06:01.482Z"
    },
    "carrier": "PY",
    "flightKey": "PY993|20230409|AMSPBM",
    "created_at": {
    "$date": "2023-03-21T21:06:01.482Z"
    },
    "updated_at": {
    "$date": "2023-03-21T21:06:01.482Z"
    }
}
# downsell_brule = copy.deepcopy(upsell_brule)  
# downsell_brule['action']['type'] = 'DOWNSELL'
flight = {
    "airline_code": "PY",
    "origin": "AMS",
    "destination": "PBM",
    "flight_number": "993",
    "flight_number_ext": "-",
    "flight_departure_date": 20230409,
    "departure_date": 20230409,
    "legs": [
    {
        "origin": "AMS",
        "destination": "PBM",
        "flight_number": "993",
        "flight_number_ext": "-",
        "departure_date": 20230409,
        "arrival_date": 20230409,
        "departure_time": 1020,
        "arrival_time": 1450,
        "cabins": [
        {
            "code": "C",
            "capacity": 18,
            "allotment": 0,
            "total_booking": 6,
            "total_group_booking": 0,
            "available_seats": 10,
            "overbooking_level": 0,
            "classes": [
            {
                "code": "C",
                "seats_available": 16,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "-",
                "authorization": 16
            },
            {
                "code": "F",
                "seats_available": 4,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "J",
                "authorization": 10
            },
            {
                "code": "I",
                "seats_available": -4,
                "total_booking": 4,
                "total_group_booking": 0,
                "parent_booking_class": "Z",
                "authorization": 4
            },
            {
                "code": "J",
                "seats_available": 8,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "C",
                "authorization": 11
            },
            {
                "code": "Z",
                "seats_available": -2,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "F",
                "authorization": 8
            }
            ]
        },
        {
            "code": "Y",
            "capacity": 246,
            "allotment": 0,
            "total_booking": 119,
            "total_group_booking": 0,
            "available_seats": 127,
            "overbooking_level": 0,
            "classes": [
            {
                "code": "A",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "O",
                "authorization": 246
            },
            {
                "code": "B",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "S",
                "authorization": 246
            },
            {
                "code": "D",
                "seats_available": -108,
                "total_booking": 22,
                "total_group_booking": 0,
                "parent_booking_class": "G",
                "authorization": 0
            },
            {
                "code": "E",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "Q",
                "authorization": 246
            },
            {
                "code": "G",
                "seats_available": -108,
                "total_booking": 1,
                "total_group_booking": 0,
                "parent_booking_class": "W",
                "authorization": 0
            },
            {
                "code": "H",
                "seats_available": 15,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "L",
                "authorization": 246
            },
            {
                "code": "K",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "H",
                "authorization": 246
            },
            {
                "code": "L",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "M",
                "authorization": 246
            },
            {
                "code": "M",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "B",
                "authorization": 246
            },
            {
                "code": "N",
                "seats_available": 15,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "V",
                "authorization": 246
            },
            {
                "code": "O",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "E",
                "authorization": 246
            },
            {
                "code": "P",
                "seats_available": 98,
                "total_booking": 2,
                "total_group_booking": 0,
                "parent_booking_class": "R",
                "authorization": 206
            },
            {
                "code": "Q",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "K",
                "authorization": 246
            },
            {
                "code": "R",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "N",
                "authorization": 246
            },
            {
                "code": "S",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "Y",
                "authorization": 246
            },
            {
                "code": "T",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "A",
                "authorization": 246
            },
            {
                "code": "U",
                "seats_available": 18,
                "total_booking": 22,
                "total_group_booking": 0,
                "parent_booking_class": "X",
                "authorization": 126
            },
            {
                "code": "V",
                "seats_available": 138,
                "total_booking": 0,
                "total_group_booking": 0,
                "parent_booking_class": "T",
                "authorization": 246
            },
            {
                "code": "W",
                "seats_available": -108,
                "total_booking": 40,
                "total_group_booking": 0,
                "parent_booking_class": "U",
                "authorization": 0
            },
            {
                "code": "X",
                "seats_available": 58,
                "total_booking": 17,
                "total_group_booking": 0,
                "parent_booking_class": "P",
                "authorization": 166
            },
            {
                "code": "Y",
                "seats_available": 138,
                "total_booking": 4,
                "total_group_booking": 0,
                "parent_booking_class": "-",
                "authorization": 246
            }
            ]
        }
        ]
    }
    ]
}
flight_total_bookings = 108
brule_set_avail = 5

