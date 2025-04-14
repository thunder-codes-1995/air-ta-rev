import pandas as pd
import pytest

from rules.flights.evaluation.fact import BestFare, HostBestFare, MainCompetitorBestFare, fare_fact


@pytest.fixture
def flight():
    return {
        "cabins": [{"code": "ECONOMY", "classes": ["Y"]}],
        "bkd_lf": 0,
        "exp_lf": 0,
        "avg_lf": 0,
        "flt_num": 4032,
        "flt_key": "CY312|20230928|LCAATH",
        "arrival_date": 20230928,
        "departure_date": 20230928,
        "origin": "LCA",
        "destination": "ATH",
        "carrier_code": "CY",
        "str_departure_date": "2023-09-28",
        "str_arrival_date": "2023-09-28",
        "arrival_day": 28,
        "arrival_month": 9,
        "arrival_time": 1445,
        "arrival_year": 2023,
        "departure_time": 1500,
        "departure_year": 2023,
        "departure_day": 28,
        "departure_month": 9,
        "dtd": 2,
        "dow": 4,
        "comp": "A3",
        "market": "LCA-ATH",
    }


@pytest.fixture
def fares():

    fares_data = {
        "carrier_code": ["CY", "CY", "CY", "OA", "OA", "OA", "W6", "GQ", "A3", "A3"],
        "origin": ["LCA"] * 10,
        "destination": ["ATH"] * 10,
        "cabin": ["ECONOMY"] * 10,
        "class": ["S", "T", "O", "F", "R", "Q", "P", "R", "E", "W"],
        "departure_date": [20230928] * 10,
        "departure_time": [1700, 1500, 1200, 1215, 1600, 1630, 1955, 2230, 1630, 1330],
        "arrival_date": [20230928] * 10,
        "arrival_time": [1845, 1445, 1345, 1400, 1730, 1800, 2150, 2400, 1800, 1500],
        "flt_num": [4024, 4032, 310, 4026, 4028, 4030, 6913, 6909, 6905, 6901],
        "fare": [198.94, 188.94, 111.94, 208.94, 95.65, 90.92, 91.94, 76.94, 118.94, 132.94],
        "currency": ["EUR"] * 10,
        "is_connecting": [False] * 10,
        "op_code": ["A3", "A3", "CY", "A3", "A3", "A3", "A3", "A3", "A3", "A3"],
        "mk_code": ["CY", "CY", "CY", "CY", "CY", "CY", "OA", "OA", "OA", "OA"],
        "lf": [10, 20, 30, 40, 50, 60, 70, None, None, 100],
    }

    return pd.DataFrame(fares_data)


def test_host_best_fare(flight, fares):
    host_fare = HostBestFare(flight=flight, fares=fares).get()
    data = host_fare.iloc[0].to_dict()

    assert type(host_fare) is pd.DataFrame
    assert host_fare.shape[0] == 1
    assert data["fare"] == 188.94
    assert data["arrival_date"] == 20230928
    assert data["arrival_time"] == 1445
    assert data["flt_num"] == 4032
    assert data["departure_time"] == 1500
    assert data["carrier_code"] == "CY"
    assert data["lf"] == 20


def test_main_comp_best_fare(flight, fares):
    comp_fare = MainCompetitorBestFare(
        flight=flight,
        fares=fares,
        host_dept_datetime="2023-09-28 15:00",
        carrier_code="A3",
        time_difference=(-2, 2),
    ).get()

    data = comp_fare.iloc[0].to_dict()

    assert type(comp_fare) is pd.DataFrame
    assert comp_fare.shape[0] == 1
    assert data["fare"] == 118.94
    assert data["arrival_date"] == 20230928
    assert data["arrival_time"] == 1800
    assert data["flt_num"] == 6905
    assert data["departure_time"] == 1630
    assert data["carrier_code"] == "A3"


def test_best_fare(flight, fares):
    best_fare = BestFare(flight=flight, fares=fares).get()
    data = best_fare.iloc[0].to_dict()

    assert type(best_fare) is pd.DataFrame
    assert best_fare.shape[0] == 1
    assert data["fare"] == 76.94
    assert data["arrival_date"] == 20230928
    assert data["arrival_time"] == 2400
    assert data["flt_num"] == 6909
    assert data["departure_time"] == 2230
    assert data["carrier_code"] == "GQ"


def test_fare_fact(flight, fares):
    fact = fare_fact(HostBestFare(flight=flight, fares=fares).get(), True)
    assert type(fact) is dict
    assert len(fact) == 8
    assert "carrierCode" in fact
    assert "fareAmount" in fact
    assert "fareCurrency" in fact
    assert "classCode" in fact
    assert "cabin" in fact
    assert "deptDate" in fact
    assert "deptTime" in fact
    assert "lf" in fact
