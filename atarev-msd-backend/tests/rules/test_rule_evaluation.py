import pytest

from rules.core.eval import Evaluate, Evaluation


@pytest.fixture
def rule():
    return {
        "conditions": {
            "all": [
                {"value": ["AMS"], "operator": "in", "path": "originCityCode", "fact": "market"},
                {"value": ["PBM"], "operator": "in", "path": "destCityCode", "fact": "market"},
                {"value": "ECONOMY", "operator": "equal", "path": "cabinCode", "fact": "cabin"},
                {"value": "target", "operator": "equal", "path": "level1.level2.level3", "fact": "target"},
                {"value": ["python", "php"], "operator": "in", "path": "programming.languages.oop", "fact": "oop"},
                {
                    "field": "departure_date",
                    "all": [
                        {"value": 20230701, "operator": "greaterThanInclusive", "path": "deptDate", "fact": "leg"},
                        {"value": 20230731, "operator": "lessThanInclusive", "path": "deptDate", "fact": "leg"},
                    ],
                },
            ]
        },
    }


@pytest.fixture
def obj():
    return {
        "originCityCode": "AMS",
        "destCityCode": "PBM",
        "cabinCode": "ECONOMY",
        "deptDate": 20230705,
        "level1": {"level2": {"level3": "target"}},
        "programming": {"languages": {"oop": "python"}},
    }


def test_valid_rule(rule, obj):
    res = Evaluate(rule["conditions"]["all"], obj)()
    assert type(res) is Evaluation
    assert res.reason is None
    assert res.result is True


def test_failed_rule(rule, obj):
    rule["conditions"]["all"].append({"value": ["m", "f"], "operator": "in", "path": "nature.creature.gender", "fact": "fact"})
    obj["nature"] = {"creature": {"gender": "unknown"}}
    res = Evaluate(rule["conditions"]["all"], obj)()
    assert type(res) is Evaluation
    assert res.result is False
    assert res.reason["operator"] == "in"
    assert res.reason["path"] == "nature.creature.gender"
    assert res.reason["value"] == "unknown"
    assert res.reason["accept"] == ["m", "f"]
