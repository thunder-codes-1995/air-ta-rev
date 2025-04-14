from rules.core.utils import parse


def test_parse_object():
    my_dict = {
        "key1": "value1",
        "key2": {
            "nested_key2a": "nested_value2a",
            "nested_key2b": {
                "nested_key2b1": "nested_value2b1",
                "nested_key2b2": "nested_value2b2",
                "nested_key2b3": "nested_value2b3",
            },
            "nested_key2c": "nested_value2c",
        },
        "key3": "value3",
    }

    assert parse(my_dict, "key2.nested_key2b.nested_key2b1") == "nested_value2b1"
    assert parse(my_dict, "not_found") is None
