def get_operators():
    return [
        {
            "label": "=",
            "value": "equal",
            "readable": "Equal",
        },
        {
            "label": "!=",
            "value": "notEqual",
            "readable": "Not Equal",
        },
        {
            "label": ">=",
            "value": "greaterThanInclusive",
            "readable": "Greater or Equal",
        },
        {
            "label": ">",
            "value": "greaterThan",
            "readable": "Greater Then",
        },
        {
            "label": "<=",
            "value": "lessThanInclusive",
            "readable": "Less or Equal",
        },
        {
            "label": "<",
            "value": "lessThan",
            "readable": "Less Then",
        },
        {
            "label": "< >",
            "value": "between",
            "readable": "Between",
        },
    ]
