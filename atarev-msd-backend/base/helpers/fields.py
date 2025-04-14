from typing import Any, Dict


def date_as_string_query(target: str) -> Dict[str, Any]:
    return {
        "$concat": [
            {
                "$substr": [f"${target}", 0, 4],
            },
            "-",
            {
                "$substr": [f"${target}", 4, 2],
            },
            "-",
            {
                "$substr": [f"${target}", 6, 2],
            },
        ]
    }


def weekday_humanized_query(target: str) -> Dict[str, Any]:
    return {
        "$switch": {
            "branches": [
                {"case": {"$eq": [{"$dayOfWeek": {"$dateFromString": {"dateString": f"${target}"}}}, 1]}, "then": "Sun"},
                {"case": {"$eq": [{"$dayOfWeek": {"$dateFromString": {"dateString": f"${target}"}}}, 2]}, "then": "Mon"},
                {"case": {"$eq": [{"$dayOfWeek": {"$dateFromString": {"dateString": f"${target}"}}}, 3]}, "then": "Tue"},
                {"case": {"$eq": [{"$dayOfWeek": {"$dateFromString": {"dateString": f"${target}"}}}, 4]}, "then": "Wed"},
                {"case": {"$eq": [{"$dayOfWeek": {"$dateFromString": {"dateString": f"${target}"}}}, 5]}, "then": "Thu"},
                {"case": {"$eq": [{"$dayOfWeek": {"$dateFromString": {"dateString": f"${target}"}}}, 6]}, "then": "Fri"},
                {"case": {"$eq": [{"$dayOfWeek": {"$dateFromString": {"dateString": f"${target}"}}}, 7]}, "then": "Sat"},
            ],
            "default": "Invalid",
        }
    }


def weekday_from_str_date_query(target: str, humanize: bool) -> Dict[str, Any]:
    if humanize:
        return weekday_humanized_query(target)
    return {"$dayOfWeek": {"$dateFromString": {"dateString": f"${target}"}}}


class Field:

    @classmethod
    def date_as_string(self, field: str, target: str) -> Dict[str, Any]:
        return {field: date_as_string_query(target)}

    @classmethod
    def weekday_from_str_date(self, field: str, target: str, humanize: bool = True) -> Dict[str, Any]:
        return {field: weekday_from_str_date_query(target, humanize)}

    @classmethod
    def weekday_humanized(self, field: str, target: str) -> Dict[str, Any]:
        return {field: weekday_humanized_query(target)}
