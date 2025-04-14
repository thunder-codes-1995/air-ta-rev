import os


def is_demo_mode():
    """Are we running a demo version?"""
    return "True" == os.getenv("DEMO_ENABLED")


class Constants:
    # Error codes
    ERROR_CODE_INVALID_REQUEST = "INVALID_REQUEST"
    ERROR_CODE_NOT_FOUND = "NOT_FOUND"
    ERROR_UNKNOWN_ERROR = "UNKNOWN_ERROR_OCCURRED"

    # Constants used in REST parameters
    AGG_VIEW_YEARLY = "yearly"
    AGG_VIEW_MONTHLY = "monthly"
    AGG_VIEW_DAILY = "daily"
    PAX_TYPE_INDIVIDUAL = "IND"
    MARKET_SHARE_TRENDS = "trends"
    MARKET_SHARE_AVG = "avg"

    CUMULATIVE_VIEW = "cumulative"
    INDIVIDUAL_VIEW = "individual"

    PAX_TYPE_GROUP = "GRP"
    ALL = "All"
    DEFAULT_CONFIG = "DEFAULT"

    CABIN_COLOR_MAP = {
        "First Class": ("rgb(96, 0, 238)", "rgb(148, 75, 255)"),
        "C": ("rgb(96, 0, 238)", "rgb(148, 75, 255)"),
        "Premium Economy Class": ("rgb(173, 22, 90)", "rgb(239, 133, 181)"),
        "Y": ("rgb(35, 144, 141)", "rgb(68, 211, 208)"),
        "Other Class": ("rgb(113, 95, 95)", "rgb(146, 126, 126)"),
        "*": ("rgb(173, 22, 90)", "rgb(239, 133, 181)"),
        "-": ("rgb(173, 22, 90)", "rgb(239, 133, 181)"),
    }

    if is_demo_mode():
        CABIN_COLOR_MAP["XQM"] = ("rgb(96, 0, 238)", "rgb(148, 75, 255)")
        CABIN_COLOR_MAP["QNI"] = ("rgb(96, 0, 238)", "rgb(148, 75, 255)")
        CABIN_COLOR_MAP["GQF"] = ("rgb(173, 22, 90)", "rgb(239, 133, 181)")
        CABIN_COLOR_MAP["SDO"] = ("rgb(35, 144, 141)", "rgb(68, 211, 208)")
        CABIN_COLOR_MAP["NKB"] = ("rgb(113, 95, 95)", "rgb(146, 126, 126)")
        CABIN_COLOR_MAP["RWG"] = ("rgb(173, 22, 90)", "rgb(239, 133, 181)")
        CABIN_COLOR_MAP["-"] = ("rgb(173, 22, 90)", "rgb(239, 133, 181)")

        CABIN_COLOR_MAP = {
            "XQM": ("rgb(96, 0, 238)", "rgb(148, 75, 255)"),
            "QNI": ("rgb(96, 0, 238)", "rgb(148, 75, 255)"),
            "GQF": ("rgb(173, 22, 90)", "rgb(239, 133, 181)"),
            "SDO": ("rgb(35, 144, 141)", "rgb(68, 211, 208)"),
            "NKB": ("rgb(113, 95, 95)", "rgb(146, 126, 126)"),
            "RWG": ("rgb(173, 22, 90)", "rgb(239, 133, 181)"),
            "-": ("rgb(173, 22, 90)", "rgb(239, 133, 181)"),
        }

    IDX2WEEKDAY = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
    IDX2DAY = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}

    AGG_TYPE_MAP = {
        "overall": "month",
        "day-of-week": "day",
        "day-of-week&time": "day_time",
        "day-of-week-time": "day_time",
    }
