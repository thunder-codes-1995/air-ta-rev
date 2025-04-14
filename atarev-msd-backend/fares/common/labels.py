from typing import Dict, List

STATS_LABELS = {
    "carrier": "Carrier",
    "all_gap_max": "Max Gap",
    "all_gap_min": "MIN Gap",
    "pos_gap_max": "+ Max Gap",
    "pos_gap_min": "+ MIN Gap",
    "neg_gap_max": "- Max Gap",
    "neg_gap_min": "- MIN Gap",
    "all_gap_max_weekday": "Max Gap Weekday",
    "all_gap_min_weekday": "+ MIN Gap Weekday",
    "pos_gap_max_weekday": "+ Max Gap Weekday",
    "pos_gap_min_weekday": "+ MIN Gap Weekday",
    "neg_gap_max_weekday": "- Max Gap Weekday",
    "neg_gap_min_weekday": "- MIN Gap Weekday",
    "all_gap_max_date": "Max Gap Date",
    "all_gap_min_date": "MIN Gap Date",
    "pos_gap_max_date": "+ Max Gap Date",
    "pos_gap_min_date": "+ MIN Gap Date",
    "neg_gap_max_date": "- Max Gap Date",
    "neg_gap_min_date": "- MIN Gap Date",
    "all_gap_max_time": "Max Gap Time",
    "all_gap_min_time": "MIN Gap Time",
    "pos_gap_max_time": "+ Max Gap Time",
    "pos_gap_min_time": "+ MIN Gap Time",
    "neg_gap_max_time": "- Max Gap Time",
    "neg_gap_min_time": "- MIN Gap Time",
    "all_avg_gap": "AVG Gap",
    "pos_avg_gap": "+ AVG Gap",
    "neg_avg_gap": "- AVG Gap",
    "all_count": "Count",
    "pos_count": "+ Count",
    "neg_count": "- Count",
    "all_avg": "AVG",
    "pos_avg": "+ AVG",
    "neg_avg": "- AVG",
    "maf": "MAF +/- %",
}

HOST_STATS_LABELS = {
    "carrier": "Carrier",
    "min_fare": "MIN FARE",
    "max_fare": "MAX FARE",
    "avg_fare": "AVG FARE",
    "count": "COUNT",
    "period": "PERIOD",
}


def get_stats_labels() -> List[Dict[str, str]]:
    return [{id: name.upper()} for id, name in STATS_LABELS.items()]


def get_table_labels(labels: Dict[str, str]) -> Dict[str, str]:
    return {k: v for k, v in labels.items() if k != "carrierCode"}


def get_host_stats_labels() -> List[Dict[str, str]]:
    return [{id: name.upper()} for id, name in HOST_STATS_LABELS.items()]
