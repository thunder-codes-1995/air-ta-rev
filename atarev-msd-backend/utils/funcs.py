import datetime
import math
import traceback
from datetime import date
from typing import Dict, List, Tuple, Union

import pandas as pd
from _plotly_utils.colors import n_colors
from flask import request
from sklearn.preprocessing import KBinsDiscretizer

from airports.repository import AirportRepository
from filters.repository import FilterRepository

filter_repository = FilterRepository()
airport_repository = AirportRepository()


def create_error_response(error_code, error_message, status_code=500, additional_details=None):
    """creates JSON with error message that should be returned to REST client, in case of issue"""
    return {
        "code": error_code,
        "message": error_message,
        "details": additional_details if additional_details is not None else [],
    }, 400


def get_date_object(dt: str) -> date:
    year, month, day = dt.split("-")
    return date(year=int(year), month=int(month), day=int(day))


def get_date_as_int(dt: str) -> int:
    year, month, day = dt.split("-")
    return int(f"{year}{month}{day}")


def get_date_as_string(dt: int) -> str:
    string = f"{dt}"
    return f"{string[0:4]}-{string[4:6]}-{string[6:]}"


def from_int_to_datetime(value) -> date:
    """20220510 to datetime"""
    if "/" not in str(value):
        value = str(value)
        return datetime.date(year=int(value[:4]), month=int(value[4:6]), day=int(value[6:]))
    else:
        return datetime.date(
            year=int(value.split("/")[2]),
            month=int(value.split("/")[0]),
            day=int(value.split("/")[1]),
        )


def format_time_duration(val: int) -> str:
    """
    takes a normalized duration value (1650) and converted it to
    human readable foramt (16:50)
    """
    val = f"{val}"
    if not val.isnumeric():
        return val
    val = val.zfill(4)
    return f"{val[0:2]}:{val[2:]}"


def get_period_from_int_range(start: int, end: int) -> int:
    """get range (days number) between 2 dates (integer represented dates)"""
    start_date = from_int_to_datetime(start)
    end_date = from_int_to_datetime(end)
    return (end_date - start_date).days


def get_is_dark_theme():
    return request.args.get("dark_theme") == "true"


def get_carriers_colors(
    list_of_carriers,
    default_color="#FFFFFF",
    is_gradient=False,
    return_list=False,
):
    """get colors for a list of carriers based on their order (order matters)"""

    theme = "dark" if get_is_dark_theme() else "light"

    mid_colors = (
        "#295CFF",
        "#943BFF",
        "#02ECE6",
        "#EF4351",
        "#FF9416",
        "#FFA9E9",
        "#5504D9",
        "#259eff",
    )
    dark_colors = (
        "#100048",
        "#230047",
        "#004A49",
        "#2F0E29",
        "#FF535C",
        "#630A3A",
        "#190c32",
        "#131c34",
    )
    light_colors = (
        "#7696FF",
        "#B78EE5",
        "#8EEBE8",
        "#F3969C",
        "#FFD09A",
        "#FFE1F8",
        "#9D74DA",
        "#ABDBFF",
    )

    gradient_colors = []
    if is_gradient:
        if theme == "light":
            for light_color, dark_color in zip(mid_colors, light_colors):
                gradient_colors.append(
                    (light_color, dark_color),
                )
        elif theme == "dark":
            for light_color, dark_color in zip(mid_colors, dark_colors):
                gradient_colors.append(
                    (light_color, dark_color),
                )

    if is_gradient:
        default_color = ("#FFFFFF", "#343434")

    _map = {}
    _colors = gradient_colors if is_gradient else mid_colors
    # set will not preserve order this is why i use dict.fromkeys(list_of_carriers).keys()
    unique_carriers = list(dict.fromkeys(list_of_carriers).keys())
    for idx, carrier in enumerate(unique_carriers):
        if idx >= len(_colors):
            _map[carrier] = default_color
            continue
        _map[carrier] = _colors[idx]

    if return_list:
        _map = list(_map.values())

    return _map


# def get_carriers_colors(
#     list_of_carriers,
#     default_color="#FFFFFF",
#     is_gradient=False,
#     return_list=False,
#     theme="light+very_dark",
# ):
#     """get colors for a list of carriers based on their order (order matters)"""

#     light_colors = (
#         "#295CFF",
#         "#943BFF",
#         "#02ECE6",
#         "#EF4351",
#         "#FF9416",
#         "#FFA9E9",
#         "#5504D9",
#         "#259eff",
#     )
#     very_dark_colors = (
#         "#100048",
#         "#230047",
#         "#004A49",
#         "#2F0E29",
#         "#FF535C",
#         "#630A3A",
#         "#190c32",
#         "#131c34",
#     )
#     dark_colors = (
#         "#14267B",
#         "#480872",
#         "#0A4D4D",
#         "#682234",
#         "#FF535C",
#         "#603A5D",
#         "#2c0a6a",
#         "#194278",
#     )

#     gradient_colors = []
#     if is_gradient:
#         if theme == "light+very_dark":
#             for light_color, dark_color in zip(light_colors, very_dark_colors):
#                 gradient_colors.append(
#                     (light_color, dark_color),
#                 )
#         elif theme == "light+dark":
#             for light_color, dark_color in zip(light_colors, dark_colors):
#                 gradient_colors.append(
#                     (light_color, dark_color),
#                 )

#     if is_gradient:
#         default_color = ("#FFFFFF", "#343434")

#     _map = {}
#     _colors = gradient_colors if is_gradient else light_colors
#     # set will not preserve order this is why i use dict.fromkeys(list_of_carriers).keys()
#     unique_carriers = list(dict.fromkeys(list_of_carriers).keys())
#     for idx, carrier in enumerate(unique_carriers):
#         if idx >= len(_colors):
#             _map[carrier] = default_color
#             continue
#         _map[carrier] = _colors[idx]

#     if return_list:
#         _map = list(_map.values())

#     return _map


def get_customer_seg_graph_colors(idx):
    colors = [  # waleed- the reason they are called by index is the labels of the graphs are changing alot
        ["#144aff", "#133dd2", "#1431a7", "#14267b", "#141a50", "#15143c"],
        ["#EF4351", "#d83652", "#c22b52", "#ad2053", "#880d54", "#7a0c4b"],
        ["#00ad92", "#038c7b", "#076d65", "#0a4d4d", "#0e2e36", "#101d2b"],
        ["#da83c0", "#d777b9", "#cb59a5", "#c64e9b", "#b73b8a", "#882c67"],
        ["#9c00ed", "#8002c4", "#65059b", "#480872", "#2f0b4a", "#220c37"],
        ["#ff9416", "#ff8231", "#ff6f4f", "#ff5e6b", "#ff4e84", "#ff4493"],
        ["#5504d9", "#4705b4", "#3a078f", "#2c0a6a", "#1f0c44", "#190c32"],
        ["#2fd9fb", "#1daee9", "#108ddb", "#0168cc", "#00479f", "#003282"],
    ]

    return colors[idx]


def add_commas(x):
    return f"{x:,d}"


days_of_week = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


def convert_currency(amount: Union[int, float], from_currency: str, to_currency: str, currency_map={}) -> Union[int, float]:
    if from_currency == to_currency:
        return amount
    ratio = currency_map.get(to_currency, 1) / currency_map.get(from_currency, 1)
    return amount * ratio


def get_airports_market(origs: List[str], dests: List[str], host: str):
    """
    takes in a list of airport origins and destinations and returns
    market data based on selected airports cities
    """
    origins = list(filter(lambda val: val.lower() != "all", origs))
    destinations = list(filter(lambda val: val.lower() != "all", dests))
    orig_city = airport_repository.get_city_code_for_airport(origins)
    dest_city = airport_repository.get_city_code_for_airport(destinations)

    # get market data based on orig_city and dest_city
    market = filter_repository.get_market(orig_city["city_code"], dest_city["city_code"], host)
    assert bool(market), "selected market is not available"
    return market


def get_market_carrier_map(
    origs: List[str],
    dests: List[str],
    host: str,
    default_color="#ffffff",
    is_gradient=False,
    return_list=False,
    theme="light+very_dark",
):
    """
    takes list of airport origins and distinations and get carrier-color map
    based on selected airports market configuration
    """
    market = get_airports_market(origs, dests, host)
    all_carriers = [host, *market["competitors"]]
    return get_carriers_colors(all_carriers, default_color, is_gradient, return_list)


def split_string(string: str, separator=",", allow_empty=True):
    arr = list(set(string.strip().split(separator)))
    if allow_empty:
        return arr
    return list(filter(lambda i: bool(i), arr))


# not used anywhere
def gradient_colors_with_three_parameters(colors: list, output_color_number: int):
    if len(colors) != 3:
        raise ValueError("gradient_colors_with_three_parameters takes a list of 3 colors in rgb")
    if output_color_number > 3:
        colors1 = n_colors(colors[0], colors[1], math.ceil(output_color_number / 2), colortype="rgb")
        colors2 = n_colors(colors[1], colors[2], int(output_color_number / 2) + 1, colortype="rgb")
        return colors1[:-1] + colors2
    elif output_color_number == 3:
        return colors
    elif output_color_number == 2:
        return [colors[0], colors[1]]

    elif output_color_number == 1:
        return [colors[0]]
    else:
        raise ValueError(f"output_color_number cannot be: {output_color_number}")


def get_dist_channel_seg_class_story(
    input_df: pd.DataFrame,
    cols: List[Tuple[str]],
    group_cols: List[str],
    input_col_name: str,
    to_field_name: Dict[str, str],
    ret: List,
) -> None:
    input_df = input_df.sort_values(by=group_cols, ascending=True)
    curr_year = input_df.travel_year.iloc[0]
    overall_df = (
        input_df.groupby(input_col_name)
        .agg(dict(cols))
        .reset_index()
        .rename(columns=dict([(c[0], "mkt_{}".format(c[0])) for c in cols]))
    )

    summary_df = input_df.merge(overall_df, on=input_col_name)
    for c in cols:
        summary_df["perc_{}".format(c[0])] = summary_df[c[0]] / summary_df["mkt_{}".format(c[0])]

    curr_major = overall_df.sort_values(by="mkt_pax", ascending=False).iloc[0]
    ret.append(
        "Top {} in {} is {} with {} passengers.".format(
            to_field_name[input_col_name],
            curr_year,
            curr_major[input_col_name],
            curr_major["mkt_pax"],
        )
    )
    curr_al_major = (
        summary_df.query("{} == '{}'".format(input_col_name, curr_major[input_col_name]))
        .sort_values(by="perc_pax", ascending=False)
        .iloc[0]
    )
    ret.append(
        "Carrier {} makes up {}% of this {} with {} passengers.".format(
            curr_al_major.dom_op_al_code,
            int(curr_al_major.perc_pax * 100),
            to_field_name[input_col_name],
            curr_al_major.pax,
        )
    )


def get_travel_day_of_week_summary(
    input_df: pd.DataFrame,
    cols: List[Tuple[str]],
    group_cols: List[str],
    input_col_name: str,
    num2month: Dict[int, str],
    num2day: Dict[int, str],
    ret: List,
) -> None:
    input_df = input_df.sort_values(by=group_cols, ascending=True)
    curr_year = input_df.travel_year.iloc[0]
    curr_month = input_df.travel_month.iloc[0]
    overall_df = (
        input_df.groupby(input_col_name)
        .agg(dict(cols))
        .reset_index()
        .rename(columns=dict([(c[0], "mkt_{}".format(c[0])) for c in cols]))
    )

    summary_df = input_df.merge(overall_df, on=input_col_name)
    for c in cols:
        summary_df["perc_{}".format(c[0])] = summary_df[c[0]] / summary_df["mkt_{}".format(c[0])]

    curr_major = overall_df.sort_values(by="mkt_blended_rev", ascending=False).iloc[0]
    ret.append(
        "Top revenue day-of-week in {} {} is {} with ${}.".format(
            num2month[curr_month],
            curr_year,
            num2day[curr_major["travel_day_of_week"]],
            curr_major["mkt_blended_rev"],
        )
    )
    curr_al_major = (
        summary_df.query("{} == {}".format(input_col_name, curr_major[input_col_name]))
        .sort_values(by="perc_blended_rev", ascending=False)
        .iloc[0]
    )
    ret.append(
        "Carrier {} makes up {}% of this with ${}.".format(
            curr_al_major.dom_op_al_code,
            int(curr_al_major.perc_blended_rev * 100),
            curr_al_major.blended_rev,
        )
    )


def get_is_group_summary(
    input_df: pd.DataFrame, cols: List[Tuple[str]], group_cols: List[str], input_col_name: str, ret: List
) -> None:
    try:
        input_df = input_df.sort_values(by=group_cols, ascending=True)
        curr_year = input_df.travel_year.iloc[0]
        overall_df = (
            input_df.groupby(input_col_name)
            .agg(dict(cols))
            .reset_index()
            .rename(columns=dict([(c[0], "mkt_{}".format(c[0])) for c in cols]))
        )

        overall_df["pax_total"] = [overall_df["mkt_pax"].sum()] * len(overall_df)
        overall_df["perc_pax"] = overall_df["mkt_pax"] / overall_df["pax_total"]

        summary_df = input_df.merge(overall_df, on=input_col_name)
        for c in cols:
            summary_df["perc_{}".format(c[0])] = summary_df[c[0]] / summary_df["mkt_{}".format(c[0])]

        curr_major = overall_df.query("is_group == 1").iloc[0]
        ret.append(
            "Group bookings in {} make up {}% of this market with {} passengers.".format(
                curr_year,
                int(curr_major["perc_pax"] * 100),
                int(curr_major["mkt_pax"]),
            )
        )
        curr_al_major = summary_df.query("is_group == 1").sort_values(by="perc_pax", ascending=False).iloc[0]
        ret.append(
            "Carrier {} makes up {}% of group bookings with {} passengers.".format(
                curr_al_major.dom_op_al_code,
                int(curr_al_major.perc_pax * 100),
                int(curr_al_major.pax),
            )
        )
    except:
        ret.append("N/A")


def get_histogram_summary(input_df: pd.DataFrame, ret: List) -> None:
    hist_df = input_df.reset_index(drop=True)[["blended_fare", "pax"]]
    hist_df = hist_df.reindex(hist_df.index.repeat(hist_df["pax"])).reset_index()

    bins = KBinsDiscretizer(n_bins=20, encode="ordinal", strategy="uniform")
    hist_df["bin_idx"] = bins.fit_transform(hist_df["blended_fare"].values.reshape(-1, 1))
    hist_df = hist_df.groupby("bin_idx").agg({"pax": "count", "blended_fare": ["min", "mean", "max"]}).droplevel(0, axis=1)

    top_bin = hist_df.sort_values(by="count", ascending=False).reset_index(drop=True)
    top_bin["cumulative"] = top_bin["count"].cumsum()
    top_bin["total_pax"] = [top_bin["count"].sum()] * len(top_bin)
    top_bin["cum_perc"] = top_bin["cumulative"] / top_bin["total_pax"]
    top_bin["diff_80"] = top_bin["cum_perc"] - 0.8
    top_bin = top_bin.iloc[0]
    ret.append(
        "Fare range ${}-{} is the most concentrated, making up {}% of all bookings in the selected month.".format(
            int(top_bin["min"]),
            int(top_bin["max"]),
            int(top_bin["cum_perc"] * 100),
        )
    )


def get_country_of_sale_summary(input_df: pd.DataFrame, ret: List) -> None:
    input_df["blended_rev"] = input_df["blended_fare"]
    input_df["blended_fare"] = input_df["blended_rev"] / input_df["pax"]
    input_df["total_pax"] = [input_df["pax"].sum()] * len(input_df)
    input_df["total_rev"] = [input_df["blended_rev"].sum()] * len(input_df)
    input_df["pax_perc"] = input_df["pax"] / input_df["total_pax"]
    input_df["rev_perc"] = input_df["blended_rev"] / input_df["total_rev"]

    host_text = input_df.query("al_type == 'Host'").sort_values(by="pax_perc", ascending=False).iloc[0]
    comp_text = input_df.query("al_type == 'Comp'").sort_values(by="pax_perc", ascending=False).iloc[0]

    ret.append(
        "{} based {} sales make up {}% of total passengers at an average fare of ${} and a revenue of ${}.".format(
            host_text.country_of_sale,
            host_text.al_type,
            int(host_text.pax_perc * 100),
            int(host_text.blended_fare),
            int(host_text.blended_rev),
        )
    )
    ret.append(
        "{} based {} sales make up {}% of total passengers at an average fare of ${} and a revenue of ${}.".format(
            comp_text.country_of_sale,
            comp_text.al_type,
            int(comp_text.pax_perc * 100),
            int(comp_text.blended_fare),
            int(comp_text.blended_rev),
        )
    )


def get_curve_summary(input_df: pd.DataFrame, group_cols: List[str], ret: List) -> None:
    num_days_dict = {}
    for g, g_df in input_df.groupby(["dom_op_al_code"] + group_cols):
        if len(g_df) > 50:
            g_df = g_df.sort_values(by="days_sold_prior_to_travel", ascending=False)
            g_df["pax_cumsum"] = g_df["pax"].cumsum()
            g_df["cumsum_perc"] = g_df["pax_cumsum"] / g_df["pax"].sum()

            if g[0] not in num_days_dict:
                num_days_dict[g[0]] = []
            num_days_dict[g[0]].append(g_df.query("cumsum_perc <= 0.8").iloc[-1].days_sold_prior_to_travel)

    for k, v in num_days_dict.items():
        curr = int(sum(v) / len(v))
        ret.append("Airline {} reaches 80% of total bookings {} days prior to travel date.".format(k, curr))

    if len(ret) == 0:
        ret.append("N/A")


def month_text(arr: List, num2month: Dict[int, str]):
    ret_text = ""
    for i, a in enumerate(arr):
        if i == (len(arr) - 1) and len(arr) > 1:
            ret_text += " and {}".format(num2month[a])
        else:
            if i > 0:
                ret_text += ", {}".format(num2month[a])
            else:
                ret_text += "{}".format(num2month[a])
    return ret_text


def get_summary(
    input_df: pd.DataFrame,
    cols: List[Tuple[str]],
    group_cols: List[str],
    num2month: Dict[int, str],
    to_field_name: Dict[str, str],
    ret: List,
) -> None:
    input_df = input_df.sort_values(by=group_cols, ascending=True)
    overall_df = input_df.groupby(group_cols).agg(dict(cols)).reset_index()

    for c in cols:
        overall_df["mean_{}".format(c[0])] = [overall_df[c[0]].mean()] * len(overall_df)
        overall_df["std_{}".format(c[0])] = [overall_df[c[0]].std()] * len(overall_df)

    dfs = []
    for g, g_df in input_df.groupby("dom_op_al_code"):
        for c in cols:
            g_df["mean_{}".format(c[0])] = [g_df[c[0]].mean()] * len(g_df)
            g_df["std_{}".format(c[0])] = [g_df[c[0]].std()] * len(g_df)
            g_df.rename(
                columns={
                    "mean_{}".format(c[0]): "al_mean_{}".format(c[0]),
                    "std_{}".format(c[0]): "al_std_{}".format(c[0]),
                    c[0]: "al_{}".format(c[0]),
                },
                inplace=True,
            )
        dfs.append(g_df)

    summary_df = pd.concat(dfs, axis=0).merge(overall_df, on=group_cols)

    for c in cols:
        summary_df["al_{}_diff".format(c[0])] = summary_df.apply(
            lambda x: (x["al_{}".format(c[0])] - x["al_mean_{}".format(c[0])]) / x["al_std_{}".format(c[0])],
            axis=1,
        )
        summary_df["al_mkt_{}_diff".format(c[0])] = summary_df.apply(
            lambda x: (x["al_{}".format(c[0])] - x["mean_{}".format(c[0])]) / x["std_{}".format(c[0])],
            axis=1,
        )
        summary_df["mkt_{}_diff".format(c[0])] = summary_df.apply(
            lambda x: (x["{}".format(c[0])] - x["mean_{}".format(c[0])]) / x["std_{}".format(c[0])],
            axis=1,
        )

    diff_cols = ["mkt_{}_diff".format(c[0]) for c in cols]
    overall = summary_df[group_cols + diff_cols].drop_duplicates()

    for c in cols:
        curr_ov = overall.query("mkt_{}_diff > 1 or mkt_{}_diff < -1".format(c[0], c[0]))
        curr_pos = curr_ov.query("mkt_{}_diff > 0".format(c[0]))
        curr_neg = curr_ov.query("mkt_{}_diff < 0".format(c[0]))
        if len(curr_pos) > 0:
            if len(curr_neg) > 0:
                ret.append(
                    "The market has above average {} in {} and below average {} in {}.".format(
                        to_field_name[c[0]].lower(),
                        month_text(curr_pos.travel_month.tolist(), num2month),
                        to_field_name[c[0]].lower(),
                        month_text(curr_neg.travel_month.tolist(), num2month),
                    )
                )
            else:
                ret.append(
                    "The market has above average {} in {}.".format(
                        to_field_name[c[0]].lower(),
                        month_text(curr_pos.travel_month.tolist(), num2month),
                    )
                )
        else:
            ret.append(
                "The market has below average {} in {}.".format(
                    to_field_name[c[0]].lower(),
                    month_text(curr_neg.travel_month.tolist(), num2month),
                )
            )

    for g, g_df in summary_df.groupby("dom_op_al_code"):
        curr_sent = {"increasing": [], "decreasing": []}
        for c in cols:
            tmp = g_df[group_cols + ["al_{}".format(c[0])]]
            tmp["moving_avg"] = tmp["al_{}".format(c[0])].rolling(3).mean()
            tmp["avg_diff"] = tmp["moving_avg"].diff()
            if tmp.avg_diff.iloc[-1] > 0:
                curr_sent["increasing"].append(to_field_name[c[0]])
            else:
                curr_sent["decreasing"].append(to_field_name[c[0]])
            curr_tmp = g_df.query("al_{}_diff > 1 or al_{}_diff < -1".format(c[0], c[0]))[
                group_cols + ["al_{}_diff".format(c[0])]
            ]
        curr_inc = ", ".join(curr_sent["increasing"])
        curr_dec = ", ".join(curr_sent["decreasing"])
        if len(curr_inc) > 0:
            if len(curr_dec) > 0:
                ret.append("Airline {} has an increasing trend in {} and a decreasing trend in {}.".format(g, curr_inc, curr_dec))
            else:
                ret.append("Airline {} has an increasing trend in {}.".format(g, curr_inc))
        else:
            ret.append("Airline {} has a decreasing trend in {}.".format(g, curr_dec))


def table_to_text(
    input_df: pd.DataFrame,
    cols: List[Tuple[str]],
    group_cols: List[str] = ["travel_year", "travel_month"],
    input_col_name="blended_fare",
):
    try:
        ret = []
        to_field_name = {
            "blended_fare": "Average fare",
            "blended_rev": "Revenue",
            "pax": "Passengers",
            "distribution_channel": "Distribution channel",
            "seg_class": "Class",
            "is_group": "Group bookings",
        }
        num2month = {
            1: "January",
            2: "February",
            3: "March",
            4: "April",
            5: "May",
            6: "June",
            7: "July",
            8: "August",
            9: "September",
            10: "October",
            11: "November",
            12: "December",
        }
        num2day = {
            1: "Monday",
            2: "Tuesday",
            3: "Wednesday",
            4: "Thursday",
            5: "Friday",
            6: "Saturday",
            7: "Sunday",
        }

        if input_col_name in ["distribution_channel", "seg_class"]:
            get_dist_channel_seg_class_story(input_df, cols, group_cols, input_col_name, to_field_name, ret)

        elif input_col_name == "travel_day_of_week":
            get_travel_day_of_week_summary(input_df, cols, group_cols, input_col_name, num2month, num2day, ret)

        elif input_col_name == "is_group":
            get_is_group_summary(input_df, cols, group_cols, input_col_name, ret)

        elif input_col_name == "histogram":
            get_histogram_summary(input_df, ret)

        elif input_col_name == "country_of_sale":
            get_country_of_sale_summary(input_df, ret)

        elif input_col_name == "curve":
            get_curve_summary(input_df, group_cols, ret)

        else:
            get_summary(input_df, cols, group_cols, num2month, to_field_name, ret)

        return "\n".join(ret)

    except Exception as ex:
        tb = traceback.format_exc()
        print(tb)
        return tb
