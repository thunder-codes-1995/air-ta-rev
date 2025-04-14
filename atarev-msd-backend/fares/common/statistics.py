from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, TypedDict, Union

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from base.entities.carrier import Carrier
from base.entities.currency import Currency
from base.helpers.theme import Handler, Theme
from utils.funcs import format_time_duration, get_period_from_int_range


class CarrierT(TypedDict):
    color: str
    value: str


class HostStats(TypedDict):
    min_fare: Union[str, None]
    max_fare: Union[str, None]
    avg_fare: Union[str, None]
    count: Union[int, None]
    period: Union[int, None]
    carrier: Union[CarrierT, None]


@dataclass
class AvailibilityTrendsStatistics:

    data: pd.DataFrame
    origin: List[str]
    destination: List[str]
    host_code: str
    theme: Theme
    date_range: Optional[Tuple[int, int]] = None
    overview_only: bool = False

    def get_correlation_coefficient(self, weekday_grouped_df: pd.DataFrame, competitor: str, weekday: str):
        """get correlation coefficient [-1,1] for fares grouped by weekday using spearman model"""
        # get coeffecient only for mutual days (days exists for both host and competitor)
        comp_df = weekday_grouped_df[weekday_grouped_df.carrierCode == competitor]
        host_df = weekday_grouped_df[weekday_grouped_df.carrierCode != competitor]
        host_days = host_df.formatted_date.unique().tolist()
        comp_days = comp_df.formatted_date.unique().tolist()
        mutual_days = [day for day in host_days if day in comp_days]

        # if comp_df.shape[0] != host_df.shape[0] else comp_df.formatted_date.tolist()
        # i need to sort values by fareAmount to prepare it for next step
        mutual_df = weekday_grouped_df[weekday_grouped_df.formatted_date.isin(mutual_days)].sort_values("fareAmount")

        # remove duplicate carrier,date pair values (taking min fare for each departure date) to handle the case of selecting flight numbers in filters
        # when user selects flight numbers we get multiple min fares for same outbound date (one for each flight number)
        # statistics should aggregate all similar outbound dates again and get result
        mutual_df = mutual_df.drop_duplicates(["carrierCode", "formatted_date"])

        if mutual_df.empty:
            return {
                "coeff": 0,
                "weekday": weekday,
                "carrierCode": competitor,
            }

        x = mutual_df[mutual_df.carrierCode != competitor].fareAmount.tolist()
        y = mutual_df[mutual_df.carrierCode == competitor].fareAmount.tolist()

        rho, _ = spearmanr(x, y)

        return {
            "coeff": round(rho) if rho >= 0.7 else 0,
            "weekday": weekday,
            "carrierCode": competitor,
        }

    def get_overview(self, coefficient_df: pd.DataFrame, diff_df: pd.DataFrame):
        """
        get overview statistics for host and competitor
        get avg fare for all targeted weekdays (coefficient_df is used to get targeted weekdays)
        get max and min percentage value for weekdays
        """

        def is_positive(g_df):
            """check if majority diff values  for a weekday are positive or negative"""

            pos_count = g_df[g_df["diff"] >= 0].shape[0]
            neg_count = g_df[g_df["diff"] < 0].shape[0]
            is_pos = (pos_count - neg_count) >= 0
            targeted_df = g_df[g_df["diff"] > 0] if is_pos else g_df[g_df["diff"] < 0]

            share = (targeted_df.shape[0] / g_df.shape[0]) if g_df.shape[0] else 1

            # if majority of diff values >= 0 consider current weekday to be positive otherwise consider it to be negative
            g_df["is_positive"] = is_pos
            g_df["share"] = share
            return g_df

        res = {"pos": {"days": None, "limit": None, "avg": None}, "neg": {"days": None, "limit": None, "avg": None}}

        if coefficient_df.empty:
            return res

        # only data with accepted correlation will be targeted
        targeted_diff = diff_df[diff_df.weekday.isin(coefficient_df.weekday)]

        if targeted_diff.empty:
            return res

        # categorize weekdays into 2 categories :positive or negative (one weekday can be categorized as positive or negative but not both)
        targeted_diff = targeted_diff.groupby("weekday", as_index=False).apply(is_positive)
        targeted_diff = targeted_diff[targeted_diff["share"] >= 0.6]
        currency = Currency(diff_df.iloc[0].fareCurrency).symbol

        # group values by is_positive (majority for each weekday)
        # and get market share values for each group of days for example :
        # Sun,Tue,Wed fares tends to be greater in general -> 80%
        # Mon fares tends to be lower in general -> 9%
        # we ignore minor values (weekdays with negative value but belong to postitive majority and vice versa)
        majority_positive_df = targeted_diff[targeted_diff.is_positive]
        majority_negative_df = targeted_diff[~targeted_diff.is_positive]
        positive_weekdays = majority_positive_df.weekday.unique().tolist()
        negative_weekdays = majority_negative_df.weekday.unique().tolist()
        pos_df = targeted_diff[(targeted_diff["diff"] > 0) & targeted_diff.weekday.isin(positive_weekdays)]
        neg_df = targeted_diff[(targeted_diff["diff"] < 0) & targeted_diff.weekday.isin(negative_weekdays)]
        pos_share = pos_df.shape[0] / majority_positive_df.shape[0] if majority_positive_df.shape[0] else 0
        neg_share = neg_df.shape[0] / majority_negative_df.shape[0] if majority_negative_df.shape[0] else 0

        if not majority_positive_df.empty:
            res["pos"].update(
                {
                    "days": ", ".join(positive_weekdays),
                    "limit": f"{round(pos_share * 100)}%",
                    "avg": f"{currency} {abs(round(majority_positive_df['diff'].mean())):,}",
                }
            )

        if not majority_negative_df.empty:
            res["neg"].update(
                {
                    "days": ", ".join(negative_weekdays),
                    "limit": f"{round(neg_share * 100)}%",
                    "avg": f"{currency} {abs(round(majority_negative_df['diff'].mean())):,}",
                },
            )
        return res

    def unique_competitors(self, data_df: pd.DataFrame) -> List[str]:
        host_code = self.host_code
        return data_df.carrierCode[data_df.carrierCode != host_code].unique().tolist()

    def get_stats(self):
        res = []
        host_data = self.data[self.data.carrierCode == self.host_code]
        if host_data.empty:
            return res

        carrier_color_map = Carrier(self.host_code).carrier_colors(
            self.origin,
            self.destination,
            self.theme,
        )

        # re-calulate dtd - original dtd is scrape date based (outbound date - scrape date), i need it to be based on today's date in stats -
        if self.date_range is not None:
            self.data["dtd"] = self.data.apply(lambda row: (row["outboundDate"] - datetime.now().date()).days, axis=1)

        # prepare diff dataframe (fare difference for host and competitors)
        diff_data = self.prepare(self.data, "diff", ["formatted_date", "dtd"])
        diff_df = pd.DataFrame(diff_data, columns=["formatted_date", "diff", "carrierCode", "abs_diff", "dtd"])
        diff_df = self.data.merge(diff_df, on=["carrierCode", "formatted_date", "dtd"], how="left")

        correlation = self.prepare(diff_df, "mono_correlation", ["weekday"])
        coefficient_df = pd.DataFrame(correlation, columns=["coeff", "weekday", "carrierCode"])
        coefficient_df = coefficient_df[coefficient_df.coeff != 0]

        if self.host_code not in diff_df.carrierCode.tolist() or len(diff_df.carrierCode.unique()) <= 1:
            return res

        blocks = ["generalInfo", "positiveInfo", "negativeInfo"]
        actions = ["gap", "avg_gap", "range"]
        period = {"period": get_period_from_int_range(self.date_range[0], self.date_range[1]) + 1} if self.date_range else {}

        for carrier in self.unique_competitors(diff_df):
            obj = {
                block: {
                    "carrier": {**self.carrier(carrier, carrier_color_map)},
                    **period,
                }
                for block in blocks
            }

            # i need this one to calculate maff values
            curr_df = diff_df[diff_df.carrierCode.isin([self.host_code, carrier])]
            comp_df = curr_df[(curr_df.carrierCode != self.host_code)].reset_index()
            pos_df = comp_df[(comp_df["diff"] >= 0) & (comp_df["diff"].notna())].reset_index()
            neg_df = comp_df[(comp_df["diff"] < 0) & (comp_df["diff"].notna())].reset_index()

            if not self.overview_only:
                for action in actions:
                    resp = self.av_stats_action(comp_df, pos_df, neg_df, action)
                    maf = self.get_maf(curr_df, carrier)
                    for k, v in resp.items():
                        obj[k].update(v)
                        obj[k].update(maf[k])

            res.append({**obj, "overview": self.get_overview(coefficient_df[coefficient_df.carrierCode == carrier], comp_df)})

        return res

    def gap_hover_text(self, diff_df: pd.DataFrame, data_idx: int, key: str, identifier: str):

        return {
            f"{key}_gap_{identifier}_weekday": diff_df.iloc[data_idx]["weekday"],
            f"{key}_gap_{identifier}_date": diff_df.iloc[data_idx]["formatted_date"],
            f"{key}_gap_{identifier}_time": format_time_duration(diff_df.iloc[data_idx]["time"]),
            f"{key}_gap_{identifier}_lf": f'{diff_df.iloc[data_idx]["lf"]}%',
        }

    def get_gap(self, df: pd.DataFrame, key: str):
        """get max | min gap (difference between competitor and host fare)"""
        data = {
            f"{key}_gap_max": None,
            f"{key}_gap_min": None,
            "hoverText": {"gap_max": {}, "gap_min": {}},
        }

        if df.empty or not df.abs_diff.notna().any():
            return data

        currency = df.iloc[0].currency_symbol
        max_idx = df[df.abs_diff == df.abs_diff.max()].index[0]
        min_idx = df[df.abs_diff == df.abs_diff.min()].index[0]
        data[f"{key}_gap_max"] = Currency.attach_currency(f'{round(df.iloc[max_idx]["abs_diff"]):,}', currency)
        data[f"{key}_gap_min"] = Currency.attach_currency(f'{round(df.iloc[min_idx]["abs_diff"]):,}', currency)
        data["hoverText"]["gap_max"].update(self.gap_hover_text(df, max_idx, key, "max"))
        data["hoverText"]["gap_min"].update(self.gap_hover_text(df, min_idx, key, "min"))
        return data

    def get_avg_gap(self, data: pd.DataFrame, key: str):
        res = {f"{key}_avg_gap": None}

        if data.empty:
            return res

        currency = Currency(data.iloc[0].fareCurrency).symbol
        res[f"{key}_avg_gap"] = Currency.attach_currency(f'{round(data["abs_diff"].sum() / data.shape[0]):,}', currency)
        return res

    def get_range_count(self, data: pd.DataFrame, key: str, all_count: int):
        res = {f"{key}_count": 0, f"{key}_avg": 0}

        if data.empty:
            return res

        res[f"{key}_count"] = data.shape[0]
        res[f"{key}_avg"] = f"{round((data.shape[0] / all_count) * 100)}%"
        return res

    def prepare(self, fares_df: pd.DataFrame, action: str, groupby: List[str]) -> List:
        """
        add requried columns to a dataframe before preforming some actions
        if action is diff -> add diff and abs_diff columns
        if action is mono_correlation -> add correlation coefficient
        """
        res = []
        competitors = self.unique_competitors(fares_df)
        host = fares_df[fares_df.carrierCode == self.host_code]
        comp = fares_df[fares_df.carrierCode != self.host_code]
        days = host.formatted_date.unique().tolist() + comp.formatted_date.unique().tolist()
        days_count = Counter(days)
        # get mutual days for both host and competitor (day count > 1)
        mut_days = [d for d, c in dict(days_count).items() if c > 1]
        func = self.diff if action == "diff" else self.get_correlation_coefficient

        for competitor in competitors:
            # get host and competitor data only if date is available in host data
            curr_df = fares_df[fares_df.carrierCode.isin([competitor, self.host_code])]
            curr_df = curr_df[curr_df.formatted_date.isin(mut_days)]
            for g, g_df in curr_df.groupby(groupby):
                res.append(func(g_df, competitor, g if len(groupby) == 1 else g[0]))

        return res

    def diff(self, date_grouped_df: pd.DataFrame, competitor: str, dt: str):
        """get difference and percentage for host and competitor fares"""

        if date_grouped_df.shape[0] <= 1:
            data = date_grouped_df.iloc[0].to_dict()
            # dtd = (data["outboundDate"] - data["scrapeTime"]).days
            return {"formatted_date": dt, "diff": 0, "carrierCode": competitor, "abs_diff": 0, "dtd": data["dtd"]}

        data = date_grouped_df.iloc[0].to_dict()
        host_carrier = self.host_code
        host_idx = 0 if date_grouped_df.carrierCode.iloc[0] == host_carrier else 1
        comp_idx = 0 if host_idx == 1 else 1
        diff = date_grouped_df.iloc[comp_idx].fareAmount - date_grouped_df.iloc[host_idx].fareAmount
        dtd = date_grouped_df.iloc[host_idx].dtd

        return {"formatted_date": dt, "diff": diff, "carrierCode": competitor, "abs_diff": abs(diff), "dtd": dtd}

    def av_stats_action(
        self,
        comp_diff_df: pd.DataFrame,
        pos_comp_diff_df: pd.DataFrame,
        neg_comp_diff_df: pd.DataFrame,
        action: str,
    ) -> Dict[str, Any]:
        """
        wrap logic for gap | avg_gap and range_count statistics
        this method will return above values for general, positive and negative values
        """

        actions_map: Dict[str, Callable] = {
            "gap": self.get_gap,
            "avg_gap": self.get_avg_gap,
            "range": lambda data_df, key: self.get_range_count(data_df, key, comp_diff_df.shape[0]),
        }

        # this line should raise Error if action is not supported
        action = actions_map[action]
        obj = {"generalInfo": {}, "positiveInfo": {}, "negativeInfo": {}}
        obj["generalInfo"].update(action(comp_diff_df, "all"))
        obj["positiveInfo"].update(action(pos_comp_diff_df, "pos"))
        obj["negativeInfo"].update(action(neg_comp_diff_df, "neg"))
        return obj

    def get_maf(self, diff_df: pd.DataFrame, competitor_code: str) -> dict:
        """
        get maf data for 3 periods : positive, negative and all
        maf = ((host fare avg - competitor fare avg) / host fare avg) * 100
        """
        host_carrier: str = self.host_code

        # get 3 periods data for host
        comp_diff_df = diff_df[diff_df.carrierCode == competitor_code]
        comp_pos_diff_df = diff_df[(diff_df["diff"] > 0)]
        comp_neg_diff_df = diff_df[(diff_df["diff"] < 0)]

        # get 3 periods data for competitor
        host_diff_df = diff_df[diff_df.carrierCode == host_carrier]
        host_pos_diff_df = host_diff_df[host_diff_df.formatted_date.isin(comp_pos_diff_df.formatted_date.unique())]
        host_neg_diff_df = host_diff_df[host_diff_df.formatted_date.isin(comp_neg_diff_df.formatted_date.unique())]

        def maf(host_diff_df: pd.DataFrame, comp_diff_df: pd.DataFrame, add_sign=False):
            host_avg_fare = host_diff_df.fareAmount.mean()
            comp_avg_fare = comp_diff_df.fareAmount.mean()

            if np.isnan(host_avg_fare) or np.isnan(comp_avg_fare):
                return

            res = round(((host_avg_fare - comp_avg_fare) / host_avg_fare) * 100)
            sign = "" if not add_sign else "+" if res > 0 else "-"
            return f"{sign}{abs(res)}%"

        all_maf, pos_maf, neg_maf = (
            maf(host_diff_df, comp_diff_df),
            maf(host_pos_diff_df, comp_pos_diff_df),
            maf(host_neg_diff_df, comp_neg_diff_df),
        )

        return {
            "generalInfo": {"maf": all_maf},
            "positiveInfo": {"maf": pos_maf},
            "negativeInfo": {"maf": neg_maf},
        }

    def carrier(self, carrier_code: str, color_map: dict) -> dict:
        return {"color": color_map.get(carrier_code, "#ffffff"), "value": carrier_code}


def host_stats(data: pd.DataFrame, host_code: str, theme: Theme, date_range: Optional[Tuple[int, int]] = None) -> HostStats:
    host_data = data[data.carrierCode == host_code]

    if host_data.empty:
        return {"max_fare": None, "min_fare": None, "avg_fare": None, "count": None, "period": None, "carrier": None}

    currency = host_data.fareCurrency.unique().tolist()[0]
    currency = Currency(currency).symbol
    mx = f"{host_data.fareAmount.max():,}"
    mn = f"{host_data.fareAmount.min():,}"
    avg = f"{round(host_data.fareAmount.mean()):,}"

    return {
        "max_fare": Currency.attach_currency(mx, currency),
        "min_fare": Currency.attach_currency(mn, currency),
        "avg_fare": Currency.attach_currency(avg, currency),
        "count": int(host_data.shape[0]),
        "carrier": {"color": Handler(theme).host_color(host_code), "value": host_code},
        "period": get_period_from_int_range(date_range[0], date_range[1]) + 1 if date_range else None,
    }
