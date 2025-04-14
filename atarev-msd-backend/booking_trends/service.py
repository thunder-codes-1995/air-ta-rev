from datetime import date, datetime, timedelta

import pandas as pd
from flask import request

from airports.repository import AirportRepository
from base.middlewares import attach_figure_id, cache
from base.service import BaseService
from base.utils import add_missing_dates
from booking_trends.forms import BookingCountryOptions, BookingCurve, BookingTrends
from dds.repository import DdsRepository
from holidays.repository import HolidayRepository
from utils.funcs import table_to_text

from .builder import FareBookingBuilder
from .figure import BookingTrendsFigure

holiday_repository = HolidayRepository()
airport_repository = AirportRepository()


class BookingTrendsService(BaseService):
    repository_class = DdsRepository
    builder_class = FareBookingBuilder
    figure_class = BookingTrendsFigure

    HOLIDAYS_LABELS = [
        {"holiday_name": "Name"},
        {"holiday_start_date": "Start Date"},
        {"holiday_end_date": "End Date"},
        {"holiday_country": "Country"},
        {"holiday_day": "Day-of-Week"},
        {"holiday_length": "Length"},
    ]

    STORY_TEXTS = {
        "get_booking_trends": {"main_card": {"title": "test", "content": "test"}},
        "get_booking_curve": {"main_card": {"title": "test", "content": "test"}},
    }

    # @cache()
    # @attach_figure_id(["fig"])
    # def get_booking_trends(self, form: BookingTrends):

    #     pipeline = self.builder.booking_trends_pipeline(form)
    #     df = self._aggregte(pipeline)

    #     if not df.empty:
    #         df = add_missing_dates(
    #             df,
    #             form.date_range_start.data,
    #             form.date_range_end.data,
    #             ["pax"],
    #             form.agg_type.data,
    #         )

    #         holidays_df = self.get_holidays(form)
    #         holidays = holidays_df.to_dict("records")
    #         # plot both main and holiday data based on selected countries
    #         fig = self.figure.booking_trend_viz(df, holidays_df, form)

    #         to_text = table_to_text(df, [("pax", "sum")])

    #     response = {
    #         "fig": fig if not df.empty else self.empty_figure,
    #         "holidays_table": {
    #             "data": holidays if not df.empty else [],
    #             "labels": self.HOLIDAYS_LABELS,
    #         },
    #         "story_text": {
    #             "main_card": {
    #                 "content": to_text if not df.empty else "",
    #                 "title": "Graph Text",
    #             }
    #         },
    #         "today_date": date.today().strftime("%d/%m/%Y"),
    #         "summary_metrics": {
    #             "totalMarketSize": {"total_capacity": 2000, "total_size": 1600},
    #             "hostMarketSize": {"total_capacity": 1000, "total_size": 800},
    #             "compMarketSize": {"total_capacity": 1000, "total_size": 800},
    #             "totalMarketTrends": {"total_capacity": 1000, "total_size": 800},
    #         },
    #     }
    #     return response

    def get_holidays(self, form):
        """get holiday data based on countries and ids"""

        # get holiday data based on selected airports' countries
        # and holiday ids
        columns = [
            "holiday_name",
            "holiday_start_date",
            "holiday_end_date",
            "holiday_country",
            "holiday_length",
            "holiday_day",
            "holiday_idx",
            "holiday_year",
            "holiday_month",
            "date_name",
        ]
        pipeline = self.builder.holiday_pipeline(form)
        holidays = self.label_holiday_data(holiday_repository.get_by_filter(pipeline))

        holidays_df = pd.DataFrame(holidays, columns=columns)
        return holidays_df

    def label_holiday_data(self, holidays):
        return [
            {
                "holiday_name": record["holiday_name"],
                "holiday_start_date": str(datetime.strptime(str(record["start_date"]), "%Y%m%d").date()),
                "holiday_end_date": str(datetime.strptime(str(record["start_date"]), "%Y%m%d").date() + timedelta(days=1)),
                "holiday_country": record["country_name"],
                "holiday_length": 1,
                "holiday_day": record["dow"],
                "holiday_idx": record["holiday_idx"],
                "holiday_year": record["holiday_year"],
                "holiday_month": record["holiday_month"],
                # in case of daily agg_view
                "date_name": record.get("date_name", "-"),
            }
            for record in holidays or []
        ]

    @attach_figure_id(["fig"])
    def get_booking_curve(self, form: BookingCurve):
        pipeline = self.builder.booking_curve_pipeline(form)
        df = self._aggregte(pipeline)
        if df.empty:
            return {"fig": {"data": [], "layout": {}}, "yaxis_title": ""}
        df = self.__get_booking_curve_market_share(df)
        fig = self.figure.booking_curve_viz(df)

        curr_agg_type = request.args.get("agg_type")
        if curr_agg_type == "overall":
            group_cols = []
        elif curr_agg_type == "day-of-week":
            group_cols = ["travel_day_of_week"]
        elif curr_agg_type == "day-of-week-time":
            group_cols = ["travel_day_of_week", "local_dep_time"]
        else:
            group_cols = []

        to_text = table_to_text(df, [("pax", "sum")], group_cols=group_cols, input_col_name="curve")

        return {
            "fig": fig,
            "yaxis_title": "Pax % of TTL DTD" if request.args.get("val_type") == "ratio" else "Pax TTL DTD",
            "story_text": {"main_card": {"content": to_text, "title": "Graph Text"}},
        }

    def __get_booking_curve_market_share(self, df: pd.DataFrame) -> pd.DataFrame:
        # get market share for each groupby combination :
        # if agg type == overall : groupby dom_op_al_code
        # if agg type == day-of-week : groupby dom_op_al_code and travel_day_of_week
        # if agg type == day-of-week-time : groupby dom_op_al_code, travel_day_of_week and dep_time_block
        agg_type = request.args.get("agg_type", "overall")
        groupby = ["dom_op_al_code"]
        agg_type == "day-of-week" and groupby.append("travel_day_of_week")
        agg_type == "day-of-week-time" and groupby.extend(["travel_day_of_week", "local_dep_time"])
        result = []

        for _, g_df in df.groupby(groupby):
            g_df["agg_pax"] = g_df.pax.cumsum()
            g_df["pax_ratio"] = round((g_df["agg_pax"] / g_df.pax.sum()) * 100, 1)
            result.extend(g_df.to_dict("records"))

        return pd.DataFrame(result)

    @cache()
    def get_booking_country_holiday_options(self, form: BookingCountryOptions):
        country_map = airport_repository.get_country_airport_map(
            [*form.get_orig_city_airports_list(), *form.get_dest_city_airports_list()]
        )

        countries = [val["country_code"] for _, val in country_map.items()]
        resp = {"options": list(set(countries))}
        return resp
