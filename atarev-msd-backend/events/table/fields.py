from enum import Enum
from typing import List

from events.common.fields import Field, Group, cabin, event_name, event_sub_type, event_type, loc


class PeriodSuffix(Enum):
    PAST = "_py"
    FUTURE = "_ny"


class CarrierSuffix(Enum):
    HOST = "_host"
    COMP = "_comp"


class PERIOD(Enum):
    FUTURE = 1
    PAST = -1


rt_market = Field(label="BI-DIRECTIONAL", value="market")
on_market = Field(label="DIRECTIONAL", value="segment")
country = Field(label="COUNTRY", value="country_code")
is_loc_fixed = Field(label="SAME LOC", value="is_loc_fixed")


start_date_this_year = Field(label="EVT ST DT TY", value=f"start_date{PeriodSuffix.FUTURE.value}")
end_date_this_year = Field(label="EVT END DT TY", value=f"end_date{PeriodSuffix.FUTURE.value}")
start_date_prev_year = Field(label="EVT ST DT LY", value=f"start_date{PeriodSuffix.PAST.value}")
end_date_prev_year = Field(label="EVT END DT LY", value=f"end_date{PeriodSuffix.PAST.value}")
start_dow_this_year = Field(label="DOW ST TY", value=f"start_dow{PeriodSuffix.FUTURE.value}")
end_dow_this_year = Field(label="DOW END TY", value=f"end_dow{PeriodSuffix.FUTURE.value}")
start_dow_prev_year = Field(label="DOW ST LY", value=f"start_dow{PeriodSuffix.PAST.value}")
end_dow_prev_year = Field(label="DOW END LY", value=f"end_dow{PeriodSuffix.PAST.value}")


is_date_fixed = Field(label="SAME DTE", value="is_date_fixed")
dept_date = Field(label="FLT DATE", value="dept_date")
impact = Field(label="IMPACT", value="", enabled=False)
impact_dmd = Field(label="IMP DEM", value="", enabled=False)
num_impact_emp_days_ty = Field(label="# IMP DS TY", value="", enabled=False)
num_impact_emp_days_ly = Field(label="# IMP DS LY", value="", enabled=False)
num_days_inc_dmd = Field(label="#DAYS INC DMD", value="", enabled=False)
booked_date = Field(label="BOOKED DATE", value="", enabled=False)


host_booking_this_year = Field(label="BKGS TY", value=f"host_bkng{PeriodSuffix.FUTURE.value}", enabled=False)
host_booking_prev_year = Field(label="BKGS LY", value=f"host_bkng{PeriodSuffix.PAST.value}", enabled=False)

comp_booking_this_year = Field(label="CO BKGS TY", value="", enabled=False)
comp_booking_prev_year = Field(label="CO BKGS LY", value="", enabled=False)

ttl_mkt_bookings_this_year = Field(label="TTL MKT BKGS TY", value="", enabled=False)
ttl_mkt_bookings_prev_year = Field(label="TTL MKT BKGS LY", value="", enabled=False)

host_lf_this_year = Field(label="HOST LF TY", value=f"lf{PeriodSuffix.FUTURE.value}")
host_lf_prev_year = Field(label="HOST LF LY", value=f"lf{PeriodSuffix.PAST.value}")
var_lf_this_year_vs_prev_year = Field(
    label="VAR LF TY/LY", value=f"var_lf{PeriodSuffix.FUTURE.value}{PeriodSuffix.PAST.value}", enabled=False
)


host_cap_this_year = Field(label="CAP TY", value=f"cap{PeriodSuffix.FUTURE.value}")
host_cap_prev_year = Field(label="CAP LY", value=f"cap{PeriodSuffix.PAST.value}")
var_host_cap_this_year_vs_prev_year = Field(label="VAR CAP TY/LY", value="", enabled=False)
mkt_cap_this_year = Field(label="MARKET CAP TY", value="", enabled=False)
mkt_cap_prev_year = Field(label="MARKET CAP LY", value="", enabled=False)
var_market_cap_this_year_vs_prev_year = Field(label="VARIANCE MARKET CAP TY VS LY", value="", enabled=False)


host_7_days_pkup_this_year = Field(label="HO 7 DS PU TY", value="", enabled=False)
host_7_days_pkup_prev_year = Field(label="HO 7 DS PU LY", value="", enabled=False)
mkt_7_days_pkup_comp_this_year = Field(label="MKT 7 DS PU TY", value="", enabled=False)
mkt_7_days_pkup_comp_prev_year = Field(label="MKT 7 DS PU LY", value="", enabled=False)
var_host_7_days_pkup_this_year_vs_prev_year = Field(label="VAR HO 7 DS PU TY/LY", value="", enabled=False)
var_mkt_7_days_pkup_this_year_vs_prev_year = Field(label="VAR MKT 7 DS PU TY/LY", value="", enabled=False)


var_host_booking_this_year_vs_prev_year = Field(
    label="VAR HO BKGS TY/LY",
    value="",
    enabled=False,
)


var_comp_booking_this_year_vs_prev_year = Field(
    label="VAR CO BKGS TY/LY",
    value="",
    enabled=False,
)


var_ttl_mkt_this_year_vs_prev_year = Field(
    label="VAR MKT BKGS TY/LY",
    value="",
    enabled=False,
)


host_mkt_share_this_year = Field(label="HO MKT SHR TY", value="", enabled=False)
host_mkt_share_prev_year = Field(label="HO MKT SHR LY", value="", enabled=False)
var_mkt_7_days_pkup_this_year_vs_prev_year = Field(label="VARIANCE MARKET 7 DAYS PICKUP TY VS LY", value="", enabled=False)
var_comp_mkt_share_this_year_vs_prev_year = Field(label="VAR CO MKT SHR TY/LY", value="", enabled=False)
var_host_mkt_share_this_year_vs_prev_year = Field(label="VAR HO MKT SHR TY/LY", value="", enabled=False)
comp_mkt_share_this_year = Field(label="CO MARKET SHARE TY", value="", enabled=False)
comp_mkt_share_prev_year = Field(label="CO MARKET SHARE LY", value="", enabled=False)


host_avg_fare_this_year = Field(
    label="HO AVG FR TY", value=f"avg_maf{CarrierSuffix.HOST.value}{PeriodSuffix.FUTURE.value}", enabled=False
)
comp_avg_fare_this_year = Field(
    label="CO AVG FR TY", value=f"avg_maf{CarrierSuffix.COMP.value}{PeriodSuffix.FUTURE.value}", enabled=False
)
host_avg_fare_prev_year = Field(
    label="HO AVG FR LY", value=f"avg_maf{CarrierSuffix.HOST.value}{PeriodSuffix.PAST.value}", enabled=False
)
comp_avg_fare_prev_year = Field(
    label="CO AVG FR LY", value=f"avg_maf{CarrierSuffix.COMP.value}{PeriodSuffix.PAST.value}", enabled=False
)
host_maf_this_year = Field(label="HOST MAF TY", value=f"maf{CarrierSuffix.HOST.value}{PeriodSuffix.FUTURE.value}")
comp_maf_this_year = Field(label="COMP MAF TY", value=f"maf{CarrierSuffix.COMP.value}{PeriodSuffix.FUTURE.value}")
host_maf_prev_year = Field(label="HOST MAF LY", value=f"maf{CarrierSuffix.HOST.value}{PeriodSuffix.PAST.value}")
comp_maf_prev_year = Field(label="COMP MAF LY", value=f"maf{CarrierSuffix.COMP.value}{PeriodSuffix.PAST.value}")
var_host_avg_fare_this_year_vs_last_year = Field(label="VAR HO AVF TY/LY", value=f"", enabled=False)
var_comp_avg_fare_this_year_vs_last_year = Field(label="VAR CO AVF TY/LY", value=f"", enabled=False)


host_rev_this_year = Field(label="HO REV TY", value=f"", enabled=False)
host_rev_prev_year = Field(label="HO REV LY", value=f"", enabled=False)
comp_rev_this_year = Field(label="CO REV TY", value=f"", enabled=False)
comp_rev_prev_year = Field(label="CO REV LY", value=f"", enabled=False)
var_host_rev_this_year_vs_prev_year = Field(label="VAR HO REV TY/LY", value=f"", enabled=False)
var_comp_rev_this_year_vs_prev_year = Field(label="VAR CO REV TY/LY", value=f"", enabled=False)


flt_num = Field(label="FLT NR", value=f"flt_num")
level = Field(label="LEVEL", value=f"level")


loc_this_year = Field(label="", value=f"city{PeriodSuffix.FUTURE.value}")
loc_prev_year = Field(label="", value=f"city{PeriodSuffix.PAST.value}")
on_market_this_year = Field(label="", value=f"segment{PeriodSuffix.FUTURE.value}")
on_market_prev_year = Field(label="", value=f"segment{PeriodSuffix.PAST.value}")
" ---------------------------------- groups -------------------------------------"
location_group = Group(
    label="LOCATION",
    value="loc",
    fields=[rt_market, on_market, loc, country, is_loc_fixed],
)

date_group = Group(
    label="DATE",
    value="date",
    fields=[
        start_date_this_year,
        end_date_this_year,
        start_date_prev_year,
        end_date_prev_year,
        start_dow_this_year,
        end_dow_this_year,
        start_dow_prev_year,
        end_dow_prev_year,
        is_date_fixed,
        dept_date,
        impact,
        impact_dmd,
        num_impact_emp_days_ty,
        num_impact_emp_days_ly,
        num_days_inc_dmd,
        booked_date,
    ],
)


event_type_group = Group(
    label="EVENT TYPE",
    value="ev_type",
    fields=[event_name, event_type, event_sub_type],
)

booking_group = Group(
    label="BOOKINGS",
    value="bkng",
    enabled=False,
    fields=[
        host_booking_this_year,
        host_booking_prev_year,
        comp_booking_this_year,
        comp_booking_prev_year,
        ttl_mkt_bookings_this_year,
        ttl_mkt_bookings_prev_year,
        var_host_booking_this_year_vs_prev_year,
        var_comp_booking_this_year_vs_prev_year,
        var_ttl_mkt_this_year_vs_prev_year,
    ],
)

lf_group = Group(
    label="LOAD FACTOR",
    value="load_factor",
    fields=[
        host_lf_this_year,
        host_lf_prev_year,
        var_lf_this_year_vs_prev_year,
    ],
)


cap_group = Group(
    label="CAPACITY",
    value="capacity",
    fields=[
        host_cap_this_year,
        host_cap_prev_year,
        var_host_cap_this_year_vs_prev_year,
        mkt_cap_this_year,
        mkt_cap_prev_year,
        var_market_cap_this_year_vs_prev_year,
    ],
)

pickup_group = Group(
    label="PICKUP",
    value="pkup",
    enabled=False,
    fields=[
        host_7_days_pkup_this_year,
        host_7_days_pkup_prev_year,
        mkt_7_days_pkup_comp_this_year,
        mkt_7_days_pkup_comp_prev_year,
        var_host_7_days_pkup_this_year_vs_prev_year,
        var_mkt_7_days_pkup_this_year_vs_prev_year,
    ],
)


market_share_group = Group(
    label="MARKET SHARE",
    value="mkt_share",
    enabled=False,
    fields=[
        host_mkt_share_this_year,
        host_mkt_share_prev_year,
        var_mkt_7_days_pkup_this_year_vs_prev_year,
        var_comp_mkt_share_this_year_vs_prev_year,
        comp_mkt_share_this_year,
        comp_mkt_share_prev_year,
    ],
)


fare_group = Group(
    label="AVERAGE FARE",
    value="fare",
    fields=[
        host_avg_fare_this_year,
        comp_avg_fare_this_year,
        host_avg_fare_prev_year,
        comp_avg_fare_prev_year,
        host_maf_this_year,
        comp_maf_this_year,
        host_maf_prev_year,
        comp_maf_prev_year,
        var_host_avg_fare_this_year_vs_last_year,
        var_comp_avg_fare_this_year_vs_last_year,
    ],
)


revenue_group = Group(
    label="REVENUE",
    value="rev",
    enabled=False,
    fields=[
        host_rev_this_year,
        host_rev_prev_year,
        comp_rev_this_year,
        comp_rev_prev_year,
        var_host_rev_this_year_vs_prev_year,
        var_comp_rev_this_year_vs_prev_year,
    ],
)


flt_num_group = Group(
    label="FLIGHT NUMBER",
    value="flight_number",
    fields=[flt_num],
)

cabin_group = Group(
    label="CABIN",
    value="cabin",
    fields=[cabin],
)


rename_group = Group(
    value="rename_fields_group",
    fields=[
        Field(value=f"market{PeriodSuffix.FUTURE.value}", rename=f"market"),
        Field(value=f"flt_num{PeriodSuffix.FUTURE.value}", rename=f"flt_num"),
        Field(value=f"dept_date{PeriodSuffix.FUTURE.value}", rename=f"dept_date"),
        Field(value=f"formatted_start_date{PeriodSuffix.PAST.value}", rename=f"start_date{PeriodSuffix.PAST.value}"),
        Field(value=f"city{PeriodSuffix.FUTURE.value}", rename=f"city"),
        Field(value=f"formatted_end_date{PeriodSuffix.FUTURE.value}", rename=f"end_date{PeriodSuffix.FUTURE.value}"),
        Field(value=f"formatted_end_date{PeriodSuffix.PAST.value}", rename=f"end_date{PeriodSuffix.PAST.value}"),
        Field(value=f"formatted_start_date{PeriodSuffix.FUTURE.value}", rename=f"start_date{PeriodSuffix.FUTURE.value}"),
    ],
)


level1_group = Group(
    value="",
    fields=[
        loc,
        # loc_prev_year,
        is_loc_fixed,
        start_date_this_year,
        end_date_this_year,
        start_date_prev_year,
        end_date_prev_year,
        start_dow_this_year,
        end_dow_this_year,
        start_dow_prev_year,
        end_dow_prev_year,
        is_date_fixed,
        event_name,
        event_type,
        event_sub_type,
        host_lf_this_year,
        host_lf_prev_year,
        host_cap_this_year,
        host_cap_prev_year,
        rt_market,
        level,
    ],
)

level2_group = Group(
    value="",
    fields=[host_lf_this_year, host_lf_prev_year, host_cap_this_year, host_cap_prev_year, level, on_market, cabin],
)


level3_group = Group(
    value="",
    fields=[
        host_lf_this_year,
        host_lf_prev_year,
        host_cap_this_year,
        host_cap_prev_year,
        level,
        on_market,
        flt_num,
        host_maf_this_year,
        host_maf_prev_year,
        comp_maf_this_year,
        comp_maf_prev_year,
    ],
)

level4_group = Group(
    value="",
    fields=[
        host_lf_this_year,
        host_lf_prev_year,
        host_cap_this_year,
        host_cap_prev_year,
        level,
        dept_date,
        host_maf_this_year,
        host_maf_prev_year,
        comp_maf_this_year,
        comp_maf_prev_year,
        on_market,
        flt_num,
    ],
)


api_groups = {
    "ev_type": event_type_group,
    "loc": location_group,
    "flight_number": flt_num_group,
    "date": date_group,
    "load_factor": lf_group,
    "fare": fare_group,
    "capacity": cap_group,
    "pkup": pickup_group,
    "bkng": booking_group,
    "mkt_share": market_share_group,
    "rev": revenue_group,
    "cbn": cabin_group,
}

labels = [
    rt_market,
    on_market,
    event_name,
    event_type,
    event_sub_type,
    loc,
    country,
    flt_num,
    cabin,
    dept_date,
    host_cap_this_year,
    host_cap_prev_year,
    var_host_cap_this_year_vs_prev_year,
    host_booking_this_year,
    host_booking_prev_year,
    host_lf_this_year,
    host_lf_prev_year,
    host_maf_this_year,
    comp_maf_this_year,
    is_loc_fixed,
    is_date_fixed,
    start_date_this_year,
    end_date_this_year,
    start_date_prev_year,
    end_date_prev_year,
    start_dow_this_year,
    end_dow_this_year,
    start_dow_prev_year,
    end_dow_prev_year,
    var_lf_this_year_vs_prev_year,
    host_avg_fare_this_year,
    comp_avg_fare_this_year,
    host_avg_fare_prev_year,
    comp_avg_fare_prev_year,
    host_maf_prev_year,
    comp_maf_prev_year,
    var_host_avg_fare_this_year_vs_last_year,
    var_comp_avg_fare_this_year_vs_last_year,
    impact,
    impact_dmd,
    num_impact_emp_days_ty,
    num_impact_emp_days_ly,
    num_days_inc_dmd,
    host_7_days_pkup_this_year,
    host_7_days_pkup_prev_year,
    var_host_7_days_pkup_this_year_vs_prev_year,
    mkt_7_days_pkup_comp_this_year,
    mkt_7_days_pkup_comp_prev_year,
    var_mkt_7_days_pkup_this_year_vs_prev_year,
    comp_booking_this_year,
    comp_booking_prev_year,
    ttl_mkt_bookings_this_year,
    ttl_mkt_bookings_prev_year,
    var_host_booking_this_year_vs_prev_year,
    var_comp_booking_this_year_vs_prev_year,
    var_ttl_mkt_this_year_vs_prev_year,
    host_mkt_share_this_year,
    host_mkt_share_prev_year,
    var_host_mkt_share_this_year_vs_prev_year,
    var_comp_mkt_share_this_year_vs_prev_year,
    host_rev_this_year,
    host_rev_prev_year,
    comp_rev_this_year,
    comp_rev_prev_year,
    var_host_rev_this_year_vs_prev_year,
    var_comp_rev_this_year_vs_prev_year,
]

" ---------------------------------- handlers -------------------------------------"


class API:
    @classmethod
    def all(cls) -> List[str]:
        res = []
        for group in api_groups.keys():
            res.extend(cls.group_vals(group))
        return res

    @classmethod
    def group_vals(cls, group: str) -> List[str]:
        return api_groups.get(group).values()
