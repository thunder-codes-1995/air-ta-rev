import copy
import json
import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import List

import numpy as np
from dataclasses_json import dataclass_json
from dateutil.relativedelta import relativedelta
from flask import request

from base.constants import is_demo_mode
from budgets.budget_repository import BudgetRecord, BudgetRepository
from kpi.forms import MsdKpiForm
from kpi.kpi_repository import KpiRepository, SalesTotalsRecord
from kpi.query_builder import DdsBuilder
from kpi.utils import format_kpi_amount, format_kpi_percentage

# in case of division by zero exception or other calculation problems - this will be returned as metric
# that should help spot the problem with data/calculation
INVALID_VALUE_PLACEHOLDER = 0.01


@dataclass_json
@dataclass
class KpiMetric:
    """DTO to store data/amount/texts for a single metric"""

    metricType: str = ""
    metricValue: str = ""
    metricSymbol: str = ""
    metricDetail: str = ""
    metricVsCurrentPct: float = 0


@dataclass_json
@dataclass
class KpiGraph:
    """DTO to store graph(X and Y amounts)"""

    x: list = field(default_factory=list)
    y: list = field(default_factory=list)


@dataclass_json
@dataclass
class KpiRecord:
    """DTO to store data for entire KPI category(e.g. passenger KPIs or revenue KPI"""

    kpiName: str = ""
    kpiType: str = ""
    metrics: list = field(default_factory=list)  # multiple metrics will be here
    graphData: KpiGraph = KpiGraph()  # graph


@dataclass_json
@dataclass
class ActualsAndBudget:
    """DTO to store actuals (e.g. sales, revenue, passengers) and budgeted amounts for a single month"""

    budget: BudgetRecord
    actual: SalesTotalsRecord


@dataclass_json
@dataclass
class KpiContext:
    current_month: ActualsAndBudget
    last_month: ActualsAndBudget
    current_year_ytd: ActualsAndBudget
    last_year_same_month: ActualsAndBudget
    last_year_ytd: ActualsAndBudget
    entire_last_year: ActualsAndBudget


class KPIService:
    budget_repo = BudgetRepository()
    kpi_repo = KpiRepository()
    builder_class = DdsBuilder

    def get_kpi(self, form: MsdKpiForm):
        # current_date = date(2019, 9, 15)
        current_date = date.today()
        substr_month = timedelta(days=30)
        current_date -= substr_month

        # in demo mode, use hardcoded data from the past (as we do not load daily files into demo database)
        if is_demo_mode():
            current_date = date(2022, 10, 15)

        response = self.get_kpis(current_date, form.get_orig_city_airports_list(), form.get_dest_city_airports_list())
        # return KpiRecord.schema().dumps(response, many=True)
        response = [r.to_dict() for r in response]
        return json.dumps(response)

    def get_kpis(self, current_date: date, orig_codes: List[str], dest_codes: List[str]):
        """generate all KPIs for a given date and origin/destination airport codes"""

        kpi_method_map = {
            "passenger": self.get_passenger_kpi,
            "revenue": self.get_revenue_kpi,
            "average_fare": self.get_avg_fare_kpi,
            "capacity": self.get_capacity_kpi,
            "rask": self.get_rask_kpi_dummy,
            "cargo": self.get_cargo_kpi_dummy,
        }

        last_year_jan_1 = date(current_date.year - 1, 1, 1)

        # retrieve actual sales(revenue, passengers, etc) data for the given date range, fill in missing months (in case there are some gaps)
        actuals = self.kpi_repo.get_sales_actuals(last_year_jan_1, current_date, orig_codes, dest_codes)
        actuals = self.sort_and_add_missing_months(actuals, last_year_jan_1, current_date, SalesTotalsRecord())

        # retrieve budgeted data for the given date range, fill in missing months (in case there are some gaps)
        budgets = self.budget_repo.get_budget_for_criteria(last_year_jan_1, current_date, orig_codes, dest_codes)
        budgets = self.sort_and_add_missing_months(budgets, last_year_jan_1, current_date, BudgetRecord())
        fields = request.user.kpis or list(kpi_method_map.keys())
       
        # now we have sales targets(budget data) and actual sales data - we can generate KPIs for various categories (revenue, passengers, cargo, etc)
        return [kpi_method_map[field](current_date, actuals, budgets) for field in fields]

    def get_revenue_kpi(self, current_date: date, actuals: List[SalesTotalsRecord], budgets: List[BudgetRecord]) -> KpiRecord:
        """Calculate 'Revenue' KPIs"""
        result = KpiRecord(kpiName="Revenue", kpiType="revenue")
        result.metrics = [
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.actual.revenue_total,
                lambda ctx: (ctx.current_month.actual.revenue_total - ctx.current_month.budget.revenue_budget)
                / ctx.current_month.budget.revenue_budget,
                "Current Month",
                "$",
                "Gap vs Budget",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.budget.revenue_budget,
                lambda ctx: (ctx.current_month.budget.revenue_budget - ctx.last_year_same_month.budget.revenue_budget)
                / ctx.last_year_same_month.budget.revenue_budget,
                "Bud Mon",
                "$",
                "Gap vs Bud Ly Mon",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_month.actual.revenue_total,
                lambda ctx: (ctx.current_month.actual.revenue_total - ctx.last_month.actual.revenue_total)
                / ctx.last_month.actual.revenue_total,
                "Last Mon",
                "$",
                "Gap vs Cur Mon",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_same_month.actual.revenue_total,
                lambda ctx: (ctx.current_month.actual.revenue_total - ctx.last_year_same_month.actual.revenue_total)
                / ctx.last_year_same_month.actual.revenue_total,
                "Ly Mon",
                "$",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_year_ytd.actual.revenue_total,
                lambda ctx: (ctx.current_year_ytd.actual.revenue_total - ctx.last_year_ytd.actual.revenue_total)
                / ctx.last_year_ytd.actual.revenue_total,
                "Cur YTD",
                "$",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_year_ytd.budget.revenue_budget,
                lambda ctx: (ctx.current_year_ytd.budget.revenue_budget - ctx.last_year_ytd.budget.revenue_budget)
                / ctx.last_year_ytd.budget.revenue_budget,
                "Bud YTD",
                "$",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_ytd.actual.revenue_total,
                lambda ctx: (ctx.last_year_ytd.actual.revenue_total - ctx.last_year_ytd.budget.revenue_budget)
                / ctx.last_year_ytd.budget.revenue_budget,
                "Ly YTD",
                "$",
                "Gap vs Cur Mon",
            ),
        ]
        # generate graph data (last 12 months of passenger totals)
        result.graphData = self.generate_graph_data(current_date, actuals, budgets, lambda actual, budget: actual.revenue_total)
        return result

    def get_passenger_kpi(self, current_date: date, actuals: List[SalesTotalsRecord], budgets: List[BudgetRecord]) -> KpiRecord:
        """Calculate 'Passenger' KPIs"""
        result = KpiRecord(kpiName="Passenger", kpiType="passenger")
        result.metrics = [
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.actual.passengers_total,
                lambda ctx: (ctx.current_month.actual.passengers_total - ctx.current_month.budget.passengers_budget)
                / ctx.current_month.budget.passengers_budget,
                "Current Month",
                "",
                "Gap vs Budget",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.budget.passengers_budget,
                lambda ctx: (ctx.current_month.budget.passengers_budget - ctx.last_year_same_month.budget.passengers_budget)
                / ctx.last_year_same_month.budget.passengers_budget,
                "Bud Mon",
                "",
                "Gap vs Bud Ly Mon",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_month.actual.passengers_total,
                lambda ctx: (ctx.current_month.actual.passengers_total - ctx.last_month.actual.passengers_total)
                / ctx.last_month.actual.passengers_total,
                "Last Mon",
                "",
                "Gap vs Cur Mon",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_same_month.actual.passengers_total,
                lambda ctx: (ctx.current_month.actual.passengers_total - ctx.last_year_same_month.actual.passengers_total)
                / ctx.last_year_same_month.actual.passengers_total,
                "Ly Mon",
                "",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_year_ytd.actual.passengers_total,
                lambda ctx: (ctx.current_year_ytd.actual.passengers_total - ctx.last_year_ytd.actual.passengers_total)
                / ctx.last_year_ytd.actual.passengers_total,
                "Cur YTD",
                "",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_year_ytd.budget.passengers_budget,
                lambda ctx: (ctx.current_year_ytd.budget.passengers_budget - ctx.last_year_ytd.budget.passengers_budget)
                / ctx.last_year_ytd.budget.passengers_budget,
                "Bud YTD",
                "",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_ytd.actual.passengers_total,
                lambda ctx: (ctx.last_year_ytd.actual.passengers_total - ctx.last_year_ytd.budget.passengers_budget)
                / ctx.last_year_ytd.budget.passengers_budget,
                "Ly YTD",
                "",
                "Gap vs Cur Mon",
            ),
        ]
        # generate graph data (last 12 months of passenger totals)
        result.graphData = self.generate_graph_data(
            current_date, actuals, budgets, lambda actual, budget: actual.passengers_total
        )
        return result

    def get_avg_fare_kpi(self, current_date: date, actuals: List[SalesTotalsRecord], budgets: List[BudgetRecord]) -> KpiRecord:
        """Calculate 'Average fare' KPIs"""
        result = KpiRecord(kpiName="Average Fare", kpiType="average_fare")
        result.metrics = [
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.actual.revenue_total / ctx.current_month.actual.passengers_total,
                lambda ctx: (
                    (
                        (ctx.current_month.actual.revenue_total / ctx.current_month.actual.passengers_total)
                        - ctx.current_month.budget.revenue_budget / ctx.current_month.budget.passengers_budget
                    )
                )
                / (ctx.current_month.budget.revenue_budget / ctx.current_month.budget.passengers_budget),
                "Current Month",
                "$",
                "Gap vs Budget",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: (ctx.current_month.budget.revenue_budget / ctx.current_month.budget.passengers_budget),
                lambda ctx: (
                    (ctx.current_month.budget.revenue_budget / ctx.current_month.budget.passengers_budget)
                    - ctx.last_year_same_month.budget.revenue_budget / ctx.last_year_same_month.budget.passengers_budget
                )
                / (ctx.last_year_same_month.budget.revenue_budget / ctx.last_year_same_month.budget.passengers_budget),
                "Bud Mon",
                "$",
                "Gap vs Bud Ly Mon",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: (ctx.last_month.actual.revenue_total / ctx.last_month.actual.passengers_total),
                lambda ctx: (
                    (ctx.last_month.actual.revenue_total / ctx.last_month.actual.passengers_total)
                    - (ctx.last_month.budget.revenue_budget / ctx.last_month.budget.passengers_budget)
                )
                / (ctx.last_month.budget.revenue_budget / ctx.last_month.budget.passengers_budget),
                "Last Mon",
                "$",
                "Gap vs LM Budget",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_same_month.actual.revenue_total / ctx.last_year_same_month.actual.passengers_total,
                lambda ctx: (
                    (ctx.last_year_same_month.actual.revenue_total / ctx.last_year_same_month.actual.passengers_total)
                    - (ctx.last_year_same_month.budget.revenue_budget / ctx.last_year_same_month.budget.passengers_budget)
                )
                / (ctx.last_year_same_month.budget.revenue_budget / ctx.last_year_same_month.budget.passengers_budget),
                "Ly Mon",
                "$",
                "Gap vs Ly YTD",
            ),  # probably this is wrong!
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.entire_last_year.actual.revenue_total / ctx.entire_last_year.actual.passengers_total,
                lambda ctx: (
                    (ctx.entire_last_year.actual.revenue_total / ctx.entire_last_year.actual.passengers_total)
                    - (ctx.entire_last_year.budget.revenue_budget / ctx.entire_last_year.budget.passengers_budget)
                )
                / (ctx.entire_last_year.budget.revenue_budget / ctx.entire_last_year.budget.passengers_budget),
                "LY Avg",
                "",
                "Gap vs Ly Budget",
            ),
            # self.calculate_single_kpi_metric(current_date, actuals, budgets,
            #                                  lambda ctx: ctx.current_year_ytd.budget.passengers_budget,
            #                                  lambda ctx: (ctx.current_year_ytd.budget.passengers_budget - ctx.last_year_ytd.budget.passengers_budget) / ctx.last_year_ytd.budget.passengers_budget,
            #                                  'Bud YTD', '', 'Gap vs Ly YTD'),
            # self.calculate_single_kpi_metric(current_date, actuals, budgets,
            #                                  lambda ctx: ctx.last_year_ytd.actual.passengers_total,
            #                                  lambda ctx: (ctx.last_year_ytd.actual.passengers_total - ctx.last_year_ytd.budget.passengers_budget) / ctx.last_year_ytd.budget.passengers_budget,
            #                                  'Ly YTD', '', 'Gap vs Cur Mon')
        ]
        # generate graph data (last 12 months of passenger totals)
        result.graphData = self.generate_graph_data(
            current_date, actuals, budgets, lambda actual, budget: (actual.revenue_total / actual.passengers_total)
        )
        return result

    def get_capacity_kpi(self, current_date: date, actuals: List[SalesTotalsRecord], budgets: List[BudgetRecord]) -> KpiRecord:
        """Calculate 'Capacity' KPIs"""
        result = KpiRecord(kpiName="Capacity", kpiType="capacity")
        result.metrics = [
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.actual.capacity_total,
                lambda ctx: (ctx.current_month.actual.capacity_total - ctx.current_month.budget.capacity_budget)
                / ctx.current_month.budget.capacity_budget,
                "Current Month",
                "",
                "Gap vs Bud",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_month.actual.capacity_total,
                lambda ctx: (ctx.last_month.actual.capacity_total - ctx.last_month.budget.capacity_budget)
                / ctx.last_month.budget.capacity_budget,
                "Last Mon",
                "",
                "Gap vs Bud",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_same_month.actual.capacity_total,
                lambda ctx: (ctx.current_month.actual.capacity_total - ctx.last_year_same_month.budget.capacity_budget)
                / ctx.last_year_same_month.budget.capacity_budget,
                "Ly Mon",
                "",
                "Gap vs Bud",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.actual.capacity_total - ctx.last_year_same_month.actual.capacity_total,
                lambda ctx: (
                    (ctx.current_month.actual.capacity_total - ctx.last_year_same_month.actual.capacity_total)
                    / ctx.last_year_same_month.actual.capacity_total
                ),
                "Var LY vs Curr",
                "",
                "Var % Var",
            ),
            # self.calculate_single_kpi_metric(current_date, actuals, budgets,
            #                                  lambda ctx: ctx.current_year_ytd.budget.passengers_budget,
            #                                  lambda ctx: (ctx.current_year_ytd.budget.passengers_budget - ctx.last_year_ytd.budget.passengers_budget) / ctx.last_year_ytd.budget.passengers_budget,
            #                                  'Bud YTD', '', 'Gap vs Ly YTD'),
            # self.calculate_single_kpi_metric(current_date, actuals, budgets,
            #                                  lambda ctx: ctx.last_year_ytd.actual.passengers_total,
            #                                  lambda ctx: (ctx.last_year_ytd.actual.passengers_total - ctx.last_year_ytd.budget.passengers_budget) / ctx.last_year_ytd.budget.passengers_budget,
            #                                  'Ly YTD', '', 'Gap vs Cur Mon')
        ]
        # generate graph data (last 12 months of passenger totals)
        result.graphData = self.generate_graph_data(current_date, actuals, budgets, lambda actual, budget: actual.capacity_total)
        return result

    def get_rask_kpi_dummy(self, current_date: date, actuals: List[SalesTotalsRecord], budgets: List[BudgetRecord]) -> KpiRecord:
        """Calculate 'Rask' KPIs"""
        result = KpiRecord(kpiName="Rask", kpiType="rask")
        result.metrics = [
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.actual.bookings_total,
                lambda ctx: (ctx.current_month.actual.bookings_total - ctx.current_month.budget.passengers_budget)
                / ctx.current_month.budget.passengers_budget,
                "Current Month",
                "",
                "Gap vs Budget",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.budget.passengers_budget,
                lambda ctx: (ctx.current_month.budget.passengers_budget - ctx.last_year_same_month.budget.passengers_budget)
                / ctx.last_year_same_month.budget.passengers_budget,
                "Bud Mon",
                "",
                "Gap vs Bud Ly Mon",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_month.actual.bookings_total,
                lambda ctx: (ctx.current_month.actual.bookings_total - ctx.last_month.actual.passengers_total)
                / ctx.last_month.actual.passengers_total,
                "Last Mon",
                "",
                "Gap vs Cur Mon",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_same_month.actual.bookings_total,
                lambda ctx: (ctx.current_month.actual.bookings_total - ctx.last_year_same_month.actual.passengers_total)
                / ctx.last_year_same_month.actual.passengers_total,
                "Ly Mon",
                "",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_year_ytd.actual.bookings_total,
                lambda ctx: (ctx.current_year_ytd.actual.bookings_total - ctx.last_year_ytd.actual.passengers_total)
                / ctx.last_year_ytd.actual.passengers_total,
                "Cur YTD",
                "",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_year_ytd.budget.passengers_budget,
                lambda ctx: (ctx.current_year_ytd.budget.passengers_budget - ctx.last_year_ytd.budget.passengers_budget)
                / ctx.last_year_ytd.budget.passengers_budget,
                "Bud YTD",
                "",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_ytd.actual.passengers_total,
                lambda ctx: (ctx.last_year_ytd.actual.passengers_total - ctx.last_year_ytd.budget.passengers_budget)
                / ctx.last_year_ytd.budget.passengers_budget,
                "Ly YTD",
                "",
                "Gap vs Cur Mon",
            ),
        ]
        # generate graph data (last 12 months of passenger totals)
        result.graphData = self.generate_graph_data(
            current_date, actuals, budgets, lambda actual, budget: actual.passengers_total
        )
        return result

    def get_cargo_kpi_dummy(self, current_date: date, actuals: List[SalesTotalsRecord], budgets: List[BudgetRecord]) -> KpiRecord:
        """Calculate 'cargo' KPIs"""
        result = KpiRecord(kpiName="Cargo", kpiType="cargo")
        result.metrics = [
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.actual.cargo_total,
                lambda ctx: (ctx.current_month.actual.cargo_total - ctx.current_month.budget.cargo_budget)
                / ctx.current_month.budget.cargo_budget,
                "Current Month",
                "",
                "Gap vs Budget",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_month.budget.cargo_budget,
                lambda ctx: (ctx.current_month.budget.cargo_budget - ctx.last_year_same_month.budget.cargo_budget)
                / ctx.last_year_same_month.budget.cargo_budget,
                "Bud Mon",
                "",
                "Gap vs Bud Ly Mon",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_month.actual.cargo_total,
                lambda ctx: (ctx.current_month.actual.cargo_total - ctx.last_month.actual.cargo_total)
                / ctx.last_month.actual.cargo_total,
                "Last Mon",
                "",
                "Gap vs Cur Mon",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_same_month.actual.cargo_total,
                lambda ctx: (ctx.current_month.actual.cargo_total - ctx.last_year_same_month.actual.cargo_total)
                / ctx.last_year_same_month.actual.cargo_total,
                "Ly Mon",
                "",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_year_ytd.actual.cargo_total,
                lambda ctx: (ctx.current_year_ytd.actual.cargo_total - ctx.last_year_ytd.actual.cargo_total)
                / ctx.last_year_ytd.actual.cargo_total,
                "Cur YTD",
                "",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.current_year_ytd.budget.cargo_budget,
                lambda ctx: (ctx.current_year_ytd.budget.cargo_budget - ctx.last_year_ytd.budget.cargo_budget)
                / ctx.last_year_ytd.budget.cargo_budget,
                "Bud YTD",
                "",
                "Gap vs Ly YTD",
            ),
            self.calculate_single_kpi_metric(
                current_date,
                actuals,
                budgets,
                lambda ctx: ctx.last_year_ytd.actual.cargo_total,
                lambda ctx: (ctx.last_year_ytd.actual.cargo_total - ctx.last_year_ytd.budget.cargo_budget)
                / ctx.last_year_ytd.budget.cargo_budget,
                "Ly YTD",
                "",
                "Gap vs Cur Mon",
            ),
        ]
        # generate graph data (last 12 months of passenger totals)
        result.graphData = self.generate_graph_data(current_date, actuals, budgets, lambda actual, budget: actual.bookings_total)
        return result

    def find_budget_and_actuals_for_date(self, all_actuals, all_budgets, date) -> ActualsAndBudget:
        """from actuals and budgeted amounts provided as parameter, find those that are for a requested date (year and month)"""
        actuals = next(
            filter(lambda rec: rec.sell_year == date.year and rec.sell_month == date.month, all_actuals), SalesTotalsRecord()
        )
        budgets = next(
            filter(lambda rec: rec.sell_year == date.year and rec.sell_month == date.month, all_budgets), BudgetRecord()
        )
        return ActualsAndBudget(actual=actuals, budget=budgets)

    def find_budget_and_actuals_for_date_range(self, all_actuals, all_budgets, start_date, end_date) -> ActualsAndBudget:
        """Same as above but returns a list of actuals/budgeted amounts for a requested date range"""
        # all_actuals and all_budgets are grouped monthly and do not have DAY component
        # hence we compare here only entire months
        start = date(start_date.year, start_date.month, 1)
        end = date(end_date.year, end_date.month, 1)
        budgets = list(
            filter(
                lambda rec: (start <= date(rec.sell_year, rec.sell_month, 1) and date(rec.sell_year, rec.sell_month, 1) <= end),
                all_budgets,
            )
        )

        actuals = list(
            filter(
                lambda rec: (start <= date(rec.sell_year, rec.sell_month, 1) and date(rec.sell_year, rec.sell_month, 1) <= end),
                all_actuals,
            )
        )

        return (budgets, actuals)

    def find_budget_and_actuals_for_date_range_aggregated(
        self, all_actuals, all_budgets, start_date, end_date
    ) -> ActualsAndBudget:
        """Same as above but this aggregates amounts for a given date range and returns single aggregeted values (totals or averages)"""
        (budgets, actuals) = self.find_budget_and_actuals_for_date_range(all_actuals, all_budgets, start_date, end_date)

        # aggregate amounts (mostly totals, load factor is averaged)
        actuals_aggregated = SalesTotalsRecord(
            revenue_total=np.sum(list(map(lambda x: x.revenue_total, actuals))),
            passengers_total=np.sum(list(map(lambda x: x.passengers_total, actuals))),
            bookings_total=np.sum(list(map(lambda x: x.bookings_total, actuals))),
            cargo_total=np.sum(list(map(lambda x: x.cargo_total, actuals))),
            avg_load_factor=np.mean(list(map(lambda x: x.avg_load_factor, actuals))),
            capacity_total=np.sum(list(map(lambda x: x.capacity_total, actuals))),
        )
        budgets_aggregated = BudgetRecord(
            revenue_budget=np.sum(list(map(lambda x: x.revenue_budget, budgets))),
            passengers_budget=np.sum(list(map(lambda x: x.passengers_budget, budgets))),
            bookings_budget=np.sum(list(map(lambda x: x.bookings_budget, budgets))),
            cargo_budget=np.sum(list(map(lambda x: x.cargo_budget, budgets))),
            load_factor_budget=np.mean(list(map(lambda x: x.load_factor_budget, budgets))),
            capacity_budget=np.sum(list(map(lambda x: x.capacity_budget, budgets))),
        )

        return ActualsAndBudget(actual=actuals_aggregated, budget=budgets_aggregated)

    def calculate_single_kpi_metric(
        self,
        current_date: date,
        actuals: List[SalesTotalsRecord],
        budgets: List[BudgetRecord],
        metric_formula_lambda,
        percentage_lambda,
        def_type="Current Month",
        def_symbol="",
        def_detail="Gap vs Budget",
    ) -> KpiMetric:
        """Calculate metric based on actuals and budget values and using custom lambda function to calculate metric value"""
        # collect metrics for current month, prev, last year same month, ...
        current_month = self.find_budget_and_actuals_for_date(actuals, budgets, current_date)  # this month
        last_month = self.find_budget_and_actuals_for_date(actuals, budgets, current_date - relativedelta(months=1))  # last month
        current_year_ytd = self.find_budget_and_actuals_for_date_range_aggregated(
            actuals,
            budgets,
            date(current_date.year, 1, 1),
            # this year s(starting from 1Jan)
            current_date,
        )

        last_year_same_month = self.find_budget_and_actuals_for_date(actuals, budgets, current_date - relativedelta(years=1))
        last_year_ytd = self.find_budget_and_actuals_for_date_range_aggregated(
            actuals, budgets, date(current_date.year - 1, 1, 1), date(current_date.year - 1, current_date.month, current_date.day)
        )

        entire_last_year = self.find_budget_and_actuals_for_date_range_aggregated(
            actuals, budgets, date(current_date.year - 1, 1, 1), date(current_date.year, 1, 1) - relativedelta(days=1)
        )

        # create KpiContext object which holds all above calculated actuals and budgets, it will be used as parameter for lambdas
        context = KpiContext(
            current_month=current_month,
            last_month=last_month,
            current_year_ytd=current_year_ytd,
            last_year_same_month=last_year_same_month,
            last_year_ytd=last_year_ytd,
            entire_last_year=entire_last_year,
        )
        # calculate metric values using custom lambda which use context from previous step to calculate required value
        try:
            metric_value = round(metric_formula_lambda(context), 2)
        except ZeroDivisionError:
            # print(f"ZeroDivisionError for metric formula for {def_type}, context:{context}")
            metric_value = INVALID_VALUE_PLACEHOLDER  # this is just to indicate something is wrong

        # same as above but here we calculate percentage (gap between actuals and budget)
        try:
            metric_vs_pct = round(percentage_lambda(context), 2)
            if math.isnan(metric_vs_pct):
                metric_vs_pct = 0

        except ZeroDivisionError:
            # print(f"ZeroDivisionError for percentage formula for {def_type}, context:{context}")
            metric_vs_pct = INVALID_VALUE_PLACEHOLDER  # this is just to indicate something is wrong

        result = KpiMetric(
            metricType=def_type,
            metricValue=format_kpi_amount(metric_value),
            metricSymbol=def_symbol,
            metricDetail=def_detail,
            metricVsCurrentPct=format_kpi_percentage(metric_vs_pct),
        )
        return result

    def generate_graph_data(
        self, current_date: date, actuals: List[SalesTotalsRecord], budgets: List[BudgetRecord], y_axis_lambda
    ) -> KpiGraph:
        """generate graph (array with X and Y values) for the previous 12 months of a custom metric (calculated with 'y_axis_lambda')"""
        end = date(current_date.year, current_date.month, 1) - relativedelta(days=1)  # end date is last day of prev month
        start = date(end.year, end.month, 1) - relativedelta(months=12)  # start = end - 12 months
        (budgets, actuals) = self.find_budget_and_actuals_for_date_range(
            actuals, budgets, start, end
        )  # get actuals and budget values for last 12 months

        graph = KpiGraph()
        minlen = min(len(actuals), len(budgets))
        for i in range(min(len(actuals), len(budgets))):
            graph.x.append(i)
            y_value = 0
            try:
                y_value = round(y_axis_lambda(actuals[i], budgets[i]), 2)
            except ZeroDivisionError:
                y_value = INVALID_VALUE_PLACEHOLDER  # this is just to indicate something is wrong
            graph.y.append(y_value)  # extract value for Y axis
        return graph

    def sort_and_add_missing_months(self, data, start_date, end_date, default_item):
        add_days = timedelta(days=31)
        start = date(start_date.year, start_date.month, 1)
        end = date(end_date.year, end_date.month, 28)
        while start <= end:
            first = next(filter(lambda rec: rec.sell_year == start.year and rec.sell_month == start.month, data), None)
            if first is None:
                missing_item = copy.copy(default_item)
                missing_item.sell_year = start.year
                missing_item.sell_month = start.month
                data.append(missing_item)
            # add a month
            start += add_days

        return self.sort_by_date(data)

    def sort_by_date(self, data: list):
        return sorted(data, key=lambda x: (x.sell_year, x.sell_month))
