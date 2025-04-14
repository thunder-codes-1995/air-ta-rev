from dataclasses import dataclass
from datetime import date
from typing import List

from dataclasses_json import dataclass_json
from flask import request

from base.constants import is_demo_mode
from dds.repository import DdsRepository


@dataclass_json
@dataclass
class SalesTotalsRecord:
    sell_year: int = 0
    sell_month: int = 0
    revenue_total: float = 0
    passengers_total: float = 0
    bookings_total: float = 0
    cargo_total: float = 0
    avg_load_factor: float = 0
    capacity_total: float = 0


class KpiRepository(DdsRepository):

    def get_sales_actuals(self, start_date: date, end_date: date, orig_codes, dest_codes) -> List[SalesTotalsRecord]:
        start = date(start_date.year, start_date.month, 1)
        start_str = start.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        host_carrier_code = request.user.carrier
        get_sales_actuals_query = [
            {
                "$match": {
                    "$and": [{"sell_date": {"$gte": int(start_str)}}, {"sell_date": {"$lte": int(end_str)}}],
                    "orig_code": {"$in": orig_codes},
                    "dest_code": {"$in": dest_codes},
                    "dom_op_al_code": host_carrier_code,
                    # 'is_ticket': True
                },
            },
            {
                "$group": {
                    "_id": {
                        "sell_year": "$sell_year",
                        "sell_month": "$sell_month",
                    },
                    "revenue_total": {"$sum": "$blended_rev"},
                    "passengers_total": {"$sum": "$pax"},
                    "bookings_total": {"$sum": 1},
                    "cargo_total": {"$sum": 0},
                    "avg_load_factor": {"$avg": 0.70},
                    "capacity_total": {"$sum": 0},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "sell_year": "$_id.sell_year",
                    "sell_month": "$_id.sell_month",
                    "revenue_total": "$revenue_total",
                    "passengers_total": "$passengers_total",
                    "bookings_total": "$bookings_total",
                    "cargo_total": "$cargo_total",
                    "avg_load_factor": "$avg_load_factor",
                    "capacity_total": "$capacity_total",
                }
            },
            {"$sort": {"sell_year": 1, "sell_month": 1}},
        ]
        cursor = self.aggregate(get_sales_actuals_query)

        results = []
        for rec in list(cursor):
            results.append(self.convert_mongo_document_to_dto(rec))

        if is_demo_mode():
            dummy_cargo_totals = [
                12000000,
                11000000,
                13000000,
                14500000,
                14000000,
                14000000,
                15000000,
                13000000,
                14000000,
                14000000,
                12000000,
                12000000,
            ]
            for rec in results:
                rec.cargo_total = dummy_cargo_totals[rec.sell_month - 1]
                rec.capacity_total = rec.passengers_total / (rec.avg_load_factor if rec.avg_load_factor > 0 else 0.5)

        return results

    def convert_mongo_document_to_dto(self, rec) -> SalesTotalsRecord:
        dto = SalesTotalsRecord()
        dto.sell_year = rec.get("sell_year")
        dto.sell_month = rec.get("sell_month")
        dto.revenue_total = rec.get("revenue_total")
        dto.passengers_total = rec.get("passengers_total")
        dto.bookings_total = rec.get("bookings_total")
        dto.cargo_total = rec.get("cargo_total")
        dto.avg_load_factor = rec.get("avg_load_factor")
        dto.capacity_total = rec.get("capacity_total")
        return dto
