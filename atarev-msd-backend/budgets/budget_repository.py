from base.repository import BaseRepository
from datetime import date, timedelta
from dataclasses import dataclass
from typing import List


@dataclass
class BudgetRecord:
    sell_year: int = 0
    sell_month: int = 0
    revenue_budget: float = 0
    passengers_budget: float = 0
    bookings_budget: float = 0
    cargo_budget: float = 0
    load_factor_budget: float = 0
    capacity_budget: float = 0


class BudgetRepository(BaseRepository):
    collection = 'budget2'

    def get_budget_for_criteria(self, start_date: date, end_date: date, orig_code, dest_code) -> List[BudgetRecord]:
        # we need to find records in budget collection that match year&months from a given range
        # so we need a query like "where (year=2019 and month=12) or (year=2020 and month=1) or (year=2020 and month=2)"
        date_query = []
        add_days = timedelta(days=31)
        start = date(start_date.year, start_date.month, 1)
        date_query.append(
            {'$and': [{'sell_year': start.year}, {'sell_month': start.month}]})
        end = date(end_date.year, end_date.month, 1)
        while start <= end:
            # add a month
            start += add_days
            date_query.append(
                {'$and': [{'sell_year': start.year}, {'sell_month': start.month}]})

        query = [
            {
                '$match': {
                    '$or': date_query,
                    'orig_code': {'$in': orig_code},
                    'dest_code': {'$in': dest_code},
                },
            },
            {
                '$group': {
                    '_id': {
                        'sell_year': '$sell_year',
                        'sell_month': '$sell_month'
                    },
                    'revenue_budget': {'$sum': '$revenue_budget'},
                    'passengers_budget': {'$sum': '$passengers_budget'},
                    'bookings_budget': {'$sum': '$bookings_budget'},
                    'cargo_budget': {'$sum': '$cargo_budget'},
                    'load_factor_budget': {'$avg': '$load_factor_budget'},
                    'capacity_budget': {'$sum': '$capacity_budget'}
                }
            },
            {
                '$project': {
                    'sell_year': '$_id.sell_year',
                    'sell_month': '$_id.sell_month',
                    'revenue_budget': 1,
                    'passengers_budget': 1,
                    'bookings_budget': 1,
                    'cargo_budget': 1,
                    'load_factor_budget': 1,
                    'capacity_budget': 1
                }
            },
            {
                '$sort': {
                    'sell_year': 1,
                    'sell_month': 1
                }
            }
        ]
        cursor = self.aggregate(query)

        results = []
        for rec in list(cursor):
            results.append(self.convert_mongo_document_to_dto(rec))
        return results

    def convert_mongo_document_to_dto(self, rec) -> BudgetRecord:
        dto = BudgetRecord()
        dto.sell_year = rec.get('sell_year')
        dto.sell_month = rec.get('sell_month')
        dto.revenue_budget = rec.get('revenue_budget')
        dto.passengers_budget = rec.get('passengers_budget')
        dto.bookings_budget = rec.get('bookings_budget')
        dto.cargo_budget = rec.get('cargo_budget')
        dto.capacity_budget = rec.get('capacity_budget')
        dto.load_factor_budget = rec.get('load_factor_budget')
        return dto
