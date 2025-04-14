import csv
from datetime import datetime
from io import StringIO

from flask import make_response, request

from base.service import BaseService
from reports.inventory.form import ReportForm
from reports.inventory.query import ReportQuery

from .repository import AuthResultRepository


class ReportService(BaseService):
    repository_class = AuthResultRepository

    def report(self, form: ReportForm):
        match = ReportQuery(form, request.user.carrier).query
        cursor = self.repository.get(match)
        si = StringIO()
        cw = csv.writer(si)

        data = [
            "origin",
            "destination",
            "airline_code",
            "cabin",
            "flight_number",
            "outbound_date",
            "old_class",
            "old_rank",
            "old_authorization",
            "class",
            "rank",
            "authorization",
        ]

        cw.writerows([data] + [list(item.values()) for item in cursor])
        output = make_response(si.getvalue())
        output.headers[
            "Content-Disposition"
        ] = f"attachment; filename={request.user.carrier}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output.headers["Content-type"] = "text/csv"
        return output
