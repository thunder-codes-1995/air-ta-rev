from flask import Response

from base.controller import BaseController
from kpi.forms import MsdKpiForm
from kpi.service import KPIService

service = KPIService()


class KPIController(BaseController):

    def get(self, endpoint):

        form = super().get(endpoint)

        if form.kpi_type.data == "ALL":
            response = service.get_kpi(form)
            return Response(response, mimetype="application/json")

    def get_validation_class(self):
        return MsdKpiForm
