from base.constants import Constants
from base.controller import BaseController
from base.helpers.routes import ProtectedRoutes
from reports.inventory.form import ReportForm
from reports.service import ReportService
from utils.funcs import create_error_response

service = ReportService


class ReportController(BaseController):

    def get(self, endpoint):

        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.INVENTORY_CHANGES_REPORT.value:
            return service.report(form)

        return create_error_response(Constants.ERROR_CODE_INVALID_REQUEST, "Invalid request", 400, "not found")

    def get_validation_class(self):
        return ReportForm
