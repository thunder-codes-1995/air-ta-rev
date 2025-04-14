from base.constants import Constants
from base.controller import BaseController
from base.helpers.routes import FreeRoutes, ProtectedRoutes
from dds.service import DdsService
from kpi.service import KPIService
from market_share.forms import MsdMarketShareTrends
from utils.funcs import create_error_response

from .forms import (
    BaseMSDFilterParams,
    MsdBaseProductOverviewForm,
    MsdCarriers,
    MsdGetBkgMix,
    MsdGetCabinMix,
    MsdGetCosBreakdown,
    MsdGetDistrMix,
    MsdGetProductMap,
    PosBreakDownTables,
    RangeSliderValues,
)

dds_service = DdsService()
kpi_service = KPIService()


class DdsController(BaseController):

    def get(self, endpoint):
        result = self.get_timed(endpoint)
        return result

    def get_timed(self, endpoint):
        if endpoint == FreeRoutes.GET_HEALTCHECK.value:
            return dds_service.get_healthcheck()

        form = super().get(endpoint)

        if endpoint == ProtectedRoutes.GET_MARKET_SHARE_TRENDS.value:
            return dds_service.get_market_share_trends(form)

        if endpoint == ProtectedRoutes.GET_PRODUCT_MATRIX.value:
            return dds_service.get_product_matrix(form)

        if endpoint == ProtectedRoutes.GET_CARRIERS.value:
            return dds_service.get_msd_carriers(form)

        if endpoint == ProtectedRoutes.GET_CONFIGURATION.value:
            return dds_service.get_msd_config()

        if endpoint == ProtectedRoutes.GET_PRODUCT_MAP.value:
            return dds_service.get_product_map(form)

        if endpoint == ProtectedRoutes.GET_DISTR_MIX.value:
            return dds_service.get_dist_mix(form)

        if endpoint == ProtectedRoutes.GET_COS_BREAKDOWN.value:
            # TODO overriding this for now
            form.pos.data = "All"
            return dds_service.get_cos_plots(form)

        if endpoint == ProtectedRoutes.GET_COS_BREAKDOWN_TABLE.value.value:
            return dds_service.get_cos_tables(form)

        if endpoint == ProtectedRoutes.GET_CABIN_MIX.value:
            return dds_service.get_cabin_mix(form)

        if endpoint == ProtectedRoutes.GET_BKG_MIX.value:
            return dds_service.get_bkg_type_mix(form)

        if endpoint == ProtectedRoutes.GET_KPI.value:
            return kpi_service.get_kpi(form)

        if endpoint == ProtectedRoutes.GET_SLIDER_RANGE.value:
            return dds_service.get_silder_range_values(form)

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def get_validation_class(self):
        type = self.get_view_parameter("endpoint")

        if type == ProtectedRoutes.GET_MARKET_SHARE_TRENDS.value:
            return MsdMarketShareTrends

        if type == ProtectedRoutes.GET_PRODUCT_MATRIX.value:
            return MsdBaseProductOverviewForm

        if type == ProtectedRoutes.GET_CARRIERS.value:
            return MsdCarriers

        if type == ProtectedRoutes.GET_PRODUCT_MAP.value:
            return MsdGetProductMap

        if type == ProtectedRoutes.GET_DISTR_MIX.value:
            return MsdGetDistrMix

        if type == ProtectedRoutes.GET_COS_BREAKDOWN.value:
            return MsdGetCosBreakdown

        if type == ProtectedRoutes.GET_COS_BREAKDOWN_TABLE.value:
            return PosBreakDownTables

        if type == ProtectedRoutes.GET_CABIN_MIX.value:
            return MsdGetCabinMix

        if type == ProtectedRoutes.GET_BKG_MIX.value:
            return MsdGetBkgMix

        if type == ProtectedRoutes.GET_KPI.value:
            return BaseMSDFilterParams

        if type == ProtectedRoutes.GET_SLIDER_RANGE.value:
            return RangeSliderValues

        return super().get_validation_class()
