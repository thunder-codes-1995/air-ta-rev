import os
from datetime import datetime
from enum import Enum

import jwt
from dotenv import load_dotenv
from flask import request

from .errors import ExpiredToken, ProtectedRouter
from .user import User

load_dotenv()


class SkipVersioningRoutes(Enum):
    GET_SCRAPER_HEALTH = "scraper-health"


class ProtectedRoutes(Enum):
    GET_CONFIGURATION = "configuration"
    GET_HOSTS = "hosts"
    GET_POS = "pos"
    GET_CURRENCIES = "currencies"
    GET_CUSTOMERS = "customers"

    FILTER_MARKET = "filter-market"
    CONFIG_MARKET = "config-market"
    GET_CONFIG_MARKET_OPTIONS = "config-market-options"
    GET_FILTER_MARKET_OPTIONS = "filter-market-options"

    GET_COS_BREAKDOWN = "cos-breakdown"
    GET_COS_BREAKDOWN_TABLE = "cos-breakdown-table"
    GET_PRODUCT_MAP = "product-map"
    GET_DISTR_MIX = "channel-mix"
    GET_CABIN_MIX = "cabin-mix"
    GET_BKG_MIX = "pax-mix"
    GET_PRODUCT_MATRIX = "product-matrix"
    GET_SLIDER_RANGE = "get-range-slider-values"
    GET_CARRIERS = "carriers"
    GET_KPI = "kpi"

    GET_FARE_BOOKING_HISTOGRAM = "fare-booking-histogram"
    GET_FARE_TRENDS = "fare-trends"
    GET_FARE_REVENUE_DOWN_REVENUE = "dow-revenue"
    GET_RBD_ELASTIC_CITIES = "rbd-elasticities"
    GET_FARE_REVENUE_CLASS_MIX = "class-mix"
    GET_FARE_REVENUE_TRENDS = "revenue-trends"

    GET_MARKET_SHARE_TRENDS = "trends"
    GET_MARKET_SHARE_AVG_FARE = "share-vs-fare"

    GET_AGENCY_TABLE = "agency-table"
    GET_AGENCY_QUADRANT = "agency-quadrant"
    GET_AGENCY_GRAPHS = "agency-graphs"

    GET_NETWORK_SCHEDULING_BEYOND_POINTS = "beyond-points"
    GET_NETWORK_SCHEDULING_COMPARISON_DETAILS = "scheduling-comparison-details"
    GET_NETWORK_SCHEDULING_CONICTIVITY_MAP = "connectivity-map-table"

    GET_BOOKING_CURVE = "booking-curve"
    GET_BOOKING_COUNTRY_HOLIDAY_OPTIONS = "holiday-country-options"

    GET_CUSTOMER_SEGMENTATION_TABLE = "segmentation-table"
    GET_CUSTOMER_SEGMENTION_GRAPHS = "segmentation-graphs"
    GET_CUSTOMER_SEGMENTION_TEXT = "segmentation-text"

    GET_FILTER_OPTIONS = "msd-filter-options"
    GET_MARKET_OPTIONS_BY_KEYWORD = "markets-by-keyword"
    GET_CUSTOMER_MARKETS = "customer-markets"
    UPDATE_USER_MODULE = "module"
    INVENTORY_CHANGES_REPORT = "inventory-changes"
    ADD_USER_VALUE = "store-value"
    STORE_EVENT = "store"


class FreeRoutes(Enum):
    GET_HEALTCHECK = "healthcheck"
    EVALUATE_FLIGHTS = "evaluate-flights"
    EVALUATE_ALERTS = "evaluate-events"


class Route:
    @classmethod
    def authorize(cls):
        if cls.__skip_all_auth():
            return

        endpoint: str = request.path.split("/")[-1]

        if endpoint in [route.value for route in FreeRoutes]:
            return
        if request.method == "OPTIONS":
            return
        if not request.headers.get("Authorization"):
            # check if request headers containt jwt token
            raise ProtectedRouter()

        request.user = cls.__get_user()

    @classmethod
    def __skip_all_auth(cls):
        return os.getenv("AUTH_ENABLED") == "False"

    @classmethod
    def __get_token(cls) -> str:
        return request.headers["Authorization"].split(" ")[1]

    @classmethod
    def __get_user(cls) -> User:
        try:
            token = cls.__get_token()
            data = jwt.decode(token, os.getenv("SECRET_KEY"), algorithms="HS256")
            cls.__check_expiry(data)
            cls.__check_version(data)
            assert bool(data.get("username")) and bool(
                data.get("carrier")
            ), "either username or carrier is not available for user"
            return User(data.get("username"), data.get("carrier"), True)
        except jwt.exceptions.InvalidSignatureError:
            raise ExpiredToken()

    @classmethod
    def __check_expiry(cls, token):
        now = datetime.now()
        expires_at = datetime.fromtimestamp(token["exp"])
        if now >= expires_at:
            raise ExpiredToken()

    @classmethod
    def __check_version(cls, token):
        endpoint = request.path.split("/")[-1]

        if os.getenv("SUPPORT_VERSIONING") == "False":
            return

        if endpoint in [route.value for route in SkipVersioningRoutes]:
            return

        version = token.get("v")
        current_version = float(os.getenv("APP_VERSION"))
        if not version or int(float(version)) != int(current_version):
            raise ExpiredToken()
