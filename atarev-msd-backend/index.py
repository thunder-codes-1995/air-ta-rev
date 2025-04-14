import os

from dotenv import load_dotenv
from flask import Flask, request
from flask_restful import Api

# from admin.controller import AdminController
from agency_analysis.controller import AgencyController
from airports.controller import AirportController
from base.helpers.errors import ExpiredToken
from base.helpers.signal import OnErrorSignal, OnInitSignal, PostRequestSignal, PreRequestSignal
from base.helpers.user import ANON_USER
from booking_trends.controller import BookingTrendsController

# from configurations.controller import ConfigController
from customer_segmentation.controller import CustomerSegmentationController
from dds.controller import DdsController
from events.controller import EventController
from fare_revenue.controller import FareRevenueController
from fares.controller import FareController
from filters.controller import FilterController
from kpi.controller import KPIController
from market_share.controller import MarketShareController
from network.controller import NetworkController
from reports.controller import ReportController
from rules.controller import RuleController
from temp.controller import TempController
from users.controller import UserController
from flask_cors import CORS
from fare_analyzer.controller import FareAnalyzer

load_dotenv()

app = Flask(__name__)
CORS(app)

app.config["SECRET_KEY"] = "key"
api = Api(app, prefix="/api/msdv2/")

# api.add_resource(ConfigController, "configuration/<type>")
api.add_resource(KPIController, "kpi")
api.add_resource(FareRevenueController, "fare-revenue/<endpoint>")
api.add_resource(DdsController, "product-overview/<endpoint>")
api.add_resource(MarketShareController, "market-share/<endpoint>")
api.add_resource(AgencyController, "agency-analysis/<endpoint>")
api.add_resource(NetworkController, "network-scheduling/<endpoint>")
api.add_resource(BookingTrendsController, "booking-trends/<endpoint>")
api.add_resource(CustomerSegmentationController, "customer-segmentation/<endpoint>")
api.add_resource(FareController, "fare-structure/<endpoint>")
api.add_resource(FilterController, "filters/<endpoint>")
api.add_resource(TempController, "temp")
api.add_resource(AirportController, "airports/<endpoint>")
api.add_resource(UserController, "users/<endpoint>")
api.add_resource(ReportController, "reports/<endpoint>")
api.add_resource(EventController, "events/<endpoint>")
api.add_resource(RuleController, "rules/<endpoint>")
api.add_resource(FareAnalyzer, "fare-analyzer/<endpoint>")


def start_app():
    @app.errorhandler(Exception)
    def generic_error_handler(e: Exception):
        if type(e) is ExpiredToken:
            return {"error": str(e)}, 401

        return OnErrorSignal.run(e) or {"error": str(e)}

    @app.before_request
    def before_request():
        request.user = ANON_USER
        PreRequestSignal.run()

    @app.after_request
    def after_request(response):
        PostRequestSignal.run(response)
        return response

    @app.route("/api/msdv2/healthcheck", methods=["GET"])
    def health_check():
        return "OK"

    OnInitSignal.run()
    return app


if __name__ == "__main__":
    app = start_app()
    app_port = os.getenv("APP_PORT")
    app.run(
        host="0.0.0.0",
        port=int(app_port) if app_port else 5000,
        debug=bool(os.getenv("IS_PRODUCTION")),
        use_reloader=True,
    )
