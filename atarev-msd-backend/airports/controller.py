from airports.service import AirportService
from base.controller import BaseController

airport_service = AirportService


class AirportController(BaseController):

    def get(self, endpoint): ...
