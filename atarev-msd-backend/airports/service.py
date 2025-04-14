from flask import request
from airports.repository import AirportRepository

from base.service import BaseService
from airports.forms import GetCountryCitiesForm


class AirportService(BaseService):
    repository_class = AirportRepository
    
    def get_countries(self):
        return self.repository.get_all_countries()
    
    def get_country_cities(self, form: GetCountryCitiesForm):
        country_code: str = form.country_code.data
        return self.repository.get_country_cities(country_code)
    
