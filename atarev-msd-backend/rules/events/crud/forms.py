from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, model_validator

from airports.repository import AirportRepository
from base.helpers.cabin import CabinMapper

airport_repo = AirportRepository()

class CreateEventRuleForm(BaseModel):
    name: str
    note: Optional[str] = None
    description: Optional[str] = None
    action: Optional[str] = None
    country_code: str
    start_date: Optional[int] = None
    end_date: Optional[int] = None
    event_id: Optional[List[str]] = None
    pickup_lf: List[int] = Field(..., min_items=2, max_items=2)
    cabin: str
    id: Optional[str] = None

    @model_validator(mode='before')
    @classmethod
    def validation(cls, values):

        ### Validate event_id
        if not values.get('start_date') or not values.get('end_date'):
            if not values.get("event_id"):
                raise ValueError(
                    'Event ID is required if start_date and end_date are not provided')

        ### Validate cabin
        cabin = CabinMapper.normalize(values.get('cabin'))
        values["cabin"] = cabin

        ### Validate pickup_lf
        valid_values = [365, 270, 180, 90, 30, 15, 7, 6, 5, 4, 3, 2, 1]
        pickup_lf = values.get('pickup_lf')
        if pickup_lf:
            if pickup_lf[0] not in valid_values or pickup_lf[1] not in valid_values:
                raise ValueError(
                    'Invalid pickup_lf values. Values must be in [365, 270, 180, 90, 30, 15, 7, 6, 5, 4, 3, 2, 1]')
            if valid_values.index(pickup_lf[0]) != valid_values.index(pickup_lf[1]) - 1:
                raise ValueError(
                    'Invalid pickup_lf values. Values must be consecutive order')

        ### Validate country_code
        country = values.get('country_code')
        if country:
            res = airport_repo.find_one({"country_code": country})
            if res is None:
                raise ValueError('Invalid country_code')

        ### Validate dates
        start_date = values.get('start_date')
        end_date = values.get('end_date')
        if start_date and end_date:
            try:
                s_obj = datetime.strptime(str(start_date), '%Y%m%d')
                e_obj = datetime.strptime(str(end_date), '%Y%m%d')


                
            except ValueError:
                raise ValueError("date must be in the format YYYYMMDD")

            if int(start_date) < int(datetime.now().strftime("%Y%m%d")) and not values.get("id"):
                raise ValueError("start_date must be later than today's date")

            if start_date > end_date:
                raise ValueError("end_date must be later than start_date")

        return values

