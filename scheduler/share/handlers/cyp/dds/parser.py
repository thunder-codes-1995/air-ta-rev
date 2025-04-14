import os
from dataclasses import dataclass
from datetime import datetime

from __handlers.cyp.dds.source import DDsSource
from core.checker import Check
from core.db import Collection
from core.logger import Logger
from core.stream import Stream
from core.validators import Validator, is_reprsenting_date
from pydantic import BaseModel, field_validator

logger = Logger(f"{os.getenv('LOG_FOLDER')}/dds.log")

none_values = [
    "pnr_idx",
    "agency_id",
    "agency_name",
    "agency_city",
    "agency_country",
    "agency_continent",
    "distribution_channel",
    "stop_1",
    "duration",
    "blended_fare",
    "blended_rev",
    "bound",
    "dep_time_block",
    "prev_dest",
    "next_dest",
    "is_ticket",
    "is_historical",
]


class FlightRecord(BaseModel):
    travel_date: int
    travel_month: int
    travel_year: int
    sell_date: int
    rbkd: str
    orig_code: str
    dest_code: str
    fare_basis: str
    op_flt_num: int
    dom_op_al_code: str
    is_group: bool
    ticket_type: str
    days_sold_prior_to_travel: int
    local_dep_time: int
    city_orig_code: str
    city_dest_code: str
    seg_class: str
    equip: str
    pax: float
    avg_fare: float
    sell_year: int
    sell_month: int
    cos_norm: str
    country_of_sale: str
    avf_fare_curr: str
    travel_day_of_week: int

    @field_validator("travel_date")
    def validate_travel_date(cls, value: int) -> int:
        assert is_reprsenting_date(value)
        return value

    @field_validator("sell_date")
    def validate_sell_date(cls, value: int) -> int:
        assert is_reprsenting_date(value)
        return value

    @field_validator("rbkd")
    def validate_rbkd(cls, value: str) -> str:
        Validator.length(value, 1)
        return value

    @field_validator("seg_class")
    def validate_seg_class(cls, value: str) -> str:
        Validator.length(value, 1)
        return value

    @field_validator("orig_code")
    def validate_orig_code(cls, value: str) -> str:
        Validator.length(value, 3)
        return value

    @field_validator("dest_code")
    def validate_dest_code(cls, value: str) -> str:
        Validator.length(value, 3)
        return value

    @field_validator("city_orig_code")
    def validate_city_orig_code(cls, value: str) -> str:
        Validator.length(value, 3)
        return value

    @field_validator("city_dest_code")
    def validate_city_dest_code(cls, value: str) -> str:
        Validator.length(value, 3)
        return value

    @field_validator("fare_basis")
    def validate_fare_basis(cls, value: str) -> str:
        Validator.min_length(value, 1)
        return value

    @field_validator("equip")
    def validate_equip(cls, value: str) -> str:
        Validator.min_length(value, 1)
        return value

    @field_validator("dom_op_al_code")
    def validate_dom_op_al_code(cls, value: str) -> str:
        Validator.min_length(value, 2)
        return value

    @field_validator("ticket_type")
    def validate_ticket_type(cls, value: str) -> str:
        Validator.is_in(value, ["OW", "RT"])
        return value

    @field_validator("local_dep_time")
    def validate_local_dep_time(cls, value: int) -> int:
        Validator.min_length(str(value), 2)
        Validator.max_length(str(value), 4)
        return value

    @field_validator("sell_year")
    def validate_sell_year(cls, value: int) -> int:
        Validator.length(str(value), 4)
        return value

    @field_validator("travel_year")
    def validate_travel_year(cls, value: int) -> int:
        Validator.length(str(value), 4)
        return value

    @field_validator("travel_month")
    def validate_travel_month(cls, value: int) -> int:
        Validator.min_length(str(value), 1)
        Validator.max_length(str(value), 2)
        return value

    @field_validator("sell_month")
    def validate_sell_month(cls, value: int) -> int:
        Validator.min_length(str(value), 1)
        Validator.max_length(str(value), 2)
        return value


@dataclass
class Handler:
    date: int
    path: str

    def __post_init__(self):
        try:
            data = DDsSource(self.date, self.path).data
            check = Check(data, "dds.log")
            check.is_empty(f"dds.{self.date}.csv", " (CY DDS)")
            self.data = data.iterrows()

        except FileNotFoundError:
            logger.error(f"file dds.{self.date}.csv was not found (CY DDS)")

    def parse(self):
        stream = Stream(Collection.DDS)
        dt = int(datetime.now().strftime("%Y%m%d%H%M%S"))

        for _, data in self.data:
            none_obj = {k: None for k in none_values}
            obj = {**none_obj, **dict(FlightRecord(**data)), "is_direct": True, "insert_timestamp": dt}

            stream.update(
                obj,
                {
                    "dom_op_al_code": obj["dom_op_al_code"],
                    "orig_code": obj["orig_code"],
                    "dest_code": obj["dest_code"],
                    "travel_date": obj["travel_date"],
                    "seg_class": obj["seg_class"],
                    "rbkd": obj["rbkd"],
                    "op_flt_num": obj["op_flt_num"],
                    "ticket_type": obj["ticket_type"],
                },
                upsert=True,
            )

        stream.update(upsert=True)
        logger.info("CY DDs data has been uploaded successfully !")


if __name__ == "__main__":
    Handler(20240328, "share/handlers/cyp/samples").parse()
