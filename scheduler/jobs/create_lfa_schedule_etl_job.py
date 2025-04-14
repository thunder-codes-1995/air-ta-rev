import logging
import sys

from jobs.base_scraped_fares_processor import BaseScrapedFareProcessingJob, ScrapedFaresProcessorFilterArguments
from jobs.lib.fares.fare_amount_extractor import FareAmountExtractor
from jobs.lib.fares.fare_record_converter import FareRecordConverter
from jobs.lib.utils.currency_converter import CurrencyConverter
from jobs.lib.utils.logger import setup_logging
from pymongo import UpdateOne

setup_logging()
logger = logging.getLogger("lfa_schedule_etl")


class CreateLFAScheduleFromScrapedFares(BaseScrapedFareProcessingJob):
    """This job processes scraped and transformed fares and creates LFA schedule from that"""

    def __init__(self, args: ScrapedFaresProcessorFilterArguments):
        super().__init__(args)
        self.currency_converter = CurrencyConverter()
        self.fare_record_converter = FareRecordConverter()
        self.fare_amount_extractor = FareAmountExtractor()

    def get_target_collection(self):
        return self.get_db_wrapper().col_lfa_schedule()

    @staticmethod
    def create_flight_key(leg):
        al_code = leg["mkAlCode"]
        if al_code is None:
            al_code = leg["mkAlCode"]
        leg_dept_date = 20000000
        if "legDepartureDate" in leg:
            leg_dept_date = leg["legDepartureDate"]
        else:
            leg_dept_date = leg["legDeptDate"]
        return f"{al_code}{leg['mkFltNum']}|{leg_dept_date}|{leg['legOriginCode']}{leg['legDestCode']}"

    @staticmethod
    def split_date(date_as_int):
        d = f"{date_as_int}"
        return int(d[0:4]), int(d[4:6]), int(d[6:8])

    @staticmethod
    def get_cabins(item):
        return ["BUSINESS", "ECONOMY"]

    @staticmethod
    def get_classes(item, cabin_code):
        if cabin_code == "BUSINESS":
            return ["C", "J", "F", "Z", "I"]
        if cabin_code == "ECONOMY":
            return ["Y", "S", "B", "M", "L", "H", "K", "Q", "E", "O", "A", "T", "V", "P", "X", "U", "W", "G", "D"]
        return []

    def process_single_record(self, item, *args):
        # convert 'itineraries' property into schedule
        for itinerary in item["itineraries"]:
            for leg in itinerary["legs"]:
                (arr_y, arr_m, arr_d) = self.split_date(leg["legArrivalDate"])
                leg_dept_date = 20000000  # below hack to handle some records with legDeptDate and some with legDepartureDate
                leg_dept_time = 0
                if "legDepartureDate" in leg:
                    leg_dept_date = leg["legDepartureDate"]
                    leg_dept_time = leg["legDepartureTime"]
                else:
                    leg_dept_date = leg["legDeptDate"]
                    leg_dept_time = leg["legDeptTime"]
                (dep_y, dep_m, dep_d) = self.split_date(leg_dept_date)
                new_leg = {
                    "flightKey": self.create_flight_key(leg),
                    "arrivalDate": int(leg["legArrivalDate"]),
                    "arrivalDay": arr_d,
                    "arrivalMonth": arr_m,
                    "arrivalTime": int(leg["legArrivalTime"]),
                    "arrivalYear": arr_y,
                    "cabins": [],
                    "deptDate": int(leg_dept_date),
                    "deptDay": dep_d,
                    "deptMonth": dep_m,
                    "deptTime": int(leg_dept_time),
                    "deptYear": dep_y,
                    "destCode": leg["legDestCode"],
                    "opAlCode": leg["opAlCode"],
                    "opFltNum": leg["mkFltNum"],
                    "originCode": leg["legOriginCode"],
                }
                for cabin_code in self.get_cabins(item):
                    cabin_record = {"code": cabin_code, "bookedLF": 0, "expectedLF": 0, "averageLF": 0, "classes": []}
                    for class_code in self.get_classes(item, cabin_code):
                        class_record = {"cls": class_code, "sa": 0, "bk": 0}
                        cabin_record["classes"].append(class_record)
                    new_leg["cabins"].append(cabin_record)
                    rec = UpdateOne({"flightKey": new_leg["flightKey"]}, {"$set": new_leg}, upsert=True)
                    self.insert(rec)


if __name__ == "__main__":
    args = sys.argv[1:]
    logger.info(f"Starting LFA_SCHEDULE_ETL job, arguments:{args}")
    parsed_args = ScrapedFaresProcessorFilterArguments.from_cli(args)
    handler = CreateLFAScheduleFromScrapedFares(parsed_args)
    handler.process()
