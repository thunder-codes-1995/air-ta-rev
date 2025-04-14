import argparse
import datetime
import csv
import os
import sys
from os import path
from dataclasses import dataclass

from jobs.lib.utils.mongo_wrapper import MongoWrapper
from typing import Optional
#import logging

#logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FareExportFilter:
    """Filter used to narrow down data which needs to be exported"""
    host_carrier_code: Optional[str]    # required, customer code
    scrape_date: Optional[str] = None         # required, scrape date (YYYYMMDD format)
    origin: Optional[str] = None               # optional, origin airport code to export fares from (e.g. LAX)
    destination: Optional[str] = None         # optional, destination airport code to export fares from (e.g. JFK)
    file_suffix: Optional[str] = None         # optional, destination airport code to export fares from (e.g. JFK)

    def __post_init__(self):
        if not self.host_carrier_code:
            raise ValueError("host_carrier_code is required")
        #check if scrape_date is in YYYYMMDD format and if it's a valid date, in the past
        try:
            datetime.datetime.strptime(self.scrape_date, '%Y%m%d')
        except ValueError:
            raise ValueError("Incorrect scrape_date format, should be YYYYMMDD")

def date_to_numeric(date):
    # convert date to numeric representation(YYYYMMDD format)
    return int(date.strftime("%Y%m%d"))

def numeric_to_date(date_int):
    # convert numeric date(YYYYMMDD format) into date
    return datetime.datetime.strptime(str(date_int), "%Y%m%d")

def cabin_name_to_code(cabin_name):
    if cabin_name == "Economy":
        return "Y"
    elif cabin_name == "Business":
        return "C"
    elif cabin_name == "First":
        return "F"
    else:
        return cabin_name

def numeric_to_datetime(date_int, time_int):
    # convert numeric date(YYYYMMDD format) into date
    str_dt: str = str(date_int)
    str_time: str = str(time_int)
    str_time = str_time.zfill(4)
    year, month, day = str_dt[0:4], str_dt[4:6], str_dt[6:8]
    hour, min = str_time[0:2], str_time[2:4]
    return datetime.datetime(
        year=int(year),
        month=int(month),
        day=int(day),
        hour=int(hour),
        minute=int(min),
    )

def format_leg_flight_number(leg:dict)->str:
    if leg.get('opAlCode') is not None:
        return f"{leg.get('opAlCode')}{leg.get('opFltNum')}"
    if leg.get('mkAlCode') is not None:
        return f"{leg.get('mkAlCode')}{leg.get('mkFltNum')}"
    return ''

def normalize_aircraft_type(aircraft_type:str)->str:
    if aircraft_type is None or aircraft_type == 'UNKNOWN':
        return ''
    return aircraft_type

class DataExtractor:

    def __init__(self, filter: FareExportFilter):
        self.db = MongoWrapper()
        self.filter = filter

    def get_competitors_iterator(self):
        """
            Create a mongo query to get list of flights (host and competitors) based on the data scraped from websites
            Returns iterator over mongo documents
        :return: mongo iterator
        """
        # we don't want to export all historical data(scraped e.g. 2 months ago), instead we want to only data that was just recently scraped (parameter driven)
        # we want to get only fares scraped on a date provided by the user
        min_scrape_datetime = datetime.datetime.strptime(self.filter.scrape_date, '%Y%m%d')
        max_scrape_datetime = min_scrape_datetime + datetime.timedelta(days=1)

        # min_scrape_datetime = datetime.datetime.today() - datetime.timedelta(hours=self.filter.scrape_date)
        # we also don't want to export data for flights that are departing in the past
        min_departure_time = date_to_numeric(datetime.datetime.today())
        criteria = {
            'outboundDate': {
                '$gte': min_departure_time
            },
            'hostCode': self.filter.host_carrier_code,
            '$and': [
                {'lowestFares.scrapeTime':{'$gte': min_scrape_datetime}},
                {'lowestFares.scrapeTime':{'$lt': max_scrape_datetime}},
            ]
            # 'type':'OW'
        }
        print("criteria", criteria)
        it = self.db.col_fares_processed().aggregate([
            {
                '$match': criteria
            },
            {
                '$project': {
                    'historicalFares': 0,
                }
            }
        ])
        return it


class DataExporter:
    def __init__(self, filter: FareExportFilter):
        self.filter = filter
    def export_competitors_to_csv(self, data_iterator, competitors_output_file_name:str = 'competitors.csv', competitors_fares_output_file_name:str = 'competitor_fares.csv'):
        # which fields should be in CSV files for competitors
        competitors_csv_field_names = ['origin', 'destination','carrier_code','dep_datetime','flight_number','arr_datetime','aircraft type','observed_at','source']
        # .... and fares
        competitor_fares_csv_field_names = ['origin','destination','carrier_code','outbound_dep_datetime','outbound_flight_numbers','outbound_cabin_classes','inbound_dep_datetime','inbound_flight_numbers','inbound_cabin_classes','observed_at','outbound_arr_datetime','inbound_arr_datetime','surcharges','outbound_base_fare','inbound_base_fare','outbound_fare_exc_tax','inbound_fare_exc_tax','tax','currency','trip_type','outbound_stopover','outbound_stopover_count','inbound_stopover','inbound_stopover_count','source']
        with open(competitors_output_file_name, 'w') as csv_competitors_file:
            with open(competitors_fares_output_file_name, 'w') as csv_fares_file:
                competitors_csv_writer = csv.DictWriter(csv_competitors_file, fieldnames=competitors_csv_field_names, extrasaction='ignore')
                fares_csv_writer = csv.DictWriter(csv_fares_file, fieldnames=competitor_fares_csv_field_names, extrasaction='ignore')
                competitors_csv_writer.writeheader()
                fares_csv_writer.writeheader()
                for record in data_iterator:
                    if self.is_record_to_be_ignored(record):
                        continue
                    competitor_data_rows = self._create_competitors_records(record)
                    competitors_csv_writer.writerows(competitor_data_rows)
                    fare_data_rows = self._create_fare_records(record)
                    fares_csv_writer.writerows(fare_data_rows)

    def is_record_to_be_ignored(self, record):
        """ check if record should be ignored (e.g. if it doesn't have any itineraries)"""
        if record.get('itineraries') is None:
            return True
        itineraries = record.get('itineraries',[])
        if len(itineraries) == 0:
            return True
        # if there are no legs, then we can't export this record - ignore it
        if itineraries[0].get('legs') is None:
            return True
        legs = itineraries[0].get('legs',[])
        if len(legs) == 0:
            return True
        return False

    def _create_competitors_records(self, row):
        """ convert raw record from fares2 collection to a list of flat records that will later be written to competitors CSV file"""
        itineraries = row.get('itineraries',[])
        legs = itineraries[0].get('legs',[])
        first_leg = legs[0]
        lowest_fares = row.get('lowestFares',[])
        # if there's no fares - return empty list
        if len(lowest_fares) == 0:
            return []

        # we dont want to have host flights in competitors CSV file
        if first_leg['opAlCode'] == self.filter.host_carrier_code:
            return []

        # sort fares by scrape time, so that the most recent fare is the first one
        lowest_fares.sort(key=lambda fare: fare.get('scrapeTime'), reverse=True)
        most_recent_fare = lowest_fares[0]
        source = most_recent_fare.get('scrapedFrom','unknown')
        observed_at = most_recent_fare.get('scrapeTime', None)
        observed_at = observed_at.replace(microsecond=0, tzinfo=None)
        record = {
            'origin':first_leg['legOriginCode'],
            'destination':first_leg['legDestCode'],
            'carrier_code':first_leg['opAlCode'],
            'dep_datetime':numeric_to_datetime(first_leg['legDeptDate'], first_leg['legDeptTime']),
            'flight_number':format_leg_flight_number(first_leg),
            'arr_datetime':numeric_to_datetime(first_leg['legArrivalDate'], first_leg['legArrivalTime']),
            'aircraft type':normalize_aircraft_type(first_leg['aircraft']),
            'observed_at':observed_at,
            'source':source
        }
        return [record]

    def _create_fare_records(self, row):
        """ convert raw record from fares2 collection to a list of flat records (dict) that will later be written to fares CSV file"""
        itineraries = row.get('itineraries',[])
        outbound_itinerary = itineraries[0]
        outbound_legs = outbound_itinerary.get('legs',[])

        inbound_itinerary = None
        inbound_legs = []

        if row.get('type') == 'RT' and len(itineraries) > 1:
            inbound_itinerary = itineraries[1]
            inbound_legs = inbound_itinerary.get('legs',[])
        lowest_fares = row.get('lowestFares',[])
        if len(lowest_fares) == 0:
            return []
        results = []
        for fare in lowest_fares:
            observed_at = fare.get('scrapeTime', None)
            observed_at = observed_at.replace(microsecond=0, tzinfo=None)
            outbound_stopover_airports=[leg['legOriginCode'] for leg in outbound_legs[1:]]
            inbound_stopover_airports=[leg['legOriginCode'] for leg in inbound_legs[1:]]
            record = {
                'origin':outbound_itinerary['itinOriginCode'],
                'destination':outbound_itinerary['itinDestCode'],
                'carrier_code':row['carrierCode'],
                'outbound_dep_datetime':numeric_to_datetime(outbound_itinerary['itinDeptDate'], outbound_itinerary['itinDeptTime']),
                'outbound_flight_numbers':','.join([format_leg_flight_number(leg) for leg in outbound_legs]),
                'outbound_cabin_classes':cabin_name_to_code(fare['cabinName']),
                'inbound_dep_datetime':numeric_to_datetime(inbound_itinerary['itinDeptDate'], inbound_itinerary['itinDeptTime']) if inbound_itinerary is not None else None,
                'inbound_flight_numbers':','.join([format_leg_flight_number(leg) for leg in inbound_legs]) if inbound_itinerary is not None else None,
                'inbound_cabin_classes':cabin_name_to_code(fare['cabinName'])  if inbound_itinerary is not None else None,
                'observed_at':observed_at,
                'outbound_arr_datetime':numeric_to_datetime(outbound_itinerary['itinArrivalDate'], outbound_itinerary['itinArrivalTime']),
                'inbound_arr_datetime':numeric_to_datetime(inbound_itinerary['itinArrivalDate'], inbound_itinerary['itinArrivalTime']) if inbound_itinerary is not None else None,
                'surcharges':round(fare['yqyrAmount'],0),
                'outbound_base_fare':round(fare['baseFare'],2),
                'inbound_base_fare':round(fare['baseFare'],2) if inbound_itinerary is not None else 0,
                'outbound_fare_exc_tax':round(fare['fareAmount']-fare['taxAmount'],2),
                'inbound_fare_exc_tax':round(fare['fareAmount']-fare['taxAmount'],2) if inbound_itinerary is not None else 0,
                'tax':round(fare['taxAmount'],2),
                'currency':fare['fareCurrency'],
                'trip_type':row['type'],
                'outbound_stopover':','.join(outbound_stopover_airports),
                'outbound_stopover_count':len(outbound_stopover_airports),
                'inbound_stopover':','.join(inbound_stopover_airports),
                'inbound_stopover_count':len(inbound_stopover_airports),
                'source':fare['scrapedFrom']
            }
            results.append(record)

        return results


def parse_args():
    """ Parse command line arguments and create a filter object"""
    parser = argparse.ArgumentParser(
        description='Extract competitors schedule and scraped fares and store it in CSV files')
    parser.add_argument('host_carrier_code', type=str, help='Customer carrier code (e.g. CY or PY)')
    parser.add_argument('scrape_date', type=str, help='Date of scraping in YYYYMMDD format')
    parser.add_argument('file_suffix', type=str, help='Date of file export in YYYYMMDD format')
    parser.add_argument('--origin', type=str, help='Origin airport code (e.g. LCA)')
    parser.add_argument('--destination', type=str, help='Destination airport code (e.g. ATH)')
    arguments = parser.parse_args(sys.argv[1:])
    _filter = FareExportFilter(host_carrier_code=arguments.host_carrier_code, origin=arguments.origin, destination=arguments.destination, scrape_date=arguments.scrape_date, file_suffix=arguments.file_suffix)
    return _filter

if __name__ == "__main__":

    #parse arguments and create a filter object from that
    filter = parse_args()
    #get data from MongoDB using filter
    data_iterator = DataExtractor(filter).get_competitors_iterator()
    exporter = DataExporter(filter)
    file_suffix = filter.file_suffix if filter.file_suffix is None else datetime.date.today().strftime('%Y%m%d')
    #export data to CSV files
    #

    output_folder=os.getenv("FARE_EXPORT_OUTPUT_FOLDER", "/tmp")
    competitors_filename=path.join(output_folder,f"competitors_{file_suffix}.csv")
    comp_fares_filename=path.join(output_folder,f"competitor_fares_{file_suffix}.csv")
    print(f"Output folder:{output_folder}")
    print(f"Competitors file:{competitors_filename}")
    print(f"Competitors fares file:{comp_fares_filename}")

    exporter.export_competitors_to_csv(data_iterator, competitors_filename, comp_fares_filename)