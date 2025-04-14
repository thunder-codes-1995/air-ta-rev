import os
import sys
from dataclasses import asdict
from datetime import datetime
project_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(project_root, '..', '..', '..'))
sys.path.append(project_root)
from validators import Leg, Cabin, Flight


class Parser:
    def __init__(self, date):
        self.date = int(date)
        self.flights = []

    def convert_data_to_dict(self, chunk):
        flight = dict()
        for i in chunk.split("\x1c"):
            groups = i.split("\x1d")
            label = groups[0]
            values = " ".join(groups[1:]).strip()
            if values == "" or label == "":
                continue
            if label == "EQI":
                if "\x1f" not in values:
                    values = "/"
            if "\x1f" in values:
                values = values.replace("\x1f", " ")
            if flight.get(label):
                flight[label].append(values)
            else:
                flight[label] = [values]
        return flight

    def open_file(self):
        with open("tmp/file.DATA", encoding="ascii") as f:
            while True:
                data = f.read(1000000)
                a = data.split("FDR")
                if data == "":
                    break
                del a[0]
                del a[-1]
                for i in a:
                    i = "FDR" + i
                    fl = self.convert_data_to_dict(i)
                    self.flights.append(fl)


    def convert_date(self, date_str):
        date_obj = datetime.strptime(date_str, '%d%m%y')

        return int(date_obj.strftime('%Y%m%d'))
    def parse(self):
        self.open_file()
        print("Parsing started..")
        hitits = []
        for flight_data in self.flights:
            origin = flight_data["APD"][0].split(" ")[0]
            destination = flight_data["APD"][-1].split(" ")[-1]
            flight_number = int(flight_data["FDR"][0].split(" ")[1])

            dp_date = self.convert_date(flight_data["FDR"][0].split(" ")[-1])

            flight = Flight(
                airline_code=flight_data["FDR"][0].split(" ")[0],
                date=int(datetime.strptime(str(self.date), '%y%m%d').strftime('%Y%m%d')),
                departure_date=dp_date,
                destination=destination,
                flight_number=flight_number,
                origin=origin,
                departure_time=int(flight_data["DAT"][0].split()[2]),
                flight_departure_date=dp_date
            )
            for leg_idx, leg in enumerate(flight_data["APD"]):
                leg_data = {
                    "origin": leg.split(" ")[0],
                    "destination": leg.split(" ")[1],
                    "flight_number": flight_number,
                    "departure_date": self.convert_date(flight_data["DAT"][leg_idx].split(" ")[1]),
                    "departure_time": int(flight_data["DAT"][0].split(" ")[2]),
                    "cabins": [],
                    "flight_number_ext": "-",
                    "load_factor": 0,
                    "arrival_date": None,
                    "arrival_time": None
                }
                leg_cap = 0
                leg_book = 0
                leg_obj = Leg(**leg_data)
                for eqi_idx, cabin in enumerate(self.seperate_eqi(flight_data["EQI"])[leg_idx]):
                    total_booking = int(flight_data["CBA"][leg_idx + eqi_idx].split(" ")[1])
                    capacity = int(cabin.split(" ")[10])

                    leg_cap += capacity
                    leg_book += total_booking

                    try:
                        lf = int((total_booking / capacity) * 100)
                    except ZeroDivisionError:
                        lf = 0
                    cabin_data = {
                        "code": cabin.split(" ")[0],
                        "total_booking": total_booking,
                        "capacity": capacity,
                        "load_factor": lf
                    }

                    cabin_obj = Cabin(**cabin_data)
                    leg_obj.cabins.append(cabin_obj)

                try:
                    leg_lf = int((leg_book / leg_cap) * 100)
                except ZeroDivisionError:
                    leg_lf = 0
                leg_obj.load_factor = leg_lf
                flight.legs.append(leg_obj)

            hitits.append(asdict(flight))

        return hitits

    def seperate_eqi(self, eqi):
        eqi_seperated = []
        tmp_list = []
        for eleman in eqi:
            if eleman == '/':
                if tmp_list:
                    eqi_seperated.append(tmp_list)
                    tmp_list = []
            else:
                tmp_list.append(eleman)
        if tmp_list:
            eqi_seperated.append(tmp_list)
        return eqi_seperated




