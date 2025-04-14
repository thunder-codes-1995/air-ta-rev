import os
from datetime import datetime

from flask import request

from base.repository import BaseRepository;


class DdsRepository(BaseRepository):
    collection = 'dds_pgs'

 
    def get_msd_carriers(self):
        return {
            'carriers': ['PK', 'EK', 'WY', 'QR'],
            'carrierDetails': {
                'PK': {'color': '#1647fb', 'name': 'Pakistan airways'},
                'EK': {'color': '#1647fb', 'name': 'Emirates Airlines'},
                'WY': {'color': '#1647fb', 'name': 'Oman Air'},
                'QR': {'color': '#1647fb', 'name': 'Qatar Airways'}
            },
            'hostCarrierCode': 'PK'
        }

    def get_msd_config(self):
        return {
            'carrierDetails': {
                'PK': {'color': '#ef4351', 'name': 'Pakistan airways'},
                'EK': {'color': '#8522ff', 'name': 'Emirates Airlines'},
                'WY': {'color': '#0afdf7', 'name': 'Oman Air'},
                'QR': {'color': '#1647fb', 'name': 'Qatar Airways'},
                'VS': {'color': '#5504d9', 'name': 'Virgin Atlantic'},
                'EY': {'color': '#004a49', 'name': 'Etihad'}
            },
            'hostCarrierCode': 'PK'
        }

    def get_main_comp(self, orig, dest):
        dom_op_al_code = self.get_column("dom_op_al_code")
        pax = self.get_column("pax")
        travel_year = self.get_column("travel_year")
        orig_code = self.get_column("orig_code")
        dest_code = self.get_column("dest_code")
        # TODO this one should get today year but we dont have data for 2022 so i'm hard-coding it for now
        # today_date = date.today()
        today_date = datetime.strptime(os.getenv("TODAY"), "%m/%d/%Y").date()
        # TODO this should probably come from JWT(request) and not be env variable
        host = request.user.carrier

        cursor = self.aggregate([
            {
                "$match": {
                    dom_op_al_code: f"{host}",
                    travel_year: today_date.year,
                    orig_code: f"{orig}",
                    dest_code: f"{dest}",
                },
            },
            {
                "$group": {
                    "_id": {
                        dom_op_al_code: f"${dom_op_al_code}"
                    },
                    pax: {"$sum": f"${pax}"}
                },
            },
            {
                "$project": {
                    "_id": 0,
                    dom_op_al_code: f"$_id.{dom_op_al_code}",
                    pax: 1
                }
            }
        ])

        return list(cursor)[0]

    def get_pax_by_field(self, field: str, match={}):
        """ get pax (number of passengers) based on a field """
        cursor = self.aggregate([
            {
                "$match": match
            },
            {
                "$group": {
                    "_id": {
                        f"{field}": f"${field}"
                    },
                    "pax": {"$sum": "$pax"}
                },
            },
            {
                "$project": {
                    "_id": 0,
                    'pax': 1,
                    f"{field}": f"$_id.{field}"
                }
            }
        ])

        return cursor
