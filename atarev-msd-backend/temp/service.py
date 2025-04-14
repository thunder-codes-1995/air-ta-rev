import re

from flask import request

from base.service import BaseService
from temp.repository import TempRepository

from .handler import FAHandler


class TempService(BaseService):
    repository_class = TempRepository
    handler_class = FAHandler

    def get(self):
        carrier = request.args.get("carrier")
        verbose = request.args.get("verbose", "")
        pipelines = [{"$limit": 100}, {"$project": {"_id": 0}}]
        if carrier:
            pattern = re.compile("TK", re.IGNORECASE)
            pipelines.insert(0, {"$match": {"carrier": {"$regex": pattern}}})

        if not verbose:
            pipelines.append(
                {
                    "$project": {
                        "tariff_num": 1,
                        "carrier": 1,
                        "orig_airport": 1,
                        "orig_country": 1,
                        "dest_airport": 1,
                        "dest_country": 1,
                        "class": 1,
                        "effective_date": 1,
                        "discontinue_date": 1,
                        "rule_number": 1,
                        "routing_number": 1,
                        "type": 1,
                        "source": 1,
                        "orig_fare_amount": 1,
                        "footnote": 1,
                        "directonal_indicator": 1,
                        "global_indicator": 1,
                        "tariff_effective_date": 1,
                        "mpm": 1,
                        "pub_fare_info": 1,
                        "sale_date": 1,
                        "action": 1,
                        "mcn": 1,
                        "link_num": 1,
                        "seq_num": 1,
                        "change_tag": 1,
                        "gfs_filling_advice": 1,
                    }
                }
            )

        df = self._aggregte(pipelines)
        df.columns = df.columns.str.strip()
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # fields
        table = {
            "tariff_num": "TAR #",
            "carrier": "CRX ",
            "orig_airport": "ORG",
            "orig_country": "OCNT",
            "dest_airport": "DES",
            "dest_country": "DCNT",
            "class": "FBASIS",
            "effective_date": "EFFDTE",
            "discontinue_date": "DISDTE",
            "rule_number": "RULE #",
            "routing_number": "RTG",
            "type": "TYPE",
            "source": "SRC ",
            "orig_fare_amount": "AMOUNT OCUR",
            "footnote": "FN",
            "directonal_indicator": "DIR IND",
            "global_indicator": "GLB",
            "tariff_effective_date": "TAR EFDT",
            "mpm": "MPM",
            "pub_fare_info": "PUBTARINF",
            "sale_date": "SALEDT",
            "action": "ACTION",
            "mcn": "MCN",
            "link_num": "LINK #",
            "seq_num": "SEQ #",
            "change_tag": "CHNGTG",
            "gfs_filling_advice": "GFSDLNADV",
        }

        if verbose:
            table = {
                "tariff_num": "TAR #",
                "carrier": "CRX ",
                "orig_airport": "ORG",
                "orig_country": "OCNT",
                "dest_airport": "DES",
                "dest_country": "DCNT",
                "class": "FBASIS",
                "effective_date": "EFFDTE",
                "discontinue_date": "DISDTE",
                "rule_number": "RULE #",
                "routing_number": "RTG",
                "type": "TYPE",
                "source": "SRC ",
                "orig_fare_amount": "AMOUNT OCUR",
                "orig_currency_code": "ORIG CURR CODE",
                "origin_num_of_decimals": "ORIG NUN DEC",
                "dest_fare_amount": "DEST FARE AMT",
                "dest_currency_code": "DEST CURRE CODE",
                "dest_num_of_decimals": "DEST NUN DEC",
                "something": "SOMETHING",
                "footnote": "FN",
                "directonal_indicator": "DIR IND",
                "global_indicator": "GLB",
                "tariff_effective_date": "TAR EFDT",
                "mpm": "MPM",
                "something2": "SOMETHING2",
                "origin_addon_class": "ORIG ADDON CLASS",
                "origin_addon_routing": "ORIG ADDON ROUTING",
                "origin_addon_footnote": "ORIG ADDON FOOTNOTE",
                "origin_addon_gateway": "ORIG ADDON GATEWAY",
                "origin_addon_amount": "ORIG ADDON AMT",
                "dest_addon_class": "DEST ADDON CLASS",
                "dest_addon_routing": "DEST ADDON ROUTING",
                "dest_addon_footnote": "DEST ADDON FOOTNOTE",
                "dest_addon_gateway": "DEST ADDON GATEWAY",
                "dest_addon_amount": "DEST ADDON AMT",
                "pub_fare_info": "PUBTARINF",
                "sale_date": "SALEDT",
                "action": "ACTION",
                "mcn": "MCN",
                "old_mcn": "OLD MCN",
                "something3": "SOMETHING3",
                "link_num": "LINK #",
                "seq_num": "SEQ #",
                "something4": "SOMETHING4",
                "change_tag": "CHNGTG",
                "gfs_filling_advice": "GFSDLNADV",
            }

        return {
            # "data": df.to_dict("records"),
            "data": self.handler.handle_rows(df),
            "table": table,
        }
