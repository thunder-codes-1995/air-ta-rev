class FareQuery:
    def query(self):
        pipelines = [
            {
                "$unwind": "$cabins"
            },
            {
                "$unwind": "$cabins.classes"
            },
            {
                "$unwind": "$cabins.classes.fares"
            },
            {
                "$match": {
                    "cabins.classes.fares.season": {"$in": ["L", "O", "H", "J", "P", "Q", "Z"]}
                }
            },
            {
                "$group": {
                    "_id": {
                        "season": "$cabins.classes.fares.season",
                        "brand": "$brand"
                    },
                    "lowest_fare": {"$min": "$cabins.classes.fares.base_fare"},
                    "fares": {"$push": {
                        "dow": "$cabins.classes.fares.travel_day_of_week",
                        "fare_basis": "$cabins.classes.fares.fare_basis",
                        "ow_rt":"$cabins.classes.fares.ow_rt",
                        "fn": "$cabins.classes.fares.footnote",
                        "currency": "$cabins.classes.fares.currency",
                        "base_fare": "$cabins.classes.fares.base_fare",
                        "q": "$cabins.classes.fares.q",
                        "yq": "$cabins.classes.fares.yq",
                        "yr": "$cabins.classes.fares.yr",
                        "taxes": "$cabins.classes.fares.tax",
                        "all_in": "$cabins.classes.fares.base_fare_yq_yr_tax",
                        "rule": "$cabins.classes.fares.rule",
                        "ap": "cabins.classes.fares.advance_purchase_day",
                        "minst": "$cabins.classes.fares.min_stay",
                        "maxst": "$cabins.classes.fares.max_stay",
                        "ref": "$cabins.classes.fares.refund",
                        "change": "$cabins.classes.fares.change",
                        "eff_dt": "$cabins.classes.fares.effected_date",
                        "dsc_dt": "$cabins.classes.fares.discontinued_date",
                        "fare_type": "$cabins.classes.fares.fare_family",
                        "dow_type": "$cabins.classes.fares.day_type",
                        "dow_outbound": "$cabins.classes.fares.day_of_week_outbound",
                        "dow_inbound": "$cabins.classes.fares.day_of_week_inbound",
                        "blackout_outbound": "$cabins.classes.fares.blackout_outbound",
                        "blackout_inbound": "$cabins.classes.fares.blackout_inbound",
                        "seasonality_inbound": "$cabins.classes.fares.seasonality_inbound",
                        "seasonality_outbound": "$cabins.classes.fares.seasonality_outbound",
                        "routing": "$cabins.classes.fares.routing"
                    }}
                }
            },
            {
                "$sort": {
                    "_id.brand": 1
                }
            },
            {
                "$group": {
                    "_id": "$_id.season",
                    "season": {"$first": "$_id.season"},
                    "brands": {
                        "$push": {
                            "brand": "$_id.brand",
                            "lowest_fare": "$lowest_fare",
                            "fares": "$fares"
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "season": 1,
                    "brands": 1
                }
            }
        ]
        return pipelines
