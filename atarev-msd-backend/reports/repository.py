from base.repository import BaseRepository


class AuthResultRepository(BaseRepository):
    collection = "authorization_results"

    def get(self, match=None):
        pipeline = [
            {"$match": match or {}},
            {
                "$project": {
                    "_id": 0,
                    "origin": "$cabin_params.origin",
                    "destination": "$cabin_params.destination",
                    "airline_code": "$cabin_params.airline_code",
                    "cabin_code": "$cabin_params.cabin_code",
                    "flight_number": "$cabin_params.flight_number",
                    "departure_date": {
                        "$concat": [
                            {
                                "$substr": ["$cabin_params.departure_date", 0, 4],
                            },
                            "-",
                            {
                                "$substr": ["$cabin_params.departure_date", 4, 2],
                            },
                            "-",
                            {
                                "$substr": ["$cabin_params.departure_date", 6, 2],
                            },
                        ],
                    },
                    "old_class_code": "$old_authorization_value.class_code",
                    "old_rank": "$old_authorization_value.rank",
                    "old_authorization": "$old_authorization_value.authorization",
                    "class_code": "$authorization_value.class_code",
                    "rank": "$authorization_value.rank",
                    "authorization": "$authorization_value.authorization",
                }
            },
        ]

        return self.aggregate(pipeline)
