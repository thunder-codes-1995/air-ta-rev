from jobs.lib.utils.mongo_wrapper import MongoWrapper


class ConfigurationRepository:
    @classmethod
    def get_config_by_key(cls, name: str, host: str):
        result = list(
            (
                MongoWrapper()
                .col_msd_config()
                .aggregate(
                    [
                        {"$match": {"customer": host}},
                        {"$unwind": {"path": "$configurationEntries"}},
                        {"$match": {"configurationEntries.key": name}},
                        {"$project": {"_id": 0, "value": "$configurationEntries.value"}},
                    ]
                )
            )
        )
        if not result:
            return
        return result[0]["value"]
