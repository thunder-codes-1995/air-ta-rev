from jobs.lib.utils.mongo_wrapper import MongoWrapper


class GroupEvents:
    def __init__(self):
        self.db = MongoWrapper()

    def group(self):
        # Aggregate to find matching events
        pipeline = [
            {"$group": {
                "_id": {"airline_code": "$airline_code", "event_name": "$event_name"},
                "count": {"$sum": 1}
            }},
            {"$match": {
                "count": {"$gt": 1}
            }},
            {"$project": {
                "_id": 0,
                "airline_code": "$_id.airline_code",
                "event_name": "$_id.event_name"
            }}
        ]
        matching_events = self.db.col_events().aggregate(pipeline)

        # Get distinct event names
        distinct_event_names = list(set(doc['event_name'] for doc in matching_events))

        # Assign group_id values
        for i, event_name in enumerate(distinct_event_names, start=1):
            self.db.col_events().update_many(
                {"event_name": event_name},
                {"$set": {"group_id": i}}
            )

        # Update non-repeat results group_id to null
        self.db.col_events().update_many(
            {"group_id": {"$exists": False}},
            {"$set": {"group_id": None}}
        )

        print("Group ids updated.")


g = GroupEvents()
g.group()