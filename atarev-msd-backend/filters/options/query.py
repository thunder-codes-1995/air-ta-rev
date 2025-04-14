from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Union

from bson import ObjectId


@dataclass
class CategoryOptionsQuery:
    host_code: str
    event_ids: list = None
    location_match : Union[str] = None
    lookup: str = None

    @property
    def query(self):
        if not self.lookup:
            self.lookup = ""
        today = datetime.today()
        past = today - timedelta(days=365)
        future = today + timedelta(days=395)
        date_range = (int(past.strftime("%Y%m%d")), int(future.strftime("%Y%m%d")))
        pipeline = [
            {
                '$match': {
                    'airline_code': self.host_code,
                    "$and": [
                        {"start_date": {"$gte": date_range[0]}},
                        {"start_date": {"$lte": date_range[1]}},
                    ]
                }
            },
            {
                '$match': self.location_match
            },
            {
                '$unwind': {
                    'path': '$categories'
                }
            },
            {
                '$addFields': {
                    'categories.category': {
                        '$trim': {
                            'input': '$categories.category'
                        }
                    },
                    'categories.sub_categories': {
                        '$map': {
                            'input': '$categories.sub_categories',
                            'in': {
                                '$trim': {
                                    'input': '$$this'
                                }
                            }
                        }
                    }
                }
            },
            {
                '$match': {
                    '$or': [
                        {
                            'event_name': {
                                '$regex': f'^{self.lookup}',
                                '$options': 'i'
                            }
                        }, {
                            'categories.category': {
                                '$regex': f'^{self.lookup}',
                                '$options': 'i'
                            }
                        }, {
                            'categories.sub_categories': {
                                '$regex': f'^{self.lookup}',
                                '$options': 'i'
                            }
                        }
                    ]
                }
            },
            {
                '$project': {
                    'category': '$categories.category',
                    'sub_category': '$categories.sub_categories',
                    'events': '$$ROOT'
                }
            },
            {
                '$unwind': {
                    'path': '$sub_category',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$addFields': {
                    'event_id_str': {
                        '$toString': '$_id'
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'category': '$category',
                        'sub_category': {
                            '$ifNull': [
                                '$sub_category', '__all__'
                            ]
                        },
                        'group_id': '$events.group_id'
                    },
                    'events': {
                        '$push': {
                            'event_id': '$event_id_str',
                            'event_name': '$events.event_name',
                            'group_id': '$events.group_id',
                            'selected': {
                                '$in': [
                                    '$event_id_str', self.event_ids
                                ]
                            }
                        }
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'category': '$_id.category',
                        'sub_category': '$_id.sub_category'
                    },
                    'events': {
                        '$push': {
                            '$cond': [
                                # Grup kimliği varsa, yalnızca bir etkinlik al; yoksa tüm etkinlikleri al
                                {'$eq': ['$_id.group_id', None]},
                                '$events',
                                {'$arrayElemAt': ['$events', 0]}
                            ]
                        }
                    }
                }
            },
            {
                '$group': {
                    '_id': '$_id.category',
                    'sub_categories': {
                        '$push': {
                            'value': '$_id.sub_category',
                            'events': '$events'
                        }
                    }
                }
            },
            {
                '$project': {
                    'category': '$_id',
                    'sub_categories': 1,
                    '_id': 0
                }
            }
        ]
        return pipeline
