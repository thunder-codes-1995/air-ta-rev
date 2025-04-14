from datetime import date

from flask import request

from base.mongo_utils import (
    merge_criterions,
    convert_list_param_to_criteria
)
from customer_segmentation.forms import CustomerSegmentationTable
from customer_segmentation.forms import (
    CustomerSegmentationGraphs
)


class CustomerSegmentationBuilder:

    def segmentation_table_pipeline(self,form: CustomerSegmentationTable):
        if date.today().month <= 3:
            seg_year = date.today().year - 1
        else:
            seg_year = date.today().year

        match = merge_criterions(
            [
                convert_list_param_to_criteria('orig_code', form.get_orig_regions_list()),
                convert_list_param_to_criteria('dest_code', form.get_dest_regions_list()),
                convert_list_param_to_criteria('orig_code', form.get_orig_country_list()),
                convert_list_param_to_criteria('dest_code', form.get_dest_country_list()),
                convert_list_param_to_criteria('orig_code', form.get_orig_city_airports_list()),
                convert_list_param_to_criteria('dest_code', form.get_dest_city_airports_list()),
                convert_list_param_to_criteria('seg_class', form.get_cabins_list()),
                convert_list_param_to_criteria('country_of_sale', form.get_point_of_sales_list()),
                convert_list_param_to_criteria('distribution_channel', form.get_sales_channels_list()),
                convert_list_param_to_criteria('is_group', form.get_pax_type()),
                convert_list_param_to_criteria('dom_op_al_code', [
                    *form.get_selected_competitors_list(),
                    request.user.carrier
                ]),

                {
                    # 'dom_op_al_code' : request.user.carrier,
                    'travel_year': seg_year,
                    'days_sold_prior_to_travel': {'$gte': 0}
                }

            ]
        )

        return [
            {"$match": match},
            {
                "$addFields": {
                    'norm_ticket_type': {"$cond": [
                        {"$in": ["$ticket_type", ['Round Trip', 'One Way']]}, '$ticket_type', 'Other']},
                }
            },
            {
                "$project": {
                    "dom_op_al_code": 1,
                    "pax": 1,
                    "is_group": 1,
                    "days_sold_prior_to_travel": 1,
                    "cos_norm": 1,
                    "blended_fare": 1,
                    "travel_day_of_week": 1,
                    "travel_date": 1,
                    "op_flt_num": 1,
                    "rbkd": 1,
                    "norm_ticket_type": 1
                }
            }
        ]


    def segmentation_grapsh_pipeline(self,form: CustomerSegmentationGraphs):
        one = [
            {
                '$group': {
                    '_id': '$dep_time_block',
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cat_name': '$_id',
                    'cat_type': 'arr_time_bd',
                    'pax': 1,
                    'avg_blended_fare': 1
                }
            }
        ]

        two = [
            {
                '$match': {
                    'prev_dest': {
                        '$ne': ''
                    }
                }
            }, {
                '$group': {
                    '_id': '$prev_dest',
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cat_name': '$_id',
                    'cat_type': 'inbound_breakdown',
                    'pax': 1,
                    'avg_blended_fare': 1
                }
            }
        ]

        three = [
            {
                '$match': {
                    'next_dest': {
                        '$ne': ''
                    }
                }
            }, {
                '$group': {
                    '_id': '$next_dest',
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cat_name': '$_id',
                    'cat_type': 'outbound_breakdown',
                    'pax': 1,
                    'avg_blended_fare': 1
                }
            }
        ]

        four = [
            {
                '$project': {
                    'pax': 1,
                    'cat_type': 'pax_type',
                    'blended_fare': 1,
                    'cat_name': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$eq': [
                                            '$pax', 5
                                        ]
                                    },
                                    'then': '1'
                                }, {
                                    'case': {
                                        '$and': [
                                            {
                                                '$gt': [
                                                    '$pax', 1
                                                ]
                                            }, {
                                                '$lt': [
                                                    '$pax', 9
                                                ]
                                            }
                                        ]
                                    },
                                    'then': '(2-9)'
                                }
                            ],
                            'default': '+9'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'cat_name': '$cat_name',
                        'cat_type': '$cat_type'
                    },
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'pax': 1,
                    'cat_name': '$_id.cat_name',
                    'cat_type': '$_id.cat_type',
                    'avg_blended_fare': 1
                }
            }
        ]

        five = [
            {
                '$group': {
                    '_id': '$travel_day_of_week',
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cat_name': '$_id',
                    'cat_type': 'dow_bd',
                    'pax': 1,
                    'avg_blended_fare': 1
                }
            }
        ]

        six = [
            {
                '$match': {
                    'stop_1': {
                        '$ne': '-'
                    }
                }
            }, {
                '$group': {
                    '_id': '$stop_1',
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cat_name': '$_id',
                    'cat_type': 'stop_bd',
                    'pax': 1,
                    'avg_blended_fare': 1
                }
            }
        ]

        seven = [
            {
                '$group': {
                    '_id': '$seg_class',
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cat_name': '$_id',
                    'cat_type': 'class_bd',
                    'pax': 1,
                    'avg_blended_fare': 1
                }
            }
        ]

        eight = [
            {
                '$group': {
                    '_id': '$country_of_sale',
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cat_name': '$_id',
                    'cat_type': 'cos_bd',
                    'pax': 1,
                    'avg_blended_fare': 1
                }
            }
        ]

        nine = [
            {
                '$group': {
                    '_id': '$ticket_type',
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'cat_name': '$_id',
                    'cat_type': 'ticket_type_bd',
                    'pax': 1,
                    'avg_blended_fare': 1
                }
            }
        ]

        ten = [
            {
                '$project': {
                    'pax': 1,
                    'cat_type': 'dtd_bd',
                    'blended_fare': 1,
                    'cat_name': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$and': [
                                            {
                                                '$gte': [
                                                    '$days_sold_prior_to_travel', 0
                                                ]
                                            }, {
                                                '$lte': [
                                                    '$days_sold_prior_to_travel', 7
                                                ]
                                            }
                                        ]
                                    },
                                    'then': '[0-7]'
                                }, {
                                    'case': {
                                        '$and': [
                                            {
                                                '$gte': [
                                                    '$days_sold_prior_to_travel', 8
                                                ]
                                            }, {
                                                '$lte': [
                                                    '$days_sold_prior_to_travel', 30
                                                ]
                                            }
                                        ]
                                    },
                                    'then': '[8-30]'
                                }, {
                                    'case': {
                                        '$and': [
                                            {
                                                '$gte': [
                                                    '$days_sold_prior_to_travel', 31
                                                ]
                                            }, {
                                                '$lte': [
                                                    '$days_sold_prior_to_travel', 60
                                                ]
                                            }
                                        ]
                                    },
                                    'then': '[31-60]'
                                }, {
                                    'case': {
                                        '$and': [
                                            {
                                                '$gte': [
                                                    '$days_sold_prior_to_travel', 61
                                                ]
                                            }, {
                                                '$lte': [
                                                    '$days_sold_prior_to_travel', 90
                                                ]
                                            }
                                        ]
                                    },
                                    'then': '[61-90]'
                                }
                            ],
                            'default': '+90'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': '$cat_name',
                    'pax': {
                        '$sum': '$pax'
                    },
                    'avg_blended_fare': {
                        '$avg': '$blended_fare'
                    }
                }
            }, {
                '$project': {
                    'cat_name': '$_id',
                    'pax': '$pax',
                    'cat_type': 'dtd_bd',
                    'avg_blended_fare': 1
                }
            }
        ]

        match = merge_criterions(
            [
                convert_list_param_to_criteria('orig_code', form.get_orig_regions_list()),
                convert_list_param_to_criteria('dest_code', form.get_dest_regions_list()),
                convert_list_param_to_criteria('orig_code', form.get_orig_country_list()),
                convert_list_param_to_criteria('dest_code', form.get_dest_country_list()),
                convert_list_param_to_criteria('orig_code', form.get_orig_city_airports_list()),
                convert_list_param_to_criteria('dest_code', form.get_dest_city_airports_list()),
                convert_list_param_to_criteria('seg_class', form.get_cabins_list()),
                convert_list_param_to_criteria('country_of_sale', form.get_point_of_sales_list()),
                convert_list_param_to_criteria('distribution_channel', form.get_sales_channels_list()),
                convert_list_param_to_criteria('is_group', form.get_pax_type()),
                {
                    'travel_year': date.today().year,
                }
            ]
        )

        return [
            {"$match": match},
            {
                "$project": {
                    "dep_time_block": 1,
                    'prev_dest': 1,
                    'next_dest': 1,
                    'travel_day_of_week': 1,
                    'stop_1': 1,
                    'seg_class': 1,
                    'country_of_sale': 1,
                    'ticket_type': 1,
                    'days_sold_prior_to_travel': 1,
                    'pax': 1,
                    "blended_fare": 1
                }
            },
            {
                "$facet": {
                    "one": one,
                    "two": two,
                    "three": three,
                    "four": four,
                    "five": five,
                    "six": six,
                    "seven": seven,
                    "eight": eight,
                    "nine": nine,
                    "ten": ten,
                },
            },

            {
                "$project": {
                    "result": {
                        "$concatArrays": ["$one", "$two", "$three", '$four', '$five',
                                          '$six', '$seven', '$eight', '$nine', '$ten'
                                          ]
                    }
                }
            },

            {
                "$unwind": {
                    "path": "$result"
                }
            },

            {
                "$project": {
                    'pax': '$result.pax',
                    'cat_type': '$result.cat_type',
                    'cat_name': '$result.cat_name',
                    'blended_fare': '$result.avg_blended_fare'}

            }

        ]
