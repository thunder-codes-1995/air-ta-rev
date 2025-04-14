from dataclasses import dataclass
from typing import Literal

import pandas as pd


@dataclass
class DemandData:

    field: Literal["lf", "pax", "favg", "frev"]
    year : int 

    def pax(self):

        return [
            {
                "year": self.year,
                "data": [
                    {
                        "title": 4,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 1,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": None},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": None},
                                    {"class": "W", "value": 1},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 7,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 3,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": None},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": 3},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 8,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 19,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": 6},
                                    {"class": "R", "value": 9},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": 4},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 9,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 4,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": 4},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 30,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": 3},
                                    {"class": "T", "value": 8},
                                    {"class": "R", "value": 5},
                                    {"class": "S", "value": 7},
                                    {"class": "N", "value": 3},
                                    {"class": "Q", "value": 4},
                                    {"class": "O", "value": None},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 10,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 6,
                                "data": [
                                    {"class": "J", "value": 6},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 229,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": 2},
                                    {"class": "V", "value": 9},
                                    {"class": "T", "value": 11},
                                    {"class": "R", "value": 40},
                                    {"class": "S", "value": 64},
                                    {"class": "N", "value": 5},
                                    {"class": "Q", "value": 4},
                                    {"class": "O", "value": 11},
                                    {"class": "W", "value": 83},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 11,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 17,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": 14},
                                    {"class": "I", "value": 3},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 478,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": 10},
                                    {"class": "L", "value": 13},
                                    {"class": "V", "value": 10},
                                    {"class": "T", "value": 16},
                                    {"class": "R", "value": 76},
                                    {"class": "S", "value": 15},
                                    {"class": "N", "value": 16},
                                    {"class": "Q", "value": 7},
                                    {"class": "O", "value": 43},
                                    {"class": "W", "value": 272},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 12,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 28,
                                "data": [
                                    {"class": "J", "value": 2},
                                    {"class": "C", "value": 6},
                                    {"class": "D", "value": 20},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 253,
                                "data": [
                                    {"class": "Y", "value": 2},
                                    {"class": "K", "value": 13},
                                    {"class": "M", "value": 15},
                                    {"class": "L", "value": 13},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": 28},
                                    {"class": "S", "value": 11},
                                    {"class": "N", "value": 21},
                                    {"class": "Q", "value": 6},
                                    {"class": "O", "value": 42},
                                    {"class": "W", "value": 102},
                                ],
                            },
                        ],
                    },
                    {
                        "title": "grand_total",
                        "data": [
                            {
                                "cabin": "J",
                                "value": 55,
                                "data": [
                                    {"class": "J", "value": 8},
                                    {"class": "C", "value": 10},
                                    {"class": "D", "value": 34},
                                    {"class": "I", "value": 3},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 1013,
                                "data": [
                                    {"class": "Y", "value": 2},
                                    {"class": "K", "value": 13},
                                    {"class": "M", "value": 25},
                                    {"class": "L", "value": 28},
                                    {"class": "V", "value": 22},
                                    {"class": "T", "value": 41},
                                    {"class": "R", "value": 158},
                                    {"class": "S", "value": 97},
                                    {"class": "N", "value": 45},
                                    {"class": "Q", "value": 21},
                                    {"class": "O", "value": 103},
                                    {"class": "W", "value": 458},
                                ],
                            },
                        ],
                    },
                ],
            }
        ]

    def lf(self):

        return [
            {
                "year": self.year,
                "data": [
                    {
                        "title": 4,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": "90%",
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": None},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": None},
                                    {"class": "W", "value": "90%"},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 7,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": "50%",
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": None},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": "50%"},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 8,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": "71%",
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": "90%"},
                                    {"class": "R", "value": "80%"},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": "40%"},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 9,
                        "data": [
                            {
                                "cabin": "J",
                                "value": "100%",
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": "100%"},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": "67%",
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": "90%"},
                                    {"class": "T", "value": "77%"},
                                    {"class": "R", "value": "58%"},
                                    {"class": "S", "value": "58%"},
                                    {"class": "N", "value": "55%"},
                                    {"class": "Q", "value": "57%"},
                                    {"class": "O", "value": None},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 10,
                        "data": [
                            {
                                "cabin": "J",
                                "value": "100%",
                                "data": [
                                    {"class": "J", "value": "100%"},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": "64%",
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": "90%"},
                                    {"class": "V", "value": "84%"},
                                    {"class": "T", "value": "75%"},
                                    {"class": "R", "value": "65%"},
                                    {"class": "S", "value": "82%"},
                                    {"class": "N", "value": "40%"},
                                    {"class": "Q", "value": "53%"},
                                    {"class": "O", "value": "56%"},
                                    {"class": "W", "value": "52%"},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 11,
                        "data": [
                            {
                                "cabin": "J",
                                "value": "31%",
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": "30%"},
                                    {"class": "I", "value": "33%"},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": "57%",
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": "85%"},
                                    {"class": "L", "value": "86%"},
                                    {"class": "V", "value": "93%"},
                                    {"class": "T", "value": "82%"},
                                    {"class": "R", "value": "73%"},
                                    {"class": "S", "value": "86%"},
                                    {"class": "N", "value": "63%"},
                                    {"class": "Q", "value": "53%"},
                                    {"class": "O", "value": "61%"},
                                    {"class": "W", "value": "46%"},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 12,
                        "data": [
                            {
                                "cabin": "J",
                                "value": "33%",
                                "data": [
                                    {"class": "J", "value": "100%"},
                                    {"class": "C", "value": "50%"},
                                    {"class": "D", "value": "24%"},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": "55%",
                                "data": [
                                    {"class": "Y", "value": "47%"},
                                    {"class": "K", "value": "90%"},
                                    {"class": "M", "value": "88%"},
                                    {"class": "L", "value": "91%"},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": "53%"},
                                    {"class": "S", "value": "77%"},
                                    {"class": "N", "value": "53%"},
                                    {"class": "Q", "value": "55%"},
                                    {"class": "O", "value": "44%"},
                                    {"class": "W", "value": "44%"},
                                ],
                            },
                        ],
                    },
                    {
                        "title": "grand_total",
                        "data": [
                            {
                                "cabin": "J",
                                "value": "39%",
                                "data": [
                                    {"class": "J", "value": "100%"},
                                    {"class": "C", "value": "60%"},
                                    {"class": "D", "value": "27%"},
                                    {"class": "I", "value": "33%"},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": "58%",
                                "data": [
                                    {"class": "Y", "value": "47%"},
                                    {"class": "K", "value": "90%"},
                                    {"class": "M", "value": "87%"},
                                    {"class": "L", "value": "89%"},
                                    {"class": "V", "value": "89%"},
                                    {"class": "T", "value": "79%"},
                                    {"class": "R", "value": "68%"},
                                    {"class": "S", "value": "79%"},
                                    {"class": "N", "value": "55%"},
                                    {"class": "Q", "value": "%54"},
                                    {"class": "O", "value": "52%"},
                                    {"class": "W", "value": "47%"},
                                ],
                            },
                        ],
                    },
                ],
            }
        ]

    def avg_rev(self):

        return [
            {
                "year": self.year,
                "data": [
                    {
                        "title": 4,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 81.42,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": None},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": None},
                                    {"class": "W", "value": 81.42},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 7,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 296.66,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": None},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": 296.66},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 8,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 2063.76,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": 945.78},
                                    {"class": "R", "value": 907.02},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": 210.96},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 9,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 3801.48,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": 3801.48},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 3510.31,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": 547.15},
                                    {"class": "T", "value": 966.09},
                                    {"class": "R", "value": 604.67},
                                    {"class": "S", "value": 811.37},
                                    {"class": "N", "value": 264.11},
                                    {"class": "Q", "value": 316.92},
                                    {"class": "O", "value": None},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 10,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 4558.28,
                                "data": [
                                    {"class": "J", "value": 4558.28},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 226071.58,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": 538.28},
                                    {"class": "V", "value": 1607.34},
                                    {"class": "T", "value": 1794.45},
                                    {"class": "R", "value": 6161.78},
                                    {"class": "S", "value": 8426.1},
                                    {"class": "N", "value": 453.89},
                                    {"class": "Q", "value": 330.9},
                                    {"class": "O", "value": 910.4},
                                    {"class": "W", "value": 5848.44},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 11,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 10371.2,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": 10332.2},
                                    {"class": "I", "value": 39},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 41210.45,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": 2670.68},
                                    {"class": "L", "value": 2771.48},
                                    {"class": "V", "value": 1557.99},
                                    {"class": "T", "value": 2122.45},
                                    {"class": "R", "value": 8973.16},
                                    {"class": "S", "value": 1625.87},
                                    {"class": "N", "value": 1166.48},
                                    {"class": "Q", "value": 494.18},
                                    {"class": "O", "value": 3009.98},
                                    {"class": "W", "value": 16818.18},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 12,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 22682.91,
                                "data": [
                                    {"class": "J", "value": 2074.6},
                                    {"class": "C", "value": 5528.84},
                                    {"class": "D", "value": 15079.47},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 31540.75,
                                "data": [
                                    {"class": "Y", "value": 1365.71},
                                    {"class": "K", "value": 5007.52},
                                    {"class": "M", "value": 4193.47},
                                    {"class": "L", "value": 2799.42},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": 3281.49},
                                    {"class": "S", "value": 1203.27},
                                    {"class": "N", "value": 2008.81},
                                    {"class": "Q", "value": 467.46},
                                    {"class": "O", "value": 3504.46},
                                    {"class": "W", "value": 7709.14},
                                ],
                            },
                        ],
                    },
                    {
                        "title": "grand_total",
                        "data": [
                            {
                                "cabin": "J",
                                "value": 41413.87,
                                "data": [
                                    {"class": "J", "value": 6632.88},
                                    {"class": "C", "value": 9330.32},
                                    {"class": "D", "value": 225411.67},
                                    {"class": "I", "value": 39},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 104774.93,
                                "data": [
                                    {"class": "Y", "value": 1365.71},
                                    {"class": "K", "value": 6007.52},
                                    {"class": "M", "value": 6864.15},
                                    {"class": "L", "value": 6109.18},
                                    {"class": "V", "value": 3712.48},
                                    {"class": "T", "value": 5828.77},
                                    {"class": "R", "value": 19928.12},
                                    {"class": "S", "value": 12066.61},
                                    {"class": "N", "value": 3893.46},
                                    {"class": "Q", "value": 1609.46},
                                    {"class": "O", "value": 7932.46},
                                    {"class": "W", "value": 30457.18},
                                ],
                            },
                        ],
                    },
                ],
            }
        ]

    def avg_fare(self):

        return [
            {
                "year": self.year,
                "data": [
                    {
                        "title": 4,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 81.42,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": None},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": None},
                                    {"class": "W", "value": 81.42},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 7,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 174.27,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": None},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": 174.27},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 8,
                        "data": [
                            {
                                "cabin": "J",
                                "value": None,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 775.79,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": 157},
                                    {"class": "R", "value": 512.68},
                                    {"class": "S", "value": None},
                                    {"class": "N", "value": None},
                                    {"class": "Q", "value": None},
                                    {"class": "O", "value": 105.48},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 9,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 950.37,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": 950.37},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 2307.22,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": None},
                                    {"class": "V", "value": 547.15},
                                    {"class": "T", "value": 482.59},
                                    {"class": "R", "value": 236.37},
                                    {"class": "S", "value": 719.09},
                                    {"class": "N", "value": 163.56},
                                    {"class": "Q", "value": 158.46},
                                    {"class": "O", "value": None},
                                    {"class": "W", "value": None},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 10,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 1887.95,
                                "data": [
                                    {"class": "J", "value": 1887.95},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": None},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 16649.3,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": None},
                                    {"class": "L", "value": 269.14},
                                    {"class": "V", "value": 1081.51},
                                    {"class": "T", "value": 1497.12},
                                    {"class": "R", "value": 4407.2},
                                    {"class": "S", "value": 4016.46},
                                    {"class": "N", "value": 369.9},
                                    {"class": "Q", "value": 330.9},
                                    {"class": "O", "value": 663.04},
                                    {"class": "W", "value": 4014.03},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 11,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 8012.73,
                                "data": [
                                    {"class": "J", "value": None},
                                    {"class": "C", "value": None},
                                    {"class": "D", "value": 8089.73},
                                    {"class": "I", "value": 13},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 30359.25,
                                "data": [
                                    {"class": "Y", "value": None},
                                    {"class": "K", "value": None},
                                    {"class": "M", "value": 1897.77},
                                    {"class": "L", "value": 1695.82},
                                    {"class": "V", "value": 1181.83},
                                    {"class": "T", "value": 1535.48},
                                    {"class": "R", "value": 6747.58},
                                    {"class": "S", "value": 733.26},
                                    {"class": "N", "value": 686.13},
                                    {"class": "Q", "value": 365.8},
                                    {"class": "O", "value": 3262.52},
                                    {"class": "W", "value": 13354.06},
                                ],
                            },
                        ],
                    },
                    {
                        "title": 12,
                        "data": [
                            {
                                "cabin": "J",
                                "value": 16940.52,
                                "data": [
                                    {"class": "J", "value": 1037.3},
                                    {"class": "C", "value": 3700.6},
                                    {"class": "D", "value": 122202.62},
                                    {"class": "I", "value": None},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 22690.87,
                                "data": [
                                    {"class": "Y", "value": 1365.71},
                                    {"class": "K", "value": 2254.06},
                                    {"class": "M", "value": 3925.94},
                                    {"class": "L", "value": 23709.56},
                                    {"class": "V", "value": None},
                                    {"class": "T", "value": None},
                                    {"class": "R", "value": 1918.79},
                                    {"class": "S", "value": 885.64},
                                    {"class": "N", "value": 1639.9},
                                    {"class": "Q", "value": 322.98},
                                    {"class": "O", "value": 2748.99},
                                    {"class": "W", "value": 5258.3},
                                ],
                            },
                        ],
                    },
                    {
                        "title": "grand_total",
                        "data": [
                            {
                                "cabin": "J",
                                "value": 27881.57,
                                "data": [
                                    {"class": "J", "value": 2925.25},
                                    {"class": "C", "value": 4650.97},
                                    {"class": "D", "value": 20292.35},
                                    {"class": "I", "value": 13},
                                ],
                            },
                            {
                                "cabin": "Y",
                                "value": 73038.12,
                                "data": [
                                    {"class": "Y", "value": 1365.71},
                                    {"class": "K", "value": 2254.06},
                                    {"class": "M", "value": 5823.71},
                                    {"class": "L", "value": 4335.52},
                                    {"class": "V", "value": 2810.49},
                                    {"class": "T", "value": 3672.82},
                                    {"class": "R", "value": 13822.62},
                                    {"class": "S", "value": 6354.45},
                                    {"class": "N", "value": 2859.49},
                                    {"class": "Q", "value": 1178.14},
                                    {"class": "O", "value": 5853.3},
                                    {"class": "W", "value": 22707.81},
                                ],
                            },
                        ],
                    },
                ],
            }
        ]

    def overview(self):
        return [
            {
                "total_pax": [
                    {"value": 1},
                    {"value": 3},
                    {"value": 19},
                    {"value": 34},
                    {"value": 235},
                    {"value": 495},
                    {"value": 281},
                    {"value": 1068},
                ]
            },
            {
                "lf_avg": [
                    {"value": "90%"},
                    {"value": "50%"},
                    {"value": "71%"},
                    {"value": "68%"},
                    {"value": "65%"},
                    {"value": "56%"},
                    {"value": "53%"},
                    {"value": "57%"},
                ]
            },
            {
                "rev_sum": [
                    {"value": 81.42},
                    {"value": 296.66},
                    {"value": 2063.76},
                    {"value": 7311.79},
                    {"value": 30629.86},
                    {"value": 51581.65},
                    {"value": 54223.66},
                    {"value": 146188.8},
                ]
            },
            {
                "avg_fare_sum": [
                    {"value": 81.42},
                    {"value": 174.27},
                    {"value": 775.79},
                    {"value": 3257.59},
                    {"value": 18537.25},
                    {"value": 38451.98},
                    {"value": 39631.39},
                    {"value": 100919.69},
                ]
            },
        ]

    def get(self):
        if self.field == "lf":
            return self.lf()

        if self.field == "pax":
            return self.pax()

        if self.field == "favg":
            return self.avg_fare()

        return self.avg_rev()
