from enum import Enum


class Route(Enum):
    GET_FLIGHTS = "flights"
    GET_MIN_FARE_TRENDS = "fare-trends"
    GET_MIN_FARE_TRENDS_TABLE = "fare-trends-table"
    GET_PRICE_EVOLUTION = "price-evolution"
    GET_PRICE_EVOLUTION_TABLE = "price-evolution-table"
    GET_SCRAPER_HEALTH = "scraper-health"
    GET_FARE_STRUCTURE = "structure"
    GET_FARE_STRUCTURE_TABLE = "fare-structure-tables"
    GET_MIN_FARE_TRENDS_REPORT = "fare-trends-report"
    GET_PRICE_EVOLUTION_REPORT = "price-evolution-report"
