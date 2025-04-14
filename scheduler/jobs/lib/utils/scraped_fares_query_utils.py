from datetime import date, datetime, timedelta

def create_scraped_fares_match(lastUpdateDateTime, departureDate=None, orig=None, dest=None, type=None, source=None,stayDuration=None):
    """ Create a filter to extract only records that match specified criteria from fares collection(scraped date/time, origin, destination, type(OW/RT) """
    filter = {
        "fares.lastUpdateDateTime": {"$gte": int(lastUpdateDateTime)},
    }

    if departureDate is not None:
        filter["itineraries.0.itinDeptDate"] = int(departureDate)

    if orig is not None:
        filter["itineraries.0.itinOriginCode"] = orig

    if dest is not None:
        filter["itineraries.0.itinDestCode"] = dest

    if type is not None:
        filter["type"] = type

    if source is not None:
        filter["fares.scrapedFrom"] = source

    if stayDuration : 
        filter['criteria.stay_duration'] = stayDuration

    return filter

def create_scraped_fares_nested_match(lastUpdateDateTime, source=None):
    """Each record in fares collection has multiple nested fares ('fares' property). If only certain fares should be extracted, this function will create a filter for that"""
    nestedFareFilters = {
        '$gte': ['$fare.lastUpdateDateTime', int(lastUpdateDateTime)]
    }
    if source is not None:
        nestedFareFilters['$in'] = ['$fare.scrapedFrom', [source]]

    # convert nested fare filters into a single condition where all items/conditions must be met
    nestedFilter = {
        '$and': [{key: nestedFareFilters[key]} for key in nestedFareFilters]
    }
    return nestedFilter


def get_date_from_int(dt:int) : 
    str_dt = f"{dt}"
    year,month,day = str_dt[0:4],str_dt[4:6],str_dt[6:8]
    return date(year=int(year),month=int(month),day=int(day))

def get_datetime_from_int(date: int, time: int) -> datetime:
    str_dt: str = str(date)
    str_time: str = str(time)
    str_time = str_time.zfill(4)
    year, month, day = str_dt[0:4], str_dt[4:6], str_dt[6:8]
    hour, min = str_time[0:2], str_time[2:4]
    return datetime(
        year=int(year),
        month=int(month),
        day=int(day),
        hour=int(hour),
        minute=int(min),
    )

def get_int_from_datetime(date: datetime) -> int:
    year: int = date.year
    month: int = date.month
    day: int = date.day
    result = year*10000 + month*100 + day
    return result

def get_second_difference(date1: datetime, date2: datetime) -> int:
    time_diff: timedelta = date1 - date2
    total_secs: int = time_diff.total_seconds()
    return total_secs

def get_day_difference(date1: datetime, date2: datetime) -> float:
    second_diff: int = get_second_difference(date1, date2)
    day_difference: int = second_diff / (3600 * 24)
    return day_difference

def get_hour_difference(date1: datetime, date2: datetime) -> float:
    second_diff: float = get_second_difference(date1, date2)
    hour_difference: int = second_diff / 3600
    return hour_difference