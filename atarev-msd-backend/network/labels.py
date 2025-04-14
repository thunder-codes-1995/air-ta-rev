from base.dataframe import DataFrame
from base.entities.currency import Currency


def get_schedule_tables(df: DataFrame):
    currencies = df.currency.unique().tolist()

    return [
        {"dom_op_al_code": "CARR"},
        {"week_day_str": "DOW"},
        {"local_dep_hour": "DEP"},
        {"equip": "EQUIP"},
        {"next_dest": "DEST"},
        {"pax": "PAX"},
        {"blended_rev": f"REV ({','.join( Currency(currencies).symbol)})"},
    ]
