from datetime import datetime, timedelta

import pandas as pd


class EventSetup:

    @classmethod
    def flatten(cls, data: pd.DataFrame, category_field: str, sub_category_field: str) -> pd.DataFrame:
        """convert categories and subcategories from list of objects into '-' separated strings"""
        df = data.copy()
        join_cats = lambda val: "-".join(item["category"] for item in val) if not pd.isna(val) else val
        join_sub_cats = lambda val: (
            "-".join([subcat for item in val for subcat in item["sub_categories"]]) if not pd.isna(val) else val
        )
        df[category_field] = df.categories.apply(join_cats)
        df[sub_category_field] = df.categories.apply(join_sub_cats)
        return df

    @classmethod
    def fill(cls, data: pd.DataFrame, s_date: str, e_date: str) -> pd.DataFrame:
        res = []
        data["sd"] = data[s_date].apply(lambda val: datetime.strptime(val, "%Y-%m-%d").date())
        data["ed"] = data[e_date].apply(lambda val: datetime.strptime(val, "%Y-%m-%d").date())
        data["days_count"] = data.apply(lambda row: (row.ed - row.sd).days + 1, axis=1)
        events = data.to_dict("records")

        for event in events:
            for i in range(event["days_count"]):
                dt = event["sd"] + timedelta(days=i)
                res.append({**event, "date": dt.strftime("%Y-%m-%d")})

        return pd.DataFrame(res)
