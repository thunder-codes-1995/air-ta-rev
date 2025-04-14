import json

import pandas as pd
import plotly.graph_objects as go

from base.figure import BaseFigure
from utils.funcs import get_customer_seg_graph_colors


class CustomerSegmentationFigure(BaseFigure):
    idx = 0

    GRPAH_TITLES = {
        "arr_time_bd": "Arr Time",
        "class_bd": "Cabin",
        "cos_bd": "Point-of-Sale",
        "dow_bd": "Day-of-Week",
        "dtd_bd": "Days-to-Departure",
        "inbound_breakdown": "Inbound",
        "outbound_breakdown": "Outbound",
        "pax_type": "Pax Type",
        "stop_bd": "Stops",
        "ticket_type_bd": "Ticket Type",
    }

    def get_graphs(self, seg_df: pd.DataFrame):
        ret_figs = {}
        seg_df.sort_values(by=["pax"], ascending=False, inplace=True)

        for g, g_df in seg_df.groupby("cat_type"):
            g_df = self.__get_ratio_for_group(g_df)
            fig = self.__get_pie_chart(g_df, g)
            ret_figs[g] = {
                "fig": fig,
                "cart": {
                    "title": self.GRPAH_TITLES[g],
                    "value": g_df.sort_values(by=["ratio"], ascending=False)
                    .iloc[0]
                    .cat_name,
                },
            }

        return ret_figs

    def __get_ratio_for_group(self, df: pd.DataFrame) -> pd.DataFrame:
        df["cat_name"] = [
            name if i < 5 else "Others" for i, name in enumerate(df.cat_name.tolist())
        ]
        df = df.groupby(["cat_name"], as_index=False).agg({"pax": "sum"})
        df["ratio"] = df["pax"] / df["pax"].sum()
        df["precent"] = df["ratio"].apply(lambda val: f"{round(val * 100)}" + "%")
        return df

    def __get_pie_chart(self, df: pd.DataFrame, g) -> go.Figure:
        if g in [
            "pax_type",
            "class_bd",
        ]:  # the reason i did not just adjust the pipline is what if they want it again ?(waleed)
            return None

        colors = get_customer_seg_graph_colors(self.idx)

        self.idx = (self.idx + 1) % 8

        fig = go.Figure(
            go.Pie(
                labels=df.cat_name.tolist(),
                values=df.pax.tolist(),
                hole=0.5,
                marker=dict(colors=colors),
                text=df.precent.tolist(),
            ),
        )

        fig.update_layout(paper_bgcolor="#131023")

        fig.update_traces(textinfo="none", hoverinfo="label+percent", textfont_size=20)
        fig.layout.template = None
        # fig.show()

        return json.loads(fig.to_json())
