from typing import List

import numpy as np
import pandas as pd
import statsmodels.api as sm
from flask import request
from plotly.colors import n_colors
from sklearn.cluster import KMeans

from base.helpers.permissions import has_access
from base.middlewares import attach_figure_id, attach_story_text, cache
from base.service import BaseService
from customer_segmentation.builder import CustomerSegmentationBuilder
from customer_segmentation.figure import CustomerSegmentationFigure
from customer_segmentation.forms import CustomerSegmentationGraphs, CustomerSegmentationTable
from customer_segmentation.handler import CustomerSegmentationGraphsHandler
from dds.repository import DdsRepository


class CustomerSegmentationService(BaseService):
    repository_class = DdsRepository
    builder_class = CustomerSegmentationBuilder
    figure_class = CustomerSegmentationFigure
    handler_class = CustomerSegmentationGraphsHandler

    STORY_TEXTS = {
        "get_segmention_graphs": {"main_card": {"title": "test", "content": "test"}},
    }

    @has_access("MSD", ["/customer-segmentation"])
    @cache()
    def get_segmention_table(self, form: CustomerSegmentationTable):
        pipeline = self.builder.segmentation_table_pipeline(form)
        seg_df = self._aggregte(pipeline)
        seg_df = self.categorize_rbkd(seg_df)
        clust_df, clust = self.get_clusts(seg_df)
        summary_df = self.get_summary_df(clust_df, clust)
        summary_df = self.label_summery_df(summary_df)
        ret_data = self.label_data(summary_df)

        # summary_df['blended_rev'] = summary_df['blended_fare'] * summary_df['pax']
        ret_arr = summary_df.sort_values(by="blended_rev", ascending=False).desc.tolist()[0]

        curr_host, curr_comp = request.user.carrier, request.args.get("main_competitor")

        RET_LABELS = [
            {"country_of_sale": "POS"},
            {"days_booked_prior_to_travel": "DTD"},
            {"travel_day_of_week": "DOW"},
            {"ticket_type": "TTYPE"},
            {"rbd_group": "RBD BAND"},
            {"pax": "TTL PAX"},
            {"blended_fare": "AVG FARE"},
            {"blended_rev": "HOST REVENUE"},
            {curr_host: "HOST"},
            {curr_comp: "COMP"},
            {"ped": "PED"},
        ]

        response = {
            "labels": RET_LABELS,
            "data": ret_data,
            "text": {"data": ret_arr},
        }

        return response

    def categorize_rbkd(self, seg_df: pd.DataFrame) -> pd.DataFrame:
        """
        categorize rbkd into four categories :
        top, second, third and fourth
        """
        flt_rbds = seg_df.groupby(["travel_date", "op_flt_num", "rbkd"], as_index=False).agg({"blended_fare": "mean"})
        rbd_order = (
            flt_rbds.groupby(["rbkd"], as_index=False).agg({"blended_fare": "mean"}).sort_values("blended_fare", ascending=False)
        )
        rbd_order["pct_rank"] = rbd_order.blended_fare.rank(pct=True)
        rbd_order["rbd_group"] = rbd_order["pct_rank"].map(self.assign_bucket)
        _map = dict(zip(rbd_order["rbkd"], rbd_order["rbd_group"]))
        seg_df["rbkd_group"] = seg_df["rbkd"].map(_map)
        return seg_df

    def prepare_model_dfs(self, seg_df: pd.DataFrame) -> pd.DataFrame:
        """add necessary columns and prepare dfs to be fed to KMeans model"""
        self.cos_dummies = pd.get_dummies(seg_df["cos_norm"], prefix="cos")
        self.ttype_dummies = pd.get_dummies(seg_df["norm_ticket_type"], prefix="ttype")
        self.rbd_dummies = pd.get_dummies(seg_df["rbkd_group"], prefix="rbkd")

        seg_df["num_days_bins"] = (
            pd.cut(
                seg_df["days_sold_prior_to_travel"],
                bins=[
                    -1,
                    0.99,
                    2.99,
                    4.99,
                    6.99,
                    8.99,
                    14.99,
                    29.99,
                    43.99,
                    71.99,
                    seg_df["days_sold_prior_to_travel"].max(),
                ],
            )
        ).astype(str)

        self.num_days_dummies = pd.get_dummies(seg_df["num_days_bins"], prefix="num_days")
        seg_df["is_weekend"] = seg_df["travel_day_of_week"].apply(lambda x: int(x in [5, 6, 7]))

        return seg_df

    def __group_into_clusters(self, seg_df: pd.DataFrame) -> List[str]:
        seg_df = self.prepare_model_dfs(seg_df)
        self.cos_dummies = pd.get_dummies(seg_df["cos_norm"], prefix="cos")
        self.ttype_dummies = pd.get_dummies(seg_df["norm_ticket_type"], prefix="ttype")
        self.rbd_dummies = pd.get_dummies(seg_df["rbkd_group"], prefix="rbkd")

        customer_feats = (
            ["is_weekend", "is_group"]
            + self.num_days_dummies.columns.values.tolist()
            + self.cos_dummies.columns.values.tolist()
            + self.rbd_dummies.columns.values.tolist()
            + self.ttype_dummies.columns.values.tolist()
        )

        conc_df = pd.concat(
            [
                seg_df,
                self.num_days_dummies,
                self.cos_dummies,
                self.rbd_dummies,
                self.ttype_dummies,
            ],
            axis=1,
        )

        X = conc_df[customer_feats]

        clust = KMeans(n_clusters=15, init="k-means++", max_iter=400, random_state=42)
        clust.fit(X.values)

        return conc_df, clust, customer_feats

    def get_clusts(self, seg_df: pd.DataFrame):
        conc_df, clust, features = self.__group_into_clusters(seg_df)
        conc_df["label"] = clust.labels_
        peds = []

        for group, plot_df in conc_df.groupby("label"):
            if plot_df["pax"].sum() >= 50:
                plot_df["price_bins"] = pd.cut(plot_df["blended_fare"].round(decimals=0), bins=20)

                plot_df["price_upper"] = plot_df["price_bins"].apply(lambda x: x.right).astype(int)

                curr_plot = plot_df[["price_upper", "pax"]].groupby(["price_upper"], as_index=False).agg({"pax": "sum"})

                curr_x = sm.add_constant(curr_plot["price_upper"])
                model = sm.OLS(curr_plot["pax"], curr_x)
                curr_res = model.fit()
                peds.append((group, curr_res.params["price_upper"]))

        peds = dict(peds)
        conc_df["ped"] = conc_df["label"].map(peds)
        self.label_df = conc_df
        clust_df = pd.DataFrame(clust.cluster_centers_, columns=features)
        clust_avgs = clust_df.mean(axis=0)
        clust_stds = clust_df.std(axis=0)
        self.clust_avgs = clust_avgs
        self.clust_stds = clust_stds
        return clust_df, clust

    def get_summary_df(self, clust_df: pd.DataFrame, clust):
        summary_info = {
            i: {
                "Country of Sale": None,
                "Days Booked Prior to Travel": None,
                "Travel Day of Week": None,
                "RBD Group": None,
                "Ticket Type": None,
            }
            for i in range(clust.n_clusters)
        }

        days_bkg_conv_dict = {
            "(14.99, 29.99]": "15-29",
            "(8.99, 14.99]": "9-14",
            "(71.99, 365.0]": "72-359",
            "(0.99, 2.99]": "1-2",
            "(2.99, 4.99]": "3-4",
            "(43.99, 71.99]": "44-71",
            "(4.99, 6.99]": "5-6",
            "(6.99, 8.99]": "7-8",
            "(29.99, 43.99]": "30-43",
            "(-1.0, 0.99]": "0",
        }

        isweekend_ind = {0: "Weekday", 1: "Weekend"}

        for index, row in clust_df.iterrows():
            curr_picked = []
            curr_bounds = self.get_bounds("is_weekend", 0.15)
            if row["is_weekend"] >= 0.5:
                curr_picked.append(isweekend_ind[1])
            else:
                curr_picked.append(isweekend_ind[0])
            summary_info[index]["Travel Day of Week"] = ", ".join(curr_picked)

            curr_picked = []
            for col in self.ttype_dummies.columns.values.tolist():
                curr_bounds = self.get_bounds(col)
                if row[col] >= 0.5:
                    curr_picked.append(col.split("_")[1])
            summary_info[index]["Ticket Type"] = ", ".join(curr_picked)

            curr_picked = []
            for col in self.cos_dummies.columns.values.tolist():
                curr_bounds = self.get_bounds(col)
                if row[col] >= 0.5:
                    curr_picked.append(col.split("_")[1])
            summary_info[index]["Country of Sale"] = ", ".join(curr_picked)

            curr_picked = []
            for col in self.num_days_dummies.columns.values.tolist():
                curr_bounds = self.get_bounds(col)
                if row[col] >= curr_bounds[1]:
                    curr_picked.append(days_bkg_conv_dict[col.split("_")[-1]])
            summary_info[index]["Days Booked Prior to Travel"] = ", ".join(curr_picked)

            curr_picked = []
            for col in self.rbd_dummies.columns.values.tolist():
                curr_bounds = self.get_bounds(col)
                if row[col] >= 0.5:
                    curr_picked.append(col.replace("_", " ").title())
            summary_info[index]["RBD Group"] = ", ".join(curr_picked)

        curr_det_bd = self.label_df.copy()
        curr_det_bd["blended_rev"] = curr_det_bd["pax"] * curr_det_bd["blended_fare"]
        curr_det_bd = (
            curr_det_bd.groupby("label")
            .agg({"pax": "sum", "blended_fare": "mean", "ped": "first"})
            .reset_index()
            .merge(
                curr_det_bd.query(
                    "dom_op_al_code in ('{}', '{}')".format(request.user.carrier, request.args.get("main_competitor"))
                )
                .groupby(["label", "dom_op_al_code"])
                .agg({"pax": "sum"})
                .unstack("dom_op_al_code")
                .droplevel(0, axis=1)
                .fillna(0)
                .astype(int)
                .reset_index(),
                on="label",
            )
            .merge(
                curr_det_bd.query(f"dom_op_al_code == '{request.user.carrier}'")
                .groupby("label")
                .agg({"blended_rev": "sum"})
                .reset_index(),
                on="label",
                how="left",
            )
            .fillna(0)
        )

        curr_det_bd["ped"] = curr_det_bd["ped"].round(decimals=1)
        for c in [request.user.carrier, request.args.get("main_competitor")]:
            try:
                curr_det_bd[c] = ((curr_det_bd[c] / curr_det_bd["pax"]) * 100).astype(int)
            except:
                continue

        summary_df = (
            pd.DataFrame.from_dict(summary_info)
            .T.reset_index()
            .rename(columns={"index": "label"})
            .merge(curr_det_bd, on="label")
            .drop_duplicates()
            .reset_index(drop=True)
        )

        return summary_df

    def label_data(self, df):
        ret_data = []
        reds = n_colors(
            "rgb(255, 200, 255)",
            "rgb(200, 0, 0)",
            int(max(np.abs(df["ped"].tolist()) * 10)) + 1,
            colortype="rgb",
        )

        for _, row in df.fillna(0).reset_index(drop=True).iterrows():
            curr_dict = {}
            for col in df.columns.values:
                if "Unnamed" not in col and col != "label" and col != "desc":
                    if col in ["blended_fare"]:
                        curr_dict[col.lower().replace(" ", "_")] = {
                            "displayVal": "$" + "{:,}".format(int(row[col])),
                            "val": int(row[col]),
                        }
                    elif col == "pax":
                        curr_dict[col.lower().replace(" ", "_")] = {
                            "displayVal": "{:,}".format(int(row[col])),
                            "val": int(row[col]),
                        }
                    elif col == "ped":
                        curr_dict["ped"] = {
                            "value": row[col],
                            "color": reds[int(np.abs(row[col])) * 10],
                        }
                    elif col in [
                        request.user.carrier,
                        request.args.get("main_competitor"),
                    ]:
                        curr_dict[col] = {
                            "displayVal": "{}%".format(row[col]),
                            "val": row[col],
                        }
                    else:
                        curr_dict[col.lower().replace(" ", "_")] = row[col]

            # t = row['blended_fare'] * row['pax']
            curr_dict["blended_rev"] = {
                "displayVal": "$" + "{:,}".format(int(row["blended_rev"])),
                "val": int(row["blended_rev"]),
            }

            ret_data.append(curr_dict)
        return ret_data

    def label_summery_df(self, summary_df):
        clust_descs = []
        feat_cols = [col for col in summary_df.columns.values if col not in ["label", "ped"] and "Unnamed" not in col]
        for _, row in summary_df.iterrows():
            curr_sent = []
            for col in feat_cols:
                if str(row[col]) != "":
                    if col == "Ticket Type":
                        if len(curr_sent) == 0:
                            curr_sent.extend(
                                [
                                    {
                                        "object_type": "cart",
                                        "cart_type": "people",
                                        "value": f"{row['pax']} people",
                                    },
                                    {"object_type": "text", "value": "buy tickets "},
                                    {
                                        "object_type": "cart",
                                        "cart_type": "days",
                                        "value": f"{row[col]} tickets",
                                    },
                                    {"object_type": "text", "value": ". "},
                                ]
                            )
                        else:
                            curr_sent.extend(
                                [
                                    {
                                        "object_type": "text",
                                        "value": "They usually buy ",
                                    },
                                    {
                                        "object_type": "cart",
                                        "cart_type": "days",
                                        "value": f"{row[col]} tickets",
                                    },
                                    {"object_type": "text", "value": ". "},
                                ]
                            )

                    if col == "Days Booked Prior to Travel":
                        if len(curr_sent) == 0:
                            curr_sent.extend(
                                [
                                    {
                                        "object_type": "cart",
                                        "cart_type": "people",
                                        "value": f"{row['pax']} people",
                                    },
                                    {"object_type": "text", "value": "buy tickets "},
                                    {
                                        "object_type": "cart",
                                        "cart_type": "days",
                                        "value": f"{row[col]} days before travel",
                                    },
                                    {"object_type": "text", "value": ". "},
                                ]
                            )
                        else:
                            curr_sent.extend(
                                [
                                    {
                                        "object_type": "text",
                                        "value": "They buy tickets ",
                                    },
                                    {
                                        "object_type": "cart",
                                        "cart_type": "days",
                                        "value": f"{row[col]} days before travel",
                                    },
                                    {"object_type": "text", "value": ". "},
                                ]
                            )

                    elif col == "Travel Day of Week":
                        if len(curr_sent) == 0:
                            curr_sent.extend(
                                [
                                    {
                                        "object_type": "cart",
                                        "cart_type": "people",
                                        "value": f"{row['pax']} people",
                                    },
                                    {
                                        "object_type": "text",
                                        "value": " mostly travel on ",
                                    },
                                ]
                            )
                        else:
                            curr_sent.append(
                                {
                                    "object_type": "text",
                                    "value": "They mostly travel on ",
                                },
                            )
                        if row[col] == "Weekday":
                            curr_sent.append(
                                {
                                    "object_type": "cart",
                                    "cart_type": "calendar",
                                    "value": "weekdays",
                                }
                            )
                        else:
                            curr_sent.append(
                                {
                                    "object_type": "cart",
                                    "cart_type": "calendar",
                                    "value": "weekends",
                                }
                            )
                        curr_sent.append({"object_type": "text", "value": ". "})

                    elif col == "Number of Passengers":
                        if len(curr_sent) == 0:
                            curr_sent.extend(
                                [
                                    {
                                        "object_type": "cart",
                                        "cart_type": "people",
                                        "value": f"{row['pax']} people",
                                    },
                                    curr_sent.append(
                                        {
                                            "object_type": "text",
                                            "value": " consist of ",
                                        },
                                    ),
                                ]
                            )
                        else:
                            curr_sent.append(
                                {
                                    "object_type": "text",
                                    "value": "This group consists of ",
                                },
                            )
                        if str(row[col]) == "1":
                            curr_sent.append(
                                {
                                    "object_type": "cart",
                                    "cart_type": "people",
                                    "value": "individual travelers",
                                },
                            )
                        else:
                            curr_sent.append(
                                {
                                    "object_type": "cart",
                                    "cart_type": "people",
                                    "value": f"tickets with {row[col]} people",
                                },
                            )
                        curr_sent.append({"object_type": "text", "value": ". "})

            clust_descs.append(curr_sent)

        summary_df["desc"] = clust_descs
        return summary_df

    def get_bounds(self, col_name, l=None):
        if l:
            lower = l
        else:
            lower = 0.4
        upper = self.clust_avgs[col_name] + 3 * self.clust_stds[col_name]
        return lower, upper

    def assign_bucket(self, pct_rank):
        if 0.75 < pct_rank <= 1:
            return "top"

        if 0.5 < pct_rank <= 0.75:
            return "second"

        if 0.25 < pct_rank <= 0.5:
            return "third"

        return "fourth"

    @has_access("MSD", ["/customer-segmentation"])
    @attach_story_text(STORY_TEXTS["get_segmention_graphs"])
    @attach_figure_id(["fig"])
    def get_segmention_graphs(self, form: CustomerSegmentationGraphs):
        pipeline = self.builder.segmentation_grapsh_pipeline(form)
        df = self._aggregte(pipeline)
        graphs = self.figure.get_graphs(pd.DataFrame(self.handler.handle_rows(df)))
        return graphs

    @has_access("MSD", ["/customer-segmentation"])
    def get_segmention_text(self):
        return {
            "data": [
                {"cart_type": "people", "object_type": "cart", "value": "843 people"},
                {"object_type": "text", "value": "buy tickets "},
                {
                    "cart_type": "days",
                    "object_type": "cart",
                    "value": "7-8 days before travel",
                },
                {"object_type": "text", "value": ". "},
                {"object_type": "text", "value": "They mostly travel on "},
                {"cart_type": "calendar", "object_type": "cart", "value": "weekends"},
                {"object_type": "text", "value": ". "},
                {"object_type": "text", "value": "They usually buy "},
                {
                    "cart_type": "days",
                    "object_type": "cart",
                    "value": "<b>One Way tickets</b>",
                },
                {"object_type": "text", "value": ". "},
            ]
        }
