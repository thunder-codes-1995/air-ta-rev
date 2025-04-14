import re
from typing import List

from flask import request
from plotly.colors import n_colors

from utils.funcs import get_market_carrier_map, split_string


class Style:
    @property
    def font_family(self):
        return "Open Sans"

    @property
    def font_color(self):
        return "rgb(174, 181, 191)"

    @property
    def main_color(self):
        return "rgb(32, 32, 68)"

    @property
    def title_font_size(self):
        return 23

    def grid_color(self, opacity=1):
        return f"rgb(248, 235, 247,{opacity})"

    def font_size(self, variant):
        _map = {"xs": 10, "sm": 12, "md": 13, "lg": 18}
        return _map.get(variant, 13)

    @property
    def tick_font_color(self):
        return "rgb(248, 235, 247)"

    @property
    def legend_font_size(self):
        return 12

    @property
    def legend_title_side(self):
        return "top"

    @property
    def legend_traceorder(self):
        return "normal"

    @property
    def legend_item_sizing(self):
        return "trace"

    @property
    def legend_item_click(self):
        return "toggle"

    @property
    def legend_item_double_click(self):
        return "toggle"

    @property
    def legend_x_anchor(self):
        return "left"

    @property
    def legend_y_anchor(self):
        return "auto"

    @property
    def legend_v_align(self):
        return "middle"

    @property
    def legend_orientation(self):
        return "v"


class BaseFigure:
    _style = Style()

    @property
    def style(self):
        return self._style

    def update_layouts(self, fig, tick_font_variant="md", annotation_font_variant="lg"):
        for e in fig.layout:
            if "xaxis" in e or "yaxis" in e:
                fig.layout[e]["gridcolor"] = self.style.grid_color()
                fig.layout[e]["tickfont_size"] = self.style.font_size(tick_font_variant)
                fig.layout[e]["tickfont_family"] = self.style.font_family
                fig.layout[e]["zeroline"] = False
                fig.layout[e]["showgrid"] = False
            for e in fig.layout["annotations"]:
                e["font"]["family"] = self.style.font_family
                e["font"]["size"] = self.style.font_size(annotation_font_variant)
        return fig

    def style_figure(self, figure, x_title: str = "", y_title: str = "", legend_title: str = "", layout={}):
        figure.update_layout(
            xaxis=dict(
                title=x_title,
                title_font_color=self.style.font_color,
                title_font_size=self.style.title_font_size,
                title_font_family=self.style.font_family,
                gridcolor=self.style.tick_font_color,
                tickfont_size=12,
                tickfont_family=self.style.font_family,
                zeroline=False,
                showgrid=False,
            ),
            yaxis=dict(
                title=y_title,
                title_font_color=self.style.font_color,
                title_font_size=self.style.font_size("md"),
                title_font_family=self.style.font_family,
                gridcolor=self.style.tick_font_color,
                tickfont_size=self.style.legend_font_size,
                tickfont_family=self.style.font_family,
                zeroline=False,
                showgrid=False,
            ),
            **{
                "legend_font_color": self.style.font_color,
                "legend_font_size": self.style.legend_font_size,
                "legend_font_family": self.style.font_family,
                "legend_borderwidth": 0,
                "legend_title_text": legend_title,
                "legend_title_font_color": self.style.font_color,
                "legend_title_font_family": self.style.font_family,
                "legend_title_font_size": self.style.font_size("md"),
                "legend_title_side": self.style.legend_title_side,
                "legend_orientation": self.style.legend_orientation,
                "legend_bgcolor": self.style.main_color,
                "paper_bgcolor": self.style.main_color,
                "plot_bgcolor": self.style.main_color,
                "width": 1000,
                "height": 350,
                "autosize": False,
                "margin": dict(l=0, r=0, b=0, t=30, pad=4),
                **layout,
            },
        )
        return figure

    def split_string(self, separator=","):
        return split_string(separator)

    def get_carrier_color_map(self, default_color="#ffffff", is_gradient=False, return_list=False, theme="light+very_dark"):
        origin_codes = self.split_string(request.args.get("orig_city_airport", ""))
        dest_codes = self.split_string(request.args.get("dest_city_airport", ""))
        host = request.user.carrier
        return get_market_carrier_map(origin_codes, dest_codes, host, default_color, is_gradient, return_list, theme=theme)

    def n_colors(self, edgs: List[str], count: int) -> List[str]:
        res = []
        colors = n_colors(*edgs, count, colortype="rgb") if count > 1 else edgs
        for i in colors:
            r, g, b = re.search("\(([^)]+)\)", i).group(1).split(", ")
            r, g, b = int(float(r)), int(float(g)), int(float(b))
            res.append(f"rgb({r},{g},{b})")
        return res
