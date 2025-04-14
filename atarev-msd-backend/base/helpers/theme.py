from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from plotly.colors import hex_to_rgb, n_colors

from configurations.repository import ConfigurationRepository

config_repo = ConfigurationRepository()


class Light(Enum):
    FIRST = "#7696FF"
    SECOND = "#B78EE5"
    THIRD = "#8EEBE8"
    FOURTH = "#F3969C"
    FIFITG = "#FFD09A"
    SIXTH = "#FFE1F8"
    SEVENTH = "#9D74DA"
    EIGHTTH = "#ABDBFF"
    NINTH = "#6b5958"
    TENTH = "#20f526"
    ELEV = "#f54eb2"
    TWE = "#ffffff"


class Dark(Enum):
    FIRST = "#0000CC"
    SECOND = "#381959"
    THIRD = "#02b2ad"
    FOURTH = "#FF535C"
    FIFITG = "#630A3A"
    SIXTH = "#FFE1F8"
    SEVENTH = "#190c32"
    EIGHTTH = "#131c34"
    NINTH = "#3d2f2e"
    TENTH = "#038707"
    ELEV = "#b5046e"
    TWE = "#ffffff"


class Mid(Enum):
    FIRST = "#295CFF"
    SECOND = "#943BFF"
    THIRD = "#02ECE6"
    FOURTH = "#EF4351"
    FIFITG = "#FF9416"
    SIXTH = "#FFA9E9"
    SEVENTH = "#5504D9"
    EIGHTTH = "#259eff"
    NINTH = "#544241"
    TENTH = "#0dde14"
    ELEV = "#f7119b"
    TWE = "#ffffff"


class Theme(Enum):
    DARK = "dark"
    LIGHT = "light"
    MID = "mid"


@dataclass
class Handler:
    theme: Theme

    def palette(self, host_code: Optional[str] = None) -> List[str]:
        theme_map = {Theme.LIGHT.value: Light, Theme.DARK.value: Dark, Theme.MID.value: Mid}
        res = [th.value for th in list(theme_map.get(self.theme.value, []))]

        if host_code:
            val = config_repo.get_by_key("CARRIERS_COLOR", host_code) or []
            colors = [rng[0] for rng in val]
            res = colors + res[len(colors) :]

        return res

    def map(self, values: List[str]) -> Dict[str, str]:
        colors = self.palette()
        return {val: colors[idx] for idx, val in enumerate(values)}

    def host_color(self, host_code: Optional[str] = None) -> str:
        return self.palette(host_code)[0]


@dataclass
class Gradient:
    theme: Theme

    def shades(self, count: int, color: str, host_code: Optional[str] = None) -> List[str]:
        if count <= 1:
            return [color]

        if host_code:
            return self._host_based_map(host_code, count, color)
        return self._default_map(host_code, count, color)

    def _default_map(self, host_code: str, count: int, color: str) -> List[str]:
        colors = Handler(self.theme).palette(host_code)
        color_idx = colors.index(color)
        dark_colors = Handler(Theme.DARK).palette()
        light_colors = Handler(Theme.MID).palette()

        return n_colors(
            str(hex_to_rgb(light_colors[color_idx])),
            str(hex_to_rgb(dark_colors[color_idx])),
            count,
            colortype="rgb",
        )

    def _host_based_map(self, host_code: str, count: int, color: str) -> List[str]:
        colors = Handler(self.theme).palette(host_code)
        color_idx = colors.index(color)
        val = config_repo.get_by_key("CARRIERS_COLOR", host_code) or []

        if color_idx < len(val):
            return val[color_idx]
        else:
            return self._default_map(host_code, count, color)

    def map(self, color: str, values: List[str], host_code: Optional[str] = None) -> Dict[str, str]:
        shades = self.shades(len(values), color, host_code)
        return {val: shades[idx] for idx, val in enumerate(values)}
