from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .theme import Handler as Thm
from .theme import Theme


@dataclass
class Handler:
    carriers: List[str] = field(default_factory=list)

    def colors(self, theme: Theme, host_code: Optional[str] = None) -> Dict[str, str]:
        colors = Thm(theme).palette(host_code)

        if len(self.carriers) > len(colors):
            diff = len(self.carriers) - len(colors)
            colors.extend(["#FFFFFF"] * diff)

        return dict((carrier, color) for carrier, color in zip(self.carriers, colors))
