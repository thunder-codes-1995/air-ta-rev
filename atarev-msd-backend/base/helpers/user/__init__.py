from dataclasses import dataclass
from typing import List, Optional, Union

from base.redis import Redis
from users.repository import UserRepository

from ..duration import Duration

user_repository = UserRepository()
redis = Redis()


@dataclass(frozen=True)
class UserModule:
    name: str
    pages: List[str]
    enabled: bool


@dataclass(frozen=True)
class User:
    username: Optional[str] = None
    carrier: Optional[str] = None
    is_authenticated: bool = False

    @property
    def data(self):
        return user_repository.get_user(self.username, drop_id=True)

    @property
    def role(self) -> str:
        key = f"user_role_{self.username}_{self.carrier}"
        cached_value = redis.get(key)
        if cached_value:
            return cached_value

        user = user_repository.get_user(self.username)

        role = user.get("role", "analyst")
        redis.set(key, role, expiration_in_seconds=Duration.months(1))
        return role

    @property
    def kpis(self) -> Union[List[str], None]:
        user = user_repository.get_user(self.username)
        kpis = user.get("kpis")
        return kpis

    @property
    def event_table_selected_fields(self) -> Union[List[str], None]:
        user = user_repository.get_user(self.username)
        fields = user.get("event_table_fields")
        return fields

    @property
    def markets(self):
        key = f"user_markets_{self.username}_{self.carrier}"
        cached_value = redis.get(key)
        if cached_value:
            return cached_value
        user = user_repository.get_user(self.username)
        markets = user.get("markets", [])
        res = [(market["origin"], market["destination"]) for market in markets]
        redis.set(key, res, expiration_in_seconds=Duration.months(1))
        return res

    @property
    def modules(self) -> List[UserModule]:
        user = user_repository.get_user(self.username)
        modules = user.get("enabledModules")

        if modules:
            return [
                UserModule(name=module, pages=modules[module]["pages"], enabled=modules[module]["enabled"]) for module in modules
            ]

        return [
            UserModule(
                name="MSD",
                pages=[
                    "/product-overview",
                    "/market-share",
                    "/network-scheduling",
                    "/booking-trends",
                    "/fare-revenue",
                    "/fare-structure",
                    "/agency-analysis",
                    "/customer-segmentation",
                    "/comparative-analysis",
                    "/strategy-actions",
                    "/performance-trends-load-factor-curve",
                ],
                enabled=True,
            ),
            UserModule(
                name="LFA",
                pages=[
                    "/lowest-fare-calendar",
                    "/availability-trends",
                    "/price-evolution",
                    "/actions",
                    "/price-recommendation",
                    "/daily-flights-overview",
                ],
                enabled=True,
            ),
            UserModule(
                name="FARE_ANALYZER",
                pages=[
                    "/lowest-fare-calendar",
                    "/availability-trends",
                    "/price-evolution",
                    "/actions",
                    "/price-recommendation",
                    "/daily-flights-overview",
                ],
                enabled=True,
            ),
        ]


ANON_USER = User(username=None, carrier=None, is_authenticated=False)
