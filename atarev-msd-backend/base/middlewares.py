import uuid
from typing import List

from flask import request

from base.entities.carrier import Carrier
from base.helpers.duration import Duration
from base.helpers.theme import Theme
from base.repository import BaseRepository
from base.service import BaseService
from users.repository import UserRepository
from utils.funcs import split_string


class Service(BaseService):
    repository_class = BaseRepository


service = Service()
user_repository = UserRepository()


def attach_carriers_colors(
    origin_key: str = "orig_city_airport",
    dest_key: str = "dest_city_airport",
):
    """
    this decorator will add a new key <carriers> to a dict response representing
    each carrier with color based on order
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)

            if type(result) is not dict:
                return result

            carrier_color_map = {
                **Carrier(request.user.carrier).carrier_colors(
                    split_string(request.args.get(origin_key)),
                    split_string(request.args.get(dest_key)),
                    Theme.MID if request.args.get("dark_theme") == "true" else Theme.LIGHT,
                ),
                "All": "#ffffff",
            }

            return {**result, "carriers": {**carrier_color_map, "All": "#ffffff"}}

        return wrapper

    return decorator


def cache(expiration_in_seconds=Duration.hours(1)):
    """
    if result for an operation is cached (based on request params) get cached version
    otherwise preform operation, cache result and return it
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            cached = service.check_cashed()
            if cached:
                return cached
            result = func(*args, **kwargs)
            service.cache(result, expiration_in_seconds=expiration_in_seconds)
            return result

        return wrapper

    return decorator


def attach_story_text(story_text):
    """Attach story text to response"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            response = func(*args, **kwargs)
            response.update(story_text=story_text)
            return response

        return wrapper

    return decorator


def attach_figure_id(fig_names: List[str] = []):
    """attach an id (random string) to each figure"""

    # frontend side needs this id to be able to display figures in full-screen mode
    def decorator(func):
        def wrapper(*args, **kwargs):
            response = func(*args, **kwargs)
            if type(response) is dict:
                if not fig_names:
                    response["layout"]["id"] = str(uuid.uuid4())
                    return response

                for name in fig_names:
                    if response.get(name) is None:
                        continue
                    response.get(name)["layout"]["id"] = str(uuid.uuid4())
            return response

        return wrapper

    return decorator
