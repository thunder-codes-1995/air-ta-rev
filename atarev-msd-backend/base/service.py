import hashlib
from typing import Union

from flask import request

from base.dataframe import DataFrame
from utils.funcs import from_int_to_datetime, get_market_carrier_map, split_string


class BaseService:
    repository_class = None
    figure_class = None
    _repository = None
    handler_class = None

    def lambda_df(self, data) -> DataFrame:
        """convert mongodb cursor into python dataframe by lambda function"""
        return DataFrame(list(map(lambda x: x, data)))

    def get_repository_class(self):
        if not self.repository_class:
            raise ValueError("repository class is required")
        return self.repository_class

    def find(self, filter={}):
        return self.repository.find(filter)

    def find_one(self, filter={}):
        return self.repository.find_one(filter)

    def aggregate(self, pipeline):
        return self.repository.aggregate(pipeline)

    # NOTE this method will return result as df instead of repeating the same process everywhere
    # aggregate should be replaced with thos one
    def _aggregte(self, pipeline) -> DataFrame:
        return self.lambda_df(self.repository.aggregate(pipeline))

    def stringify(self, data):
        return self.repository.stringify(data)

    @property
    def repository(self):
        if self._repository:
            return self._repository
        self._repository = self.get_repository_class()()
        return self._repository
        # return self.repository_class()

    @property
    def builder(self):
        """get builder instance"""
        return self.get_builder_class()()

    @property
    def figure(self):
        """get figure instance"""
        return self.get_figure_class()()

    @property
    def handler(self):
        """get handler instance"""
        return self.get_handler_class()()

    # user can override this for custom logic
    def get_builder_class(self):
        return self.builder_class

    # user can override this for custom logic
    def get_figure_class(self):
        if not self.figure_class:
            raise ValueError("figure class is not provided")
        return self.figure_class

    def genereate_hased_key(self, params={}):
        if not params:
            params = {**request.args}
            if request.path:
                params["__path__"] = request.path
            # if request.user :
            #     params["__host__"] = request.user.carrier

        string = "_".join([f"{k}={v}" for k, v in params.items() if v])
        return hashlib.sha256(string.encode("utf8")).hexdigest()

    def cache(self, value, key: Union[str, None] = None, expiration_in_seconds=3600):
        hashed = key if key else self.genereate_hased_key()
        self.repository.redis.set(hashed, value, expiration_in_seconds)

    def check_cashed(self, key: Union[str, None] = None):
        cache_key = key if key else self.genereate_hased_key()
        cached = self.repository.redis.get(cache_key)
        if cached:
            return cached
        return

    def get_handler_class(self):
        """can be overwritten for custom logic"""
        return self.handler_class

    def split_string(self, val: str, separator=",", allow_empty=True):
        return split_string(val, separator, allow_empty)

    def get_carrier_color_map(self, default_color="#ffffff", is_gradient=False, return_list=False):
        origin_codes = self.split_string(request.args.get("orig_city_airport", ""))
        dest_codes = self.split_string(request.args.get("dest_city_airport", ""))
        host = request.user.carrier
        return get_market_carrier_map(origin_codes, dest_codes, host, default_color, is_gradient, return_list)

    @property
    def empty_figure(self):
        return {"data": [], "layout": {}}
