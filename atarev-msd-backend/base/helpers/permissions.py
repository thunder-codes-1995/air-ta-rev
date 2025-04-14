from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, List, Union

from flask import request

from base.helpers.duration import Duration
from base.redis import Redis

from . import is_iterable_collection
from .errors import UnAccessibleModulePage, Unauthorized
from .user import User

redis = Redis()


class Role(ABC):
    @abstractmethod
    def role(self) -> str:
        ...


class SuperUser(Role):
    def role(self):
        return "superuser"


class Admin(Role):
    def role(self):
        return "admin"


class Analyst(Role):
    def role(self):
        return "analyst"


@dataclass
class HasAccess:
    """this class is to check whether a user has access to a module and its pages"""

    user: User

    def __post_init__(self):
        self.user_accessed_modules = self.user.modules

    def to_module(self, module: str) -> bool:
        """check if user has access to a module"""
        try:
            key = f"user_modules_{self.user.username}_{self.user.carrier}"
            cached = redis.get(key)
            if cached:
                return cached
            targeted_module = filter(lambda m: m.name == module)
            targeted_module = next(targeted_module)
            res = targeted_module.enabled

        except StopIteration:
            res = False

        redis.set(key, res, expiration_in_seconds=Duration.months(1))
        return res

    def to_pages(self, module: str, pages: Iterable[str]) -> bool:
        """check if user has access to a list of pages that belong to a module"""
        try:
            key = f"user_pages_{self.user.username}_{self.user.carrier}"
            cached = redis.get(key)
            if cached:
                return cached

            user_modules = self.user_accessed_modules
            targeted_module = filter(lambda m: m.name == module, user_modules)
            targeted_module = next(targeted_module)
            if not targeted_module.enabled:
                res = False
            else:
                res = all(page in targeted_module.pages for page in pages)

        except StopIteration:
            res = False

        redis.set(key, res, expiration_in_seconds=Duration.months(1))
        return res


@dataclass
class HasRole:
    user: User
    role: Union[Role, Iterable[Role]]

    def check(self):
        if is_iterable_collection(self.role):
            return any(self.user.role == R().role() for R in self.role)
        return self.user.role == self.role().role()


def has_access(module: str, pages: List[str]):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not HasAccess(request.user).to_pages(module, pages):
                raise UnAccessibleModulePage(module, pages)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def has_role(roles: Iterable[Role]):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not HasRole(request.user, roles).check():
                raise Unauthorized()
            return func(*args, **kwargs)

        return wrapper

    return decorator
