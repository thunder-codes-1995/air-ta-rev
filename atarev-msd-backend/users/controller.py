from base.controller import BaseController
from base.helpers.permissions import Admin, has_role
from base.helpers.routes import ProtectedRoutes

from .service import UserService

service = UserService()


class UserController(BaseController):

    # @has_role(Admin)
    def put(self, endpoint: str):
        if endpoint == ProtectedRoutes.ADD_USER_VALUE.value:
            return service.store_value()

        return service.update_user(endpoint)

    def get(self, endpoint: str):
        if endpoint == ProtectedRoutes.ADD_USER_VALUE.value:
            return service.store_value()
