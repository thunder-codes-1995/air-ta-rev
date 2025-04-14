from flask import request

from base.helpers.user import User
from base.helpers.user.actions import UserAction
from base.service import BaseService
from users.store_value import StoreUserValueService

from .repository import UserRepository


class UserService(BaseService):
    repository_class = UserRepository

    def update_user(self, endpoint: str):
        username = request.json.get("username")
        user = self.repository.get_user(username)
        user = User(username=username, carrier=user["clientCode"], is_authenticated=False)

        if endpoint == "module":
            UserAction(user).toggle_module(request.json["module_name"], request.json["is_enabled"])
            return "Done"

        if endpoint == "add_page":
            UserAction(user).add_page_to_module(request.json["module_name"], request.json["page_name"])
            return "Done"

        if endpoint == "remove_page":
            UserAction(user).remove_page_from_module(request.json["module_name"], request.json["page_name"])
            return "Done"

        return "Unknown"

    def store_value(self):
        if request.method == "GET":
            return StoreUserValueService().get_stored_value(key=request.args.get("key"))
        return StoreUserValueService().store()

