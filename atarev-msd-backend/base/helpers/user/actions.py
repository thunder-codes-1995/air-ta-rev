from dataclasses import asdict, dataclass
from typing import Any

from base.redis import Redis
from users.repository import UserRepository

from . import User

repo = UserRepository()
redis = Redis()


@dataclass
class UserAction:
    user: User

    def enable(self): ...

    def disable(self): ...

    def add_page_to_module(self, module_name: str, page_name: str):
        p_name = f"/{page_name.replace('/','')}"
        modules = [asdict(m) for m in self.user.modules]

        if not list(filter(lambda m: m["name"] == module_name, modules)):
            modules.append({module_name: {"enabled": True, "pages": [p_name]}})
        else:
            for m in modules:
                if module_name == m["name"]:
                    m["pages"].append(p_name)

        redis.delete(f"user_modules_{self.user.username}_{self.user.carrier}")
        redis.delete(f"user_pages_{self.user.username}_{self.user.carrier}")

        repo.update_one(
            {"username": self.user.username, "clientCode": self.user.carrier},
            {**self.user.data, "enabledModules": modules},
        )

    def remove_page_from_module(self, module_name: str, page_name: str):
        p_name = f"/{page_name.replace('/','')}"
        modules = [asdict(m) for m in self.user.modules]
        module = list(filter(lambda m: m["name"] == module_name, modules))

        if not module:
            return

        module = module[0]
        updated_pages = list(filter(lambda page: page != p_name, module["pages"]))

        for m in modules:
            if m["name"] == module_name:
                m["pages"] = updated_pages

        redis.delete(f"user_modules_{self.user.username}_{self.user.carrier}")
        redis.delete(f"user_pages_{self.user.username}_{self.user.carrier}")

        repo.update_one(
            {"username": self.user.username, "clientCode": self.user.carrier},
            {
                **self.user.data,
                "enabledModules": modules,
            },
        )

    def toggle_module(self, module_name: str, enabled: bool):
        modules = [asdict(m) for m in self.user.modules]
        for module in modules:
            if module["name"] == module_name:
                module["enabled"] = enabled

        repo.update_one(
            {"username": self.user.username, "clientCode": self.user.carrier},
            {
                **self.user.data,
                "enabledModules": modules,
            },
        )

    def get_value(self, key: str):
        p = repo.find_one({"username": self.user.username,
                           "clientCode": self.user.carrier})
        return p[key]

    def add_value(self, key: str, value: Any) -> None:
        repo.update_one({"username": self.user.username, "clientCode": self.user.carrier}, {key: value})
