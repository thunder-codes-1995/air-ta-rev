from typing import Dict, List

from base.helpers.duration import Duration
from base.repository import BaseRepository


class UserRepository(BaseRepository):
    collection = "profiles"

    def store_filter_options(self, username: str, **kwargs):
        user = self.find_one({"username": username})
        assert bool(user), "user is not found"

        selected_filter_options = user.get("selected_filter_options", {})
        selected_filter_options = {**selected_filter_options, **kwargs}
        self.update_one({"username": username}, {**user, "selected_filter_options": selected_filter_options})

    def get_user(self, username: str, drop_id=False) -> Dict:
        redis_key = f"user_{username}"
        # generrate_prefix = false (prevent redis from creating prefix which needs access to reqeust.user before assigning user to request"
        cached = self.redis.get(redis_key, generate_prefix=False)
        if cached:
            return cached

        user: Dict = self.stringify(self.find_one({"username": username}))
        if drop_id:
            del user["id"]
        assert bool(user), "user is not found"
        # generrate_prefix = false (prevent redis from creating prefix which needs access to reqeust.user before assigning user to request"
        self.redis.set(redis_key, user, Duration.hours(3), generate_prefix=False)

        return user

    def get_user_roles_perms(self, username: str) -> List[set]:
        user = self.get_user(username)
        return [
            set(user.get("roles") or []),
            set(user.get("permissions") or []),
        ]
