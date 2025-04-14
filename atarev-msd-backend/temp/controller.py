from base.controller import BaseController
from temp.service import TempService


class TempController(BaseController):
    repository_class = TempService

    def get(self):
        return self.repository.get()
