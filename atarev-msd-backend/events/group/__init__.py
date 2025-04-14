from dataclasses import dataclass

from events.group.data import GroupEventData


@dataclass
class GroupedEvents:
    host_code: str

    def get(self):
        events = GroupEventData(self.host_code).get()

        return events
