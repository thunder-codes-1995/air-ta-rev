from dataclasses import dataclass

from events.demand.data import DemandData
from events.demand.figure import Figure
from events.demand.form import EventDemandForm


@dataclass
class EventDemand:

    form: EventDemandForm
    host_code: str

    def get(self):
        current_fig = Figure(self.form, self.host_code, 2024).get()
        prev_fig = Figure(self.form, self.host_code, 2023).get()

        curr = DemandData(self.form.field.data, 2023).get()
        prev = DemandData(self.form.field.data, 2024).get()

        overview = DemandData(self.form.field.data, 2024).overview()

        return {"current": {"fig": current_fig, "table": curr}, "prev": {"fig": prev_fig, "table": prev}, "overview": overview}
