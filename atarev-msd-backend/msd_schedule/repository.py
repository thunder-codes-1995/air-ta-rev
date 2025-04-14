from base.repository import BaseRepository


class MsdScheduleRepository(BaseRepository):
    collection = 'msd_schedule'

    def get_list(self, pipeline=[]):
        return self.aggregate(pipeline)
