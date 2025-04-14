# import mongoengine as me
# from .models import Hitit



# class ClassHierarchyGetter:
#     db_conn = False
#     db_host = 'mongodb+srv://scraper:rpSoT91YMlaWtMSe@cluster0.px6yh.mongodb.net/scraper'

#     def __init__(self, airline_code, seg_origin, seg_destination, flight_number,
#                  departure_date, leg_origin, leg_destination, cabin_code):
#         self.airline_code = airline_code
#         self.origin = seg_origin
#         self.seg_destination = seg_destination
#         self.flight_number = flight_number
#         self.departure_date = departure_date
#         self.leg_origin = leg_origin
#         self.leg_destination = leg_destination
#         self.cabin_code = cabin_code
#         # self.db_connect()
#         handler = RevenueManagmentHandler("data2.xml",airlineCode="PY",arrPort="AMS",depPort="PBM",
#                                         dayCount=1,fltNo='994',startDate='2023-03-15')
#         seg = handler.get()
#         leg = seg._legs(origin=leg_origin, destination=leg_destination)[0]
#         cabin = leg._cabins(code=cabin_code)[0]
#         self.classes = cabin._classes()

    # def db_connect(self):
    #     if not ClassHierarchyGetter.db_conn:
    #         me.connect(host=self.db_host)
    #     ClassHierarchyGetter.db_conn = True

    # def _get_seg(self):
    #     seg = Hitit.objects(
    #         airline_code=self.airline_code, 
    #         origin=self.origin, 
    #         destination=self.seg_destination, 
    #         flight_number=self.flight_number, 
    #         departure_date=self.departure_date,
    #     ).first()
    #     return seg

    # def _get_leg(self):
    #     seg = self._get_seg()
    #     leg = seg.legs.get(origin=self.leg_origin, destination=self.leg_destination)
    #     return leg

    # def _get_cabin(self):
    #     leg = self._get_leg()
    #     cabin = leg.cabins.get(code=self.cabin_code)
    #     return cabin

    # def get_class_hierarchy(self):
    #     cabin = self.classes()
    #     sum([cl.bookings for cl in cabin.classes])
    #     cls_hierarchy = [c['code'] for c in cabin.classes]
    #     return cls_hierarchy


class HititAuthorizationCalculator:

    def __init__(self, cabin, set_avail, step=1):
        self.cabin = cabin
        self.classes = cabin.classes
        self.set_avail = set_avail
        self.step = step

    def _find_target_class_code(self):
        target_ix = self._find_current_class_ix() + self.step
        return self.classes[target_ix].code

    def _find_current_class_ix(self):
        for i in reversed(range(len(self.classes))):
            if self.classes[i].seats_available > 0:
                return i

    def _get_total_bookings(self):
        return sum([cls.bookings for cls in self.classes])
    
    def calculate(self):
        code = self._find_target_class_code()
        auth = self._get_total_bookings + self.set_avail
        return {'code': code, 'authorizations': auth}
