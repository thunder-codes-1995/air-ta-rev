import mongoengine as me


class Class(me.EmbeddedDocument):
    code = me.StringField(required=True)
    seats_available = me.IntField()
    total_booking = me.IntField()
    total_group_booking = me.IntField()
    authorization = me.IntField()
    parent_booking_class = me.StringField()
    display_sequence = me.IntField()


class Cabin(me.EmbeddedDocument):
    code = me.StringField()
    capacity = me.IntField()
    allotment = me.IntField()
    total_booking = me.IntField()
    total_group_booking = me.IntField()
    available_seats = me.IntField()
    overbooking_level = me.IntField()
    classes = me.EmbeddedDocumentListField(Class)


class Leg(me.EmbeddedDocument):
    origin = me.StringField(required=True)
    destination = me.StringField(required=True)
    flight_number = me.StringField()
    departure_date = me.IntField()
    arrival_date = me.IntField()
    departure_time = me.IntField()
    arrival_time = me.IntField()
    flight_number_ext = me.StringField()
    cabins = me.EmbeddedDocumentListField(Cabin)
    

class Hitit(me.Document):
    airline_code = me.StringField(required=True)
    origin = me.StringField(required=True)
    destination = me.StringField(required=True)
    flight_number = me.StringField()
    flight_number_ext = me.StringField()
    flight_departure_date = me.IntField()
    departure_date = me.IntField()
    legs = me.EmbeddedDocumentListField(Leg)
