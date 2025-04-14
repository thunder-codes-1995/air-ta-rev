import mongoengine as me


class Class(me.EmbeddedDocument):
    class_code = me.StringField(required=True)
    seats_available = me.IntField()
    authorizations = me.IntField()
    bookings = me.IntField()
    group_bookings = me.IntField()
    waitlisted = me.IntField()


class Cabin(me.EmbeddedDocument):
    code = me.StringField()
    cabin_code = me.StringField()
    classes = me.EmbeddedDocumentListField(Class)


class Leg(me.EmbeddedDocument):
    origin = me.StringField(required=True)
    destination = me.StringField(required=True)
    flight_number = me.IntField()
    departure_date = me.IntField()
    arrival_date = me.IntField()
    departure_time = me.IntField()
    arrival_time = me.IntField()
    cabins = me.EmbeddedDocumentListField(Cabin)
    

class Hitit(me.Document):
    airline_code = me.StringField(required=True)
    origin = me.StringField(required=True)
    destination = me.StringField(required=True)
    flight_number = me.IntField()
    flight_departure_date = me.IntField()
    departure_date = me.IntField()
    legs = me.EmbeddedDocumentListField(Leg)
