import mongoengine as me


class AuthorizationValue(me.EmbeddedDocument):
    class_code = me.StringField()
    authorization = me.IntField()
    rank = me.IntField()


class CabinParams(me.EmbeddedDocument):
    airline_code = me.StringField()
    origin = me.StringField()
    destination = me.StringField()
    flight_number = me.IntField()
    departure_date = me.IntField()
    leg_origin = me.StringField()
    leg_destination = me.StringField()
    cabin_code = me.StringField()


class AuthorizationResults(me.Document):
    authorization_value = me.EmbeddedDocumentField(AuthorizationValue)
    cabin_params = me.EmbeddedDocumentField(CabinParams)
    old_authorization_value = me.EmbeddedDocumentField(AuthorizationValue)
