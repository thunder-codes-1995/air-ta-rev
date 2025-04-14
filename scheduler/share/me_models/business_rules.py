import mongoengine as me


class Facts(me.EmbeddedDocument):
    leg = me.DictField()
    market = me.DictField()
    cabin = me.DictField()
    hostFare = me.DictField()
    mainCompetitorFare = me.DictField()
    lowestFare = me.DictField()
    legForecast = me.DictField()
    fares = me.DictField()


class Action(me.EmbeddedDocument):
    type = me.StringField()
    params = me.DictField()


class ActionResults(me.EmbeddedDocument):
    effectiveClass = me.StringField()
    effectiveAvailability = me.IntField()
    effectiveFare = me.IntField()

class BreRulesResults(me.Document):
    creationDate = me.DateField()
    flightKey = me.StringField()
    carrier = me.StringField()
    updated_at = me.StringField()
    created_at = me.StringField()
    facts = me.EmbeddedDocumentField(Facts)
    action = me.EmbeddedDocumentField(Action)
    actionResults = me.EmbeddedDocumentField(ActionResults)
    ruleId = me.ObjectIdField()




