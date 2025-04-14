import mongoengine as me


class LowestFare(me.EmbeddedDocument):
    travelAgency = me.StringField()
    scrapedFrom = me.StringField()
    fareAmount = me.DecimalField()
    fareCurrency = me.StringField()
    lastUpdateDateTime = me.DecimalField()
    scrapeTime = me.DateTimeField()
    cabinName = me.StringField()
    classCode = me.StringField()
    baseFare = me.DecimalField()
    taxAmount = me.DecimalField()
    yqyrAmount = me.DecimalField()

class Leg(me.EmbeddedDocument):
    legOriginCode = me.StringField()
    legDestCode = me.StringField()
    legDeptDate = me.IntField()
    legDeptTime = me.IntField()
    legArrivalDate = me.IntField()
    legArrivalTime = me.IntField()
    mkAlCode = me.StringField()
    mkFltNum = me.IntField()
    opAlCode = me.StringField()
    opFltNum = me.IntField()
    flightDuration = me.IntField()
    aircraft = me.StringField()


class Itinerary(me.EmbeddedDocument):
    itinOriginCode = me.StringField()
    itinDestCode = me.StringField()
    itinId = me.IntField()
    itinDuration = me.IntField()
    itinDeptDate = me.IntField()
    itinDeptTime = me.IntField()
    itinArrivalDate = me.IntField()
    itinArrivalTime = me.IntField()
    direction = me.StringField()
    legs = me.EmbeddedDocumentListField(Leg)


class HistoricalFare(me.EmbeddedDocument):
    travelAgency = me.StringField()
    scrapedFrom = me.StringField()
    fareAmount = me.DecimalField()
    fareCurrency = me.StringField()
    baseFare = me.DecimalField()
    taxAmount = me.DecimalField()
    yqyrAmount = me.DecimalField()
    lastUpdateDateTime = me.DecimalField()
    cabinName = me.StringField()
    classCode = me.StringField()
    dtd = me.IntField()
    

class Fares2(me.Document):
    flightKey = me.StringField()
    carrierCode = me.StringField()
    historicalFares = me.EmbeddedDocumentListField(HistoricalFare)
    hostCode = me.StringField()
    insertDate = me.IntField()
    itineraries = me.EmbeddedDocumentListField(Itinerary)
    lowestFares = me.EmbeddedDocumentListField(LowestFare)
    lowestFare = me.DynamicField()
    marketDestination = me.StringField()
    marketOrigin = me.StringField()
    outboundDate = me.IntField()
    returnDate = me.IntField()
    type = me.StringField()
    outboundDayOfWeek = me.IntField()
    soldOut = me.DynamicField()
    state = me.StringField()

    def save(self):
        raise AttributeError('Save is not enabled on Fares2 model')


