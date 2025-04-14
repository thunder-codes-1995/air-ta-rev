import mongoengine as me
from typing import List
from mongoengine.base.datastructures import EmbeddedDocumentList, BaseList

class ExpiryDateValue(me.EmbeddedDocument):
    daysToDepartureMin = me.IntField()
    daysToDepartureMax = me.IntField()
    expireInHours = me.IntField()

class ConfigurationEntry(me.EmbeddedDocument):
    key = me.StringField()
    description = me.StringField()
    value = me.DynamicField()

class Configuration(me.Document):
    customer = me.StringField()
    configurationEntries = me.EmbeddedDocumentListField(ConfigurationEntry)

    # def get_scraped_fare_expiry_rules(self):
    #     confs: EmbeddedDocumentList = self.configurationEntries
    #     expiry_conf: ConfigurationEntry = confs.get(key='SCRAPED_FARE_EXPIRY_RULES')
    #     if not self.is_scraped_fare_expiry_rules_parsed:
    #         value: BaseList = expiry_conf.value
    #         value_arr: List = [ExpiryDateValue(**data) for data in value]
    #         expiry_conf.value = EmbeddedDocumentList(value_arr, expiry_conf, 'value')
    #         self.validate()
    #     self.is_scraped_fare_expiry_rules_parsed = True
    #     return expiry_conf

    def get_scraped_fare_expiry_rules(self) -> List[ExpiryDateValue]:
        confs: EmbeddedDocumentList = self.configurationEntries
        expiry_conf: ConfigurationEntry = confs.get(key='SCRAPED_FARE_EXPIRY_RULES')
        return [ExpiryDateValue(**data) for data in expiry_conf.value]
    
    def save():
        raise AttributeError('Save is not enabled on configuration collection')

