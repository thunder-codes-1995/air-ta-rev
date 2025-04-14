import logging
from abc import abstractmethod

logger = logging.getLogger('fares_etl')

BUSINESS="BUSINESS"
ECONOMY="ECONOMY"
PREFERRED="PREFERRED"
FIRST="FIRST"
UNKNOWN="UNKNOWN"

class AbstractCabinNameTranslator():
    def __init__(self, website_name):
        self.website_name=website_name

    @abstractmethod
    def translate_product_name_to_cabin(self, product_name):
        pass

class PegasusCabinNameTranslator(AbstractCabinNameTranslator):
    def __init__(self):
        super().__init__('flypgs.com')

    def translate_product_name_to_cabin(self, product_name):
        return ECONOMY

class ThyCabinNameTranslator(AbstractCabinNameTranslator):
    def __init__(self):
        super().__init__('turkishairlines.com')
    def translate_product_name_to_cabin(self, product_name):
        return product_name

class Rome2RioCabinNameTranslator(AbstractCabinNameTranslator):
    def __init__(self):
        super().__init__('rome2rio.com')
    def translate_product_name_to_cabin(self, product_name):
        return f"{product_name}".upper()

class SlmCabinNameTranslator(AbstractCabinNameTranslator):
    def __init__(self):
        super().__init__('slm')
    def translate_product_name_to_cabin(self, product_name):
        if "BUSINESSS" in product_name.upper():
            return BUSINESS
        return ECONOMY

class ItaCabinNameTranslator(AbstractCabinNameTranslator):
    def __init__(self):
        super().__init__('itasoftware')
    def translate_product_name_to_cabin(self, product_name):
        if "COACH" in product_name.upper():
            return ECONOMY
        if "BUSINESS" in product_name.upper():
            return BUSINESS
        return ECONOMY

class DefaultCabinNameTranslator(AbstractCabinNameTranslator):
    def __init__(self):
        super().__init__('')

    def translate_product_name_to_cabin(self, product_name):
        if "COACH" in product_name.upper() or "ECONOMY" in product_name.upper():
            return ECONOMY
        if "BUSINESS" in product_name.upper():
            return BUSINESS
        if "PREFER" in product_name.upper():
            return PREFERRED
        if "FIRST" in product_name.upper():
            return FIRST
        return product_name

class CabinNameTranslator():
    """This class is supposed to normalize cabin names (translate custom name from scraper/website to standarized name used by Atarev) """
    translators = [
        PegasusCabinNameTranslator(),
        ThyCabinNameTranslator(),
        Rome2RioCabinNameTranslator(),
        SlmCabinNameTranslator(),
        ItaCabinNameTranslator()
    ]
    default_translator = DefaultCabinNameTranslator()
    def getItinCabin(self, fare):
        if type(fare['itinCabins']) is list:
            return fare['itinCabins'][0]
        else:
            return fare['itinCabins']

    def translate(self, fare):
        #get cabin name
        itinCabin = self.getItinCabin(fare)
        return itinCabin['cabin'][0]
        # website_name = fare['scrapedFrom']
        # matching_translator=None
        # cabinNormalized=None
        # #find translator for website
        # for translator in self.translators:
        #     if translator.website_name == website_name:
        #         matching_translator=translator
        #         break
        # if matching_translator is None:
        #     logger.warning(f"Unable to find appropriate CabinNameTranslator for website:{website_name}, will use default translator to translate product:{productName}")
        #     matching_translator=self.default_translator

        # try:
        #     cabinNormalized = matching_translator.translate_product_name_to_cabin(productName)
        #     if cabinNormalized is None:
        #         logger.error(f"Could not normalize cabin name:{productName} scraped from website:{website_name}, using default")
        #         #return UNKNOWN
        # except Exception as e:
        #     logger.error(f"Error while trying to normalize cabin name:{productName} scraped from website:{website_name}, error:{e}")
        # if cabinNormalized is None:
        #     cabinNormalized = UNKNOWN
        # return cabinNormalized
        

