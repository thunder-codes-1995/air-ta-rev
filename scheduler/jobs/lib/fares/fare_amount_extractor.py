import logging
from abc import abstractmethod


logger = logging.getLogger('fares_etl')


class AbstractFareAmountExtractor():
    @abstractmethod
    def extract_fare_amount(self, itineraries, fare):
        pass


class DefaultTripFareAmountExtractorStrategy(AbstractFareAmountExtractor):
    def extract_fare_amount(self, itineraries, fare):
        return (fare['fareAmount'],fare['fareCurrency'])

class HalfRoundTripFareAmountExtractorStrategy(AbstractFareAmountExtractor):
    def extract_fare_amount(self, itineraries, fare):
        return (round(fare['fareAmount']/2,2),fare['fareCurrency'])

class FareAmountExtractor():
    half_rt_websites=['rome2rio.com']
    half_rt_extractor = HalfRoundTripFareAmountExtractorStrategy()
    default_extractor = DefaultTripFareAmountExtractorStrategy()

    def extract_fare_amount(self, itineraries, fare):
        website_name = fare['scrapedFrom']
        if website_name in self.half_rt_websites:
            return self.half_rt_extractor.extract_fare_amount(itineraries, fare)

        return self.default_extractor.extract_fare_amount(itineraries, fare)