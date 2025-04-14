import copy

from jobs.lib.fares.cabin_name_translator import CabinNameTranslator


class FareRecordConverter:
    cabin_name_translator = CabinNameTranslator()

    def convert(self, type, itineraries, fares):
        """Converts 'raw' fares into 'processed'. Steps:
        - extract fares for outbound itineraries (in case or RT fares)
        - normalize product names to cabin name (e.g.ECO_FLEX -->ECONOMY)
        - extract class name
        - extract fare
        """
        if type == "OW":
            return self.convert_fares(fares)
        else:
            outboundFares = self.extract_outbound_fares(itineraries, fares)
            return self.convert_fares(outboundFares)
        return None

    def getItinCabin(self, fare):
        if type(fare["itinCabins"]) is list:
            return fare["itinCabins"][0]
        else:
            return fare["itinCabins"]

    def extract_outbound_fares(self, itineraries, fares):
        """get outbound fares only"""
        itinID = itineraries[0]["itinId"]
        # then find all fares for that itinerary only
        outboundFares = list(filter(lambda fare: str(self.getItinCabin(fare)["itinId"]) == str(itinID), fares))

        return outboundFares

    def convert_fares(self, fares):
        result = []
        for fare in fares:
            result.append(self.convert_single_fare(fare))
        return result

    def convert_single_fare(self, fare):
        (cabinName, classCode) = self.extract_class(fare)
        newFareRecord = copy.deepcopy(fare)
        newFareRecord["cabinName"] = cabinName
        newFareRecord["classCode"] = classCode
        newFareRecord.pop("itinCabins")
        return newFareRecord

    def extract_class(self, fare):
        itinCabin = self.getItinCabin(fare)
        if "cabinClass" not in itinCabin:
            classCode = ""
        else:
            classCode = itinCabin["cabinClass"]
        # NOTE: this is not translating anymore it only gets me cabinCode
        cabinName = self.cabin_name_translator.translate(fare)
        return (cabinName, classCode)
