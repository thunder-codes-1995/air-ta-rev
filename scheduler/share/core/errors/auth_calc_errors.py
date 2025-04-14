class UnknownActionTypeError(Exception):
    pass

class UnknownActionRankError(Exception):
    pass

class NoBusinessRuleFoundError(Exception):
    pass

class NoFlightFoundForBusinessRuleError(Exception):
    pass

class NoLegFoundForBusinessRuleError(Exception):
    pass

class NoCabinFoundForBusinessRuleError(Exception):
    pass

class NoAvailableClassError(Exception):
    pass

class ActionRankOutOfRangeError(Exception):
    pass