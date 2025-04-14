from base.constants import Constants


def convert_list_param_to_criteria(param_name, param_value):
    """convert parameter and it's value to a format expected in mongo match (e.g. param = 'ORIG'. value='LHR' will be converted to {'ORIG':'LHR'}"""
    if type(param_value) == list:
        if Constants.ALL in param_value:
            return {}
        if len(param_value) == 0:
            return {}
        elif len(param_value) == 1:
            return {param_name: param_value[0]}
        else:
            return {param_name: {"$in": param_value}}
    else:
        if param_value in [Constants.ALL, "", None]:
            return {}
        return {param_name: param_value}


def merge_criterions(criterions: list):
    """combine multiple mongo conditions (dictionaries) into one match condition (one dictionary)"""
    result = {}
    for criterion in criterions:
        keys = criterion.keys()
        for key in keys:
            result[key] = criterion[key]
    return result
