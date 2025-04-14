import math


def format_kpi_amount(amount: float = 0) -> str:
    if amount is None:
        return "0"

    suffixes = ['', 'K', 'M', 'B', 'T', '^', '^', '^', '^']
    new_value = amount
    suffix_idx = 0
    while new_value >= 1000:
        new_value /= 1000
        suffix_idx += 1

    if len(f"{round(new_value, 1)}") > 3:
        return f"{int(new_value)}{suffixes[suffix_idx]}"
    return f"{round(new_value, 1)}{suffixes[suffix_idx]}"


def format_kpi_percentage(pct: float = 0) -> str:
    if math.fabs(pct) < 0.0001 or math.isinf(pct) or math.isnan(pct):
        pct = 0
    return pct


# instead of scientific notation (e.g. "1e-08") format float as number with zeroes (e.g. 0.0000000001)
def format_float_no_scientific_notation(flt):
    str_vals = str(flt).split('e')
    coef = float(str_vals[0])
    exp = int(str_vals[1])
    return_val = ''
    if int(exp) > 0:
        return_val += str(coef).replace('.', '')
        return_val += ''.join(['0' for _ in range(0, abs(exp - len(str(coef).split('.')[1])))])
    elif int(exp) < 0:
        return_val += '0.'
        return_val += ''.join(['0' for _ in range(0, abs(exp) - 1)])
        return_val += str(coef).replace('.', '')
    return return_val
