from wtforms.validators import AnyOf, Regexp


def characters_only_rule(field):
    return [Regexp("^[a-zA-Z]+$", message=f"{field} should consists of characters only")]


def comma_separated_characters_only(field):
    return [Regexp("^[a-zA-Z0-9,]+$", message=f"{field} should consists of (comma separated) characters only")]


def point_rule(field):
    return [
        Regexp("^[a-zA-Z]+-[a-zA-Z]+$", message=f"{field} should be of form 'aa-bb'"),
    ]


def one_of(field, values):
    return [AnyOf(values=values, message=f"{field} should be one of {','.join(values)}")]


def date_range_rule(field, year: bool = True, month: bool = True, day: bool = True):
    patt = ""
    message = ""

    if year:
        patt += "\d{4}"
        message = f"{field} should be of form 'YYYY'"

    if month:
        patt += "-\d{2}"
        message = f"{field} should be of form 'YYYY-MM'"

    if day:
        patt += "-\d{2}"
        message = f"{field} should be of form 'YYYY-MM-DD'"

    if patt.startswith("-"):
        patt.replace("-", "")

    return [
        Regexp(f"^{patt}$", message=message),
    ]
