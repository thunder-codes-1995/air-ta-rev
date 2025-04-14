from core.validators import is_string, min_len, max_len, is_number,length
from functools import partial
from attrs import NOTHING
from attr import attrib


def string(
        *,
        default=NOTHING,
        validator=[],
        repr=True,
        hash=None,
        init=True,
        metadata=None,
        converter=None,
        factory=None,
        kw_only=False,
        eq=None,
        order=None,
        on_setattr=None,
        **kwargs,
):
    """
        function to check if a value is not empty string (in case allow_empty is False) 
        this function is to avoid checking 2 conditions again and again : 
        1 - value is_instance(str)
        2 - value is not empty string if allow_empty = False (default behaviour)
    """
    min_length = kwargs.get("min_length")
    max_length = kwargs.get("max_length")
    _length = kwargs.get("length")
    allow_empty = kwargs.get("allow_empty", False)

    validators = [partial(is_string, allow_empty=allow_empty)]
    if min_length: validators.append(partial(min_len, limit=min_length))
    if max_length: validators.append(partial(max_len, limit=max_length))
    if _length : validators.append(partial(length,limit=_length))
    
    return attrib(
        default=default,
        validator=validators,
        repr=repr,
        hash=hash,
        init=init,
        metadata=metadata,
        converter=converter,
        factory=factory,
        kw_only=kw_only,
        eq=eq,
        order=order,
        on_setattr=on_setattr,
    )


def number(
        *,
        default=NOTHING,
        validator=[],
        repr=True,
        hash=None,
        init=True,
        metadata=None,
        converter=None,
        factory=None,
        kw_only=False,
        eq=None,
        order=None,
        on_setattr=None,
        **kwargs,
):
    _min = kwargs.get("min")
    _max = kwargs.get("max")

    return attrib(
        default=default,
        validator=[partial(is_number, min=_min, max=_max)],
        repr=repr,
        hash=hash,
        init=init,
        metadata=metadata,
        converter=converter,
        factory=factory,
        kw_only=kw_only,
        eq=eq,
        order=order,
        on_setattr=on_setattr,
    )
