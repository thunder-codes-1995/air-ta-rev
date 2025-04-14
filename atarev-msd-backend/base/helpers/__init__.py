def is_iterable_collection(value) -> bool:
    """checks if a variable is a collection (list, tuple or set)"""
    if type(value) is [str, dict]:
        return False
    try:
        iter(value)
        return True
    except:
        return False
