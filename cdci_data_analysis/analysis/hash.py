import hashlib
import json
from collections import OrderedDict

def make_hash(o):
    """
    Makes a hash from a dictionary, list, tuple or set to any level, that contains
    only other hashable types (including any lists, tuples, sets, and
    dictionaries).

    """

    # note that even strings change hash() value between python invocations, so it's not safe to do so
    def format_hash(x): return hashlib.md5(
        json.dumps(sorted(x)).encode()
    ).hexdigest()[:16]

    if isinstance(o, (set, tuple, list)):
        return format_hash(tuple(map(make_hash, o)))

    elif isinstance(o, (dict, OrderedDict)):
        return make_hash(tuple(o.items()))        

    # this takes care of various strange objects which can not be properly represented
    return format_hash(json.dumps(o))
