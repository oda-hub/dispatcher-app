import hashlib
import json
from collections import OrderedDict

default_kw_black_list = ('session_id',
                 'job_id',
                 'token',
                 'dry_run',
                 'oda_api_version',
                 'api',
                 'off_line',
                 'query_status',
                 'async_dispatcher',
                 'file_paths')


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


def make_hash_file(file_path):

    # TODO to adapt in case of big files and reading in chunks will be necessary
    def read_file():
        with open(file_path, 'rb') as f_fn:
            return f_fn.read()

    return hashlib.md5(read_file()).hexdigest()[:16]
