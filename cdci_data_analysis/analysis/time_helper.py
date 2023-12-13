from dateutil import parser
from datetime import datetime

from ..app_logging import app_logging

time_helper_logger = app_logging.getLogger('time_helper')

def validate_time(timestamp_to_validate):
    try:
        datetime_obj = datetime.fromtimestamp(float(timestamp_to_validate))
    except (ValueError, OverflowError, TypeError, OSError) as e:
        time_helper_logger.warning(f'Error when constructing the datetime object from the timestamp {timestamp_to_validate}:\n{e}')
        raise
    return datetime_obj


def format_time(time_str, tz_to_apply):
    # format the time fields, from the format request
    t_parsed = parser.parse(time_str, ignoretz=True)
    t_formatted = t_parsed.astimezone(tz_to_apply).strftime('%Y-%m-%dT%H:%M:%S%z')

    return t_formatted
