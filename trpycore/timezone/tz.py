import calendar
import datetime
import time

import pytz

def utcnow():
    """Return a datetime object in UTC with tzinfo properly set."""
    return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

def utc_to_timestamp(utc_datetime):
    """Convert UTC datetime object to seconds since epoch timestamp."""
    return calendar.timegm(utc_datetime.timetuple()) + utc_datetime.microsecond/1000000.0

def timestamp():
    """Return seconds since epoch timestamp."""
    return time.time()
