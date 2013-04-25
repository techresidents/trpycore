import calendar
import datetime
import re
import time

import pytz

#Regex for parsing iso 8601 dates.
#Examples include '2013-01-13', '20130112','2013-W02-7',
#'2013-013', '2013W02', '2013-W02', etc.
#See http://en.wikipedia.org/wiki/ISO_8601 for details.
ISO_8601_DATE_REGEX = re.compile(
    r"^(?P<year>\d{4})(?:" + \
    r"(?:-?(?P<month>\d{2})(?:-?(?P<day>\d{2})))|" + \
    r"(?:-?(?P<day_of_year>\d{3}))|" + \
    r"(?:-?W(?P<week>\d{2})(?:-?(?P<weekday>[1-7]))?)" + \
    r")?$")

#Regex for parsing iso 8601 times (no timezone).
#Examples include '20:30:45.123', '203045.123', '20:30:45', etc.
#See http://en.wikipedia.org/wiki/ISO_8601 for details.
ISO_8601_TIME_REGEX = re.compile(
        r"^(?P<hour>\d{2})(?:" + \
        r"(?::?(?P<minute>\d{2}))(?:" + \
        r"(?::?(?P<second>\d{2}))(?:" + \
        r"(?P<fractional_seconds>\.\d+)?" + \
        r")?)?)?$")

#Regex for parsing iso 8601 timezones.
#Examples include 'Z', '+04:00', '-0400', '-07:00', '+04', etc.
#See http://en.wikipedia.org/wiki/ISO_8601 for details.
ISO_8601_TIMEZONE_REGEX = re.compile(
        r"(?P<utc>Z)|" + \
        r"(?:(?P<plus_or_minus>[+-])" + \
        r"(?P<hour_offset>\d{2})(?::?(?P<minute_offset>\d{2}))?)$")

#Regex for parsing datetimes relatvie to 'now'.
#Examples include 'now', 'now+1h', 'now+10d+1h+1m-1s', etc.
NOW_REGEX = re.compile(
    r"^now(?:(?P<plus_or_minus>[+-])(?P<offset>\d+)(?P<type>[dhms]))*$")
NOW_REGEX_TERM = re.compile(
    r"(?P<plus_or_minus>[+-])(?P<offset>\d+)(?P<type>[dhms])")

def utcnow():
    """Return a datetime object in UTC with tzinfo properly set."""
    return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

def utc_to_local(utc_datetime, timezone="US/Eastern"):
    """Convert utc_datetime to a local datetime."""
    utc_datetime = utc_datetime.replace(tzinfo=pytz.utc)
    return utc_datetime.astimezone(pytz.timezone(timezone))

def utc_to_timestamp(utc_datetime):
    """Convert UTC datetime object to seconds since epoch timestamp."""
    return calendar.timegm(utc_datetime.timetuple()) + utc_datetime.microsecond/1000000.0


def isodate_to_utc(formatted):
    """Convert iso date string to a UTC datetime object."""
    formatted = formatted.strip()
    
    #If not a valid iso date return None
    match = ISO_8601_DATE_REGEX.match(formatted)
    if match is None:
        return None

    year = int(match.groupdict()["year"])
    month = match.groupdict()["month"]
    day = match.groupdict()["day"]
    day_of_year = match.groupdict()["day_of_year"]
    week = match.groupdict()["week"]
    weekday = match.groupdict()["weekday"]
    
    if day_of_year:
        #handle case of ordinal iso date, '2013-012'
        day_of_year = int(day_of_year)
        result = datetime.datetime(year, 1, 1)
        #subtract one from the days offset since 1 day is
        #already included in result by setting it to the 1st.
        result += datetime.timedelta(days=int(day_of_year)-1)
    elif week:
        #handle the case of a iso week date, '2013-W02-6'
        week = int(week)
        weekday = int(weekday) if weekday else 1
        result = datetime.datetime(year, 1, 1)

        #The first week of an ISO 8601 year is the week with the
        #majority of its days in the new Gregorian year. ISO weeks always
        #start on Monday, so the first ISO week may contain a few days
        #from the previous Gregorian calendar. ISO weeks always end on
        #Sunday, so the last ISO week (52 or 53) may conatain a few days
        #from the following Gregorian year.
        #Example: '1997-W01' is from 1996-12-20 to 1997-01-05.
        jan1_weekday = result.isoweekday()
        if jan1_weekday <= 4:
            delta_start = 1 - jan1_weekday
        else:
            delta_start = 8 - jan1_weekday
        
        absolute_days = weekday + (7 * (week -1))
        delta = delta_start + absolute_days - 1
        result += datetime.timedelta(days=delta)
    else:
        #handle simple case, '2013-01-13'
        month = int(month) if month else 1
        day = int(day) if day else 1
        result = datetime.datetime(year, month, day)
    
    result = result.replace(tzinfo=pytz.utc)
    return result

def iso_to_utc(formatted):
    """Convert iso datetime string to a UTC datetime object."""
    formatted = formatted.strip()
    delim = ' ' if formatted.find('T') == -1 else 'T'
    parts = formatted.split(delim)
    formatted_date = parts[0]
    result = isodate_to_utc(formatted_date)
    
    #if time was not provided return the result
    if len(parts) < 2:
        return result
    
    #parse iso timezone from time
    formatted_time = parts[1]
    timezone_match = ISO_8601_TIMEZONE_REGEX.search(formatted_time)
    utc_offset = 0
    if timezone_match is not None:
        utc = timezone_match.groupdict()["utc"]
        plus_or_minus = timezone_match.groupdict()["plus_or_minus"]
        hour_offset = timezone_match.groupdict()["hour_offset"]
        minute_offset = timezone_match.groupdict()["minute_offset"]

        if utc is None:
            hour_offset = int(hour_offset)
            minute_offset = int(minute_offset) if minute_offset else 0
            utc_offset = hour_offset * 60 + minute_offset
            utc_offset *= 1 if plus_or_minus == '-' else -1
        formatted_time = formatted_time[:timezone_match.start()]

    #parse iso time
    match = ISO_8601_TIME_REGEX.match(formatted_time)
    if match is None:
        return None

    hour = int(match.groupdict()["hour"])
    minute = match.groupdict()["minute"]
    second = match.groupdict()["second"]
    fractional_seconds = match.groupdict()["fractional_seconds"]

    minute = int(minute) if minute else 0
    second = int(second) if second else 0
    if fractional_seconds:
        microsecond = int(float(fractional_seconds) * 1000000)
    else:
        microsecond = 0

    result = result.replace(
            hour=hour,
            minute=minute,
            second=second,
            microsecond=microsecond)

    if utc_offset != 0:
        result += datetime.timedelta(minutes=utc_offset)

    return result

def timestamp_to_utc(timestamp):
    """Convert seconds since epoch timestamp to UTC datetime object."""
    return datetime.datetime.fromtimestamp(timestamp, tz=pytz.utc)

def timestamp():
    """Return seconds since epoch timestamp."""
    return time.time()

def now_to_utc(formatted):
    """Convert a now string to a UTC datetime object."""
    formatted = formatted.strip()
    if NOW_REGEX.match(formatted) is None:
        return None
    
    offset_seconds = 0
    for plus_or_minus, offset, type in NOW_REGEX_TERM.findall(formatted):
        offset = int(offset)
        offset *= 1 if plus_or_minus == '+' else -1
        if type == 's':
            offset_seconds += offset
        elif type == 'm':
            offset_seconds += offset * 60
        elif type == 'h':
            offset_seconds += offset * 3600
        elif type == 'd':
            offset_seconds += offset * 24 * 3600
    
    result = utcnow() + datetime.timedelta(seconds=offset_seconds)
    return result
