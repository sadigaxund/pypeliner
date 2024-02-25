
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import *
import calendar

__all__ = [
    "DEFAULT_LONG_TIMESTAMP_FORMAT",
    "DEFAULT_DATE_FORMAT",
    "DEFAULT_TIMESTAMP_FORMAT",
    "monthly_intervals",
    "to_datetime",
    "next_month",
    "last_month",
    "most_recent",
    "get_date_time",
    "n_days_before",
    "date_to_string",
    "timestamp",
    "convert_to_timestamp"
]

DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%d"
DEFAULT_LONG_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def monthly_intervals(
    start_date: datetime, 
    end_date: datetime, 
) -> Generator[Tuple[str, str], None, None]:
    # Initialize the current_month to start_date
    current_month = start_date

    # Iterate through months and yield the intervals
    while current_month <= end_date:
        _next_month = next_month(current_month, set_day=1)
        if _next_month > end_date:
            yield current_month, end_date
        else:
            yield current_month, _next_month - timedelta(days=1)
        current_month = _next_month


def next_month(
    target_date: datetime=datetime.today(), 
    set_day: int=None
) -> datetime:
    # Extract the current month and year
    current_year = target_date.year
    current_month = target_date.month
    # Calculate the total number of days in the current month
    total_days_in_month = calendar.monthrange(current_year, current_month)[1]
    # add one month to today's date
    next_month = target_date + timedelta(days=total_days_in_month)

    max_month_days = calendar.monthrange(next_month.year, next_month.month)[1]
    if set_day:
        next_month = next_month.replace(day=max(min(max_month_days, set_day), 1))
    # return the date of next month
    return next_month


def last_month(target_date=datetime.today(), set_day=None):
    """
    Calculates the date of the last month based on the provided parameters.

    Parameters:
    - target_date: datetime, optional
        The target date for which the last month's date is calculated. Default is today.
    - set_day: int, optional
        The day of the month to set for the result. If None, it will use the day from target_date.

    Returns:
    - datetime
        The calculated last month's date.
    """
    # Set the day to the provided set_day or use the day from target_date
    day = set_day if set_day is not None else target_date.day

    # Calculate the last month
    if target_date.month == 1:
        last_month_date = target_date.replace(
            year=target_date.year - 1, month=12, day=1)
    else:
        last_month_date = target_date.replace(
            month=target_date.month - 1, day=1)

    # Determine the number of days in the last month
    _, last_month_days = calendar.monthrange(
        last_month_date.year, last_month_date.month)

    # Set the day for the result, considering the length of the last month
    result_day = min(day, last_month_days)
    result_date = last_month_date.replace(day=result_day)

    return result_date


def to_datetime(
    value: Union[str, datetime], 
    formats: Iterable = [DEFAULT_DATE_FORMAT]
) -> datetime:
    
    if not isinstance(formats, Iterable) or isinstance(formats, str):
        formats = [formats]
    
    if value is None:
        return None

    if isinstance(value, datetime):
        return value
    elif isinstance(value, str):
        
        for format in formats:
            try:
                return datetime.strptime(value, format)
            except ValueError as e:
                ...
    else:
        raise ValueError("Invalid input type. Supported types are datetime and str.")


convert_to_timestamp = partial(
    to_datetime, formats=[DEFAULT_TIMESTAMP_FORMAT, DEFAULT_LONG_TIMESTAMP_FORMAT])


def date_to_string(
    value: datetime, 
    format_str: str = DEFAULT_DATE_FORMAT
) -> str:
    """
    Converts a datetime object to a string based on the provided format.

    Parameters:
    - value: datetime
        The datetime object to be converted to a string.
    - format_str: str, optional
        The format string to be used for formatting the datetime object. Default is '%Y-%m-%d %H:%M:%S'.

    Returns:
    - str
        The formatted string representation of the datetime object.
    """
    return value.strftime(format_str)


def timestamp(format_str: str = DEFAULT_TIMESTAMP_FORMAT):
    return date_to_string(n_days_before(0, False), format_str)
    

def get_date_time(
    year: int, 
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0
):
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


# trunk-ignore(ruff/D417)
def n_days_before(
    days: int, 
    midnight: bool=True, 
) -> datetime:
    """
    Get the date for the latest reviews.

    Args:
    ----
        ndays: Number of days to subtract from the current date.

    Returns:
    -------
        Formatted date string for the start date of the reviews.
    """
    now = datetime.now()
    if midnight:
        now = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return_date = now - timedelta(days=days)
    return return_date



def most_recent(
    datetime1: Union[datetime, str], 
    datetime2: Union[datetime, str], 
) -> datetime:
    return datetime1 if datetime1 > datetime2 else datetime2



