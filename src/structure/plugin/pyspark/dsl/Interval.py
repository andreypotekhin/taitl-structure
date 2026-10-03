"""Canonical strings for interval units and qualifiers."""

from enum import StrEnum


class Interval(StrEnum):
    """Use a named interval unit or qualifier wherever its string is accepted."""

    YEAR = "year"
    MONTH = "month"
    YEAR_TO_MONTH = "year_to_month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"
    DAY_TO_HOUR = "day_to_hour"
    DAY_TO_MINUTE = "day_to_minute"
    DAY_TO_SECOND = "day_to_second"
    HOUR_TO_MINUTE = "hour_to_minute"
    HOUR_TO_SECOND = "hour_to_second"
    MINUTE_TO_SECOND = "minute_to_second"
    CALENDAR = "calendar"
