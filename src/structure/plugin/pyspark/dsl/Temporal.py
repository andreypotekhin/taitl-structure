"""Canonical string choices for temporal field, truncation, and weekday arguments."""

from enum import StrEnum


class Temporal(StrEnum):
    """Use a named temporal choice wherever the corresponding string is accepted."""

    YEAR = "year"
    YEAR_OF_WEEK = "yearofweek"
    QUARTER = "quarter"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    DAY_OF_WEEK = "dayofweek"
    ISO_DAY_OF_WEEK = "dayofweek_iso"
    DAY_OF_YEAR = "doy"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"
    MILLISECOND = "millisecond"
    MICROSECOND = "microsecond"
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"
