"""ISO year-week helpers for the Allocation report's weekly tracking."""

from datetime import date, datetime, timedelta

import pandas as pd


def get_year_week(dt):
    """Return an ISO 'YYYYWW' string (zero-padded week) for a date or date-string."""
    if isinstance(dt, str):
        dt = datetime.strptime(dt, "%Y-%m-%d").date()
    iso_year, iso_week, _ = dt.isocalendar()
    return f"{iso_year}{iso_week:02d}"


def get_prior_x_week_yearweek(x, base_date=None):
    """Return the 'YYYYWW' string for the week *x* weeks before *base_date* (today)."""
    if base_date is None:
        base_date = date.today()
    else:
        base_date = pd.to_datetime(base_date).date()
    target_date = base_date - timedelta(weeks=x)
    return get_year_week(target_date)


def get_week_start(base_date=None):
    """Return the Monday of the ISO week containing *base_date* (today)."""
    if base_date is None:
        base_date = date.today()
    else:
        base_date = pd.to_datetime(base_date).date()
    return base_date - timedelta(days=base_date.weekday())


def week_starts_in_previous_month(base_date=None):
    """True when the ISO week containing *base_date* started in an earlier month.

    The vendor export only carries allocation weeks whose *start* date falls in
    the month the file was generated for, so on these runs the current-month
    file has no row for the week in progress (run on 2026-09-02, the week
    starts 2026-08-31) and the report's YearWeek filter would show nothing.
    Those runs must also read the companion "Previous Month" export.
    """
    if base_date is None:
        base_date = date.today()
    else:
        base_date = pd.to_datetime(base_date).date()
    week_start = get_week_start(base_date)
    return (week_start.year, week_start.month) != (base_date.year, base_date.month)
