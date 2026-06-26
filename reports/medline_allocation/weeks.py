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
