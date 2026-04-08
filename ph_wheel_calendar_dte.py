"""Effective calendar DTE for Wheel Alpha and Mo. Return % (America/Chicago civil day)."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

_CHI = ZoneInfo("America/Chicago")
# Sub-second guard so callers never divide by zero; ~one second expressed as a day fraction.
WHEEL_ALPHA_MIN_CALENDAR_DTE_DAYS = 1.0 / 86400.0


def wheel_alpha_effective_calendar_dte(expiration_date: dt.date) -> float:
    """
    Calendar span to expiration where 'today' is only the fraction of the Chicago
    calendar day remaining until midnight (e.g. at 10:00 PM, today adds 2/24 of a day).

    For expiration after today: (exp − today).days − 1 + frac_today.
    For 0DTE (expiration today): frac_today only.

    Returns -1.0 if expiration_date is before today's Chicago date.
    """
    now = dt.datetime.now(_CHI)
    today = now.date()
    if expiration_date < today:
        return -1.0
    midnight_next = dt.datetime.combine(
        today + dt.timedelta(days=1),
        dt.time(0, 0, 0),
        tzinfo=_CHI,
    )
    frac_today = (midnight_next - now).total_seconds() / 86400.0
    frac_today = max(frac_today, WHEEL_ALPHA_MIN_CALENDAR_DTE_DAYS)
    d_int = (expiration_date - today).days
    if d_int == 0:
        return float(frac_today)
    return float(d_int - 1) + frac_today
