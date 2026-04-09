"""
Effective calendar span from *now* to option expiration (America/Chicago).

**Single source of truth** for any “calendar DTE” fed into Mo. Return %, Wheel Alpha
(expected 1SD, DTE weight, gamma tax), and related sqrt(DTE/365) math.

**Must stay identical** across:
  - ``pages/1_Discover.py`` (Options Scanner)
  - ``pages/2_Analyzer.py`` (matrix Wheel Alpha)
  - ``scripts/watchlist_snapshot_to_postgres.py`` (GitHub Actions → Postgres)

Do not reimplement this logic elsewhere; import ``wheel_alpha_effective_calendar_dte`` (alias:
``effective_calendar_days_to_expiration``). Expiry clock: ``PH_EXPIRY_WALL_TIME_CHI``.
"""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

_CHI = ZoneInfo("America/Chicago")
# Sub-second floor so callers never divide by zero (~1 second as a day fraction).
WHEEL_ALPHA_MIN_CALENDAR_DTE_DAYS = 1.0 / 86400.0

# Expiration *date* ends at this Chicago wall time (not midnight). Example: Fri exp at noon → 12/24 of that day.
PH_EXPIRY_WALL_TIME_CHI = dt.time(12, 0, 0)


def wheel_alpha_effective_calendar_dte(expiration_date: dt.date) -> float:
    """
    Calendar days from *now* (Chicago) to expiration, for annualizing premium:

    - **Today:** hours until end of Chicago calendar day (midnight) ÷ 24.
    - **Strictly between** today and expiration date: each counts as 1.
    - **Expiration date:** midnight → PH_EXPIRY_WALL_TIME_CHI on that date ÷ 24
      (e.g. noon expiry → 0.5).

    Same calendar day as expiry: (expiry moment − now) ÷ 24h.

    Returns -1.0 if expiration is before today's Chicago date or expiry moment is not after now.
    """
    now = dt.datetime.now(_CHI)
    today = now.date()
    if expiration_date < today:
        return -1.0

    exp_dt = dt.datetime.combine(expiration_date, PH_EXPIRY_WALL_TIME_CHI, tzinfo=_CHI)
    if exp_dt <= now:
        return -1.0

    if expiration_date == today:
        return max(
            (exp_dt - now).total_seconds() / 86400.0,
            WHEEL_ALPHA_MIN_CALENDAR_DTE_DAYS,
        )

    midnight_after_today = dt.datetime.combine(
        today + dt.timedelta(days=1),
        dt.time(0, 0, 0),
        tzinfo=_CHI,
    )
    frac_today = (midnight_after_today - now).total_seconds() / 86400.0
    frac_today = max(frac_today, WHEEL_ALPHA_MIN_CALENDAR_DTE_DAYS)

    midnight_exp = dt.datetime.combine(
        expiration_date, dt.time(0, 0, 0), tzinfo=_CHI
    )
    frac_exp_day = (exp_dt - midnight_exp).total_seconds() / 86400.0

    full_middle = (expiration_date - today).days - 1
    if full_middle < 0:
        full_middle = 0

    return float(frac_today + full_middle + frac_exp_day)


# Clearer name for new code; same implementation as the scanner / CI job.
effective_calendar_days_to_expiration = wheel_alpha_effective_calendar_dte
