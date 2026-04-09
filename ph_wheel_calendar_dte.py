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

``wheel_calendar_chicago_now()`` is the clock used (``datetime.now(Chicago)`` by default). Optional
debug: env or Streamlit secret ``PH_CHICAGO_NOW_OVERRIDE`` = ISO datetime (naive = Chicago local).

Call ``log_wheel_calendar_clock("label")`` from scans to emit UTC / naive local / Chicago to stderr + logging.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import sys
from zoneinfo import ZoneInfo

_log = logging.getLogger(__name__)

_CHI = ZoneInfo("America/Chicago")
# Sub-second floor so callers never divide by zero (~1 second as a day fraction).
WHEEL_ALPHA_MIN_CALENDAR_DTE_DAYS = 1.0 / 86400.0

# Expiration *date* ends at this Chicago wall time (not midnight). Example: Fri exp at noon → 12/24 of that day.
PH_EXPIRY_WALL_TIME_CHI = dt.time(12, 0, 0)


def wheel_calendar_chicago_now() -> dt.datetime:
    """
    “Now” in America/Chicago for calendar-DTE math.

    Uses the host clock converted to Chicago (correct on Streamlit Cloud: servers are UTC;
    this is not “server local wall time”).

    Optional override (debug / demos): ISO string in env ``PH_CHICAGO_NOW_OVERRIDE``, or the
    same key in ``st.secrets`` on Streamlit. Naive values are interpreted as Chicago local.
    """
    raw = (os.environ.get("PH_CHICAGO_NOW_OVERRIDE") or "").strip()
    if not raw:
        try:
            import streamlit as st

            if hasattr(st, "secrets"):
                sec = st.secrets.get("PH_CHICAGO_NOW_OVERRIDE")
                if sec is not None and str(sec).strip():
                    raw = str(sec).strip()
        except Exception:
            pass
    if raw:
        part = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if part.tzinfo is None:
            return part.replace(tzinfo=_CHI)
        return part.astimezone(_CHI)
    return dt.datetime.now(_CHI)


def log_wheel_calendar_clock(event: str = "premiumhunter") -> None:
    """
    Log host time perception (UTC, naive process local, America/Chicago used for DTE).
    Goes to stderr and the premiumhunter.wheel_calendar logger for Streamlit Cloud / CI.
    """
    utc = dt.datetime.now(dt.timezone.utc)
    naive_local = dt.datetime.now()
    chi = wheel_calendar_chicago_now()
    ov = wheel_calendar_override_active()
    msg = (
        f"[{event}] clock UTC={utc.isoformat()} | "
        f"process_local_naive={naive_local.isoformat()} | "
        f"America/Chicago={chi.isoformat()} | "
        f"TZ_env={os.environ.get('TZ', '')!r} | "
        f"PH_CHICAGO_NOW_OVERRIDE={ov}"
    )
    _log.info(msg)
    print(msg, file=sys.stderr, flush=True)


def wheel_calendar_override_active() -> bool:
    """True if Mo. Return / calendar DTE use PH_CHICAGO_NOW_OVERRIDE (env or Streamlit secrets)."""
    if (os.environ.get("PH_CHICAGO_NOW_OVERRIDE") or "").strip():
        return True
    try:
        import streamlit as st

        if hasattr(st, "secrets") and st.secrets.get("PH_CHICAGO_NOW_OVERRIDE"):
            return True
    except Exception:
        pass
    return False


def wheel_alpha_effective_calendar_dte_detail(
    expiration_date: dt.date,
) -> tuple[float, dict[str, float | str]]:
    """
    Same total as ``wheel_alpha_effective_calendar_dte``, plus a breakdown dict for logging/UI.
    On invalid dates returns (-1.0, {"reason": "..."}).
    """
    now = wheel_calendar_chicago_now()
    today = now.date()
    if expiration_date < today:
        return -1.0, {"reason": "expiration_date before Chicago today", "chicago_date": str(today)}

    exp_dt = dt.datetime.combine(expiration_date, PH_EXPIRY_WALL_TIME_CHI, tzinfo=_CHI)
    if exp_dt <= now:
        return -1.0, {"reason": "expiry moment not after now", "expiry_iso": exp_dt.isoformat()}

    if expiration_date == today:
        raw_sec = (exp_dt - now).total_seconds()
        total = max(raw_sec / 86400.0, WHEEL_ALPHA_MIN_CALENDAR_DTE_DAYS)
        return float(total), {
            "branch": "same_calendar_day_as_expiry",
            "chicago_now": now.isoformat(),
            "seconds_to_expiry": raw_sec,
            "total_days": total,
        }

    midnight_after_today = dt.datetime.combine(
        today + dt.timedelta(days=1),
        dt.time(0, 0, 0),
        tzinfo=_CHI,
    )
    frac_today_raw = (midnight_after_today - now).total_seconds() / 86400.0
    frac_today = max(frac_today_raw, WHEEL_ALPHA_MIN_CALENDAR_DTE_DAYS)

    midnight_exp = dt.datetime.combine(
        expiration_date, dt.time(0, 0, 0), tzinfo=_CHI
    )
    frac_exp_day = (exp_dt - midnight_exp).total_seconds() / 86400.0

    full_middle = (expiration_date - today).days - 1
    if full_middle < 0:
        full_middle = 0

    total = float(frac_today + full_middle + frac_exp_day)
    return total, {
        "branch": "multi_day",
        "chicago_now": now.isoformat(),
        "chicago_today": str(today),
        "frac_today_days": frac_today,
        "full_middle_calendar_days": float(full_middle),
        "frac_expiry_date_days": frac_exp_day,
        "expiry_wall_chi": PH_EXPIRY_WALL_TIME_CHI.isoformat(),
        "total_days": total,
    }


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
    total, _ = wheel_alpha_effective_calendar_dte_detail(expiration_date)
    return total


def log_calendar_dte_breakdown(
    event: str,
    expiration_date: dt.date,
    *,
    detail: tuple[float, dict[str, float | str]] | None = None,
) -> None:
    """Emit calendar-DTE total + slice breakdown to stderr and logging (Streamlit Cloud / local)."""
    total, br = (
        detail
        if detail is not None
        else wheel_alpha_effective_calendar_dte_detail(expiration_date)
    )
    msg = f"[{event}] exp={expiration_date} calendar_dte={total:.6f} detail={br!r}"
    _log.info(msg)
    print(msg, file=sys.stderr, flush=True)


# Clearer name for new code; same implementation as the scanner / CI job.
effective_calendar_days_to_expiration = wheel_alpha_effective_calendar_dte
