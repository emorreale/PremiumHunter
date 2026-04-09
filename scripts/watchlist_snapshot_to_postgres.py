#!/usr/bin/env python3
"""
Scan E*Trade option chains for watchlist tickers, compute Wheel Alpha,
and persist results to PostgreSQL:

  etrade_sessions — access tokens (upserted); PK column session_id
  options_scans   — one row per contract, FK session_id → etrade_sessions (same job’s session)

Designed for GitHub Actions (no Streamlit).

Mo. Return % and Wheel Alpha use the same calendar-DTE span as the Streamlit app:
``ph_wheel_calendar_dte.wheel_alpha_effective_calendar_dte`` (do not duplicate).

Required env:
  DATABASE_URL          — Postgres connection URI
  ETRADE_CONSUMER_KEY, ETRADE_CONSUMER_SECRET
  ETRADE_SANDBOX        — "true" or "false"

Watchlist source (in priority order):
  1. WATCHLIST_FILE     — local JSON path (if set, skips DB + secret).
  2. watchlists table   — merge symbols from every row (all owners), deduped; if table has no rows, fall back.
  3. WATCHLIST_JSON     — env / secret fallback, e.g. ["AAPL","NVDA"]

Token source (in priority order):
  1. ETRADE_OAUTH_TOKEN + ETRADE_OAUTH_TOKEN_SECRET env vars (legacy / manual).
  2. Most-recent row from etrade_sessions table (written by etrade_token_refresh.py).

Optional:
  WATCHLIST_FILE        — path to JSON file; overrides DB and WATCHLIST_JSON
  DATABASE_FORCE_IPV4   — if "1"/"true"/"yes", resolve DB host to IPv4 and set libpq hostaddr
                          (GitHub-hosted runners often cannot reach IPv6-only / AAAA-first hosts)
  DATABASE_IPV4         — explicit IPv4 for hostaddr (overrides DATABASE_FORCE_IPV4 resolution)

Supabase from GitHub Actions: use the dashboard *connection pooler* URI (Transaction or Session,
host like *.pooler.supabase.com, often port 6543) if direct db.*.supabase.co still fails; ensure
the password in DATABASE_URL is URL-encoded (e.g. @ → %40).
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
import socket
import sys
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

# Calendar DTE: must match pages/1_Discover.py and pages/2_Analyzer.py (single implementation).
from ph_wheel_calendar_dte import (
    log_calendar_dte_breakdown,
    log_wheel_calendar_clock,
    wheel_alpha_effective_calendar_dte_detail,
)
from watchlist_db import normalize_watchlist_symbols

load_dotenv(_ROOT / ".env")

# ── Constants (same as app) ─────────────────────────────────────────────────
PH_AVG_CALENDAR_DAYS_PER_MONTH = 30.42
PH_WHEEL_MO_RETURN_PENALTY_LOW_PCT = 2.0
PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT = 3.0
PH_WHEEL_DTE_TARGET_DAYS = 5
PH_WHEEL_DTE_GAMMA_POWER = 3.0
PH_GAMMA_TAX_YIELD_REF_PCT = 20.0
PH_GAMMA_TAX_MULT_MIN = 0.5
PH_GAMMA_TAX_MULT_MAX = 1.0
PH_WHEEL_OTM_SAFETY_STD = 0.75  # must match Discover / 2_Analyzer
PH_SYNC_MAX_EXPIRY_DAYS = 45
PH_SYNC_MIN_MO_YIELD_PCT = 2.0


def _scan_date_chicago() -> dt.date:
    """Trading-calendar 'today' for DTE / mo_yield (Discover uses local date; CI runners are UTC)."""
    return dt.datetime.now(ZoneInfo("America/Chicago")).date()


def _trading_dte_anchor_chicago() -> dt.date:
    """Start date for stored trading DTE only (mirrors Discover `_scanner_trading_dte_anchor_date`)."""
    chi = ZoneInfo("America/Chicago")
    now = dt.datetime.now(chi)
    d = now.date()
    wd = d.weekday()
    is_weekend = wd >= 5
    close_et = dt.datetime.combine(
        d, dt.time(16, 0), tzinfo=ZoneInfo("America/New_York")
    )
    close_chi = close_et.astimezone(chi)
    past_close = (not is_weekend) and (now >= close_chi)
    if is_weekend:
        nxt = np.busday_offset(np.datetime64(d), 0, roll="forward")
    elif past_close:
        nxt = np.busday_offset(np.datetime64(d), 1)
    else:
        return d
    return dt.date.fromisoformat(str(nxt))


def _iv_chain_numeric(raw) -> float | None:
    """Raw IV from the chain for DB storage (same scale as Discover’s IV column, not decimalized)."""
    if raw is None or raw == "":
        return None
    try:
        x = float(raw)
    except (TypeError, ValueError):
        return None
    if x <= 0:
        return None
    return x


# ── Wheel Alpha helpers (mirror of 2_Analyzer.py, no Streamlit) ─────────────

def _scan_iv_to_decimal(raw) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        x = float(raw)
    except (TypeError, ValueError):
        return None
    if x <= 0:
        return None
    return x / 100.0 if x > 2.0 else x


def _iv_rank_bounds(symbol: str) -> tuple[float | None, float | None]:
    import yfinance as yf
    try:
        h = yf.Ticker(symbol).history(period="1y", interval="1d", auto_adjust=False)
    except Exception:
        return None, None
    if h is None or len(h) < 45:
        return None, None
    import pandas as pd
    close = pd.to_numeric(h["Close"], errors="coerce").dropna()
    if len(close) < 45:
        return None, None
    lr = np.log(close / close.shift(1))
    hv = lr.rolling(30, min_periods=20).std() * np.sqrt(252)
    hv = hv.dropna()
    if hv.empty:
        return None, None
    lo, hi = float(hv.min()), float(hv.max())
    return (None, None) if hi - lo < 1e-9 else (lo, hi)


def _next_earnings_date(symbol: str, ref_day: dt.date) -> dt.date | None:
    """Next earnings on/after ref_day (US/Eastern calendar); same sources as Discover `_cached_next_earnings_date_str`."""
    import pandas as pd
    import yfinance as yf

    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    try:
        tk = yf.Ticker(sym)
        cal = tk.calendar
        if isinstance(cal, dict):
            eds = cal.get("Earnings Date")
            if eds is not None and eds != "":
                if not isinstance(eds, (list, tuple)):
                    eds = [eds]
                for d in eds:
                    if isinstance(d, dt.datetime):
                        d = d.date()
                    elif isinstance(d, pd.Timestamp):
                        d = d.date()
                    if isinstance(d, dt.date) and d >= ref_day:
                        return d
        edf = tk.get_earnings_dates(limit=20)
        if edf is not None and not edf.empty:
            for ts in sorted(edf.index):
                tsn = pd.Timestamp(ts)
                d = (
                    tsn.tz_convert("America/New_York").date()
                    if tsn.tzinfo is not None
                    else tsn.date()
                )
                if d >= ref_day:
                    return d
        inf = tk.info or {}
        ts = (
            inf.get("earningsTimestamp")
            or inf.get("earningsTimestampStart")
            or inf.get("earningsTimestampEnd")
        )
        if ts is not None:
            tsn = pd.Timestamp(int(ts), unit="s", tz="UTC").tz_convert("America/New_York")
            d = tsn.date()
            if d >= ref_day:
                return d
    except Exception:
        pass
    return None


def _iv_rank_pct(iv_dec, lo, hi):
    if iv_dec is None or lo is None or hi is None:
        return None
    span = hi - lo
    if span <= 1e-10:
        return None
    return float(max(0.0, min(100.0, (iv_dec - lo) / span * 100.0)))


def _income_scaling_factor(mo_return_pct: float) -> float:
    lo = PH_WHEEL_MO_RETURN_PENALTY_LOW_PCT
    hi = PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT
    if mo_return_pct >= hi:
        raw_mod = np.log2(float(mo_return_pct) - hi + 1.0) / np.log2(hi)
        return float(0.60 + np.clip(raw_mod, 0.0, 1.0) * 0.40)
    if mo_return_pct <= lo:
        return 0.0
    return float((mo_return_pct - lo) / (hi - lo))


def _dte_weight(calendar_dte: float) -> float:
    d = max(float(calendar_dte), 0.0)
    t = float(PH_WHEEL_DTE_TARGET_DAYS)
    return 1.0 if d >= t else float((d / t) ** PH_WHEEL_DTE_GAMMA_POWER)


def _gamma_tax_multiplier(mo_return_pct: float, calendar_dte: float) -> float:
    dte_cal = max(float(calendar_dte), 1e-9)
    grf = float(np.sqrt(1.0 / dte_cal))
    gt = (float(mo_return_pct) / PH_GAMMA_TAX_YIELD_REF_PCT) / grf
    return float(np.clip(gt, PH_GAMMA_TAX_MULT_MIN, PH_GAMMA_TAX_MULT_MAX))


def _calculate_wheel_alpha(
    mo_return_pct, otm_pct, calendar_dte, iv_dec, iv_rank, strike,
    *, cost_basis=None, is_put=True,
) -> float:
    """Same formula and order as Discover `_calculate_wheel_alpha`: 0–100 with gamma tax in-score."""
    if (not is_put) and cost_basis and strike < cost_basis:
        return 0.0
    if iv_dec is None or iv_dec <= 0:
        return float("nan")
    net_monthly_yield = mo_return_pct - (4.5 / 12.0 if is_put else 0.0)
    exp1 = float(iv_dec * np.sqrt(max(float(calendar_dte), 1e-9) / 365.0) * 100.0)
    tgt = max(exp1 * PH_WHEEL_OTM_SAFETY_STD, 0.01)
    safety_factor = (abs(float(otm_pct)) / tgt) ** 2
    ir = float(iv_rank) if iv_rank is not None and not (isinstance(iv_rank, float) and math.isnan(iv_rank)) else 50.0
    vol_penalty = (iv_dec ** 0.9) * (1.0 + (100.0 - ir) / 100.0)
    if vol_penalty <= 0 or not np.isfinite(vol_penalty):
        vol_penalty = 1e-9
    dw = _dte_weight(calendar_dte)
    score = (net_monthly_yield * safety_factor * dw) / vol_penalty
    score *= _income_scaling_factor(float(mo_return_pct))
    score *= _gamma_tax_multiplier(float(mo_return_pct), calendar_dte)
    return float(np.clip(score * 10.0, 0.0, 100.0))


# ── Loaders ─────────────────────────────────────────────────────────────────

def _load_symbols_from_db_all(conn) -> list[str] | None:
    """Merge symbols from every watchlists row (deduped); None if no rows (caller may fall back to env)."""
    with conn.cursor() as cur:
        cur.execute("SELECT symbols FROM watchlists ORDER BY owner")
        rows = cur.fetchall()
    if not rows:
        return None
    seen: set[str] = set()
    out: list[str] = []
    for (sym_json,) in rows:
        for s in normalize_watchlist_symbols(sym_json):
            if s not in seen:
                seen.add(s)
                out.append(s)
    return out


def _load_symbols(conn) -> list[str]:
    fp = os.environ.get("WATCHLIST_FILE", "").strip()
    if fp:
        p = Path(fp)
        if not p.is_file():
            print(f"WATCHLIST_FILE not found: {fp}", file=sys.stderr)
            sys.exit(1)
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, list) and not isinstance(raw, dict):
            print("Watchlist must be a JSON array or {tickers: [...]}", file=sys.stderr)
            sys.exit(1)
        return normalize_watchlist_symbols(raw)

    from_db = _load_symbols_from_db_all(conn)
    if from_db is not None:
        print(f"Using merged watchlist from Postgres ({len(from_db)} unique symbol(s), all owners).")
        return from_db

    raw_s = (os.environ.get("WATCHLIST_JSON") or "[]").strip() or "[]"
    raw = json.loads(raw_s)
    if not isinstance(raw, list) and not isinstance(raw, dict):
        print("Watchlist must be a JSON array or {tickers: [...]}", file=sys.stderr)
        sys.exit(1)
    return normalize_watchlist_symbols(raw)


# ── DB helpers ──────────────────────────────────────────────────────────────
# Postgres: wall time America/Chicago, second precision (matches schema defaults).
_SQL_TS_CHICAGO_SEC = (
    "(date_trunc('second', timezone('America/Chicago', now())))::timestamp(0)"
)


def _ensure_tables(conn) -> None:
    schema_sql = (Path(__file__).resolve().parent / "schema_watchlist_snapshots.sql").read_text()
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()


def _upsert_session(conn, token: str, secret: str) -> int:
    """Insert or update session row; return session_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT session_id FROM etrade_sessions
            WHERE access_token = %s AND access_token_secret = %s
            ORDER BY last_renewed DESC LIMIT 1
            """,
            (token, secret),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                f"UPDATE etrade_sessions SET last_renewed = {_SQL_TS_CHICAGO_SEC} WHERE session_id = %s",
                (row[0],),
            )
            conn.commit()
            return row[0]
        cur.execute(
            f"""
            INSERT INTO etrade_sessions (access_token, access_token_secret, last_renewed)
            VALUES (%s, %s, {_SQL_TS_CHICAGO_SEC}) RETURNING session_id
            """,
            (token, secret),
        )
        sid = cur.fetchone()[0]
    conn.commit()
    return sid


def _insert_scan_row(
    cur,
    *,
    session_id: int,
    symbol: str,
    strategy: str,
    strike: float,
    underlying_price: float,
    expiry: dt.date,
    dte: int,
    otm_pct: float,
    mo_yield: float,
    iv: float | None,
    iv_rank_val: float | None,
    earn_date: dt.date | None,
    gamma_val: float | None,
    wheel_alpha: float,
) -> None:
    scan_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO options_scans
            (session_id, scan_id, symbol, strategy, strike, underlying_price, expiry, dte,
             otm_pct, mo_yield, iv, iv_rank, earn_date, gamma, wheel_alpha)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            session_id,
            scan_id,
            symbol,
            strategy,
            strike,
            underlying_price,
            expiry,
            dte,
            otm_pct,
            mo_yield,
            iv,
            iv_rank_val,
            earn_date,
            gamma_val,
            wheel_alpha,
        ),
    )


def _load_tokens_from_db(conn) -> tuple[str, str, int] | None:
    """Read the most recent access tokens from etrade_sessions; returns (token, secret, session_id) or None."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT session_id, access_token, access_token_secret
            FROM etrade_sessions
            ORDER BY last_renewed DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        return None
    return row[1], row[2], row[0]


def _prepare_psycopg_dsn(database_url: str) -> str:
    """Build a libpq DSN, optionally pinning hostaddr to IPv4 for CI (matches psycopg URL parsing)."""
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    try:
        params = dict(conninfo_to_dict(database_url))
    except Exception:
        return database_url

    explicit = (os.environ.get("DATABASE_IPV4") or "").strip()
    flag = (os.environ.get("DATABASE_FORCE_IPV4") or "").strip().lower()
    want_v4 = flag in ("1", "true", "yes", "on")
    host = (params.get("host") or "").strip()
    path_host = host.startswith("/")

    if explicit:
        params["hostaddr"] = explicit
    elif want_v4 and host and not path_host:
        try:
            infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
        except OSError:
            infos = []
        if infos:
            params["hostaddr"] = infos[0][4][0]
        else:
            print(
                f"DATABASE_FORCE_IPV4 is set but no IPv4 (A record) was found for host {host!r}. "
                "If you use Supabase, try the pooler connection string from the dashboard "
                "(host *.pooler.supabase.com, port 6543 for transaction mode). "
                "Ensure special characters in the DB password are URL-encoded in DATABASE_URL.",
                file=sys.stderr,
            )

    if "hostaddr" not in params:
        return database_url
    return make_conninfo("", **params)


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 1

    if not os.environ.get("ETRADE_CONSUMER_KEY") or not os.environ.get("ETRADE_CONSUMER_SECRET"):
        print("ETRADE_CONSUMER_KEY and ETRADE_CONSUMER_SECRET are required", file=sys.stderr)
        return 1

    try:
        import psycopg
    except ImportError:
        print("Install psycopg: pip install 'psycopg[binary]'", file=sys.stderr)
        return 1

    dsn = _prepare_psycopg_dsn(database_url)
    with psycopg.connect(dsn) as conn:
        _ensure_tables(conn)
        symbols = _load_symbols(conn)
        if not symbols:
            print("No symbols to sync (empty watchlist).")
            return 0

        tok = (os.environ.get("ETRADE_OAUTH_TOKEN") or "").strip()
        sec = (os.environ.get("ETRADE_OAUTH_TOKEN_SECRET") or "").strip()
        session_id: int | None = None

        if tok and sec:
            print("Using OAuth tokens from environment.")
            session_id = _upsert_session(conn, tok, sec)
        else:
            db_result = _load_tokens_from_db(conn)
            if db_result:
                tok, sec, session_id = db_result
                print(f"Using OAuth tokens from etrade_sessions (session_id={session_id}).")
            else:
                print(
                    "No OAuth tokens found. Set ETRADE_OAUTH_TOKEN + ETRADE_OAUTH_TOKEN_SECRET, "
                    "or run etrade_token_refresh.py first to populate etrade_sessions.",
                    file=sys.stderr,
                )
                return 1

        import etrade_market as em

        market = em.create_market_session(
            {"oauth_token": tok, "oauth_token_secret": sec},
        )

        total_rows = 0
        today = _scan_date_chicago()
        dte_anchor = _trading_dte_anchor_chicago()
        log_wheel_calendar_clock("watchlist_snapshot_to_postgres")

        for sym in symbols:
            print(f"Scanning {sym}…")
            try:
                q = em.get_quote(market, sym)
                price, _ = em.get_equity_display_price(q)
                spot = float(price) if price is not None else 0.0
            except Exception as e:
                print(f"  {sym}: quote failed ({e}), skipping", file=sys.stderr)
                continue
            if spot <= 0:
                print(f"  {sym}: no valid price, skipping")
                continue

            try:
                expiry_raw = em.get_expiry_dates(market, sym)
            except Exception:
                expiry_raw = []
            expiries: list[dt.date] = []
            for entry in expiry_raw:
                y = int(entry.get("year", 0))
                m = int(entry.get("month", 0))
                d = int(entry.get("day", 0))
                if y and m and d:
                    expiries.append(dt.date(y, m, d))
            expiries.sort()
            cutoff = today + dt.timedelta(days=PH_SYNC_MAX_EXPIRY_DAYS)
            selected = sorted(d for d in expiries if today < d <= cutoff)
            if not selected:
                selected = sorted(d for d in expiries if d > today)[:2]

            iv_lo, iv_hi = _iv_rank_bounds(sym)
            earn_d = _next_earnings_date(sym, today)

            with conn.cursor() as cur:
                for exp_date in selected:
                    _cal_t = wheel_alpha_effective_calendar_dte_detail(exp_date)
                    calendar_dte = _cal_t[0]
                    if calendar_dte <= 0:
                        continue
                    log_calendar_dte_breakdown(
                        f"watchlist_snapshot {sym}",
                        exp_date,
                        detail=_cal_t,
                    )
                    raw_bus = int(
                        np.busday_count(
                            np.datetime64(dte_anchor),
                            np.datetime64(exp_date),
                        )
                    )
                    trading_dte = raw_bus + 1 if exp_date > dte_anchor else raw_bus
                    if trading_dte <= 0:
                        continue
                    for chain_type, is_put in (("PUT", True), ("CALL", False)):
                        try:
                            chain = em.get_option_chain(market, sym, expiry_date=exp_date, chain_type=chain_type)
                        except Exception as e:
                            print(f"  {sym} {exp_date} {chain_type}: chain failed ({e})", file=sys.stderr)
                            continue
                        if chain.empty:
                            continue
                        for _, row in chain.iterrows():
                            bid = float(row.get("Bid", 0) or 0)
                            strike = float(row.get("Strike", 0) or 0)
                            if strike <= 0 or bid <= 0:
                                continue
                            # Same as Discover: OTM puts → strike below spot (negative %); OTM calls → positive %.
                            otm_pct = ((strike / spot) - 1.0) * 100.0 if spot > 0 else 0.0
                            if is_put and otm_pct >= 0:
                                continue
                            if (not is_put) and otm_pct <= 0:
                                continue
                            raw_return = bid / strike if is_put else bid / spot
                            mo_yield = raw_return * (PH_AVG_CALENDAR_DAYS_PER_MONTH / calendar_dte) * 100.0
                            if mo_yield <= PH_SYNC_MIN_MO_YIELD_PCT:
                                continue
                            iv_dec = _scan_iv_to_decimal(row.get("IV"))
                            iv_stored = _iv_chain_numeric(row.get("IV"))
                            iv_rank_val = _iv_rank_pct(iv_dec, iv_lo, iv_hi)
                            gamma_raw = row.get("Gamma", 0) or 0
                            try:
                                gamma_val = float(gamma_raw) if gamma_raw else None
                            except (TypeError, ValueError):
                                gamma_val = None
                            wa = _calculate_wheel_alpha(
                                mo_yield, otm_pct, calendar_dte, iv_dec, iv_rank_val, strike,
                                cost_basis=spot if not is_put else None,
                                is_put=is_put,
                            )
                            if not (wa == wa and np.isfinite(wa)):
                                continue
                            strat = "cash_secured_put" if is_put else "covered_call"
                            _insert_scan_row(
                                cur,
                                session_id=session_id,
                                symbol=sym,
                                strategy=strat,
                                strike=strike,
                                underlying_price=round(spot, 2),
                                expiry=exp_date,
                                dte=trading_dte,
                                otm_pct=round(otm_pct, 2),
                                mo_yield=round(mo_yield, 2),
                                iv=round(float(iv_stored), 6) if iv_stored is not None else None,
                                iv_rank_val=round(float(iv_rank_val), 1) if iv_rank_val is not None else None,
                                earn_date=earn_d,
                                gamma_val=round(float(gamma_val), 6) if gamma_val is not None else None,
                                wheel_alpha=round(float(wa), 1),
                            )
                            total_rows += 1
            conn.commit()
            print(f"  {sym}: committed rows so far: {total_rows}")

    print(f"Done. Inserted {total_rows} options_scans row(s) for {', '.join(symbols)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
