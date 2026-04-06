#!/usr/bin/env python3
"""
Scan E*Trade option chains for watchlist tickers, compute Wheel Alpha,
and persist results to three PostgreSQL tables:

  etrade_sessions      — current access tokens (upserted)
  options_scans        — one row per scanned strike/expiry
  wheel_alpha_results  — computed alpha linked to each scan row

Designed for GitHub Actions (no Streamlit).

Required env:
  DATABASE_URL          — Postgres connection URI
  WATCHLIST_JSON        — JSON array of tickers, e.g. ["AAPL","NVDA"]
  ETRADE_CONSUMER_KEY, ETRADE_CONSUMER_SECRET
  ETRADE_OAUTH_TOKEN, ETRADE_OAUTH_TOKEN_SECRET
  ETRADE_SANDBOX        — "true" or "false"

Optional:
  WATCHLIST_FILE        — path to JSON file; overrides WATCHLIST_JSON
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
import sys
import uuid
from pathlib import Path

import numpy as np

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")

# ── Constants (same as app) ─────────────────────────────────────────────────
PH_AVG_CALENDAR_DAYS_PER_MONTH = 30.42
PH_WHEEL_MO_RETURN_PENALTY_LOW_PCT = 2.0
PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT = 3.0
PH_WHEEL_DTE_TARGET_DAYS = 5
PH_WHEEL_DTE_GAMMA_POWER = 3.0
PH_GAMMA_TAX_YIELD_REF_PCT = 10.0
PH_GAMMA_TAX_MULT_MIN = 0.5
PH_GAMMA_TAX_MULT_MAX = 1.0
PH_MATRIX_MAX_EXPIRIES = 5
PH_MATRIX_MIN_MO_RETURN_PCT = 3.0


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


def _dte_weight(calendar_dte: int) -> float:
    d = max(int(calendar_dte), 0)
    t = float(PH_WHEEL_DTE_TARGET_DAYS)
    return 1.0 if d >= t else float((d / t) ** PH_WHEEL_DTE_GAMMA_POWER)


def _gamma_tax_multiplier(mo_return_pct: float, calendar_dte: int) -> float:
    dte_cal = max(int(calendar_dte), 1)
    grf = float(np.sqrt(1.0 / dte_cal))
    gt = (float(mo_return_pct) / PH_GAMMA_TAX_YIELD_REF_PCT) / grf
    return float(np.clip(gt, PH_GAMMA_TAX_MULT_MIN, PH_GAMMA_TAX_MULT_MAX))


def _calculate_wheel_alpha(
    mo_return_pct, otm_pct, calendar_dte, iv_dec, iv_rank, strike,
    *, cost_basis=None, is_put=True,
) -> tuple[float, float, float]:
    """Returns (base_score, gamma_tax_mult, final_alpha)."""
    if (not is_put) and cost_basis and strike < cost_basis:
        return (0.0, 1.0, 0.0)
    if iv_dec is None or iv_dec <= 0:
        return (float("nan"), float("nan"), float("nan"))
    net_monthly_yield = mo_return_pct - (4.5 / 12.0 if is_put else 0.0)
    exp1 = float(iv_dec * np.sqrt(max(int(calendar_dte), 1) / 365.0) * 100.0)
    safety_factor = (abs(float(otm_pct)) / max(exp1, 0.01)) ** 2
    ir = float(iv_rank) if iv_rank is not None and not (isinstance(iv_rank, float) and math.isnan(iv_rank)) else 50.0
    vol_penalty = (iv_dec ** 0.9) * (1.0 + (100.0 - ir) / 100.0)
    if vol_penalty <= 0 or not np.isfinite(vol_penalty):
        vol_penalty = 1e-9
    dw = _dte_weight(calendar_dte)
    score = (net_monthly_yield * safety_factor * dw) / vol_penalty
    score *= _income_scaling_factor(float(mo_return_pct))
    base = float(score * 10.0)
    gt = _gamma_tax_multiplier(float(mo_return_pct), calendar_dte)
    final = float(np.clip(base * gt, 0.0, 100.0))
    return (base, gt, final)


# ── Loaders ─────────────────────────────────────────────────────────────────

def _load_symbols() -> list[str]:
    fp = os.environ.get("WATCHLIST_FILE", "").strip()
    if fp:
        p = Path(fp)
        if not p.is_file():
            print(f"WATCHLIST_FILE not found: {fp}", file=sys.stderr)
            sys.exit(1)
        raw = json.loads(p.read_text(encoding="utf-8"))
    else:
        raw = json.loads(os.environ.get("WATCHLIST_JSON", "[]"))
    if isinstance(raw, dict):
        raw = raw.get("tickers") or raw.get("symbols") or []
    if not isinstance(raw, list):
        print("Watchlist must be a JSON array or {tickers: [...]}", file=sys.stderr)
        sys.exit(1)
    out: list[str] = []
    seen: set[str] = set()
    for x in raw:
        s = str(x).upper().strip()[:10]
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


# ── DB helpers ──────────────────────────────────────────────────────────────

def _ensure_tables(conn) -> None:
    schema_sql = (Path(__file__).resolve().parent / "schema_watchlist_snapshots.sql").read_text()
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()


def _upsert_session(conn, token: str, secret: str) -> int:
    """Insert or update session row; return its id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM etrade_sessions
            WHERE access_token = %s AND access_token_secret = %s
            ORDER BY last_renewed DESC LIMIT 1
            """,
            (token, secret),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE etrade_sessions SET last_renewed = NOW() WHERE id = %s",
                (row[0],),
            )
            conn.commit()
            return row[0]
        cur.execute(
            """
            INSERT INTO etrade_sessions (access_token, access_token_secret, last_renewed)
            VALUES (%s, %s, NOW()) RETURNING id
            """,
            (token, secret),
        )
        sid = cur.fetchone()[0]
    conn.commit()
    return sid


def _insert_scan_and_result(
    cur,
    *,
    symbol: str,
    strike: float,
    expiry: dt.date,
    dte: int,
    otm_pct: float,
    mo_yield: float,
    iv: float | None,
    iv_rank_val: float | None,
    gamma_val: float | None,
    base_score: float,
    gamma_tax: float,
    final_alpha: float,
) -> None:
    scan_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO options_scans
            (scan_id, symbol, strike, expiry, dte, otm_pct, mo_yield, iv, iv_rank, gamma)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (scan_id, symbol, strike, expiry, dte, otm_pct, mo_yield, iv, iv_rank_val, gamma_val),
    )
    cur.execute(
        """
        INSERT INTO wheel_alpha_results
            (scan_id, base_score, gamma_tax_applied, final_alpha_score)
        VALUES (%s, %s, %s, %s)
        """,
        (scan_id, base_score, gamma_tax, final_alpha),
    )


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> int:
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 1

    symbols = _load_symbols()
    if not symbols:
        print("No symbols to sync (empty watchlist).")
        return 0

    tok = (os.environ.get("ETRADE_OAUTH_TOKEN") or "").strip()
    sec = (os.environ.get("ETRADE_OAUTH_TOKEN_SECRET") or "").strip()
    if not tok or not sec:
        print("ETRADE_OAUTH_TOKEN and ETRADE_OAUTH_TOKEN_SECRET are required", file=sys.stderr)
        return 1
    if not os.environ.get("ETRADE_CONSUMER_KEY") or not os.environ.get("ETRADE_CONSUMER_SECRET"):
        print("ETRADE_CONSUMER_KEY and ETRADE_CONSUMER_SECRET are required", file=sys.stderr)
        return 1

    import etrade_market as em

    market = em.create_market_session(
        {"oauth_token": tok, "oauth_token_secret": sec},
    )

    try:
        import psycopg
    except ImportError:
        print("Install psycopg: pip install 'psycopg[binary]'", file=sys.stderr)
        return 1

    with psycopg.connect(database_url) as conn:
        _ensure_tables(conn)
        _upsert_session(conn, tok, sec)

        total_rows = 0
        today = dt.date.today()

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
            cutoff = today + dt.timedelta(days=60)
            selected = sorted(d for d in expiries if today < d <= cutoff)
            if not selected:
                selected = sorted(d for d in expiries if d > today)[:2]
            selected = selected[:PH_MATRIX_MAX_EXPIRIES]

            iv_lo, iv_hi = _iv_rank_bounds(sym)

            with conn.cursor() as cur:
                for exp_date in selected:
                    calendar_dte = (exp_date - today).days
                    if calendar_dte <= 0:
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
                            otm_pct = ((strike / spot) - 1.0) * 100.0
                            if is_put and otm_pct >= 0:
                                continue
                            if (not is_put) and otm_pct <= 0:
                                continue
                            raw_return = bid / strike if is_put else bid / spot
                            mo_yield = raw_return * (PH_AVG_CALENDAR_DAYS_PER_MONTH / calendar_dte) * 100.0
                            if mo_yield <= PH_MATRIX_MIN_MO_RETURN_PCT:
                                continue
                            iv_dec = _scan_iv_to_decimal(row.get("IV"))
                            iv_rank_val = _iv_rank_pct(iv_dec, iv_lo, iv_hi)
                            gamma_raw = row.get("Gamma", 0) or 0
                            try:
                                gamma_val = float(gamma_raw) if gamma_raw else None
                            except (TypeError, ValueError):
                                gamma_val = None
                            base, gt, final = _calculate_wheel_alpha(
                                mo_yield, otm_pct, calendar_dte, iv_dec, iv_rank_val, strike,
                                cost_basis=spot if not is_put else None,
                                is_put=is_put,
                            )
                            if not (final == final and np.isfinite(final)):
                                continue
                            _insert_scan_and_result(
                                cur,
                                symbol=sym,
                                strike=strike,
                                expiry=exp_date,
                                dte=calendar_dte,
                                otm_pct=round(otm_pct, 4),
                                mo_yield=round(mo_yield, 4),
                                iv=round(float(iv_dec), 6) if iv_dec is not None else None,
                                iv_rank_val=round(float(iv_rank_val), 2) if iv_rank_val is not None else None,
                                gamma_val=round(float(gamma_val), 6) if gamma_val is not None else None,
                                base_score=round(base, 4),
                                gamma_tax=round(gt, 4),
                                final_alpha=round(final, 2),
                            )
                            total_rows += 1
            conn.commit()
            print(f"  {sym}: committed rows so far: {total_rows}")

    print(f"Done. Inserted {total_rows} scan+result row(s) for {', '.join(symbols)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
