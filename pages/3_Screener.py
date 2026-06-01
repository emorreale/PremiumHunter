"""
Screener — Universe-wide Wheel Alpha leaderboard.

Reads pre-computed scans from the `options_scans` Postgres table (populated by
scripts/watchlist_snapshot_to_postgres.py with UNIVERSE_FILE) and surfaces the
highest-scoring contracts across all scanned tickers.
"""
from __future__ import annotations

import datetime as dt
import html
import os
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env")

# ── DB connection ────────────────────────────────────────────────────────────

def _get_connection():
    """Return a psycopg connection (cached in session_state for the run)."""
    if "ph_screener_conn" in st.session_state:
        conn = st.session_state.ph_screener_conn
        if not conn.closed:
            return conn

    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        return None

    try:
        import psycopg
        from watchlist_db import prepare_psycopg_dsn
    except ImportError:
        return None

    dsn = prepare_psycopg_dsn(database_url)
    try:
        conn = psycopg.connect(dsn)
        st.session_state.ph_screener_conn = conn
        return conn
    except Exception:
        return None


@st.cache_data(ttl=300, show_spinner=False)
def _load_scans(
    strategy_filter: str,
    min_alpha: float,
    max_age_hours: int,
) -> pd.DataFrame:
    """Load recent option scans from Postgres, filtered server-side."""
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        return pd.DataFrame()

    try:
        import psycopg
        from watchlist_db import prepare_psycopg_dsn
    except ImportError:
        return pd.DataFrame()

    dsn = prepare_psycopg_dsn(database_url)
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=max_age_hours)

    strategy_clause = ""
    params: dict = {
        "min_alpha": min_alpha,
        "cutoff": cutoff,
    }
    if strategy_filter != "All":
        strategy_clause = "AND strategy = %(strategy)s"
        params["strategy"] = (
            "cash_secured_put" if strategy_filter == "Cash Secured Puts" else "covered_call"
        )

    query = f"""
        SELECT
            symbol,
            strategy,
            strike,
            underlying_price,
            expiry,
            dte,
            otm_pct,
            mo_yield,
            iv,
            iv_rank,
            earn_date,
            wheel_alpha,
            create_ts
        FROM options_scans
        WHERE wheel_alpha >= %(min_alpha)s
          AND create_ts >= %(cutoff)s
          {strategy_clause}
        ORDER BY wheel_alpha DESC
        LIMIT 500
    """

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=cols)


@st.cache_data(ttl=300, show_spinner=False)
def _load_top_tickers(max_age_hours: int) -> pd.DataFrame:
    """Per-symbol best Wheel Alpha (top-3 average) from recent scans."""
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        return pd.DataFrame()

    try:
        import psycopg
        from watchlist_db import prepare_psycopg_dsn
    except ImportError:
        return pd.DataFrame()

    dsn = prepare_psycopg_dsn(database_url)
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=max_age_hours)

    query = """
        WITH ranked AS (
            SELECT
                symbol,
                strategy,
                wheel_alpha,
                underlying_price,
                ROW_NUMBER() OVER (PARTITION BY symbol, strategy ORDER BY wheel_alpha DESC) AS rn
            FROM options_scans
            WHERE wheel_alpha IS NOT NULL
              AND create_ts >= %(cutoff)s
        )
        SELECT
            symbol,
            strategy,
            ROUND(AVG(wheel_alpha)::numeric, 1) AS avg_top3_alpha,
            ROUND(MAX(underlying_price)::numeric, 2) AS price
        FROM ranked
        WHERE rn <= 3
        GROUP BY symbol, strategy
        ORDER BY avg_top3_alpha DESC
    """

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, {"cutoff": cutoff})
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=cols)


# ── Page UI ──────────────────────────────────────────────────────────────────

st.markdown(
    '<div class="section-label" style="font-size:1.15rem;margin-bottom:6px">'
    "Screener</div>",
    unsafe_allow_html=True,
)
st.caption(
    "Universe-wide Wheel Alpha leaderboard — powered by scheduled scans. "
    "Run the snapshot script with `UNIVERSE_FILE=universe.json` to populate data."
)

# Filters
_f1, _f2, _f3, _f4 = st.columns([1.2, 1.2, 1.0, 1.0])

with _f1:
    strategy_pick = st.selectbox(
        "Strategy",
        options=["All", "Cash Secured Puts", "Covered Calls"],
        key="ph_scr_strategy",
    )

with _f2:
    age_hours = st.selectbox(
        "Scan freshness",
        options=[6, 12, 24, 48, 72],
        index=2,
        format_func=lambda h: f"Last {h}h",
        key="ph_scr_age",
    )

with _f3:
    min_alpha = st.number_input(
        "Min Wheel Alpha",
        min_value=0.0,
        max_value=100.0,
        value=20.0,
        step=5.0,
        key="ph_scr_min_alpha",
    )

with _f4:
    view_mode = st.radio(
        "View",
        options=["Top Tickers", "All Contracts"],
        horizontal=True,
        key="ph_scr_view",
    )

st.markdown("---")

# ── Top Tickers view ─────────────────────────────────────────────────────────

def _alpha_pill(val) -> str:
    if val is None or pd.isna(val):
        return ""
    v = float(val)
    if v >= 85:
        bg, fg = "#00FF88", "#0d1117"
    elif v >= 65:
        bg, fg = "#228B22", "#ffffff"
    elif v >= 40:
        bg, fg = "#FFBF00", "#1a1a1a"
    else:
        bg, fg = "#1A1C23", "#c9d1d9"
    return (
        f'<span style="display:inline-block;padding:0.18rem 0.55rem;border-radius:8px;'
        f'font-weight:600;background-color:{bg};color:{fg}">{v:.1f}</span>'
    )


def _strategy_label(s: str) -> str:
    if s == "cash_secured_put":
        return "CSP"
    if s == "covered_call":
        return "CC"
    return str(s)


if view_mode == "Top Tickers":
    with st.spinner("Loading leaderboard…"):
        top_df = _load_top_tickers(int(age_hours))

    if top_df.empty:
        st.info(
            "No scan data available. Run the snapshot script with "
            "`UNIVERSE_FILE=universe.json` to populate the screener."
        )
    else:
        if strategy_pick != "All":
            strat_code = (
                "cash_secured_put"
                if strategy_pick == "Cash Secured Puts"
                else "covered_call"
            )
            top_df = top_df[top_df["strategy"] == strat_code]

        top_df = top_df[top_df["avg_top3_alpha"] >= min_alpha].reset_index(drop=True)

        if top_df.empty:
            st.info("No tickers match your filters.")
        else:
            st.markdown(
                f'<p style="color:#9aa0a6;font-size:0.88rem;margin:0 0 8px 0">'
                f'{len(top_df)} ticker(s) ranked by Top-3 Wheel Alpha average</p>',
                unsafe_allow_html=True,
            )

            display = top_df.copy()
            display["Strategy"] = display["strategy"].map(_strategy_label)
            display["Wheel Alpha"] = display["avg_top3_alpha"]
            display["Price"] = display["price"]
            display = display.rename(columns={"symbol": "Symbol"})[
                ["Symbol", "Strategy", "Wheel Alpha", "Price"]
            ]

            st.dataframe(
                display,
                hide_index=True,
                use_container_width=True,
                height=min(600, 35 * len(display) + 38),
                column_config={
                    "Symbol": st.column_config.TextColumn(width="small"),
                    "Strategy": st.column_config.TextColumn(width="small"),
                    "Wheel Alpha": st.column_config.ProgressColumn(
                        min_value=0,
                        max_value=100,
                        format="%.1f",
                    ),
                    "Price": st.column_config.NumberColumn(format="$%.2f"),
                },
            )

# ── All Contracts view ───────────────────────────────────────────────────────

else:
    with st.spinner("Loading contracts…"):
        contracts_df = _load_scans(strategy_pick, min_alpha, int(age_hours))

    if contracts_df.empty:
        st.info(
            "No scan data available. Run the snapshot script with "
            "`UNIVERSE_FILE=universe.json` to populate the screener."
        )
    else:
        st.markdown(
            f'<p style="color:#9aa0a6;font-size:0.88rem;margin:0 0 8px 0">'
            f'{len(contracts_df)} contract(s) — sorted by Wheel Alpha descending</p>',
            unsafe_allow_html=True,
        )

        display = contracts_df.copy()
        display["Strategy"] = display["strategy"].map(_strategy_label)
        display["Expiry"] = pd.to_datetime(display["expiry"]).dt.strftime("%Y-%m-%d")
        display["Earn. Date"] = pd.to_datetime(display["earn_date"]).dt.strftime("%Y-%m-%d")
        display["Earn. Date"] = display["Earn. Date"].fillna("")

        display = display.rename(columns={
            "symbol": "Symbol",
            "strike": "Strike",
            "underlying_price": "Price",
            "dte": "DTE",
            "otm_pct": "OTM %",
            "mo_yield": "Mo. Return %",
            "iv_rank": "IV Rank",
            "wheel_alpha": "Wheel Alpha",
        })[
            [
                "Symbol", "Strategy", "Expiry", "DTE", "Strike", "Price",
                "OTM %", "Mo. Return %", "IV Rank", "Wheel Alpha", "Earn. Date",
            ]
        ]

        st.dataframe(
            display,
            hide_index=True,
            use_container_width=True,
            height=min(700, 35 * len(display) + 38),
            column_config={
                "Symbol": st.column_config.TextColumn(width="small"),
                "Strategy": st.column_config.TextColumn(width="small"),
                "Expiry": st.column_config.TextColumn(width="small"),
                "DTE": st.column_config.NumberColumn(format="%d"),
                "Strike": st.column_config.NumberColumn(format="$%.2f"),
                "Price": st.column_config.NumberColumn(format="$%.2f"),
                "OTM %": st.column_config.NumberColumn(format="%.2f%%"),
                "Mo. Return %": st.column_config.NumberColumn(format="%.2f%%"),
                "IV Rank": st.column_config.NumberColumn(format="%.1f"),
                "Wheel Alpha": st.column_config.ProgressColumn(
                    min_value=0,
                    max_value=100,
                    format="%.1f",
                ),
                "Earn. Date": st.column_config.TextColumn(width="small"),
            },
        )

# ── Scan metadata ────────────────────────────────────────────────────────────

with st.expander("About the Screener"):
    st.markdown(
        """
**How it works:**

1. The GitHub Actions job (`scripts/watchlist_snapshot_to_postgres.py`) runs on a schedule
   with `UNIVERSE_FILE` pointing to `universe.json` (~150 liquid tickers).
2. It scans E\\*Trade option chains for each ticker, calculates Wheel Alpha for every
   OTM put and call, and writes results to your Postgres `options_scans` table.
3. This page reads from that table and ranks tickers by their **Top-3 Wheel Alpha average**
   (same metric as the Candidate Badge on Discover).

**Top Tickers** — One row per symbol showing the average of its 3 best-scoring contracts.
Great for picking *which* stocks to look at.

**All Contracts** — Every individual contract that scored above your minimum, sorted by
Wheel Alpha. Use this to find specific strikes/expirations to trade.

**Freshness** — Controls how far back to look. "Last 24h" means only scans from the past
day are included; older data is ignored.
"""
    )
