import datetime as dt
import html
import math
import re
import uuid

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf

from etrade_market import (
    get_equity_display_price,
    get_expiry_dates,
    get_option_chain,
    get_quote,
)
from watchlist_persist import ensure_session_watchlist, save_watchlist

if st.session_state.get("market") is None:
    st.info("Connect to E-Trade using the sidebar to get started.")
    st.stop()

market = st.session_state.market

# Plotly price chart height (peer/loser tables use the same pixel height)
PH_PRICE_CHART_HEIGHT = 400
# Wheel Alpha safety divisor = expected 1SD % (IV × sqrt(DTE/365) × 100), not a fixed OTM %
# Monthly return %: (premium / ref) × (avg days per month / calendar DTE) × 100
PH_AVG_CALENDAR_DAYS_PER_MONTH = 30.42  # ~365.25 / 12
# Mo. Return % hinge: below hinge = strict linear penalty (0× at ≤2%, →1× at 3%);
# at/above hinge = log₂-tuned factor in [0.60, 1.0] to favor higher yields vs heavy log smoothing.
PH_WHEEL_MO_RETURN_PENALTY_LOW_PCT = 2.0
PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT = 3.0
# Short-DTE “gamma” weight: (calendar DTE / target)^power for DTE < target; 1.0 at/above target.
PH_WHEEL_DTE_TARGET_DAYS = 5
PH_WHEEL_DTE_GAMMA_POWER = 3.0  # >2 steeper than (DTE/5)²; 1DTE → 0.008 vs 0.04 at power 2
# Gamma tax: reward-to-risk vs short DTE — (mo_return/ref) / sqrt(1/DTE), clipped, scales core score.
PH_GAMMA_TAX_YIELD_REF_PCT = 10.0
PH_GAMMA_TAX_MULT_MIN = 0.5
PH_GAMMA_TAX_MULT_MAX = 1.0

# ── Cached API wrappers ─────────────────────────────────────────────────────

@st.cache_data(ttl=120, show_spinner=False)
def _cached_quote(_market_id, symbol: str) -> dict | None:
    try:
        q = get_quote(market, symbol)
        if q:
            q.pop("_raw_response", None)
        return q or None
    except Exception:
        return None

@st.cache_data(ttl=600, show_spinner=False)
def _cached_yf_info(symbol: str) -> dict:
    try:
        return yf.Ticker(symbol).info or {}
    except Exception:
        return {}


@st.cache_data(ttl=600, show_spinner=False)
def _cached_next_earnings_date_str(symbol: str) -> str:
    """Next upcoming earnings date (YYYY-MM-DD, US/Eastern calendar day), or ""."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return ""
    today = dt.date.today()
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
                    if isinstance(d, dt.date) and d >= today:
                        return d.strftime("%Y-%m-%d")
        edf = tk.get_earnings_dates(limit=20)
        if edf is not None and not edf.empty:
            for ts in sorted(edf.index):
                tsn = pd.Timestamp(ts)
                d = (
                    tsn.tz_convert("America/New_York").date()
                    if tsn.tzinfo is not None
                    else tsn.date()
                )
                if d >= today:
                    return d.strftime("%Y-%m-%d")
        inf = _cached_yf_info(sym)
        ts = (
            inf.get("earningsTimestamp")
            or inf.get("earningsTimestampStart")
            or inf.get("earningsTimestampEnd")
        )
        if ts is not None:
            tsn = pd.Timestamp(int(ts), unit="s", tz="UTC").tz_convert(
                "America/New_York"
            )
            d = tsn.date()
            if d >= today:
                return d.strftime("%Y-%m-%d")
    except Exception:
        pass
    return ""


@st.cache_data(ttl=120, show_spinner=False)
def _cached_yf_history(symbol: str, period: str, interval: str) -> pd.DataFrame | None:
    try:
        h = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=False)
        return h if h is not None and not h.empty else None
    except Exception:
        return None

@st.cache_data(ttl=600, show_spinner=False)
def _cached_expiry_dates(_market_id, symbol: str) -> list[dt.date]:
    try:
        raw = get_expiry_dates(market, symbol)
        dates: list[dt.date] = []
        for entry in raw:
            y = int(entry.get("year", 0))
            m = int(entry.get("month", 0))
            d = int(entry.get("day", 0))
            if y and m and d:
                dates.append(dt.date(y, m, d))
        dates.sort()
        return dates
    except Exception:
        return []

@st.cache_data(ttl=120, show_spinner=False)
def _cached_option_chain(
    _market_id, symbol: str, expiry: dt.date, chain_type: str
) -> pd.DataFrame:
    try:
        return get_option_chain(
            market, symbol, expiry_date=expiry, chain_type=chain_type
        )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_52w_iv_rank_bounds(symbol: str) -> tuple[float | None, float | None]:
    """
    Annualized-volatility low/high (decimal) over the past year for IV Rank.

    Yahoo / E*Trade do not expose a full 52-week implied-volatility series here,
    so we use the trailing min and max of 30-day historical volatility
    (close-to-close, annualized) as a stand-in for 52-week IV low / high.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return None, None
    h = _cached_yf_history(sym, "1y", "1d")
    if h is None or len(h) < 45:
        return None, None
    close = pd.to_numeric(h["Close"], errors="coerce").dropna()
    if len(close) < 45:
        return None, None
    lr = np.log(close / close.shift(1))
    hv = lr.rolling(30, min_periods=20).std() * np.sqrt(252)
    hv = hv.dropna()
    if hv.empty:
        return None, None
    lo, hi = float(hv.min()), float(hv.max())
    if hi - lo < 1e-9:
        return None, None
    return lo, hi


def _scan_iv_to_decimal(raw) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        x = float(raw)
    except (TypeError, ValueError):
        return None
    if x <= 0:
        return None
    # E*Trade / Yahoo-style: decimals in (0, ~2]; larger values treated as percent.
    if x > 2.0:
        x = x / 100.0
    return x


def _scan_iv_rank_pct(iv_dec: float | None, lo: float | None, hi: float | None):
    if iv_dec is None or lo is None or hi is None:
        return None
    span = hi - lo
    if span <= 1e-10:
        return None
    r = (iv_dec - lo) / span * 100.0
    return float(max(0.0, min(100.0, r)))


def _mo_return_penalty_factor(mo_return_pct: float) -> float:
    """Linear penalty for Mo. Return % strictly below hinge: 0× at ≤low, 1× at hinge."""
    lo = PH_WHEEL_MO_RETURN_PENALTY_LOW_PCT
    hi = PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT
    if mo_return_pct <= lo:
        return 0.0
    if mo_return_pct >= hi:
        return 1.0
    return float((mo_return_pct - lo) / (hi - lo))


def _income_scaling_factor(mo_return_pct: float) -> float:
    """
    Yield-side scaling of Wheel Alpha (safety / vol / time unchanged).
    Below hinge: same strict linear ramp as before (0 at ≤2%, →1 at 3%).
    At/above hinge: log₂-based factor in [0.60, 1.0], gentler on high yields than log1p.
    """
    hi = PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT
    if mo_return_pct >= hi:
        raw_modifier = np.log2(float(mo_return_pct) - hi + 1.0) / np.log2(hi)
        return float(0.60 + np.clip(raw_modifier, 0.0, 1.0) * 0.40)
    return _mo_return_penalty_factor(float(mo_return_pct))


def _expected_1sd_move_pct(iv_dec: float, calendar_dte: int) -> float:
    """
    Expected one-standard-deviation move (%), annualized IV in decimal:
    IV × sqrt(calendar DTE / 365) × 100.
    """
    dte = max(int(calendar_dte), 1)
    return float(iv_dec * np.sqrt(dte / 365.0) * 100.0)


def _dte_weight(calendar_dte: int) -> float:
    """(DTE/target)^power below target calendar DTE; 1.0 at/above target (gamma-style short-DTE penalty)."""
    d = max(int(calendar_dte), 0)
    t = float(PH_WHEEL_DTE_TARGET_DAYS)
    if d >= t:
        return 1.0
    return float((d / t) ** PH_WHEEL_DTE_GAMMA_POWER)


def _gamma_tax_multiplier(mo_return_pct: float, calendar_dte: int) -> float:
    """
    gamma_risk_factor = sqrt(1 / calendar_DTE); gamma_tax = (mo_return_pct / ref) / that factor.
    Clipped to [min, max] and applied to core alpha so high-yield short DTE can still score well.
    """
    dte_cal = max(int(calendar_dte), 1)
    gamma_risk_factor = float(np.sqrt(1.0 / dte_cal))
    gamma_tax = (float(mo_return_pct) / PH_GAMMA_TAX_YIELD_REF_PCT) / gamma_risk_factor
    return float(np.clip(gamma_tax, PH_GAMMA_TAX_MULT_MIN, PH_GAMMA_TAX_MULT_MAX))


def _calculate_wheel_alpha(
    mo_return_pct: float,
    otm_pct: float,
    calendar_dte: int,
    iv_dec: float | None,
    iv_rank: float | None,
    strike: float,
    *,
    cost_basis: float | None,
    is_put: bool,
) -> float:
    """
    Wheel Alpha: yield × safety vs vol × time, scaled to 0–100.
    Safety factor = (|OTM %| / expected 1SD %)² with expected 1SD % = IV×√(DTE/365)×100.
    DTE weight = (calendar DTE / target)^power below target days, else 1; scales yield×safety.
    Mo. Return % below 3%: same linear penalty (0× at ≤2%, →1× at 3%). At/above 3%: log₂-tuned
    factor from 0.60 to 1.0 so higher yields are favored slightly vs heavier log smoothing.
    Gamma tax: core score × clip((Mo. Return % / ref) / sqrt(1/calendar DTE), 0.5, 1.0).
    """
    if (not is_put) and cost_basis and strike < cost_basis:
        return 0.0

    if iv_dec is None or iv_dec <= 0:
        return float("nan")

    net_monthly_yield = mo_return_pct - (4.5 / 12.0 if is_put else 0.0)

    _exp1 = _expected_1sd_move_pct(float(iv_dec), calendar_dte)
    _tgt = max(_exp1, 0.01)
    _cushion = abs(float(otm_pct))
    safety_factor = (_cushion / _tgt) ** 2

    if iv_rank is None or (isinstance(iv_rank, float) and np.isnan(iv_rank)):
        ir = 50.0
    else:
        ir = float(iv_rank)

    vol_penalty = (iv_dec**0.9) * (1.0 + (100.0 - ir) / 100.0)
    if vol_penalty <= 0 or not np.isfinite(vol_penalty):
        vol_penalty = 1e-9

    _dw = _dte_weight(calendar_dte)
    score = (net_monthly_yield * safety_factor * _dw) / vol_penalty
    score *= _income_scaling_factor(float(mo_return_pct))
    score *= _gamma_tax_multiplier(float(mo_return_pct), calendar_dte)
    return float(np.clip(score * 10.0, 0.0, 100.0))


@st.cache_data(ttl=3600, show_spinner=False)
def _yf_company_name(symbol: str) -> str:
    sym = (symbol or "").strip().upper()
    if not sym:
        return ""
    try:
        inf = yf.Ticker(sym).info or {}
        n = inf.get("shortName") or inf.get("longName") or inf.get("displayName")
        return n.strip() if isinstance(n, str) else ""
    except Exception:
        return ""


def _bar_calendar_date(ts):
    """Convert a bar timestamp to a US-Eastern calendar date."""
    t = pd.Timestamp(ts)
    if t.tzinfo is not None:
        t = t.tz_convert("America/New_York")
    return t.date()


def _previous_session_close(ticker_sym: str, hist) -> float | None:
    """Last daily Close strictly before the first bar's trading day (Yahoo baseline)."""
    first_day = _bar_calendar_date(hist.index[0])
    try:
        end_dt = pd.Timestamp(first_day)
        start_dt = end_dt - pd.Timedelta(days=45)
        daily = yf.Ticker(ticker_sym).history(
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_dt.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=False,
        )
    except Exception:
        daily = None
    if daily is None or daily.empty:
        try:
            daily = yf.Ticker(ticker_sym).history(
                period="max", interval="1d", auto_adjust=False
            )
        except Exception:
            return None
    if daily is None or daily.empty:
        return None
    ref = None
    for idx in daily.index:
        d = _bar_calendar_date(idx)
        if d < first_day:
            ref = float(daily.loc[idx, "Close"])
    return ref


def _make_google_style_chart(ticker: str, period_key: str, yf_info: dict):
    """Price + area chart (Google Finance–style); returns (fig, end_price, chg_pct, baseline)."""
    pmap = {
        "1D": ("1d", "5m"),
        "5D": ("5d", "30m"),
        "1M": ("1mo", "1d"),
        "6M": ("6mo", "1d"),
        "1Y": ("1y", "1d"),
        "5Y": ("5y", "1wk"),
    }
    period, interval = pmap.get(period_key, ("1mo", "1d"))
    hist = _cached_yf_history(ticker, period, interval)
    if hist is None or hist.empty:
        return None, None, None, None
    hist = hist.sort_index()
    closes = hist["Close"].astype(float)

    # End price: prefer live yfinance quote, fall back to last bar close.
    cur_m = yf_info.get("regularMarketPrice") or yf_info.get("currentPrice")
    last_hist = float(closes.iloc[-1])
    try:
        end = float(cur_m) if cur_m is not None and not pd.isna(cur_m) else last_hist
    except (TypeError, ValueError):
        end = last_hist

    # Baseline (start price) — matches Yahoo Finance convention:
    #   1D  → previousClose from yfinance info
    #   All others → last daily Close before the first bar's calendar date
    _prev_close_line: float | None = None
    if period_key == "1D":
        prev_close = yf_info.get("previousClose") or yf_info.get(
            "regularMarketPreviousClose"
        )
        if prev_close is not None and not pd.isna(prev_close):
            base = float(prev_close)
            _prev_close_line = base
        else:
            base = float(hist["Open"].iloc[0])
            _psc = _previous_session_close(ticker, hist)
            if _psc is not None:
                _prev_close_line = float(_psc)
    else:
        psc = _previous_session_close(ticker, hist)
        base = psc if psc is not None else float(hist["Open"].iloc[0])

    chg = ((end - base) / base * 100) if base else 0.0
    up = chg >= 0
    line_c = "#34a853" if up else "#ea4335"
    fill_c = "rgba(52, 168, 83, 0.18)" if up else "rgba(234, 67, 53, 0.14)"

    y_min = float(closes.min())
    y_pad = max(y_min * 0.002, (float(closes.max()) - y_min) * 0.02)
    y_floor = y_min - y_pad

    fig = go.Figure()

    if period_key == "1D" and _prev_close_line is not None:
        _x0, _x1 = hist.index[0], hist.index[-1]
        fig.add_trace(
            go.Scatter(
                x=[_x0, _x1],
                y=[_prev_close_line, _prev_close_line],
                mode="lines",
                line=dict(
                    color="rgba(154, 160, 166, 0.75)",
                    width=1,
                    dash="dot",
                ),
                showlegend=False,
                name="",
                hovertemplate="Prev close $%{y:.2f}<extra></extra>",
            )
        )

    def _add_floor_price_pair(
        xs_d: list,
        ys_d: list,
        *,
        customdata=None,
        hover_tmpl: str | None = None,
    ) -> None:
        if not xs_d:
            return
        fl_d = [y_floor] * len(xs_d)
        fig.add_trace(
            go.Scatter(
                x=xs_d,
                y=fl_d,
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
                name="",
            )
        )
        _ht = hover_tmpl or "%{x}<br>$%{y:.2f}<extra></extra>"
        _kw = dict(
            x=xs_d,
            y=ys_d,
            mode="lines",
            line=dict(color=line_c, width=2),
            fill="tonexty",
            fillcolor=fill_c,
            showlegend=False,
            name="",
            hovertemplate=_ht,
        )
        if customdata is not None:
            _kw["customdata"] = customdata
        fig.add_trace(go.Scatter(**_kw))

    xaxis_cfg: dict = dict(
        showgrid=False,
        zeroline=False,
        tickfont=dict(size=11, color="#9aa0a6"),
        linecolor="rgba(255,255,255,0.08)",
    )

    if interval in ("1d", "1wk", "1mo"):
        _add_floor_price_pair(list(hist.index), closes.tolist())
    elif period_key == "5D":
        # Sequential integer x-axis so overnight/weekend gaps are collapsed.
        # One continuous line across all sessions (Google Finance style).
        _seq_x = list(range(len(closes)))
        _ys_all = closes.tolist()
        _real_ts = hist.index

        # Place tick labels at the first bar of each new calendar day.
        _tvals: list[int] = []
        _ttxt: list[str] = []
        _prev_d = None
        for i, idx in enumerate(_real_ts):
            d = pd.Timestamp(idx).date()
            if d != _prev_d:
                _tvals.append(i)
                _ttxt.append(f"{pd.Timestamp(idx).strftime('%b')} {d.day}")
                _prev_d = d

        _cd = [[pd.Timestamp(t).strftime("%b %d, %I:%M %p")] for t in _real_ts]
        _add_floor_price_pair(
            _seq_x,
            _ys_all,
            customdata=_cd,
            hover_tmpl="%{customdata[0]}<br>$%{y:.2f}<extra></extra>",
        )
        xaxis_cfg.update(
            type="linear",
            tickmode="array",
            tickvals=_tvals,
            ticktext=_ttxt,
            range=[-0.5, len(_seq_x) - 0.5],
        )
    else:
        # 1D intraday: real timestamps, single trace.
        _add_floor_price_pair(list(hist.index), closes.tolist())

    _hover_mode = "closest" if period_key == "5D" else "x unified"
    fig.update_layout(
        height=PH_PRICE_CHART_HEIGHT,
        margin=dict(l=8, r=48, t=12, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        xaxis=xaxis_cfg,
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(255,255,255,0.06)",
            zeroline=False,
            tickfont=dict(size=11, color="#9aa0a6"),
            tickprefix="$",
            side="right",
        ),
        hovermode=_hover_mode,
    )
    return fig, end, chg, base


@st.cache_data(ttl=90, show_spinner=False)
def _yf_day_moves(symbols: tuple[str, ...]) -> pd.DataFrame:
    syms = [s.strip().upper() for s in symbols if s and str(s).strip()]
    if not syms:
        return pd.DataFrame()
    try:
        raw = yf.download(syms, period="10d", interval="1d",
                          auto_adjust=False, progress=False, threads=True)
    except Exception:
        return pd.DataFrame()
    if raw.empty:
        return pd.DataFrame()
    rows = []
    for sym in syms:
        try:
            c = raw[("Close", sym)].dropna()
        except (KeyError, TypeError):
            continue
        if len(c) < 2:
            continue
        prev, last = float(c.iloc[-2]), float(c.iloc[-1])
        if prev == 0:
            continue
        rows.append({"Symbol": sym, "Last": round(last, 2),
                      "Δ%": round((last - prev) / prev * 100, 2)})
    return pd.DataFrame(rows)


def _peer_table_style(df: pd.DataFrame):
    """Blue symbol text so the cell reads as clickable (matches app link color)."""

    def _row_colors(row: pd.Series) -> pd.Series:
        return pd.Series(
            [
                "color: #58a6ff; font-weight: 600;" if c == "Symbol" else ""
                for c in row.index
            ],
            index=row.index,
        )

    return df.style.apply(_row_colors, axis=1)


def _wheel_alpha_pill_html(v) -> str:
    """Tight pill/badge around Wheel Alpha value (not full cell)."""
    if v is None or pd.isna(v):
        return ""
    try:
        vv = float(v)
    except (TypeError, ValueError):
        return ""
    pill = (
        "display:inline-block;padding:0.18rem 0.55rem;border-radius:8px;"
        "font-weight:600;line-height:1.2;vertical-align:middle;"
    )
    if vv >= 85:
        pill += "background-color:#00FF88;color:#0d1117;"
    elif vv >= 65:
        pill += "background-color:#228B22;color:#ffffff;"
    elif vv >= 40:
        pill += "background-color:#FFBF00;color:#1a1a1a;"
    else:
        pill += "background-color:#1A1C23;color:#c9d1d9;"
    return f'<span style="{pill}">{vv:.1f}</span>'


def _earn_date_pill_html(earn_s: str, exp_s: str) -> str:
    """Plain date text, or pink pill when earnings fall before option expiration."""
    earn_s = (earn_s or "").strip()
    if not earn_s:
        return ""
    esc = html.escape(earn_s)
    exp_s = (exp_s or "").strip()
    if not exp_s:
        return esc
    try:
        e_d = dt.datetime.strptime(earn_s, "%Y-%m-%d").date()
        x_d = dt.datetime.strptime(exp_s, "%Y-%m-%d").date()
    except ValueError:
        return esc
    if e_d < x_d:
        pill = (
            "display:inline-block;padding:0.18rem 0.55rem;border-radius:8px;"
            "font-weight:600;line-height:1.2;vertical-align:middle;"
            "background-color:rgba(255,170,170,0.55);color:#1a1a1a;"
        )
        return f'<span style="{pill}">{esc}</span>'
    return esc


def _scan_table_styler(df: pd.DataFrame):
    """
    Build Styler with pill HTML in Wheel Alpha / Earn. Date cells.
    Use with st.html(styler.to_html()); st.dataframe escapes HTML and shows raw tags.
    """
    view = df.copy()
    view["Wheel Alpha"] = df["Wheel Alpha"].map(_wheel_alpha_pill_html)
    view["Earn. Date"] = pd.Series(
        [
            _earn_date_pill_html(e, x)
            for e, x in zip(df["Earn. Date"], df["Expiration Date"])
        ],
        index=df.index,
    )

    def _money(x):
        if pd.isna(x):
            return ""
        return f"${float(x):,.2f}"

    def _pct(x):
        if pd.isna(x):
            return ""
        return f"{float(x):.2f}%"

    def _num1(x):
        if pd.isna(x):
            return ""
        return f"{float(x):.1f}"

    def _esc(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        return html.escape(str(x).strip())

    fmt = {
        "Expiration Date": _esc,
        "DTE (Trading Days)": lambda x: ""
        if pd.isna(x)
        else str(int(x)),
        "Strike": _money,
        "OTM %": _pct,
        "Mo. Return %": _pct,
        "IV": _esc,
        "IV Rank": _num1,
        "Wheel Alpha": lambda x: x,
        "Earn. Date": lambda x: x,
        "Bid": _money,
        "Ask": _money,
        "Volume": lambda x: "" if pd.isna(x) else f"{int(x):,}",
    }
    return view.style.format(fmt, escape=None, na_rep="").hide(axis="index")


def _scan_html_header_tooltips(
    table_html: str,
    column_order: tuple[str, ...],
    tips: dict[str, str],
) -> str:
    """Insert title=\"...\" on <th> for native browser tooltips (hover)."""
    out = table_html
    for col in column_order:
        tip = tips.get(col, "")
        if not tip:
            continue
        ta = html.escape(tip).replace('"', "&quot;")
        pat = rf'(<th[^>]*?)(\s*>)(\s*{re.escape(col)}\s*</th>)'
        out, n = re.subn(pat, rf'\1 title="{ta}"\2\3', out, count=1)
        if n == 0:
            continue
    return out


def _scan_table_html_fragment(
    styler,
    *,
    max_height_px: int,
    column_order: tuple[str, ...] | None = None,
    column_tips: dict[str, str] | None = None,
    initial_sort_col: str | None = None,
) -> str:
    """
    Wrapped pandas HTML table + client-side sort on header click.
    Render with components.html(...) so <script> runs (st.html may strip scripts).
    """
    host_id = f"ph-scan-{uuid.uuid4().hex[:12]}"
    inner = styler.to_html()
    if column_tips and column_order:
        inner = _scan_html_header_tooltips(inner, column_order, column_tips)
    _init_sort_js = ""
    if column_order and initial_sort_col and initial_sort_col in column_order:
        _isc = list(column_order).index(initial_sort_col)
        _init_sort_js = f"""
  sortCol = {_isc};
  sortDir = -1;
  resort();
"""
    # f-string: double {{ }} for literal braces inside JavaScript
    _sort_script = f"""
<script>
(function() {{
  var host = document.getElementById("{host_id}");
  if (!host) return;
  var table = host.querySelector("table");
  if (!table) return;
  var theadRow = table.querySelector("thead tr");
  var tbody = table.querySelector("tbody");
  if (!theadRow || !tbody) return;
  var ths = theadRow.querySelectorAll("th");
  var sortCol = -1;
  var sortDir = 1;

  function cellSortKey(text) {{
    text = (text || "").replace(/\\u00a0/g, " ").trim();
    if (text === "" || text.toLowerCase() === "nan") return {{ t: "empty", v: 0 }};
    if (/^\\d{{4}}-\\d{{2}}-\\d{{2}}$/.test(text)) return {{ t: "str", v: text }};
    var cleaned = text.replace(/[$,]/g, "").replace(/%$/, "").trim();
    var num = parseFloat(cleaned);
    if (!isNaN(num) && cleaned !== "") return {{ t: "num", v: num }};
    return {{ t: "str", v: text.toLowerCase() }};
  }}

  function cmpKey(ka, kb) {{
    if (ka.t === "empty" && kb.t === "empty") return 0;
    if (ka.t === "empty") return 1;
    if (kb.t === "empty") return -1;
    if (ka.t === "num" && kb.t === "num") return ka.v - kb.v;
    if (ka.t === "str" && kb.t === "str") return ka.v < kb.v ? -1 : ka.v > kb.v ? 1 : 0;
    return String(ka.v).localeCompare(String(kb.v));
  }}

  function compareRows(a, b, colIdx) {{
    var ka = cellSortKey(a.cells[colIdx] ? a.cells[colIdx].textContent : "");
    var kb = cellSortKey(b.cells[colIdx] ? b.cells[colIdx].textContent : "");
    return cmpKey(ka, kb);
  }}

  function resort() {{
    if (sortCol < 0) return;
    var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr"));
    rows.sort(function(a, b) {{ return sortDir * compareRows(a, b, sortCol); }});
    for (var r = 0; r < rows.length; r++) tbody.appendChild(rows[r]);
  }}

  for (var i = 0; i < ths.length; i++) {{
    (function(colIdx) {{
      var th = ths[colIdx];
      th.addEventListener("click", function() {{
        if (sortCol === colIdx) sortDir = -sortDir;
        else {{ sortCol = colIdx; sortDir = 1; }}
        resort();
      }});
    }})(i);
  }}
{_init_sort_js}
}})();
</script>
"""
    return (
        f'<div id="{host_id}" class="ph-scan-table-host" style="overflow:auto;max-height:{max_height_px}px;'
        f'width:100%;margin:0;padding:0;box-sizing:border-box;">'
        "<style>"
        ".ph-scan-table-host table { border-collapse:collapse;width:100%;"
        "font-size:0.875rem;font-family:Inter,system-ui,sans-serif;color:#e6edf3; }"
        ".ph-scan-table-host thead th { background:#262730;color:#9aa0a6;font-weight:600;"
        "padding:0.5rem 0.65rem;text-align:left;border-bottom:1px solid rgba(255,255,255,0.08);"
        "cursor:pointer;user-select:none; }"
        ".ph-scan-table-host thead th:hover { color:#e6edf3; }"
        ".ph-scan-table-host tbody td { padding:0.45rem 0.65rem;"
        "border-bottom:1px solid rgba(255,255,255,0.06);vertical-align:middle; }"
        ".ph-scan-table-host tbody tr:hover { background:rgba(255,255,255,0.03); }"
        "</style>"
        f"{inner}{_sort_script}</div>"
    )


def _render_peer_table(
    ticker: str,
    syms: tuple[str, ...],
    df_key_suffix: str,
    sort_losers: bool,
    table_height_px: int,
) -> None:
    mov = _yf_day_moves(syms)
    if mov.empty:
        st.caption("No data.")
        return
    if sort_losers:
        mov = mov.nsmallest(8, "Δ%").reset_index(drop=True)
    else:
        mov = mov.sort_values("Symbol").reset_index(drop=True)
    peer_df_key = f"ph_peer_df_{ticker}_{df_key_suffix}"
    mov_styled = _peer_table_style(mov)
    # single-cell: click the Symbol cell to load (no row checkbox column)
    st.dataframe(
        mov_styled,
        hide_index=True,
        width="stretch",
        height=table_height_px,
        on_select="rerun",
        selection_mode="single-cell",
        key=peer_df_key,
        column_config={
            "Symbol": st.column_config.TextColumn(
                help="Click a symbol to open it",
            ),
            "Last": st.column_config.NumberColumn(format="$%.2f"),
            "Δ%": st.column_config.NumberColumn(format="%.2f"),
        },
    )
    picked = st.session_state.get(peer_df_key)
    cells = (picked or {}).get("selection", {}).get("cells") or []
    if cells:
        row_idx, col = cells[0]
        if col == "Symbol":
            ri = int(row_idx)
            if 0 <= ri < len(mov):
                new_sym = str(mov.iloc[ri]["Symbol"]).upper().strip()
                if new_sym and new_sym != ticker:
                    st.session_state.ph_ticker_pending = new_sym
                    st.rerun()


# ── Sector data ──────────────────────────────────────────────────────────────

_SECTOR_PEERS: dict[str, tuple[str, ...]] = {
    "Technology": ("AAPL","MSFT","NVDA","AVGO","CRM","AMD","ADBE","ORCL","CSCO","INTC"),
    "Communication Services": ("META","GOOGL","GOOG","NFLX","DIS","CMCSA","T","VZ","TMUS"),
    "Consumer Cyclical": ("AMZN","TSLA","HD","MCD","NKE","LOW","SBUX","TGT","BKNG"),
    "Consumer Defensive": ("WMT","COST","PG","KO","PEP","PM","MO","MDLZ","CL"),
    "Financial Services": ("JPM","BAC","WFC","GS","MS","BLK","SCHW","C","AXP"),
    "Healthcare": ("UNH","JNJ","LLY","ABBV","MRK","PFE","TMO","ABT","DHR"),
    "Industrials": ("CAT","DE","HON","UPS","RTX","GE","BA","LMT","MMM"),
    "Energy": ("XOM","CVX","COP","SLB","EOG","MPC","PSX","VLO"),
    "Utilities": ("NEE","DUK","SO","D","AEP","SRE","EXC"),
    "Real Estate": ("PLD","AMT","EQIX","SPG","PSA","O","WELL"),
    "Basic Materials": ("LIN","APD","ECL","SHW","NEM","FCX","NUE"),
}
_DEFAULT_SECTOR_PEERS = ("SPY","QQQ","DIA","IWM","VTI","XLK","XLF","XLE","XLV")
_LOSER_UNIVERSE = (
    "AAPL","MSFT","GOOGL","GOOG","AMZN","NVDA","META","TSLA","BRK-B","UNH",
    "JNJ","V","XOM","JPM","WMT","MA","PG","HD","CVX","MRK",
    "ABBV","PEP","KO","COST","AVGO","BAC","PFE","TMO","CSCO","ACN",
    "DIS","ADBE","NFLX","CRM","AMD","INTC","QCOM","TXN","IBM","GE",
)


# ── Ticker + quote fetch ────────────────────────────────────────────────────

if "ph_ticker" not in st.session_state:
    st.session_state.ph_ticker = "AAPL"
_pending_sym = st.session_state.pop("ph_ticker_pending", None)
if _pending_sym:
    _s = str(_pending_sym).upper().strip()[:10]
    if _s:
        st.session_state.ph_ticker = _s

# Slim input row
_inp_c, _fav_c, _spacer = st.columns([2, 1, 5])
with _inp_c:
    ticker = st.text_input("Ticker", key="ph_ticker", max_chars=10,
                           label_visibility="collapsed", placeholder="Ticker…")
    ticker = (ticker or "").upper().strip()
with _fav_c:
    if ticker:
        ensure_session_watchlist()
        _in_wl = ticker in st.session_state.ph_watchlist
        if st.button("★ Watching" if _in_wl else "☆ Watch", key="ph_fav_btn",
                     use_container_width=True):
            if _in_wl:
                st.session_state.ph_watchlist.remove(ticker)
            else:
                st.session_state.ph_watchlist.append(ticker)
            save_watchlist(st.session_state.ph_watchlist)
            st.rerun()

if not ticker:
    st.warning("Enter a ticker symbol.")
    st.stop()

_mkt_id = id(market)
quote = _cached_quote(_mkt_id, ticker)
if not quote:
    st.error(f"No quote returned for {ticker}.")
    st.stop()
try:
    price, _ = get_equity_display_price(quote)
except Exception as e:
    st.error(f"Could not parse quote: {e}")
    st.stop()

current_price = float(price) if price is not None else 0.0
yf_info = _cached_yf_info(ticker)
_co = _yf_company_name(ticker)
sector = (yf_info.get("sector") or yf_info.get("sectorDisp") or "").strip()
if sector == "Financials":
    sector = "Financial Services"

if "ph_chart_period" not in st.session_state:
    st.session_state.ph_chart_period = "1M"

_PH_PERIOD_LABEL = {
    "1D": "today",
    "5D": "past week",
    "1M": "past month",
    "6M": "past 6 months",
    "1Y": "past year",
    "5Y": "5 years",
}

_fig_px, _chart_end, _chart_chg, _chart_base = _make_google_style_chart(
    ticker, st.session_state.ph_chart_period, yf_info
)

if _co:
    st.markdown(
        f'<p style="color:#9aa0a6;font-size:0.95rem;margin:0 0 0.25rem 0">{html.escape(_co)}</p>',
        unsafe_allow_html=True,
    )

if (
    _chart_end is not None
    and _chart_chg is not None
    and _chart_base is not None
):
    _hdr_ep = float(_chart_end)
    _hdr_chg = float(_chart_chg)
    _dollar_delta = _hdr_ep - float(_chart_base)
    _hdr_up = _hdr_chg >= 0
    _hdr_c = "#34a853" if _hdr_up else "#ea4335"
    _dsign = "+" if _dollar_delta >= 0 else "-"
    _dabs = abs(_dollar_delta)
    _arrow = "▲" if _hdr_up else "▼"
    _span = html.escape(
        _PH_PERIOD_LABEL.get(st.session_state.ph_chart_period, "period")
    )
    _pct_txt = f"{_hdr_chg:+.2f}%"
    st.markdown(
        f'<div style="margin:0 0 0.55rem 0;font-family:Inter,sans-serif">'
        f'<div style="display:flex;align-items:baseline;gap:8px;line-height:1">'
        f'<span class="mono" style="font-size:2rem;font-weight:500;color:#fff;letter-spacing:-0.02em">'
        f"{_hdr_ep:.2f}</span>"
        f'<span style="font-size:0.8rem;color:#9aa0a6;font-weight:400">USD</span>'
        f"</div>"
        f'<div style="color:{_hdr_c};font-size:0.95rem;font-weight:500;margin-top:4px;'
        f'letter-spacing:0.01em">'
        f"{_dsign}{_dabs:.2f} ({_pct_txt}) "
        f'<span style="font-size:0.7rem">{_arrow}</span> {_span}'
        f"</div>"
        f"</div>",
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        f'<div style="margin:0 0 0.55rem 0;font-family:Inter,sans-serif">'
        f'<div style="display:flex;align-items:baseline;gap:8px;line-height:1">'
        f'<span class="mono" style="font-size:2rem;font-weight:500;color:#fff;letter-spacing:-0.02em">'
        f"{float(current_price):.2f}</span>"
        f'<span style="font-size:0.8rem;color:#9aa0a6;font-weight:400">USD</span>'
        f"</div>"
        f'<div style="color:#9aa0a6;font-size:0.9rem;margin-top:4px">No chart data</div>'
        f"</div>",
        unsafe_allow_html=True,
    )

# ═══════════════════════════════════════════════════════════════════════════════
# TOP — Google-style chart + market snapshot (peers | losers)
# ═══════════════════════════════════════════════════════════════════════════════

_snap_l, _snap_m, _snap_r = st.columns([2.35, 1.0, 1.0])
with _snap_l:
    st.radio(
        "Chart range",
        options=["1D", "5D", "1M", "6M", "1Y", "5Y"],
        horizontal=True,
        key="ph_chart_period",
        label_visibility="collapsed",
    )
    if _fig_px is not None:
        st.plotly_chart(
            _fig_px,
            use_container_width=True,
            config={"displayModeBar": False, "scrollZoom": False},
        )
    else:
        st.caption("No price history for this range.")

with _snap_m:
    _sec_lbl = html.escape(sector) if sector else "Unknown"
    st.markdown(
        f'<div class="section-label" style="margin-bottom:6px">'
        f"Sector peers ({_sec_lbl})</div>",
        unsafe_allow_html=True,
    )
    _peer_src = _SECTOR_PEERS.get(sector, _DEFAULT_SECTOR_PEERS)
    _peer_syms = tuple(s for s in _peer_src if s != ticker)[:10]
    _render_peer_table(
        ticker, _peer_syms, "0", False, PH_PRICE_CHART_HEIGHT
    )

with _snap_r:
    st.markdown(
        '<div class="section-label" style="margin-bottom:6px">Large-cap losers</div>',
        unsafe_allow_html=True,
    )
    _los_syms = tuple(s for s in _LOSER_UNIVERSE if s != ticker)
    _render_peer_table(ticker, _los_syms, "1", True, PH_PRICE_CHART_HEIGHT)

# ═══════════════════════════════════════════════════════════════════════════════
# OPTIONS SCANNER
# ═══════════════════════════════════════════════════════════════════════════════

def _scan_filter_heading(label: str) -> None:
    st.markdown(
        '<p style="font-size:0.88rem;color:#9aa0a6;margin:0 0 6px 0;font-weight:600">'
        f"{html.escape(label)}</p>",
        unsafe_allow_html=True,
    )


st.markdown("---")
st.markdown(
    '<div class="section-label" style="margin-bottom:10px;font-size:1.15rem">'
    "Options Scanner</div>",
    unsafe_allow_html=True,
)

_scan_strategy, _scan_dates, _scan_return = st.columns([1.2, 2.0, 1.5])

with _scan_strategy:
    _scan_filter_heading("STRATEGY")
    _strategy = st.radio(
        "Strategy",
        options=["Cash Secured Puts", "Covered Calls"],
        key="ph_scan_strategy",
        horizontal=False,
        label_visibility="collapsed",
    )

_chain_type = "PUT" if _strategy == "Cash Secured Puts" else "CALL"

# Fetch available expirations for the current ticker
_all_expiries = _cached_expiry_dates(id(market), ticker)

if not _all_expiries:
    st.warning(f"No option expiration dates found for **{ticker}**.")
    st.stop()

_min_exp = _all_expiries[0]
_max_exp = _all_expiries[-1]

with _scan_dates:
    _scan_filter_heading("EXPIRATION DATE")
    _dc_from, _dc_to = st.columns(2)
    with _dc_from:
        _exp_from = st.date_input(
            "From",
            value=_min_exp,
            min_value=_min_exp,
            max_value=_max_exp,
            key="ph_scan_exp_from",
        )
    with _dc_to:
        _default_to = min(
            _min_exp + dt.timedelta(days=60), _max_exp
        )
        _exp_to = st.date_input(
            "To",
            value=_default_to,
            min_value=_min_exp,
            max_value=_max_exp,
            key="ph_scan_exp_to",
        )

with _scan_return:
    _scan_filter_heading("MONTHLY RETURN %")
    _ret_range = st.slider(
        "Monthly return %",
        min_value=0.0,
        max_value=10.0,
        value=(3.0, 10.0),
        step=0.5,
        format="%.1f%%",
        key="ph_scan_return_range",
        label_visibility="collapsed",
    )

_scan_cc_basis = None
_cb_default = float(current_price) if current_price and current_price > 0 else 0.0
_cb_col, _ = st.columns([0.26, 0.74])
with _cb_col:
    _cb_in = st.number_input(
        "Share cost basis ($, optional — covered calls)",
        min_value=0.0,
        value=_cb_default,
        step=0.01,
        key=f"ph_scan_cost_basis__{ticker}",
        disabled=_strategy != "Covered Calls",
        help="Wheel Alpha is 0 when strike is below this basis (covered calls only). "
        "Defaults to the current quote for this ticker.",
    )
if _strategy == "Covered Calls" and _cb_in > 0:
    _scan_cc_basis = float(_cb_in)

# Filter expiries within the selected date range
_selected_expiries = [
    d for d in _all_expiries if _exp_from <= d <= _exp_to
]

if not _selected_expiries:
    st.info("No expiration dates in the selected range.")
    st.stop()

# ── Scan across selected expiries ────────────────────────────────────────────

_iv_bounds_lo, _iv_bounds_hi = _cached_52w_iv_rank_bounds(ticker)
_earn_date_str = _cached_next_earnings_date_str(ticker)

_scan_rows: list[dict] = []

with st.spinner(f"Scanning {len(_selected_expiries)} expiration(s)…"):
    for exp_date in _selected_expiries:
        chain = _cached_option_chain(id(market), ticker, exp_date, _chain_type)
        if chain.empty:
            continue

        _today = dt.date.today()
        # busday_count excludes the start date but includes the end date, so it does
        # not count "today" as a session. When expiration is after today, add 1 so DTE
        # is trading days from today through expiration, inclusive (Mon–Fri; NumPy
        # does not apply exchange holidays). Same calendar day → 0 (0DTE).
        _raw_dte = int(
            np.busday_count(
                np.datetime64(_today),
                np.datetime64(exp_date),
            )
        )
        dte = _raw_dte + 1 if exp_date > _today else _raw_dte
        if dte <= 0:
            continue

        calendar_dte = (exp_date - _today).days
        if calendar_dte <= 0:
            continue

        for _, row in chain.iterrows():
            bid = float(row.get("Bid", 0) or 0)
            strike = float(row.get("Strike", 0) or 0)
            if strike <= 0 or bid <= 0:
                continue

            otm_pct = (
                ((strike / current_price) - 1) * 100
                if current_price > 0
                else 0.0
            )
            # CSP: OTM puts only (strike below spot → negative OTM % here).
            # CC: OTM calls only (strike above spot → positive OTM % here).
            if _chain_type == "PUT" and otm_pct >= 0:
                continue
            if _chain_type == "CALL" and otm_pct <= 0:
                continue

            if _chain_type == "PUT":
                # CSP: premium / strike × (30.42 / calendar DTE) × 100
                raw_return = bid / strike
            else:
                # CC: premium / spot × (30.42 / calendar DTE) × 100
                raw_return = bid / current_price

            monthly_return = (
                raw_return * (PH_AVG_CALENDAR_DAYS_PER_MONTH / calendar_dte) * 100.0
            )

            if _ret_range[0] <= monthly_return <= _ret_range[1]:
                _iv_dec = _scan_iv_to_decimal(row.get("IV"))
                _iv_rank = _scan_iv_rank_pct(
                    _iv_dec, _iv_bounds_lo, _iv_bounds_hi
                )
                _wheel_alpha = _calculate_wheel_alpha(
                    monthly_return,
                    otm_pct,
                    calendar_dte,
                    _iv_dec,
                    _iv_rank,
                    strike,
                    cost_basis=_scan_cc_basis,
                    is_put=(_chain_type == "PUT"),
                )
                _scan_rows.append({
                    "Expiration Date": exp_date.strftime("%Y-%m-%d"),
                    "DTE (Trading Days)": dte,
                    "Strike": strike,
                    "OTM %": round(otm_pct, 2),
                    "Mo. Return %": round(monthly_return, 2),
                    "IV": row.get("IV", ""),
                    "IV Rank": (
                        np.nan
                        if _iv_rank is None
                        else round(_iv_rank, 1)
                    ),
                    "Wheel Alpha": (
                        np.nan
                        if _wheel_alpha != _wheel_alpha
                        else round(_wheel_alpha, 1)
                    ),
                    "Earn. Date": _earn_date_str,
                    "Bid": bid,
                    "Ask": float(row.get("Ask", 0) or 0),
                    "Volume": int(row.get("Volume", 0) or 0),
                })

_SCAN_COL_ORDER = (
    "Expiration Date",
    "DTE (Trading Days)",
    "Strike",
    "OTM %",
    "Mo. Return %",
    "IV",
    "IV Rank",
    "Wheel Alpha",
    "Earn. Date",
    "Bid",
    "Ask",
    "Volume",
)

_PH_SCAN_COL_HELP: dict[str, str] = {
    "Expiration Date": "Option expiration date (YYYY-MM-DD).",
    "DTE (Trading Days)": (
        "Trading days from today through expiration, inclusive (Mon–Fri only; "
        "exchange holidays are not excluded)."
    ),
    "Strike": "Strike price for this contract.",
    "OTM %": "Moneyness vs spot: negative for OTM puts, positive for OTM calls.",
    "Mo. Return %": (
        "(Premium / reference) × (30.42 / calendar days to expiration) × 100. "
        "Puts: reference = strike. Calls: reference = spot. "
        "Calendar days = expiration date − today (not trading days)."
    ),
    "IV": "Implied volatility from the chain (format varies by broker).",
    "IV Rank": (
        "(Current IV − 52w low) / (52w high − 52w low) × 100, clamped 0–100. "
        "52w range uses trailing min/max of 30-day historical vol (annualized) as an IV proxy."
    ),
    "Wheel Alpha": (
        "Yield × safety vs IV × time (0–100). Safety factor = (|OTM %| / expected 1SD %)² "
        "with expected 1SD % = IV × √(calendar DTE / 365) × 100 (IV annualized as decimal). "
        f"DTE weight = (calendar DTE / {PH_WHEEL_DTE_TARGET_DAYS})^"
        f"{PH_WHEEL_DTE_GAMMA_POWER:g} below {PH_WHEEL_DTE_TARGET_DAYS} days, else 1. "
        f"Gamma tax × clip((Mo. Return % / {PH_GAMMA_TAX_YIELD_REF_PCT:g}) / √(1/calendar DTE), "
        f"{PH_GAMMA_TAX_MULT_MIN:g}, {PH_GAMMA_TAX_MULT_MAX:g}). "
        f"Below {PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT:g}% Mo. Return: linear 0× at ≤{PH_WHEEL_MO_RETURN_PENALTY_LOW_PCT:g}% "
        f"to 1× at the hinge. At/above hinge: log₂-tuned factor 0.60–1.0 (favors higher yield). "
        "Puts: monthly return minus 4.5% annual risk-free / 12. "
        "Calls: strikes below optional cost basis score 0."
    ),
    "Earn. Date": (
        "Next upcoming earnings (Yahoo Finance). Pink pill: earnings before option expiration."
    ),
    "Bid": "Option bid.",
    "Ask": "Option ask.",
    "Volume": "Contract volume.",
}

if _scan_rows:
    _scan_df = (
        pd.DataFrame(_scan_rows)
        .sort_values(
            ["Wheel Alpha", "Mo. Return %"],
            ascending=[False, False],
            na_position="last",
        )[list(_SCAN_COL_ORDER)]
    )
    _scan_styled = _scan_table_styler(_scan_df)
    _scan_h = min(400, 35 * len(_scan_df) + 38)
    st.markdown(
        f'<p style="color:#9aa0a6;font-size:0.88rem;margin:2px 0 4px 0">'
        f'{len(_scan_df)} contracts found &nbsp;·&nbsp; '
        f'{_strategy} on <b style="color:#c9d1d9">{ticker}</b> '
        f'@ ${current_price:,.2f}</p>',
        unsafe_allow_html=True,
    )
    components.html(
        _scan_table_html_fragment(
            _scan_styled,
            max_height_px=_scan_h,
            column_order=_SCAN_COL_ORDER,
            column_tips=_PH_SCAN_COL_HELP,
            initial_sort_col="Wheel Alpha",
        ),
        height=min(520, _scan_h + 22),
        scrolling=True,
    )
else:
    st.info("No contracts match the selected filters. Try widening the date range or return %.")

# ═══════════════════════════════════════════════════════════════════════════════
# CANDIDATE BADGE  (Top-3 Weighted Alpha)
# ═══════════════════════════════════════════════════════════════════════════════

def _top3_weighted_alpha(scan_rows: list[dict]) -> float | None:
    scores = [
        float(r["Wheel Alpha"])
        for r in scan_rows
        if r.get("Wheel Alpha") is not None
        and not (isinstance(r["Wheel Alpha"], float) and np.isnan(r["Wheel Alpha"]))
    ]
    if not scores:
        return None
    scores.sort(reverse=True)
    return float(np.mean(scores[:3]))


def _candidate_tier(score: float) -> tuple[str, str, str, str]:
    """(label, colour, glow_colour, context)"""
    if score >= 90:
        return (
            "Extreme",
            "#00FF88",
            "rgba(0, 255, 136, 0.35)",
            'Multiple "mispriced" gems. High IV Rank + High Yield. Strike while the iron is hot.',
        )
    if score >= 70:
        return (
            "Strong",
            "#228B22",
            "rgba(34, 139, 34, 0.30)",
            "Reliable 3–5% monthly returns with solid cushions. Perfect for the Standard Wheel.",
        )
    if score >= 40:
        return (
            "Moderate",
            "#FFBF00",
            "rgba(255, 191, 0, 0.25)",
            "Safe, but quiet. Good for capital preservation, lower monthly income.",
        )
    return (
        "Weak",
        "#ff6b6b",
        "rgba(255, 107, 107, 0.20)",
        "Options are cheap (Low IV Rank) or too risky for the pay. Skip for now.",
    )


def _candidate_bar_meter_html(score: float, n_bars: int = 11) -> str:
    """
    Signal-style bars as HTML divs (Streamlit's st.html sanitizer often strips SVG).
    Short red (left) → tall green (right); lit bar count from Top-3 score 0–100.
    """
    s = float(max(0.0, min(100.0, score)))
    active = min(n_bars, max(0, math.ceil(s / 100.0 * n_bars - 1e-9)))
    max_h = 34
    min_h = 5
    w_bar = 5
    r0, g0, b0 = 234, 67, 53
    r1, g1, b1 = 0, 255, 136
    parts: list[str] = []
    for i in range(n_bars):
        t = i / (n_bars - 1) if n_bars > 1 else 1.0
        h = min_h + (max_h - min_h) * t
        rr = int(r0 + (r1 - r0) * t)
        gg = int(g0 + (g1 - g0) * t)
        bb = int(b0 + (b1 - b0) * t)
        fill = f"#{rr:02x}{gg:02x}{bb:02x}"
        op = 1.0 if i < active else 0.35
        parts.append(
            f'<div style="width:{w_bar}px;height:{h:.1f}px;background:{fill};'
            f"border-radius:2px;opacity:{op:.2f};flex-shrink:0"
            f'"></div>'
        )
    inner = "".join(parts)
    return (
        f'<div role="img" aria-label="Conviction level about {s:.0f} out of 100" '
        f'style="display:flex;align-items:flex-end;gap:3px;height:40px;min-height:40px;'
        f"min-width:100px;padding:2px 4px;box-sizing:border-box;flex-shrink:0"
        f'">'
        f"{inner}</div>"
    )


_t3_score = _top3_weighted_alpha(_scan_rows)

if _t3_score is not None:
    _tier_lbl, _tier_c, _tier_glow, _tier_ctx = _candidate_tier(_t3_score)
    _strat_short = "CSP" if _strategy == "Cash Secured Puts" else "CC"
    st.html(
        f'<div style="'
        f"display:flex;align-items:center;gap:18px;"
        f"margin:0.35rem 0 0 0;padding:0.7rem 1.1rem;"
            f"border-radius:12px;"
            f"border:1px solid {_tier_c}40;"
            f"background:linear-gradient(135deg, {_tier_glow} 0%, transparent 60%);"
            f'">'
            f'<div title="Top-3 weighted alpha: {_t3_score:.1f} / 100">'
            f"{_candidate_bar_meter_html(_t3_score)}"
            f"</div>"
            # text block
            f'<div style="min-width:0">'
            f'<div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap">'
            f'<span style="font-size:1.15rem;font-weight:700;color:{_tier_c};'
            f'font-family:Inter,sans-serif;letter-spacing:0.01em">{_tier_lbl}</span>'
            f'<span style="font-size:0.82rem;color:#9aa0a6;font-weight:500">'
            f"{_strat_short} Candidate</span>"
            f"</div>"
            f'<p style="margin:4px 0 0 0;font-size:0.85rem;color:#c9d1d9;line-height:1.4;'
            f'font-family:Inter,sans-serif">{html.escape(_tier_ctx)}</p>'
            f"</div>"
            f"</div>"
    )
else:
    st.html(
        '<div style="'
        "display:flex;align-items:center;gap:14px;"
        "margin:0.35rem 0 0 0;padding:0.7rem 1.1rem;"
        "border-radius:12px;border:1px solid #333;background:#1A1C23;"
        '">'
        '<span style="font-size:0.9rem;color:#9aa0a6;font-family:Inter,sans-serif">'
        "No Wheel Alpha scores available to classify this ticker."
        "</span></div>"
    )
