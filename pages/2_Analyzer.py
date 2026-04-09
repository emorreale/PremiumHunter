import datetime as dt
import html
import json
import math
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from etrade_market import (
    get_equity_display_price,
    get_equity_quotes_batch,
    get_expiry_dates,
    get_option_chain,
    get_quote,
)
# Calendar DTE for Mo. Return + Wheel Alpha: shared with Discover + watchlist_snapshot_to_postgres.
from ph_wheel_calendar_dte import wheel_alpha_effective_calendar_dte
from watchlist_persist import ensure_session_watchlist, save_watchlist

if st.session_state.get("market") is None:
    st.info("Connect to E-Trade using the sidebar to get started.")
    st.stop()

market = st.session_state.market

ensure_session_watchlist()
watchlist: list[str] = st.session_state.ph_watchlist

# ── Constants (same as Discover) ────────────────────────────────────────────
PH_AVG_CALENDAR_DAYS_PER_MONTH = 30.42
PH_WHEEL_MO_RETURN_PENALTY_LOW_PCT = 2.0
PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT = 3.0
PH_WHEEL_DTE_TARGET_DAYS = 5
PH_WHEEL_DTE_GAMMA_POWER = 3.0
PH_GAMMA_TAX_YIELD_REF_PCT = 20.0
PH_GAMMA_TAX_MULT_MIN = 0.5
PH_GAMMA_TAX_MULT_MAX = 1.0
PH_WHEEL_OTM_SAFETY_STD = 0.75  # must match Discover / watchlist_snapshot_to_postgres
# Matrix wheel scan: cap E*Trade option-chain calls (CSP puts only — matches Discover default scanner).
PH_MATRIX_MAX_EXPIRIES = 5
PH_MATRIX_CSP_SPOT_CACHE_DECIMALS = 4
# Mo. Return % band: min fixed; max must match Discover PH_SCAN_MO_RETURN_SLIDER_MAX (inclusive).
PH_MATRIX_MIN_MO_RETURN_PCT = 3.0
PH_MATRIX_MAX_MO_RETURN_PCT = 20.0
# Plain text for hover tooltip on matrix Wheel Alpha gauge (no markdown).
_PH_MATRIX_GAUGE_TIP = (
    "Wheel Alpha is the mean of the three highest scores from the same CSP scan as "
    "Discover’s Options Scanner (default): OTM puts only, America/Chicago calendar days, "
    f"expirations from the first listed through +60 days (capped at {PH_MATRIX_MAX_EXPIRIES} nearest), "
    f"and {PH_MATRIX_MIN_MO_RETURN_PCT:g}%–{PH_MATRIX_MAX_MO_RETURN_PCT:g}% monthly return filter (scanner slider max). "
    "Spot is E*Trade only (matches the scanner chain math)."
)


def _matrix_calendar_today() -> dt.date:
    """Match Discover option scanner (`_scanner_calendar_today`); avoids UTC-vs-Chicago skew on Cloud."""
    return dt.datetime.now(ZoneInfo("America/Chicago")).date()


def _inject_matrix_gauge_hover_tip() -> None:
    """
    Floating tooltip near the cursor (position:fixed on document.body) so the gauge
    does not grow scrollbars. iframe pointer-events disabled so the wrapper receives moves.
    """
    tip_js = json.dumps(_PH_MATRIX_GAUGE_TIP)
    st.html(
        f"""<script>
(function () {{
  const TIP = {tip_js};
  function tipStyle(el) {{
    el.className = "ph-matrix-gauge-tooltip";
    el.textContent = TIP;
    el.setAttribute("role", "tooltip");
    el.style.cssText =
      "display:none;position:fixed;left:-9999px;top:-9999px;max-width:min(300px,90vw);"
      + "padding:10px 12px;font-family:Inter,system-ui,sans-serif;font-size:0.8rem;line-height:1.4;"
      + "color:#e6edf3;background:rgba(22,27,34,0.98);border:1px solid rgba(255,255,255,0.12);"
      + "border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,0.5);z-index:100000;"
      + "pointer-events:none;text-align:left;white-space:normal;";
  }}
  function placeTip(tip, e) {{
    const pad = 14;
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    tip.style.display = "block";
    const tw = tip.offsetWidth;
    const th = tip.offsetHeight;
    let x = e.clientX + pad;
    let y = e.clientY + pad;
    if (x + tw > vw - 10) x = e.clientX - tw - pad;
    if (y + th > vh - 10) y = e.clientY - th - pad;
    if (x < 6) x = 6;
    if (y < 6) y = 6;
    tip.style.left = x + "px";
    tip.style.top = y + "px";
  }}
  function wire(container) {{
    if (!container || container.getAttribute("data-ph-gauge-tip") === "1") return;
    container.setAttribute("data-ph-gauge-tip", "1");
    const iframe = container.querySelector("iframe");
    if (iframe) iframe.style.pointerEvents = "none";
    const tip = document.createElement("div");
    tipStyle(tip);
    document.body.appendChild(tip);
    container.addEventListener("mouseenter", function (e) {{ placeTip(tip, e); }});
    container.addEventListener("mousemove", function (e) {{
      if (tip.style.display === "block") placeTip(tip, e);
    }});
    container.addEventListener("mouseleave", function () {{
      tip.style.display = "none";
      tip.style.left = "-9999px";
    }});
  }}
  function scan() {{
    document.querySelectorAll('[class*="st-key-ph_mtx_gauge_"]').forEach(wire);
  }}
  scan();
  [200, 600, 1400].forEach(function (ms) {{ setTimeout(scan, ms); }});
  if (!window.__phMtxGaugeObs) {{
    window.__phMtxGaugeObs = new MutationObserver(scan);
    window.__phMtxGaugeObs.observe(document.body, {{ childList: true, subtree: true }});
  }}
}})();
</script>""",
        width="stretch",
        unsafe_allow_javascript=True,
    )


# ── Cached helpers ──────────────────────────────────────────────────────────

@st.cache_data(ttl=120, show_spinner=False)
def _cached_quotes_batch(_market_id: int, symbols_key: tuple[str, ...]) -> dict[str, dict]:
    if not symbols_key:
        return {}
    return get_equity_quotes_batch(market, list(symbols_key))


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
    if x > 2.0:
        x = x / 100.0
    return x


def _scan_iv_rank_pct(iv_dec, lo, hi):
    if iv_dec is None or lo is None or hi is None:
        return None
    span = hi - lo
    if span <= 1e-10:
        return None
    r = (iv_dec - lo) / span * 100.0
    return float(max(0.0, min(100.0, r)))


def _mo_return_penalty_factor(mo_return_pct: float) -> float:
    lo = PH_WHEEL_MO_RETURN_PENALTY_LOW_PCT
    hi = PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT
    if mo_return_pct <= lo:
        return 0.0
    if mo_return_pct >= hi:
        return 1.0
    return float((mo_return_pct - lo) / (hi - lo))


def _income_scaling_factor(mo_return_pct: float) -> float:
    hi = PH_WHEEL_MO_RETURN_PENALTY_HIGH_PCT
    if mo_return_pct >= hi:
        raw_modifier = np.log2(float(mo_return_pct) - hi + 1.0) / np.log2(hi)
        return float(0.60 + np.clip(raw_modifier, 0.0, 1.0) * 0.40)
    return _mo_return_penalty_factor(float(mo_return_pct))


def _expected_1sd_move_pct(iv_dec: float, calendar_dte: float) -> float:
    dte = max(float(calendar_dte), 1e-9)
    return float(iv_dec * np.sqrt(dte / 365.0) * 100.0)


def _dte_weight(calendar_dte: float) -> float:
    d = max(float(calendar_dte), 0.0)
    t = float(PH_WHEEL_DTE_TARGET_DAYS)
    if d >= t:
        return 1.0
    return float((d / t) ** PH_WHEEL_DTE_GAMMA_POWER)


def _gamma_tax_multiplier(mo_return_pct: float, calendar_dte: float) -> float:
    dte_cal = max(float(calendar_dte), 1e-9)
    gamma_risk_factor = float(np.sqrt(1.0 / dte_cal))
    gamma_tax = (float(mo_return_pct) / PH_GAMMA_TAX_YIELD_REF_PCT) / gamma_risk_factor
    return float(np.clip(gamma_tax, PH_GAMMA_TAX_MULT_MIN, PH_GAMMA_TAX_MULT_MAX))


def _calculate_wheel_alpha(
    mo_return_pct, otm_pct, calendar_dte, iv_dec, iv_rank, strike,
    *, cost_basis=None, is_put=True,
) -> float:
    if (not is_put) and cost_basis and strike < cost_basis:
        return 0.0
    if iv_dec is None or iv_dec <= 0:
        return float("nan")
    net_monthly_yield = mo_return_pct - (4.5 / 12.0 if is_put else 0.0)
    _exp1 = _expected_1sd_move_pct(float(iv_dec), calendar_dte)
    _tgt = max(_exp1 * PH_WHEEL_OTM_SAFETY_STD, 0.01)
    safety_factor = (abs(float(otm_pct)) / _tgt) ** 2
    ir = float(iv_rank) if iv_rank is not None and not (isinstance(iv_rank, float) and np.isnan(iv_rank)) else 50.0
    vol_penalty = (iv_dec ** 0.9) * (1.0 + (100.0 - ir) / 100.0)
    if vol_penalty <= 0 or not np.isfinite(vol_penalty):
        vol_penalty = 1e-9
    _dw = _dte_weight(calendar_dte)
    score = (net_monthly_yield * safety_factor * _dw) / vol_penalty
    score *= _income_scaling_factor(float(mo_return_pct))
    score *= _gamma_tax_multiplier(float(mo_return_pct), calendar_dte)
    return float(np.clip(score * 10.0, 0.0, 100.0))


def _top3_weighted_alpha(alphas: list[float]) -> float | None:
    """Arithmetic mean of the three largest finite scores (not a weighted average)."""
    valid = [a for a in alphas if a == a and np.isfinite(a)]
    if not valid:
        return None
    valid.sort(reverse=True)
    return float(np.mean(valid[:3]))


def _matrix_wheel_alphas_from_chain(
    chain: pd.DataFrame,
    spot: float,
    calendar_dte: float,
    *,
    is_put: bool,
    iv_lo,
    iv_hi,
) -> list[float]:
    """OTM puts or OTM calls; Mo. Return % must exceed PH_MATRIX_MIN_MO_RETURN_PCT before IV/alpha."""
    out: list[float] = []
    if (
        chain.empty
        or "Bid" not in chain.columns
        or "Strike" not in chain.columns
        or spot <= 0
        or calendar_dte <= 0
    ):
        return out
    for row in chain.itertuples(index=False):
        bid = float(row.Bid or 0)
        strike = float(row.Strike or 0)
        if strike <= 0 or bid <= 0:
            continue
        otm_pct = ((strike / spot) - 1.0) * 100.0
        if is_put:
            if otm_pct >= 0:
                continue
            raw_return = bid / strike
            cost_basis = None
        else:
            if otm_pct <= 0:
                continue
            raw_return = bid / spot
            cost_basis = spot
        monthly_return = raw_return * (PH_AVG_CALENDAR_DAYS_PER_MONTH / calendar_dte) * 100.0
        if monthly_return <= PH_MATRIX_MIN_MO_RETURN_PCT:
            continue
        if monthly_return > PH_MATRIX_MAX_MO_RETURN_PCT:
            continue
        iv_dec = _scan_iv_to_decimal(getattr(row, "IV", None))
        iv_rank = _scan_iv_rank_pct(iv_dec, iv_lo, iv_hi)
        wa = _calculate_wheel_alpha(
            monthly_return,
            otm_pct,
            calendar_dte,
            iv_dec,
            iv_rank,
            strike,
            cost_basis=cost_basis,
            is_put=is_put,
        )
        if wa == wa:
            out.append(wa)
    return out


def _matrix_wheel_scan_body(_market_id: int, sym: str, spot: float) -> list[float]:
    """
    CSP (OTM puts) only, same calendar and expiry window defaults as Discover Options Scanner.
    """
    if spot <= 0:
        return []
    expiries = _cached_expiry_dates(_market_id, sym)
    if not expiries:
        return []
    today = _matrix_calendar_today()
    _min_exp = expiries[0]
    _max_exp = expiries[-1]
    exp_to = min(_min_exp + dt.timedelta(days=60), _max_exp)
    selected = sorted(d for d in expiries if _min_exp <= d <= exp_to and d > today)
    if not selected:
        selected = sorted(d for d in expiries if d > today)[:2]
    selected = selected[:PH_MATRIX_MAX_EXPIRIES]
    iv_lo, iv_hi = _cached_52w_iv_rank_bounds(sym)
    alphas: list[float] = []
    for exp_date in selected:
        # ph_wheel_calendar_dte only (parity with Discover + watchlist_snapshot_to_postgres).
        calendar_dte = wheel_alpha_effective_calendar_dte(exp_date)
        if calendar_dte <= 0:
            continue
        put_chain = _cached_option_chain(_market_id, sym, exp_date, "PUT")
        alphas.extend(
            _matrix_wheel_alphas_from_chain(
                put_chain, spot, calendar_dte, is_put=True, iv_lo=iv_lo, iv_hi=iv_hi
            )
        )
    return alphas


@st.cache_data(ttl=120, show_spinner=False)
def _cached_matrix_wheel_alphas(_market_id: int, symbol: str, spot_key: str) -> list[float]:
    """Cache CSP-only alpha list per symbol + rounded E*Trade spot."""
    return _matrix_wheel_scan_body(_market_id, symbol, float(spot_key))


# ── Plotly charts (st.html strips SVG; Plotly renders reliably) ─────────────


def _tier_color(score: float) -> str:
    s = float(score)
    if s >= 70:
        return "#00FF88"
    if s >= 40:
        return "#FFBF00"
    return "#ff6b6b"


def _sparkline_figure(
    closes: list[float],
    prev_close: float | None,
    *,
    height: int = 78,
) -> go.Figure | None:
    """1-day style area sparkline with dashed prior-close baseline and end dot."""
    if len(closes) < 2:
        return None
    n = len(closes)
    x = list(range(n))
    mn, mx = min(closes), max(closes)
    pc_ok = prev_close is not None and float(prev_close) > 0
    pc = float(prev_close) if pc_ok else None
    # Expand vertical span when prior close is outside the session range (gaps) so hline stays visible.
    mn_e = min(mn, pc) if pc_ok else mn
    mx_e = max(mx, pc) if pc_ok else mx
    span_e = mx_e - mn_e
    pad_e = max(span_e * 0.08, abs(mn_e) * 0.002 if mn_e else 0.01, 0.01) or 0.01
    y_floor = mn_e - pad_e
    y_ceiling = mx_e + pad_e
    # Match header semantics: vs prior close when known, else first→last bar in this series.
    if pc_ok:
        up = closes[-1] >= pc
    else:
        up = closes[-1] >= closes[0]
    line_c = "#34a853" if up else "#ea4335"
    fill_c = "rgba(52,168,83,0.22)" if up else "rgba(234,67,53,0.18)"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x,
            y=[y_floor] * n,
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    m_sizes = [0] * (n - 1) + [7]
    m_colors = [line_c] * n
    fig.add_trace(
        go.Scatter(
            x=x,
            y=closes,
            mode="lines+markers",
            line=dict(color=line_c, width=2),
            marker=dict(size=m_sizes, color=m_colors, line=dict(width=0)),
            fill="tonexty",
            fillcolor=fill_c,
            showlegend=False,
            hoverinfo="skip",
        )
    )
    if pc_ok:
        fig.add_hline(
            y=pc,
            line_dash="dash",
            line_color="rgba(255,255,255,0.28)",
            line_width=1,
        )

    fig.update_layout(
        height=height,
        margin=dict(l=0, r=0, t=2, b=2),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        xaxis=dict(
            showgrid=False,
            showticklabels=False,
            zeroline=False,
            fixedrange=True,
        ),
        yaxis=dict(
            showgrid=False,
            showticklabels=False,
            zeroline=False,
            fixedrange=True,
            range=[y_floor, y_ceiling],
        ),
    )
    return fig


def _gauge_figure(score: float | None, *, height: int = 104) -> go.Figure:
    """
    Semi-circular gauge for Top-3 wheel alpha (0–100).
    Built-in threshold line acts as the needle; percent annotation below; WHEEL ALPHA title.
    """
    margins = dict(l=2, r=2, t=24, b=8)
    title_style = dict(
        text="WHEEL ALPHA",
        font=dict(size=10, color="#8b949e", family="Inter, sans-serif"),
        align="center",
    )

    def _invalid(x) -> bool:
        if x is None:
            return True
        try:
            v = float(x)
        except (TypeError, ValueError):
            return True
        return not math.isfinite(v)

    if _invalid(score):
        dash_ann = dict(
            x=0.5,
            y=0.36,
            xref="paper",
            yref="paper",
            text="—",
            showarrow=False,
            font=dict(size=22, color="#6e7681", family="Roboto Mono, monospace"),
        )
        sub_ann = dict(
            x=0.5,
            y=0.20,
            xref="paper",
            yref="paper",
            text="WHEEL ALPHA",
            showarrow=False,
            font=dict(size=10, color="#8b949e", family="Inter, sans-serif"),
        )
        fig = go.Figure(
            go.Indicator(
                mode="gauge",
                value=0,
                gauge=dict(
                    shape="angular",
                    axis=dict(range=[0, 100], visible=False),
                    bgcolor="rgba(255,255,255,0.06)",
                    borderwidth=0,
                    bar=dict(color="rgba(255,255,255,0.10)", thickness=0.85),
                ),
            )
        )
        fig.update_layout(
            height=height,
            margin=margins,
            paper_bgcolor="rgba(0,0,0,0)",
            annotations=[dash_ann, sub_ann],
        )
        return fig

    s = max(0.0, min(100.0, float(score)))
    bar_c = _tier_color(s)
    pct_ann = dict(
        x=0.5,
        y=0.20,
        xref="paper",
        yref="paper",
        text=f"{s:.0f}%",
        showarrow=False,
        font=dict(size=20, color=bar_c, family="Roboto Mono, monospace"),
        xanchor="center",
        yanchor="middle",
    )
    fig = go.Figure(
        go.Indicator(
            mode="gauge",
            value=s,
            title=title_style,
            gauge=dict(
                shape="angular",
                axis=dict(range=[0, 100], visible=False),
                bgcolor="rgba(255,255,255,0.06)",
                borderwidth=0,
                bar=dict(color=bar_c, thickness=0.85),
                threshold=dict(
                    line=dict(color="#e6edf3", width=3),
                    thickness=0.92,
                    value=s,
                ),
            ),
        )
    )
    fig.update_layout(
        height=height,
        margin=margins,
        paper_bgcolor="rgba(0,0,0,0)",
        annotations=[pct_ann],
    )
    return fig


# ── Fetch data per ticker ──────────────────────────────────────────────────

def _card_data(sym: str, quotes_batch: dict[str, dict] | None = None) -> dict:
    """
    Gather everything needed for one matrix card.
    Header price / day change match Discover's 1D chart (Yahoo last + Yahoo prior close).
    Wheel Alpha uses E*Trade spot only (same as Discover scanner); no Yahoo fallback for OTM math.
    """
    out: dict = {
        "sym": sym, "price": 0.0, "prev_close": 0.0,
        "chg": 0.0, "pct": 0.0, "closes": [], "t3_alpha": None,
    }
    info = _cached_yf_info(sym)
    # Display last: same source as Discover _make_google_style_chart (Yahoo), then E*Trade.
    y_last = info.get("regularMarketPrice") or info.get("currentPrice")
    try:
        if y_last is not None and not pd.isna(y_last):
            out["price"] = float(y_last)
    except (TypeError, ValueError):
        pass
    su = sym.upper().strip()
    if quotes_batch is not None and su in quotes_batch:
        q = quotes_batch[su]
    else:
        q = _cached_quote(id(market), sym)
    spot_etrade = 0.0
    if q:
        try:
            p, _ = get_equity_display_price(q)
            if p is not None:
                spot_etrade = float(p)
        except Exception:
            pass
    if out["price"] <= 0 and spot_etrade > 0:
        out["price"] = spot_etrade
    # Sparkline: prefer 1d/5m; fall back when market closed or API returns empty
    hist = None
    for period, interval in (
        ("1d", "5m"),
        ("1d", "15m"),
        ("5d", "15m"),
        ("5d", "1h"),
        ("1mo", "1d"),
    ):
        h = _cached_yf_history(sym, period, interval)
        if h is not None and len(h) >= 2:
            hist = h
            break
    if hist is not None:
        close_s = pd.to_numeric(hist["Close"], errors="coerce").dropna()
        if len(close_s) >= 2:
            out["closes"] = close_s.tolist()
    # Yahoo `info` often 401s on Cloud while `history` still returns bars — fill header + alpha spot.
    if out["price"] <= 0 and out["closes"]:
        out["price"] = float(out["closes"][-1])
    # Prior close: Discover 1D baseline (Yahoo info first), then daily history fallback.
    prev = info.get("previousClose") or info.get("regularMarketPreviousClose")
    try:
        if prev is not None and not pd.isna(prev):
            out["prev_close"] = float(prev)
    except (TypeError, ValueError):
        pass
    if out["prev_close"] <= 0:
        h_daily = _cached_yf_history(sym, "10d", "1d")
        if h_daily is not None and len(h_daily) >= 2:
            dc = pd.to_numeric(h_daily["Close"], errors="coerce").dropna()
            if len(dc) >= 2:
                out["prev_close"] = float(dc.iloc[-2])
    if out["prev_close"] <= 0 and len(out["closes"]) >= 2:
        out["prev_close"] = float(out["closes"][0])
    if out["price"] > 0 and out["prev_close"] > 0:
        out["chg"] = out["price"] - out["prev_close"]
        out["pct"] = (out["chg"] / out["prev_close"]) * 100
    # Wheel Alpha: E*Trade spot only (Discover scanner never uses Yahoo for chain/OTM).
    if spot_etrade > 0:
        sk = f"{spot_etrade:.{PH_MATRIX_CSP_SPOT_CACHE_DECIMALS}f}"
        alphas = _cached_matrix_wheel_alphas(id(market), sym, sk)
        out["t3_alpha"] = _top3_weighted_alpha(alphas)
    else:
        out["t3_alpha"] = None
    return out


def _card_data_parallel(symbols: list[str]) -> list[dict]:
    """
    One batched E*Trade quote request (≤25 symbols per API call), then sequential
    option-chain scans (pyetrade session is not thread-safe for chains).
    """
    syms = [s for s in symbols if s]
    key = tuple(sorted({str(s).upper().strip() for s in syms}))
    batch = _cached_quotes_batch(id(market), key) if key else {}
    return [_card_data(s, batch) for s in syms]


# ── Page layout ─────────────────────────────────────────────────────────────

st.markdown(
    '<div class="section-label" style="margin-top:0.65rem;margin-bottom:12px;font-size:1.15rem">'
    "Performance Matrix Watchlist</div>",
    unsafe_allow_html=True,
)

if not watchlist:
    st.markdown(
        """<div class="glass-card" style="text-align:center;padding:2.5rem 1rem">
        <div style="font-size:1.8rem;margin-bottom:8px">☆</div>
        <div style="font-size:1rem;color:#888">Your watchlist is empty</div>
        <div style="font-size:0.8rem;color:#555;margin-top:4px">
            Go to <b>Discover</b> and click <b>☆ Watch</b> to add tickers.
        </div>
        </div>""",
        unsafe_allow_html=True,
    )
    st.stop()

# Fetch all card data (parallel per ticker + cached CSP/CC matrix scan)
with st.spinner("Loading watchlist data…"):
    cards = _card_data_parallel(watchlist)

# Render 4-column grid
COLS = 4
rows_needed = math.ceil(len(cards) / COLS)

for row_idx in range(rows_needed):
    cols = st.columns(COLS)
    for col_idx in range(COLS):
        card_idx = row_idx * COLS + col_idx
        if card_idx >= len(cards):
            break
        cd = cards[card_idx]
        with cols[col_idx]:
            sym = cd["sym"]
            price = cd["price"]
            chg = cd["chg"]
            pct = cd["pct"]
            up = chg >= 0
            chg_color = "#34a853" if up else "#ea4335"
            prev_ok = cd["prev_close"] > 0 and cd["price"] > 0
            if prev_ok:
                money_ch = f"{'+' if chg >= 0 else '-'}${abs(chg):,.2f}"
                chg_line = f"{money_ch} ({pct:+.2f}%)"
            else:
                chg_line = "—"
                chg_color = "#666"
            t3 = cd["t3_alpha"]
            prev = cd["prev_close"] if cd["prev_close"] > 0 else None

            spark_fig = _sparkline_figure(cd["closes"], prev)

            with st.container(border=True):
                top_l, top_r = st.columns([1, 1.15])
                with top_l:
                    st.markdown(
                        f'<p style="margin:0;font-family:Inter,sans-serif;font-weight:700;'
                        f'font-size:1rem;color:#e6edf3">{html.escape(sym)}</p>',
                        unsafe_allow_html=True,
                    )
                    st.caption("1-day")
                with top_r:
                    price_c, x_c = st.columns([1, 0.14], gap="small")
                    with price_c:
                        st.markdown(
                            f'<p style="text-align:right;margin:0;font-family:Roboto Mono,monospace;'
                            f'font-weight:600;font-size:0.95rem;color:#e6edf3">${price:,.2f}</p>',
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            f'<p style="text-align:right;margin:0;font-family:Roboto Mono,monospace;'
                            f'font-size:0.72rem;color:{chg_color};font-weight:500">'
                            f"{html.escape(chg_line)}</p>",
                            unsafe_allow_html=True,
                        )
                    with x_c:
                        if st.button(
                            "×",
                            key=f"ph_mtx_rm_{sym}_{row_idx}_{col_idx}",
                            help=f"Remove {sym} from watchlist",
                            type="tertiary",
                            width="content",
                        ):
                            wl = st.session_state.ph_watchlist
                            if sym in wl:
                                wl.remove(sym)
                            save_watchlist(wl)
                            st.rerun()

                if spark_fig:
                    st.plotly_chart(
                        spark_fig,
                        width="stretch",
                        key=f"ph_mtx_spark_{sym}_{row_idx}_{col_idx}",
                        config={"displayModeBar": False},
                    )
                else:
                    st.markdown(
                        '<p style="color:#666;font-size:0.78rem;margin:8px 0">'
                        "No intraday series (Yahoo).</p>",
                        unsafe_allow_html=True,
                    )

                st.markdown(
                    '<div style="border-top:1px solid rgba(255,255,255,0.06);'
                    'margin:4px 0 0 0;padding-top:6px"></div>',
                    unsafe_allow_html=True,
                )

                g_left, g_rest = st.columns([0.48, 0.52])
                with g_left:
                    st.plotly_chart(
                        _gauge_figure(t3),
                        width="stretch",
                        key=f"ph_mtx_gauge_{sym}_{row_idx}_{col_idx}",
                        config={"displayModeBar": False},
                    )
                with g_rest:
                    st.empty()

_inject_matrix_gauge_hover_tip()
