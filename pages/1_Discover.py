import datetime as dt
import html
import math

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from etrade_market import (
    get_equity_display_price,
    get_expiry_dates,
    get_option_chain,
    get_quote,
)

if st.session_state.get("market") is None:
    st.info("Connect to E-Trade using the sidebar to get started.")
    st.stop()

market = st.session_state.market

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
def _cached_expiry_dates(_market_id, symbol: str) -> list[dict]:
    return get_expiry_dates(market, symbol)

@st.cache_data(ttl=120, show_spinner=False)
def _cached_option_chain(_market_id, symbol: str, expiry_iso: str):
    expiry_date = dt.date.fromisoformat(expiry_iso)
    return get_option_chain(market, symbol, expiry_date=expiry_date)

@st.cache_data(ttl=600, show_spinner=False)
def _cached_yf_info(symbol: str) -> dict:
    try:
        return yf.Ticker(symbol).info or {}
    except Exception:
        return {}

@st.cache_data(ttl=120, show_spinner=False)
def _cached_yf_history(symbol: str, period: str, interval: str) -> pd.DataFrame | None:
    try:
        h = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=False)
        return h if h is not None and not h.empty else None
    except Exception:
        return None

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


# ── Scoring helpers ──────────────────────────────────────────────────────────

def _dte_weight(dd: int) -> float:
    dd = max(int(dd), 1)
    if 14 <= dd <= 45:
        return 1.0
    if dd < 14:
        return float(dd / 14.0)
    return float(math.exp(-0.04 * (dd - 45)))

def _iv_to_decimal(iv) -> float:
    try:
        v = float(iv)
    except (TypeError, ValueError):
        v = 0.25
    if v > 1.5:
        v /= 100.0
    return min(max(v, 0.06), 2.0)

def _otm_pct(option_type: str, strike: float, spot: float) -> float:
    if spot <= 0 or strike <= 0:
        return 0.0
    if option_type == "Call":
        return max((strike - spot) / spot * 100.0, 0.0)
    return max((spot - strike) / spot * 100.0, 0.0)

def _gamma_penalty(dd: int) -> float:
    dd = max(dd, 1)
    return 1.0 if dd >= 30 else math.sqrt(30.0 / dd)

def _score_raw(r, dte_days: int, spot: float) -> float:
    y = max(float(r["Monthly Yield %"]), 0.0)
    if y <= 0:
        return 0.0
    strike = float(r["Strike"])
    otm = _otm_pct(r["Type"], strike, spot)
    time_scale = math.sqrt(max(dte_days, 1) / 30.0)
    adj_cushion = otm / time_scale if time_scale > 0 else otm
    iv = _iv_to_decimal(r.get("IV", 0))
    iv_scaled = iv * time_scale
    dte_w = _dte_weight(dte_days)
    gamma_p = _gamma_penalty(dte_days)
    numerator = y * dte_w * (1.0 + 0.12 * adj_cushion)
    denom = (iv_scaled ** 0.95) * gamma_p
    return float(12.0 * numerator / denom)

def _score_components(r, dte_days: int, spot: float) -> dict:
    y = max(float(r.get("Monthly Yield %", 0)), 0.0)
    income = min(y / 5.0 * 100, 100)
    strike = float(r.get("Strike", 0))
    otm = _otm_pct(r.get("Type", "Put"), strike, spot)
    safety = min(otm / 8.0 * 100, 100)
    dte_w = _dte_weight(dte_days)
    gamma_p = _gamma_penalty(dte_days)
    velocity = min((dte_w / gamma_p) * 100, 100)
    iv = _iv_to_decimal(r.get("IV", 0))
    volatility = max(0, min(100, (1.0 - iv) * 100))
    return {"Income": round(income), "Safety": round(safety),
            "Velocity": round(velocity), "Volatility": round(volatility)}

def _top_mean(raw, top_k=5):
    s = raw.fillna(0.0)
    if s.empty:
        return 0.0
    k = min(top_k, len(s))
    return float(s.nlargest(k).mean())

def _wheel_candidates(chain, spot):
    wc = (chain["Type"] == "Call") & (chain["Strike"] >= spot) if spot > 0 else (chain["Type"] == "Call")
    wp = (chain["Type"] == "Put") & (chain["Strike"] <= spot) if spot > 0 else (chain["Type"] == "Put")
    return chain[(chain["Monthly Yield %"] >= 0.5) & (chain["Strike"] > 0) & (wc | wp)]


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
_BENCH_UNIVERSE = (
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","V",
    "UNH","XOM","JNJ","WMT","PG","HD","BAC","DIS","NFLX","AMD","INTC",
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
        if "ph_watchlist" not in st.session_state:
            st.session_state.ph_watchlist = []
        _in_wl = ticker in st.session_state.ph_watchlist
        if st.button("★ Watching" if _in_wl else "☆ Watch", key="ph_fav_btn",
                     use_container_width=True):
            if _in_wl:
                st.session_state.ph_watchlist.remove(ticker)
            else:
                st.session_state.ph_watchlist.append(ticker)
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


# ── Fetch expirations & compute scores ───────────────────────────────────────

try:
    expiry_dates_raw = _cached_expiry_dates(_mkt_id, ticker)
except Exception:
    expiry_dates_raw = []
expiry_options = []
for d in expiry_dates_raw:
    if not isinstance(d, dict):
        continue
    try:
        expiry_options.append(dt.date(
            int(d.get("year", d.get("Year", 0))),
            int(d.get("month", d.get("Month", 0))),
            int(d.get("day", d.get("Day", 0)))))
    except (ValueError, TypeError):
        continue
expiry_options = sorted(set(expiry_options))
today = dt.date.today()
future = [d for d in expiry_options if d > today]
if future:
    expiry_options = future
if not expiry_options:
    st.warning(f"No options available for {ticker}.")
    st.stop()

_cutoff = today + dt.timedelta(days=45)
_rec_candidates = sorted([d for d in expiry_options if today <= d <= _cutoff])
_exp_raw_scores = []
for _exp in _rec_candidates:
    try:
        _chain = _cached_option_chain(_mkt_id, ticker, _exp.isoformat())
    except Exception:
        continue
    if _chain is None or _chain.empty:
        continue
    _chain = _chain.copy()
    _d = max((_exp - today).days, 1)
    _chain["Monthly Yield %"] = _chain.apply(
        lambda r, dd=_d: round((r["Bid"] / r["Strike"]) * (30 / dd) * 100, 2)
        if r["Strike"] > 0 else 0.0, axis=1)
    _cands = _wheel_candidates(_chain, current_price)
    _pc = _cands[_cands["Type"] == "Put"]
    _cc = _cands[_cands["Type"] == "Call"]
    _pr = _pc.apply(lambda row, dd=_d: _score_raw(row, dd, current_price), axis=1)
    _cr = _cc.apply(lambda row, dd=_d: _score_raw(row, dd, current_price), axis=1)
    _exp_raw_scores.append({"expiry": _exp, "put_raw": _top_mean(_pr), "call_raw": _top_mean(_cr)})

_all_raws = [e["put_raw"] for e in _exp_raw_scores] + [e["call_raw"] for e in _exp_raw_scores]
_global_min = min(_all_raws) if _all_raws else 0.0
_global_max = max(_all_raws) if _all_raws else 0.0
def _normalize(v):
    if _global_max <= _global_min:
        return 100.0 if v > 0 else 0.0
    return float(min(100.0, max(0.0, (v - _global_min) / (_global_max - _global_min) * 100.0)))
for e in _exp_raw_scores:
    e["put_score"] = _normalize(e["put_raw"])
    e["call_score"] = _normalize(e["call_raw"])
    e["overall"] = max(e["put_score"], e["call_score"])
_rec_best = max(_exp_raw_scores, key=lambda e: e["overall"]) if _exp_raw_scores else None


# ═══════════════════════════════════════════════════════════════════════════════
# HEADER BAR — At-a-Glance (sticky)
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown('<div class="sticky-header">', unsafe_allow_html=True)
h_left, h_center, h_right = st.columns([1.2, 1.5, 1.0])

# LEFT: Ticker + Sparkline
with h_left:
    hist = _cached_yf_history(ticker, "1mo", "1d")
    _spark_html = ""
    _price_change_html = ""
    if hist is not None and not hist.empty:
        hist = hist.sort_index()
        cur_m = yf_info.get("regularMarketPrice") or yf_info.get("currentPrice")
        ep = float(cur_m) if cur_m else float(hist["Close"].iloc[-1])
        sp = float(hist["Close"].iloc[0])
        chg_pct = ((ep - sp) / sp * 100) if sp else 0
        is_up = chg_pct >= 0
        _color = "#00ff88" if is_up else "#ff5252"
        _sign = "+" if is_up else ""

        closes = list(hist["Close"])
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            y=closes, mode="lines", line=dict(color=_color, width=1.5),
            showlegend=False, hoverinfo="skip"))
        fig.update_layout(
            height=45, margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(visible=False), yaxis=dict(visible=False))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        _price_change_html = (
            f'<span class="mono" style="font-size:1.3rem;font-weight:700">${ep:.2f}</span>'
            f'<span style="color:{_color};font-size:0.85rem;margin-left:8px">{_sign}{chg_pct:.1f}%</span>'
        )
    else:
        _price_change_html = f'<span class="mono" style="font-size:1.3rem;font-weight:700">${current_price:.2f}</span>'

    st.markdown(
        f'<div style="margin-top:-8px">'
        f'<span style="color:#94a3b8;font-size:0.75rem">{html.escape(_co)}</span><br>'
        f'<span class="mono" style="font-size:1.5rem;font-weight:800;color:#fff">{ticker}</span> '
        f'{_price_change_html}</div>',
        unsafe_allow_html=True)

# CENTER: Expiry + Hero scores
with h_center:
    selected_expiry = st.selectbox(
        "Expiry", options=expiry_options,
        format_func=lambda d: d.strftime("%b %d, %Y"),
        label_visibility="collapsed")
    dte = max((selected_expiry - today).days, 1)
    _cur = next((e for e in _exp_raw_scores if e["expiry"] == selected_expiry), None)
    put_score = _cur["put_score"] if _cur else 0.0
    call_score = _cur["call_score"] if _cur else 0.0

    def _hero_cls(v):
        if v >= 66:
            return "hero-score"
        if v >= 33:
            return "hero-score-yellow"
        return "hero-score-red"

    st.markdown(
        f"""<div style="display:flex;justify-content:center;align-items:center;gap:28px;margin-top:-4px">
        <div style="text-align:center">
            <div class="section-label">Puts</div>
            <div class="{_hero_cls(put_score)}" style="font-size:3rem">{put_score:.0f}</div>
        </div>
        <div style="width:1px;height:48px;background:rgba(255,255,255,0.08)"></div>
        <div style="text-align:center">
            <div class="section-label">Calls</div>
            <div class="{_hero_cls(call_score)}" style="font-size:3rem">{call_score:.0f}</div>
        </div>
        </div>
        <div style="text-align:center;font-size:0.65rem;color:#555;margin-top:2px">{dte}d to expiry</div>""",
        unsafe_allow_html=True)

# RIGHT: Recommended + watch status
with h_right:
    if _rec_best:
        st.markdown(
            f'<div class="section-label" style="margin-bottom:4px">Best Expiry</div>'
            f'<div class="mono" style="font-size:1rem;font-weight:600;color:#00bdff">'
            f'{_rec_best["expiry"].strftime("%b %d")}</div>'
            f'<div style="font-size:0.7rem;color:#555">Score {_rec_best["overall"]:.0f}/100</div>',
            unsafe_allow_html=True)
    wl = st.session_state.get("ph_watchlist", [])
    if ticker in wl:
        st.markdown('<div style="margin-top:6px"><span class="badge badge-green">WATCHING</span></div>',
                    unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# GRADIENT GAUGE ARCS — directly under hero scores
# ═══════════════════════════════════════════════════════════════════════════════

def _gauge_arc(value, label):
    """Plotly half-circle gauge with gradient needle."""
    if value >= 66:
        bar_color = "#00ff88"
        text = "Strong"
    elif value >= 33:
        bar_color = "#ffc107"
        text = "Moderate"
    else:
        bar_color = "#ff5252"
        text = "Weak"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        number=dict(font=dict(size=22, color="#e0e0e0", family="Roboto Mono"),
                    suffix="", valueformat=".0f"),
        gauge=dict(
            axis=dict(range=[0, 100], tickwidth=0, tickcolor="rgba(0,0,0,0)",
                      tickfont=dict(size=1, color="rgba(0,0,0,0)")),
            bar=dict(color=bar_color, thickness=0.3),
            bgcolor="rgba(255,255,255,0.04)",
            borderwidth=0,
            steps=[
                dict(range=[0, 33], color="rgba(255,82,82,0.12)"),
                dict(range=[33, 66], color="rgba(255,193,7,0.12)"),
                dict(range=[66, 100], color="rgba(0,255,136,0.12)"),
            ],
            threshold=dict(line=dict(color="#fff", width=2), thickness=0.75, value=value),
        ),
    ))
    fig.update_layout(
        height=140, margin=dict(l=20, r=20, t=30, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter"),
        annotations=[dict(
            text=f'<b>{text}</b>', x=0.5, y=0.15,
            font=dict(size=13, color=bar_color, family="Inter"),
            showarrow=False, xref="paper", yref="paper")],
    )
    return fig

_g1, _g2 = st.columns(2)
with _g1:
    st.markdown('<div class="section-label" style="text-align:center">Put vs Market</div>',
                unsafe_allow_html=True)
    st.plotly_chart(_gauge_arc(put_score, "Put"), use_container_width=True,
                    config={"displayModeBar": False})
with _g2:
    st.markdown('<div class="section-label" style="text-align:center">Call vs Market</div>',
                unsafe_allow_html=True)
    st.plotly_chart(_gauge_arc(call_score, "Call"), use_container_width=True,
                    config={"displayModeBar": False})


# ═══════════════════════════════════════════════════════════════════════════════
# BODY — 60/40 Split: Option Chain | Score Breakdown
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("---")

try:
    df = _cached_option_chain(_mkt_id, ticker, selected_expiry.isoformat())
except Exception as e:
    st.error(f"Could not fetch option chain: {e}")
    st.stop()
if df is None or df.empty:
    st.warning("No option chain data for this expiration.")
    st.stop()

df["Monthly Yield %"] = df.apply(
    lambda r: round((r["Bid"] / r["Strike"]) * (30 / dte) * 100, 2)
    if r["Strike"] > 0 else 0.0, axis=1)
drop_cols = ["Open Interest", "In The Money", "Last"]
df = df.drop(columns=[c for c in drop_cols if c in df.columns])
_pct = pd.Series(index=df.index, dtype="float64")
_ok = (df["Strike"] > 0) & (current_price > 0)
_pct.loc[_ok] = ((current_price - df.loc[_ok, "Strike"]) / df.loc[_ok, "Strike"] * 100).round(2)
df.insert(df.columns.get_loc("Strike") + 1, "spot vs strike %", _pct)

calls_df = df[df["Type"] == "Call"].drop(columns=["Type"])
if current_price > 0:
    calls_df = calls_df[calls_df["Strike"] >= current_price]
calls_df = calls_df[calls_df["Monthly Yield %"] >= 0.5].sort_values(
    "Monthly Yield %", ascending=False).reset_index(drop=True)

puts_df = df[df["Type"] == "Put"].drop(columns=["Type"])
if current_price > 0:
    puts_df = puts_df[puts_df["Strike"] <= current_price]
puts_df = puts_df[puts_df["Monthly Yield %"] >= 0.5].sort_values(
    "Monthly Yield %", ascending=False).reset_index(drop=True)


def _badge(text, cls):
    return f'<span class="badge badge-{cls}">{text}</span>'

def _yield_pill(y):
    if y >= 3.0:
        bg, fg = "rgba(0,255,136,0.15)", "#00ff88"
    elif y >= 1.5:
        bg, fg = "rgba(255,193,7,0.12)", "#ffc107"
    else:
        bg, fg = "rgba(255,255,255,0.06)", "#94a3b8"
    return f'<span class="yield-pill" style="background:{bg};color:{fg}">{y:.2f}%</span>'

def _strike_badges(row):
    tags = []
    y = float(row.get("Monthly Yield %", 0))
    iv = _iv_to_decimal(row.get("IV", 0))
    if y >= 3.0:
        tags.append(_badge("TARGET YIELD", "green"))
    if iv > 0.6:
        tags.append(_badge("HIGH IV", "red"))
    if dte <= 14:
        tags.append(_badge("HIGH GAMMA", "yellow"))
    svs = row.get("spot vs strike %", None)
    if svs is not None and not (isinstance(svs, float) and math.isnan(svs)):
        if abs(float(svs)) >= 5:
            tags.append(_badge("DEEP OTM", "blue"))
    return " ".join(tags)

def _render_strike_row(r, idx, side):
    badges = _strike_badges(r)
    pill = _yield_pill(float(r["Monthly Yield %"]))
    iv_val = _iv_to_decimal(r.get("IV", 0))
    return f"""<div class="strike-row" id="strike-{side}-{idx}">
        <div>
            <span class="mono" style="font-weight:700;font-size:1rem">${r['Strike']:.2f}</span>
            {badges}
        </div>
        <div style="text-align:right;display:flex;align-items:center;gap:14px">
            {pill}
            <span class="dim" style="font-size:0.75rem">Bid ${r['Bid']:.2f}</span>
            <span class="dim" style="font-size:0.75rem">IV {iv_val:.0%}</span>
        </div>
    </div>"""


col_chain, col_breakdown = st.columns([3, 2])

with col_chain:
    tab_calls, tab_puts = st.tabs(["Calls", "Puts"])
    _TOP_N = 3

    with tab_calls:
        if calls_df.empty:
            st.caption("No covered call strikes above spot with yield >= 0.5%.")
        else:
            for i in range(min(_TOP_N, len(calls_df))):
                st.markdown(_render_strike_row(calls_df.iloc[i], i, "call"),
                            unsafe_allow_html=True)
            if len(calls_df) > _TOP_N:
                with st.expander(f"View all {len(calls_df)} call strikes"):
                    st.dataframe(calls_df, width="stretch", hide_index=True)

    with tab_puts:
        if puts_df.empty:
            st.caption("No cash secured put strikes below spot with yield >= 0.5%.")
        else:
            for i in range(min(_TOP_N, len(puts_df))):
                st.markdown(_render_strike_row(puts_df.iloc[i], i, "put"),
                            unsafe_allow_html=True)
            if len(puts_df) > _TOP_N:
                with st.expander(f"View all {len(puts_df)} put strikes"):
                    st.dataframe(puts_df, width="stretch", hide_index=True)

# RIGHT: Score breakdown (radar + component detail)
with col_breakdown:
    st.markdown('<div class="section-label" style="margin-bottom:8px">Score Breakdown</div>',
                unsafe_allow_html=True)

    _cands_for_radar = _wheel_candidates(df, current_price)
    def _avg_components(cands_df, side):
        sub = cands_df[cands_df["Type"] == side]
        if sub.empty:
            return {"Income": 0, "Safety": 0, "Velocity": 0, "Volatility": 0}
        comps = sub.head(5).apply(lambda r: _score_components(r, dte, current_price), axis=1)
        comp_df = pd.DataFrame(list(comps))
        return {k: round(comp_df[k].mean()) for k in comp_df.columns}

    put_comp = _avg_components(_cands_for_radar, "Put")
    call_comp = _avg_components(_cands_for_radar, "Call")

    def _make_radar(components, color_rgb):
        cats = list(components.keys()) + [list(components.keys())[0]]
        vals = list(components.values()) + [list(components.values())[0]]
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=vals, theta=cats, fill="toself",
            fillcolor=f"rgba({color_rgb},0.12)",
            line=dict(color=f"rgb({color_rgb})", width=2)))
        fig.update_layout(
            polar=dict(
                bgcolor="rgba(0,0,0,0)",
                radialaxis=dict(visible=True, range=[0, 100],
                                gridcolor="rgba(255,255,255,0.05)",
                                tickfont=dict(size=8, color="#444")),
                angularaxis=dict(gridcolor="rgba(255,255,255,0.05)",
                                 tickfont=dict(size=10, color="#94a3b8", family="Inter"))),
            showlegend=False, height=210,
            margin=dict(l=40, r=40, t=20, b=10),
            paper_bgcolor="rgba(0,0,0,0)")
        return fig

    _r_tabs = st.tabs(["Put Breakdown", "Call Breakdown"])
    with _r_tabs[0]:
        st.plotly_chart(_make_radar(put_comp, "0,255,136"),
                        use_container_width=True, config={"displayModeBar": False})
        for k, v in put_comp.items():
            _bar_c = "#00ff88" if v >= 60 else "#ffc107" if v >= 30 else "#ff5252"
            st.markdown(
                f'<div style="display:flex;align-items:center;margin-bottom:4px">'
                f'<span style="width:80px;font-size:0.75rem;color:#94a3b8">{k}</span>'
                f'<div style="flex:1;height:6px;background:rgba(255,255,255,0.04);border-radius:3px;overflow:hidden">'
                f'<div style="width:{v}%;height:100%;background:{_bar_c};border-radius:3px"></div>'
                f'</div>'
                f'<span class="mono" style="width:32px;text-align:right;font-size:0.75rem;color:#888;margin-left:6px">{v}</span>'
                f'</div>', unsafe_allow_html=True)

    with _r_tabs[1]:
        st.plotly_chart(_make_radar(call_comp, "0,189,255"),
                        use_container_width=True, config={"displayModeBar": False})
        for k, v in call_comp.items():
            _bar_c = "#00bdff" if v >= 60 else "#ffc107" if v >= 30 else "#ff5252"
            st.markdown(
                f'<div style="display:flex;align-items:center;margin-bottom:4px">'
                f'<span style="width:80px;font-size:0.75rem;color:#94a3b8">{k}</span>'
                f'<div style="flex:1;height:6px;background:rgba(255,255,255,0.04);border-radius:3px;overflow:hidden">'
                f'<div style="width:{v}%;height:100%;background:{_bar_c};border-radius:3px"></div>'
                f'</div>'
                f'<span class="mono" style="width:32px;text-align:right;font-size:0.75rem;color:#888;margin-left:6px">{v}</span>'
                f'</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET PEERS (compact, bottom)
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("---")
_p_left, _p_right = st.columns(2)
with _p_left:
    st.markdown('<div class="section-label">Sector Peers</div>', unsafe_allow_html=True)
    peer_src = _SECTOR_PEERS.get(sector, _DEFAULT_SECTOR_PEERS)
    syms = tuple(s for s in peer_src if s != ticker)[:10]
    mov = _yf_day_moves(syms)
    if not mov.empty:
        mov = mov.sort_values("Symbol").reset_index(drop=True)
        peer_df_key = f"ph_peer_df_{ticker}_0"
        st.dataframe(mov, hide_index=True, width="stretch",
                     height=min(220, 36 + 28 * len(mov)),
                     on_select="rerun", selection_mode="single-row", key=peer_df_key,
                     column_config={"Last": st.column_config.NumberColumn(format="$%.2f"),
                                    "Δ%": st.column_config.NumberColumn(format="%.2f")})
        picked = st.session_state.get(peer_df_key)
        rows = (picked or {}).get("selection", {}).get("rows") or []
        if rows and 0 <= rows[0] < len(mov):
            new_sym = str(mov.iloc[rows[0]]["Symbol"]).upper().strip()
            if new_sym and new_sym != ticker:
                st.session_state.ph_ticker_pending = new_sym
                st.rerun()
    else:
        st.caption("No peer data.")

with _p_right:
    st.markdown('<div class="section-label">Large-Cap Losers</div>', unsafe_allow_html=True)
    l_syms = tuple(s for s in _LOSER_UNIVERSE if s != ticker)
    l_mov = _yf_day_moves(l_syms)
    if not l_mov.empty:
        l_mov = l_mov.nsmallest(8, "Δ%").reset_index(drop=True)
        loser_df_key = f"ph_peer_df_{ticker}_1"
        st.dataframe(l_mov, hide_index=True, width="stretch",
                     height=min(220, 36 + 28 * len(l_mov)),
                     on_select="rerun", selection_mode="single-row", key=loser_df_key,
                     column_config={"Last": st.column_config.NumberColumn(format="$%.2f"),
                                    "Δ%": st.column_config.NumberColumn(format="%.2f")})
        picked = st.session_state.get(loser_df_key)
        rows = (picked or {}).get("selection", {}).get("rows") or []
        if rows and 0 <= rows[0] < len(l_mov):
            new_sym = str(l_mov.iloc[rows[0]]["Symbol"]).upper().strip()
            if new_sym and new_sym != ticker:
                st.session_state.ph_ticker_pending = new_sym
                st.rerun()
    else:
        st.caption("No loser data.")
