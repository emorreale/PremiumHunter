import html

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from etrade_market import get_equity_display_price, get_quote

if st.session_state.get("market") is None:
    st.info("Connect to E-Trade using the sidebar to get started.")
    st.stop()

market = st.session_state.market

# Plotly price chart height (peer/loser tables use the same pixel height)
PH_PRICE_CHART_HEIGHT = 400

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
