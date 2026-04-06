import os
import subprocess
import sys

_STREAMLIT_CHILD = "PREMIUMHUNTER_STREAMLIT_CHILD"

if __name__ == "__main__":
    _already_streamlit = "streamlit" in sys.modules
    _our_child = os.environ.get(_STREAMLIT_CHILD) == "1"
    if not _already_streamlit and not _our_child:
        _env = os.environ.copy()
        _env[_STREAMLIT_CHILD] = "1"
        rc = subprocess.call(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                __file__,
                "--server.headless",
                "true",
            ],
            env=_env,
        )
        raise SystemExit(rc)

import html as _html

import streamlit as st
import yfinance as yf

from etrade_auth import (
    IS_SANDBOX,
    clear_persisted_tokens,
    fetch_latest_tokens_from_postgres,
    get_access_tokens,
    get_oauth,
    load_persisted_tokens,
    save_persisted_tokens,
)
from etrade_market import create_market_session, probe_etrade_tokens
from watchlist_persist import ensure_session_watchlist


st.set_page_config(page_title="PremiumHunter", page_icon="assets/logo_icon.svg", layout="wide")
ensure_session_watchlist()

st.logo(
    "assets/logo.svg",
    size="large",
    icon_image="assets/logo_icon.svg",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Roboto+Mono:wght@400;500;700&display=swap');

/* ── Base ────────────────────────────────────────────────────── */
.stApp {
    background-color: #0e1117;
    font-family: 'Inter', sans-serif;
}
html, body {
    font-family: 'Inter', sans-serif;
}
/*
 * Do NOT use [class*="st-"] { font-family: Inter !important } — it overrides
 * Streamlit's Material Symbols font so sidebar/header icons show as raw text
 * (e.g. "keyboard_double_arrow_...").
 */
span[data-testid="stIconMaterial"] {
    font-family: "Material Symbols Rounded" !important;
    font-weight: normal !important;
    font-style: normal !important;
    font-feature-settings: "liga" !important;
    -webkit-font-feature-settings: "liga" !important;
    letter-spacing: normal !important;
    text-transform: none !important;
    white-space: nowrap !important;
}
/* Inter for primary reading surfaces (Streamlit chrome keeps its default fonts) */
section[data-testid="stMain"],
section[data-testid="stSidebar"] .block-container {
    font-family: 'Inter', sans-serif;
}
section[data-testid="stMain"] [data-testid="stMarkdownContainer"],
section[data-testid="stMain"] [data-testid="stHeading"],
section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
    font-family: 'Inter', sans-serif !important;
}
code, .stDataFrame td, .stDataFrame th,
.stMetric [data-testid="stMetricValue"] {
    font-family: 'Roboto Mono', monospace !important;
}

/* ── Glassmorphism cards ─────────────────────────────────────── */
.glass-card {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 4px 30px rgba(0,0,0,0.5);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
}

/* Analyzer matrix: watchlist remove — text-style × (no pill box) */
section[data-testid="stMain"] div[class*="st-key-ph_mtx_rm_"] {
    display: flex !important;
    justify-content: flex-end !important;
    align-items: flex-start !important;
}
section[data-testid="stMain"] div[class*="st-key-ph_mtx_rm_"] button {
    padding: 0 2px !important;
    min-height: 1.1rem !important;
    font-weight: 300 !important;
    font-size: 1.2rem !important;
    line-height: 1 !important;
    color: #7d8590 !important;
}
section[data-testid="stMain"] div[class*="st-key-ph_mtx_rm_"] button:hover {
    color: #f85149 !important;
}

/* ── Hero score with gradient glow ───────────────────────────── */
.hero-score {
    font-family: 'Roboto Mono', monospace;
    font-size: 4rem;
    font-weight: 800;
    background: linear-gradient(45deg, #00ff88, #00bdff);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    text-align: center;
    line-height: 1;
}
.hero-score-red {
    font-family: 'Roboto Mono', monospace;
    font-size: 4rem;
    font-weight: 800;
    background: linear-gradient(45deg, #ff5252, #ff9800);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    text-align: center;
    line-height: 1;
}
.hero-score-yellow {
    font-family: 'Roboto Mono', monospace;
    font-size: 4rem;
    font-weight: 800;
    background: linear-gradient(45deg, #ffc107, #ff9800);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    text-align: center;
    line-height: 1;
}

/* ── Badge pills ─────────────────────────────────────────────── */
.badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 999px;
    font-size: 0.68rem;
    font-weight: 600;
    letter-spacing: 0.04em;
    margin-left: 6px;
    vertical-align: middle;
}
.badge-green  { background: rgba(0,255,136,0.12); color: #00ff88; }
.badge-yellow { background: rgba(255,193,7,0.15); color: #ffc107; }
.badge-red    { background: rgba(255,82,82,0.15); color: #ff5252; }
.badge-blue   { background: rgba(0,189,255,0.12); color: #00bdff; }

/* yield pill (used in option chain rows) */
.yield-pill {
    display: inline-block;
    padding: 3px 12px;
    border-radius: 999px;
    font-family: 'Roboto Mono', monospace;
    font-size: 0.82rem;
    font-weight: 600;
}

/* ── Data table overrides ────────────────────────────────────── */
table {
    border-collapse: separate !important;
    border-spacing: 0 8px !important;
    background-color: transparent !important;
}
thead th {
    background-color: #1a1c23 !important;
    color: #94a3b8 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-size: 0.75rem;
    border: none !important;
}
tbody tr {
    background-color: #161b22 !important;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
tbody tr:hover {
    transform: scale(1.01);
    background-color: #21262d !important;
    box-shadow: 0 0 15px rgba(0,255,136,0.1);
}

/* ── Streamlit widget polish ─────────────────────────────────── */
.stDataFrame { border-radius: 10px; overflow: hidden; }
div[data-testid="stMetric"] {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 0.8rem 1rem;
    box-shadow: 0 2px 12px rgba(0,0,0,0.3);
}
div[data-testid="stExpander"] {
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
}

/* ── Sticky header ───────────────────────────────────────────── */
.sticky-header {
    position: sticky;
    top: 0;
    z-index: 999;
    background: rgba(14,17,23,0.92);
    backdrop-filter: blur(10px);
    padding: 8px 0;
    margin: -1rem -1rem 0.5rem -1rem;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}

/* ── Strike row cards ────────────────────────────────────────── */
.strike-row {
    background: #161b22;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px;
    padding: 0.65rem 1rem;
    margin-bottom: 6px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    transition: transform 0.15s ease, box-shadow 0.15s ease;
}
.strike-row:hover {
    transform: translateY(-1px);
    box-shadow: 0 0 18px rgba(0,255,136,0.08);
    border-color: rgba(0,255,136,0.2);
}

/* ── Label helpers ───────────────────────────────────────────── */
.section-label {
    font-size: 0.7rem;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-weight: 600;
}
.dim { color: #555; }
.mono { font-family: 'Roboto Mono', monospace; }

/* ── Top header bar ──────────────────────────────────────────── */
.stApp > header,
header[data-testid="stHeader"] {
    font-family: 'Inter', sans-serif !important;
    background: #161b22 !important;
    border-bottom: 1px solid rgba(255,255,255,0.08) !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.40) !important;
}

/* Wordmark + icon in header (wide SVG) */
header[data-testid="stHeader"] img[data-testid="stLogo"] {
    max-height: 30px !important;
    width: auto !important;
}

/* Breathing room below app header so first content (index tape) is not clipped */
section[data-testid="stMain"] > div.block-container {
    padding-top: 2.65rem !important;
    overflow: visible !important;
}
/* Tape row: padding only above; no bottom pad so the strip sits tight on the divider */
section[data-testid="stMain"] [data-testid="stHorizontalBlock"]:has([data-testid="stColumn"] [class*="st-key-ph_idx_pick_"]) {
    padding-top: 0.55rem !important;
    padding-bottom: 0 !important;
}

/* ── Index tape: st-key-<key> on widget; stColumn (not "column") is the flex child wrapper ─ */
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]),
section[data-testid="stMain"] [data-testid="column"]:has([class*="st-key-ph_idx_pick_"]) {
    position: relative !important;
}
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"],
section[data-testid="stMain"] [data-testid="column"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"] {
    position: absolute !important;
    inset: 0 !important;
    z-index: 2 !important;
    margin: 0 !important;
    height: auto !important;
}
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"],
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"] *,
section[data-testid="stMain"] [data-testid="column"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"],
section[data-testid="stMain"] [data-testid="column"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"] * {
    background: transparent !important;
    background-color: transparent !important;
    box-shadow: none !important;
}
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"] button,
section[data-testid="stMain"] [data-testid="column"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"] button {
    width: 100% !important;
    height: 100% !important;
    min-height: 0 !important;
    padding: 0 !important;
    margin: 0 !important;
    border: none !important;
    background: transparent !important;
    box-shadow: none !important;
    opacity: 0 !important;
    cursor: pointer !important;
}
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"] button:focus-visible,
section[data-testid="stMain"] [data-testid="column"]:has([class*="st-key-ph_idx_pick_"]) [class*="st-key-ph_idx_pick_"] button:focus-visible {
    opacity: 0.12 !important;
    outline: 1px solid rgba(88, 166, 255, 0.5) !important;
}
/* Invisible pick button is position:absolute — kill the widget row’s layout height so it doesn’t add a gap under the tape */
section[data-testid="stMain"] [data-testid="element-container"]:has([class*="st-key-ph_idx_pick_"]) {
    margin-bottom: 0 !important;
    height: 0 !important;
    min-height: 0 !important;
    padding: 0 !important;
    overflow: visible !important;
}
/* Collapse Streamlit’s default vertical gaps between stacked widgets in each tape cell */
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]) [data-testid="element-container"] {
    margin-top: 0 !important;
    margin-bottom: 0 !important;
}
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]) [data-testid="stVerticalBlock"] {
    gap: 0 !important;
    align-items: flex-start !important;
}
/* st.html in tape cells: no extra Streamlit spacing */
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]) .stHtml,
section[data-testid="stMain"] [data-testid="stColumn"]:has([class*="st-key-ph_idx_pick_"]) [data-testid="stHtml"] {
    margin: 0 !important;
    padding: 0 !important;
}

/* ── Top nav page links ──────────────────────────────────────── */
header nav a,
header [data-testid="stSidebarNav"] a {
    font-family: 'Inter', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.82rem !important;
    letter-spacing: 0.04em !important;
    text-transform: uppercase !important;
    color: #8b949e !important;
    text-decoration: none !important;
    padding: 0.35rem 0.85rem !important;
    border-radius: 6px !important;
    transition: color 0.15s ease, background 0.15s ease !important;
}
header nav a:hover,
header [data-testid="stSidebarNav"] a:hover {
    color: #f0f6fc !important;
    background: rgba(255,255,255,0.06) !important;
}
header nav a[aria-current="page"],
header [data-testid="stSidebarNav"] a[aria-current="page"] {
    color: #00ff88 !important;
    background: rgba(0,255,136,0.10) !important;
}
</style>
""", unsafe_allow_html=True)

if IS_SANDBOX:
    st.warning(
        "**Sandbox mode** — The E*Trade sandbox returns static sample data (GOOG from 2012) "
        "for all tickers. Prices, expirations, and option chains will not reflect real market data. "
        "To use live data, request **production API keys** from E*Trade and set "
        "`ETRADE_SANDBOX=False` in your `.env` file."
    )

# ── Authentication ──────────────────────────────────────────────────────────────

if "tokens" not in st.session_state:
    st.session_state.tokens = None
if "oauth" not in st.session_state:
    st.session_state.oauth = None
if "market" not in st.session_state:
    st.session_state.market = None
if "_disk_tokens_tried" not in st.session_state:
    st.session_state._disk_tokens_tried = False

if st.session_state.tokens is None and not st.session_state._disk_tokens_tried:
    st.session_state._disk_tokens_tried = True
    saved = load_persisted_tokens()
    if saved:
        st.session_state.tokens = saved
        st.session_state.market = create_market_session(saved)

with st.sidebar:
    st.header("E-Trade Authentication")

    if st.session_state.tokens is None:
        if st.button("Connect to E-Trade"):
            db_tokens = fetch_latest_tokens_from_postgres()
            if db_tokens:
                if probe_etrade_tokens(db_tokens):
                    save_persisted_tokens(db_tokens)
                    st.session_state.tokens = db_tokens
                    st.session_state.market = create_market_session(db_tokens)
                    st.session_state.oauth = None
                    st.rerun()
                st.warning(
                    "Tokens in the database were found, but E*Trade did not accept them. "
                    "Use manual authorization below."
                )
            try:
                oauth, auth_url = get_oauth()
                st.session_state.oauth = oauth
                st.session_state.auth_url = auth_url
            except Exception as e:
                st.error(f"Failed to start OAuth: {e}")

        if st.session_state.oauth is not None:
            st.info("1. Click the link below to authorize PremiumHunter")
            st.markdown(f"[Authorize on E-Trade]({st.session_state.auth_url})")
            verifier = st.text_input("2. Paste the verification code here:")
            if verifier:
                try:
                    tokens = get_access_tokens(st.session_state.oauth, verifier)
                    save_persisted_tokens(tokens)
                    st.session_state.tokens = tokens
                    st.session_state.market = create_market_session(tokens)
                    st.rerun()
                except Exception as e:
                    st.error(f"Authentication failed: {e}")
    else:
        st.success("Connected to E-Trade")
        st.caption("Session is saved on this machine; refresh keeps you signed in.")
        if st.button("Disconnect"):
            clear_persisted_tokens()
            st.session_state.tokens = None
            st.session_state.oauth = None
            st.session_state.market = None
            st.session_state._disk_tokens_tried = True
            st.rerun()

# ── Page navigation ─────────────────────────────────────────────────────────────

discover_page = st.Page("pages/1_Discover.py", title="Discover", default=True)
analyzer_page = st.Page(
    "pages/2_Analyzer.py",
    title="Analyzer",
    url_path="analyzer",
)

pg = st.navigation([discover_page, analyzer_page], position="top")

# ── Market index ticker tape (Discover only; hidden on Analyzer) ───────────
# Yahoo symbols drive the tape numbers; E*Trade needs plain equity/ETF tickers
# for quotes and option chains (e.g. ^GSPC → SPY). Index picks use st.button
# (tertiary) so the URL does not change — no full browser navigation.

_INDEX_ROWS: list[dict[str, str]] = [
    {"label": "S&P 500", "yahoo": "^GSPC", "trade": "SPY"},
    {"label": "Dow 30", "yahoo": "^DJI", "trade": "DIA"},
    {"label": "Nasdaq", "yahoo": "^IXIC", "trade": "QQQ"},
    {"label": "Russell 2000", "yahoo": "^RUT", "trade": "IWM"},
    {"label": "VIX", "yahoo": "^VIX", "trade": "VXX"},
    {"label": "Gold", "yahoo": "GC=F", "trade": "GLD"},
    {"label": "Bitcoin USD", "yahoo": "BTC-USD", "trade": "IBIT"},
]


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_indices() -> list[dict]:
    results = []
    for row in _INDEX_ROWS:
        label = row["label"]
        ysym = row["yahoo"]
        trade = row["trade"]
        try:
            info = yf.Ticker(ysym).fast_info
            price = float(info.get("lastPrice", 0) or info.get("last_price", 0))
            prev = float(info.get("previousClose", 0) or info.get("previous_close", 0))
            chg = price - prev
            chg_pct = (chg / prev * 100) if prev else 0.0
            results.append(
                dict(
                    label=label,
                    yahoo=ysym,
                    trade_sym=trade,
                    price=price,
                    chg=chg,
                    pct=chg_pct,
                )
            )
        except Exception:
            results.append(
                dict(
                    label=label,
                    yahoo=ysym,
                    trade_sym=trade,
                    price=0,
                    chg=0,
                    pct=0,
                )
            )
    return results


if pg.url_path != "analyzer":
    _idx_data = _fetch_indices()

    _tape_cols = st.columns(len(_idx_data))
    for i, ix in enumerate(_idx_data):
        with _tape_cols[i]:
            up = ix["chg"] >= 0
            color = "#3fb950" if up else "#f85149"
            sign = "+" if up else ""
            lbl = _html.escape(ix["label"])
            st.html(
                f'<div style="line-height:1;margin:0;padding:0.42rem 0 0 0">'
                f'<div style="color:#58a6ff;font-family:Inter,sans-serif;'
                f'font-size:0.95rem;font-weight:600;line-height:1.2;margin:0 0 2px 0">{lbl}</div>'
                f'<div style="font-family:Roboto Mono,monospace;font-size:0.86rem;'
                f'font-weight:500;color:#c9d1d9;white-space:nowrap;margin:0;line-height:1.25">'
                f'{ix["price"]:,.2f}</div>'
                f'<div style="font-family:Roboto Mono,monospace;font-size:0.78rem;'
                f'font-weight:500;color:{color};white-space:nowrap;margin:0;line-height:1.25">'
                f'{sign}{ix["chg"]:,.2f} {sign}{ix["pct"]:.2f}%</div>'
                f'</div>'
            )
            if st.button(
                "\u200b",
                key=f"ph_idx_pick_{i}",
                type="tertiary",
                use_container_width=True,
                help=f"Load {ix['trade_sym']} (tracks {ix['label']})",
            ):
                st.session_state.ph_ticker_pending = ix["trade_sym"]
                st.session_state.ph_ticker = ix["trade_sym"]
                st.rerun()

    st.markdown(
        '<div style="border-bottom:1px solid rgba(255,255,255,0.06);'
        'margin:0"></div>',
        unsafe_allow_html=True,
    )

pg.run()
