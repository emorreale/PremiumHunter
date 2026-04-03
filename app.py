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

import streamlit as st

from etrade_auth import (
    IS_SANDBOX,
    clear_persisted_tokens,
    get_access_tokens,
    get_oauth,
    load_persisted_tokens,
    save_persisted_tokens,
)
from etrade_market import create_market_session


st.set_page_config(page_title="PremiumHunter", page_icon="🎯", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Roboto+Mono:wght@400;500;700&display=swap');

/* ── Base ────────────────────────────────────────────────────── */
.stApp {
    background-color: #0e1117;
    font-family: 'Inter', sans-serif;
}
html, body, [class*="st-"] {
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
</style>
""", unsafe_allow_html=True)

st.title("🎯 Premium Hunter")
st.caption("Hunt for the best option premiums — Cash Secured Puts & Covered Calls")
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

discover_page = st.Page("pages/1_Discover.py", title="Discover", icon="🔍", default=True)
watchlist_page = st.Page("pages/2_Watchlist.py", title="Watchlist", icon="⭐")

pg = st.navigation([discover_page, watchlist_page])
pg.run()
