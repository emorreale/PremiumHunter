import os
import subprocess
import sys

_STREAMLIT_CHILD = "PREMIUMHUNTER_STREAMLIT_CHILD"

if __name__ == "__main__":
    # Hand off to Streamlit only for `python app.py`. Skip when Streamlit is already
    # running this file (`streamlit run` or our subprocess child) so we never call
    # Streamlit APIs without a ScriptRunContext (avoids the "bare mode" warning).
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

import datetime as dt

import streamlit as st

from etrade_auth import (
    clear_persisted_tokens,
    get_access_tokens,
    get_oauth,
    load_persisted_tokens,
    save_persisted_tokens,
)
from etrade_market import create_market_session, get_expiry_dates, get_option_chain, get_quote


st.set_page_config(page_title="PremiumHunter", page_icon="🎯", layout="wide")

st.title("🎯 Premium Hunter")
st.caption("Hunt for the best option premiums — Cash Secured Puts & Covered Calls")

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

# ── Main Content ────────────────────────────────────────────────────────────────

if st.session_state.market is None:
    st.info("Connect to E-Trade using the sidebar to get started.")
    st.stop()

market = st.session_state.market

col1, col2 = st.columns([1, 3])

with col1:
    ticker = st.text_input("Ticker Symbol", value="AAPL", max_chars=10).upper().strip()

if not ticker:
    st.warning("Enter a ticker symbol.")
    st.stop()

# Fetch underlying quote
try:
    quote = get_quote(market, ticker)
    all_data = quote.get("All", {})
    last_price = all_data.get("lastTrade", "N/A")
    with col2:
        st.metric(label=f"{ticker} Last Price", value=f"${last_price}")
except Exception as e:
    st.error(f"Could not fetch quote for {ticker}: {e}")
    st.stop()

st.divider()

# Fetch expiration dates
try:
    expiry_dates_raw = get_expiry_dates(market, ticker)
except Exception as e:
    st.error(f"Could not fetch expiration dates: {e}")
    st.stop()

if not expiry_dates_raw:
    st.warning(f"No options available for {ticker}.")
    st.stop()

expiry_options = []
for d in expiry_dates_raw:
    year = d.get("year", 0)
    month = d.get("month", 0)
    day = d.get("day", 0)
    try:
        date_obj = dt.date(int(year), int(month), int(day))
        expiry_options.append(date_obj)
    except (ValueError, TypeError):
        continue

col_exp, col_type = st.columns(2)

with col_exp:
    selected_expiry = st.selectbox(
        "Expiration Date",
        options=expiry_options,
        format_func=lambda d: d.strftime("%b %d, %Y"),
    )

with col_type:
    chain_type = st.selectbox(
        "Option Type",
        options=["Both", "CALL", "PUT"],
    )

chain_type_param = None if chain_type == "Both" else chain_type

# Fetch option chain
try:
    df = get_option_chain(
        market,
        ticker,
        expiry_date=selected_expiry,
        chain_type=chain_type_param,
    )
except Exception as e:
    st.error(f"Could not fetch option chain: {e}")
    st.stop()

if df.empty:
    st.warning("No option chain data returned.")
    st.stop()

st.subheader(f"Option Chain — {ticker} — {selected_expiry.strftime('%b %d, %Y')}")

# Split into calls and puts for a cleaner view
if chain_type == "Both":
    tab_calls, tab_puts = st.tabs(["Calls", "Puts"])
    calls_df = df[df["Type"] == "Call"].drop(columns=["Type"]).reset_index(drop=True)
    puts_df = df[df["Type"] == "Put"].drop(columns=["Type"]).reset_index(drop=True)

    with tab_calls:
        st.dataframe(calls_df, width="stretch", hide_index=True)
    with tab_puts:
        st.dataframe(puts_df, width="stretch", hide_index=True)
elif chain_type == "CALL":
    st.dataframe(
        df.drop(columns=["Type"]).reset_index(drop=True),
        width="stretch",
        hide_index=True,
    )
else:
    st.dataframe(
        df.drop(columns=["Type"]).reset_index(drop=True),
        width="stretch",
        hide_index=True,
    )
