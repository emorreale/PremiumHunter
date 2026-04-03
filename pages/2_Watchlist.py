import streamlit as st

if st.session_state.get("market") is None:
    st.info("Connect to E-Trade using the sidebar to get started.")
    st.stop()

if "ph_watchlist" not in st.session_state:
    st.session_state.ph_watchlist = []

watchlist = st.session_state.ph_watchlist

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

for sym in list(watchlist):
    col_sym, col_btn = st.columns([8, 1])
    with col_sym:
        st.markdown(
            f"""<div class="glass-card" style="padding:0.7rem 1.2rem;margin-bottom:6px">
            <span style="font-family:'Roboto Mono',monospace;font-size:1.1rem;font-weight:600">{sym}</span>
            </div>""",
            unsafe_allow_html=True,
        )
    with col_btn:
        if st.button("✕", key=f"wl_remove_{sym}", help=f"Remove {sym}"):
            watchlist.remove(sym)
            st.rerun()
