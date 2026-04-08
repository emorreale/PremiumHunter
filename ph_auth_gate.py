"""
Streamlit login gate (streamlit-authenticator). Configure users in .streamlit/secrets.toml.
No self-service registration; only listed users may sign in.
"""

from __future__ import annotations

import streamlit as st
import streamlit_authenticator as stauth


def _build_credentials() -> dict:
    auth = st.secrets.get("authenticator")
    if not auth:
        raise KeyError(
            "Missing [authenticator] in .streamlit/secrets.toml — add [authenticator] and users there."
        )
    users = auth.get("users")
    if not isinstance(users, list) or not users:
        raise ValueError("authenticator.users must be a non-empty list in secrets.toml")
    usernames: dict[str, dict] = {}
    for u in users:
        if not isinstance(u, dict):
            continue
        un = str(u.get("username", "")).strip()
        if not un:
            continue
        pw = u.get("password")
        if pw is None or str(pw).strip() == "":
            continue
        usernames[un] = {
            "email": str(u.get("email", "") or f"{un}@local")[:200],
            "name": str(u.get("name", un))[:200],
            "password": pw,
        }
    if not usernames:
        raise ValueError("No valid usernames in authenticator.users")
    return {"usernames": usernames}


def require_login() -> stauth.Authenticate:
    """
    Call once from app.py right after st.set_page_config. Sets ph_watchlist_owner and stops
    until the user is authenticated. Returns Authenticator for sidebar logout.
    """
    try:
        credentials = _build_credentials()
        cfg = st.secrets["authenticator"]
        cookie_name = str(cfg.get("cookie_name", "premiumhunter_auth"))
        cookie_key = str(cfg["cookie_key"])
        if len(cookie_key) < 16:
            raise ValueError("authenticator.cookie_key must be at least 16 characters")
    except Exception as e:
        st.error(f"Authentication configuration error: {e}")
        st.stop()

    # One Authenticate only: each instance registers extra_streamlit_components.CookieManager
    # with the same default Streamlit key ("init") — a second instance causes
    # StreamlitDuplicateElementKey.
    authenticator = stauth.Authenticate(
        credentials,
        cookie_name,
        cookie_key,
        cookie_expiry_days=30.0,
        auto_hash=True,
    )
    try:
        authenticator.login(location="unrendered", key="ph_premiumhunter_cookie_probe")
    except Exception as e:
        st.error(f"Login failed to start: {e}")
        st.stop()

    if st.session_state.get("authentication_status"):
        if "ph_cookie_expiry_locked" not in st.session_state:
            st.session_state.ph_cookie_expiry_locked = 30.0
        un = (st.session_state.get("username") or "").strip()
        st.session_state.ph_watchlist_owner = un or "default"
        return authenticator

    st.checkbox(
        "Remember me on this device (~30 days)",
        value=True,
        key="ph_remember_me",
    )
    cookie_days = 30.0 if st.session_state.get("ph_remember_me", True) else 1.0
    authenticator.cookie_controller.cookie_model.cookie_expiry_days = cookie_days

    try:
        authenticator.login(location="main", key="ph_premiumhunter_login")
    except Exception as e:
        st.error(f"Login failed to start: {e}")
        st.stop()

    if st.session_state.get("authentication_status"):
        if "ph_cookie_expiry_locked" not in st.session_state:
            st.session_state.ph_cookie_expiry_locked = float(cookie_days)
        un = (st.session_state.get("username") or "").strip()
        st.session_state.ph_watchlist_owner = un or "default"
        return authenticator

    if st.session_state.get("authentication_status") is False:
        st.error("Username or password is incorrect.")
    else:
        st.warning("Please sign in to continue.")
    st.stop()
