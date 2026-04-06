"""Persist watchlist symbols to disk so they survive refresh and app restarts."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

from watchlist_db import ensure_watchlist_logging, sync_watchlist_to_postgres

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")

_WATCHLIST_PATH = _ROOT / ".ph_watchlist.json"
_MAX_SYM_LEN = 10

_LOG = logging.getLogger("premiumhunter.watchlist")


def _dedupe(symbols: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for s in symbols:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def load_watchlist() -> list[str]:
    if not _WATCHLIST_PATH.is_file():
        return []
    try:
        raw = json.loads(_WATCHLIST_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    if isinstance(raw, list):
        syms = [
            str(x).upper().strip()[:_MAX_SYM_LEN]
            for x in raw
            if isinstance(x, str) and str(x).strip()
        ]
        return _dedupe(syms)
    if isinstance(raw, dict):
        tickers = raw.get("tickers")
        if isinstance(tickers, list):
            syms = [
                str(x).upper().strip()[:_MAX_SYM_LEN]
                for x in tickers
                if isinstance(x, str) and str(x).strip()
            ]
            return _dedupe(syms)
    return []


def save_watchlist(symbols: list[str]) -> None:
    ensure_watchlist_logging()

    payload = _dedupe(
        [
            str(x).upper().strip()[:_MAX_SYM_LEN]
            for x in symbols
            if x is not None and str(x).strip()
        ]
    )
    _LOG.info("save_watchlist invoked (%d symbol(s))", len(payload))

    try:
        _WATCHLIST_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError as e:
        _LOG.error("Local watchlist write failed %s — %s", _WATCHLIST_PATH, e)
    else:
        _LOG.info("Local file OK → %s", _WATCHLIST_PATH)

    try:
        sync_watchlist_to_postgres(payload)
    except Exception:
        _LOG.exception(
            "Postgres sync failed after local save (check DATABASE_URL, psycopg, RLS, and pooler)"
        )


def ensure_session_watchlist() -> None:
    if "ph_watchlist" not in st.session_state:
        st.session_state.ph_watchlist = load_watchlist()
