"""Persist watchlist symbols to disk so they survive refresh and app restarts."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

from watchlist_db import ensure_watchlist_logging, sync_watchlist_to_postgres

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")

_LEGACY_WATCHLIST_PATH = _ROOT / ".ph_watchlist.json"
_MAX_SYM_LEN = 10

_LOG = logging.getLogger("premiumhunter.watchlist")


def _owner_slug(owner: str) -> str:
    o = (owner or "default").strip().lower() or "default"
    o = re.sub(r"[^a-z0-9_-]+", "_", o)
    o = o.strip("_")[:48]
    return o or "default"


def watchlist_path_for_owner(owner: str) -> Path:
    return _ROOT / f".ph_watchlist.{_owner_slug(owner)}.json"


def _dedupe(symbols: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for s in symbols:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _parse_watchlist_json(raw: object) -> list[str]:
    if isinstance(raw, list):
        syms = [
            str(x).upper().strip()[:_MAX_SYM_LEN]
            for x in raw
            if isinstance(x, str) and str(x).strip()
        ]
        return _dedupe(syms)
    if isinstance(raw, dict):
        tickers = raw.get("tickers") or raw.get("symbols")
        if isinstance(tickers, list):
            syms = [
                str(x).upper().strip()[:_MAX_SYM_LEN]
                for x in tickers
                if isinstance(x, str) and str(x).strip()
            ]
            return _dedupe(syms)
    return []


def load_watchlist_for_owner(owner: str) -> list[str]:
    path = watchlist_path_for_owner(owner)
    for candidate in (path, _LEGACY_WATCHLIST_PATH):
        if not candidate.is_file():
            continue
        try:
            raw = json.loads(candidate.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        syms = _parse_watchlist_json(raw)
        if syms or candidate == path:
            return syms
    return []


def save_watchlist(symbols: list[str]) -> None:
    ensure_watchlist_logging()

    owner = (st.session_state.get("ph_watchlist_owner") or "default").strip() or "default"
    path = watchlist_path_for_owner(owner)

    payload = _dedupe(
        [
            str(x).upper().strip()[:_MAX_SYM_LEN]
            for x in symbols
            if x is not None and str(x).strip()
        ]
    )
    _LOG.info("save_watchlist owner=%r (%d symbol(s))", owner, len(payload))

    try:
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError as e:
        _LOG.error("Local watchlist write failed %s — %s", path, e)
    else:
        _LOG.info("Local file OK → %s", path)

    try:
        sync_watchlist_to_postgres(payload, owner=owner)
    except Exception:
        _LOG.exception(
            "Postgres sync failed after local save (check DATABASE_URL, psycopg, RLS, and pooler)"
        )


def ensure_session_watchlist() -> None:
    owner = (st.session_state.get("ph_watchlist_owner") or "default").strip() or "default"
    prev = st.session_state.get("ph_watchlist_loaded_for_owner")
    if prev != owner or "ph_watchlist" not in st.session_state:
        st.session_state.ph_watchlist_loaded_for_owner = owner
        st.session_state.ph_watchlist = load_watchlist_for_owner(owner)
