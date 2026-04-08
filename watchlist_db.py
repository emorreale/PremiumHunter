"""Postgres `watchlists` table — shared by Streamlit saves and scripts/ CLI tools."""

from __future__ import annotations

import logging
import os
import socket
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent

_LOG = logging.getLogger("premiumhunter.watchlist")


def ensure_watchlist_logging() -> None:
    """Send premiumhunter.watchlist to stderr once (Streamlit does not configure root logging)."""
    if _LOG.handlers:
        return
    _LOG.setLevel(logging.INFO)
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("[premiumhunter.watchlist] %(levelname)s %(message)s"))
    _LOG.addHandler(_h)
    _LOG.propagate = False


def _describe_database_url(database_url: str) -> str:
    """Host/db/user for logs only (no password)."""
    try:
        from psycopg.conninfo import conninfo_to_dict
    except ImportError:
        return "(psycopg not installed)"

    try:
        p = conninfo_to_dict(database_url)
        host = (p.get("host") or "?").strip()
        db = (p.get("dbname") or p.get("database") or "").strip()
        user = (p.get("user") or "").strip()
        port = (p.get("port") or "").strip()
        parts = [f"host={host!r}"]
        if user:
            parts.append(f"user={user!r}")
        if port:
            parts.append(f"port={port}")
        if db:
            parts.append(f"dbname={db!r}")
        return " ".join(parts)
    except Exception as e:
        return f"(parse error: {e})"

# Only what Streamlit needs on save — avoids running the full migration (ALTER/FK) every click.
_WATCHLISTS_DDL = """
CREATE TABLE IF NOT EXISTS watchlists (
    owner        VARCHAR(64) PRIMARY KEY,
    symbols      JSONB NOT NULL,
    updated_at   TIMESTAMP(0) DEFAULT (date_trunc('second', timezone('America/Chicago', now())))::timestamp(0)
);
"""


def normalize_watchlist_symbols(raw) -> list[str]:
    """Normalize JSON-decoded payload: list of tickers, or dict with tickers/symbols."""
    if isinstance(raw, dict):
        raw = raw.get("tickers") or raw.get("symbols") or []
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for x in raw:
        s = str(x).upper().strip()[:10]
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def prepare_psycopg_dsn(database_url: str) -> str:
    try:
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
    except ImportError:
        return database_url

    try:
        params = dict(conninfo_to_dict(database_url))
    except Exception:
        return database_url

    explicit = (os.environ.get("DATABASE_IPV4") or "").strip()
    flag = (os.environ.get("DATABASE_FORCE_IPV4") or "").strip().lower()
    want_v4 = flag in ("1", "true", "yes", "on")
    host = (params.get("host") or "").strip()
    path_host = host.startswith("/")

    if explicit:
        params["hostaddr"] = explicit
    elif want_v4 and host and not path_host:
        try:
            infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
        except OSError:
            infos = []
        if infos:
            params["hostaddr"] = infos[0][4][0]

    if "hostaddr" not in params:
        return database_url
    return make_conninfo("", **params)


def ensure_watchlists_table_minimal(conn) -> None:
    """Create watchlists only (safe for Supabase pooler; no full schema replay)."""
    with conn.cursor() as cur:
        cur.execute(_WATCHLISTS_DDL)
    conn.commit()


def _canonical_watchlist_owner(cur, owner: str) -> str:
    """
    Map app owner to the watchlists.owner primary key. streamlit-authenticator lowercases
    usernames; legacy rows may differ in case. Postgres PKs are case-sensitive, so both
    'evan' and 'Evan' can exist — prefer the row with the most symbols, then newest
    updated_at, so an empty duplicate row does not shadow the real one.
    """
    o = (owner or "default").strip() or "default"
    cur.execute(
        """
        SELECT owner, symbols, updated_at
        FROM watchlists
        WHERE lower(owner) = lower(%s)
        """,
        (o,),
    )
    rows = cur.fetchall()
    if not rows:
        return o

    def _score(row: tuple) -> tuple:
        _ow, sym, ts = row
        n = len(normalize_watchlist_symbols(sym))
        try:
            tval = float(ts.timestamp()) if ts is not None and hasattr(ts, "timestamp") else 0.0
        except Exception:
            tval = 0.0
        return (n, tval)

    best = max(rows, key=_score)
    chosen = str(best[0])
    if len(rows) > 1:
        _LOG.warning(
            "Multiple watchlists rows for login %r: %s — using %r (%d symbol(s))",
            o,
            [str(r[0]) for r in rows],
            chosen,
            _score(best)[0],
        )
    return chosen


def sync_watchlist_to_postgres(symbols: list[str], *, owner: str | None = None) -> None:
    """Upsert watchlists row. No-op if DATABASE_URL is unset. Raises on DB errors."""
    ensure_watchlist_logging()

    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        _LOG.warning("DATABASE_URL is empty — skipping Postgres watchlist sync")
        return

    o = (owner or "default").strip() or "default"
    if len(symbols) <= 12:
        _sym_preview = repr(symbols)
    else:
        _sym_preview = f"{symbols[:12]!r} … (+{len(symbols) - 12} more)"
    _LOG.info(
        "Sync start owner=%r count=%d preview=%s | %s",
        o,
        len(symbols),
        _sym_preview,
        _describe_database_url(database_url),
    )

    try:
        import psycopg
        from psycopg.types.json import Json
    except ImportError:
        _LOG.error(
            "psycopg is not installed for this Python: %s\n"
            "  Install with:  %s -m pip install \"psycopg[binary]>=3.2,<4\"\n"
            "  (Use the same interpreter you use to run Streamlit — Cursor/VS Code may pick a different one than your terminal.)",
            sys.executable,
            sys.executable,
        )
        return

    dsn = prepare_psycopg_dsn(database_url)
    resolved_owner = o
    try:
        with psycopg.connect(dsn) as conn:
            ensure_watchlists_table_minimal(conn)
            with conn.cursor() as cur:
                resolved_owner = _canonical_watchlist_owner(cur, o)
                if resolved_owner != o:
                    _LOG.info("Owner %r resolved to existing row PK %r", o, resolved_owner)
                cur.execute(
                    """
                    INSERT INTO watchlists (owner, symbols, updated_at)
                    VALUES (
                        %s,
                        %s::jsonb,
                        (date_trunc('second', timezone('America/Chicago', now())))::timestamp(0)
                    )
                    ON CONFLICT (owner) DO UPDATE SET
                        symbols = EXCLUDED.symbols,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (resolved_owner, Json(symbols)),
                )
            conn.commit()
    except Exception:
        _LOG.exception(
            "watchlists upsert failed (%s)",
            _describe_database_url(database_url),
        )
        raise

    _LOG.info(
        "Sync OK — watchlists row owner=%r updated with %d symbol(s)",
        resolved_owner,
        len(symbols),
    )


def fetch_watchlist_from_postgres(*, owner: str | None = None) -> list[str] | None:
    """
    Load symbols for one owner from watchlists. Returns None if DATABASE_URL is unset or
    the read fails (caller should fall back to local-only). Returns [] if the row is missing
    or the payload normalizes to empty.
    """
    ensure_watchlist_logging()

    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        return None

    o = (owner or "default").strip() or "default"
    _LOG.info(
        "Fetch start owner=%r | %s",
        o,
        _describe_database_url(database_url),
    )

    try:
        import psycopg
    except ImportError:
        _LOG.warning("psycopg not installed — cannot load watchlist from Postgres")
        return None

    dsn = prepare_psycopg_dsn(database_url)
    try:
        with psycopg.connect(dsn) as conn:
            ensure_watchlists_table_minimal(conn)
            with conn.cursor() as cur:
                co = _canonical_watchlist_owner(cur, o)
                if co != o:
                    _LOG.info("Fetch owner %r matched row PK %r", o, co)
                cur.execute(
                    "SELECT symbols FROM watchlists WHERE owner = %s",
                    (co,),
                )
                row = cur.fetchone()
    except Exception:
        _LOG.exception(
            "watchlists read failed (%s)",
            _describe_database_url(database_url),
        )
        return None

    if not row:
        _LOG.info("Fetch OK — no row for owner=%r (canonical %r)", o, co)
        return []

    raw = row[0]
    out = normalize_watchlist_symbols(raw)
    _LOG.info("Fetch OK — owner=%r count=%d", co, len(out))
    return out
