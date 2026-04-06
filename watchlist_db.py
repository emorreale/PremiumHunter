"""Postgres `watchlists` table — shared by Streamlit saves and scripts/ CLI tools."""

from __future__ import annotations

import os
import socket
from pathlib import Path

_ROOT = Path(__file__).resolve().parent


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
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

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


def ensure_watchlist_tables(conn) -> None:
    schema_path = _ROOT / "scripts" / "schema_watchlist_snapshots.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()


def sync_watchlist_to_postgres(symbols: list[str], *, owner: str | None = None) -> None:
    """Upsert watchlists row. No-op if DATABASE_URL is unset. Raises on DB errors."""
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        return

    o = (owner or os.environ.get("WATCHLIST_OWNER") or "default").strip() or "default"

    import psycopg
    from psycopg.types.json import Json

    dsn = prepare_psycopg_dsn(database_url)
    with psycopg.connect(dsn) as conn:
        ensure_watchlist_tables(conn)
        with conn.cursor() as cur:
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
                (o, Json(symbols)),
            )
        conn.commit()
