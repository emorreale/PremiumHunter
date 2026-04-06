import json
import os
import socket
from pathlib import Path

import pyetrade
from dotenv import load_dotenv

load_dotenv()

CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY", "")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET", "")
IS_SANDBOX = os.getenv("ETRADE_SANDBOX", "True").lower() == "true"

_TOKEN_PATH = Path(__file__).resolve().parent / ".etrade_tokens.json"


def load_persisted_tokens() -> dict | None:
    """Load OAuth access tokens from disk if present and environment matches."""
    if not _TOKEN_PATH.is_file():
        return None
    try:
        data = json.loads(_TOKEN_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if data.get("sandbox") != IS_SANDBOX:
        _TOKEN_PATH.unlink(missing_ok=True)
        return None
    key = data.get("oauth_token")
    secret = data.get("oauth_token_secret")
    if not key or not secret:
        return None
    return {"oauth_token": key, "oauth_token_secret": secret}


def save_persisted_tokens(tokens: dict) -> None:
    """Persist access tokens for reuse across browser sessions."""
    payload = {
        "oauth_token": tokens["oauth_token"],
        "oauth_token_secret": tokens["oauth_token_secret"],
        "sandbox": IS_SANDBOX,
    }
    _TOKEN_PATH.write_text(json.dumps(payload), encoding="utf-8")


def clear_persisted_tokens() -> None:
    _TOKEN_PATH.unlink(missing_ok=True)


def get_oauth():
    """Create an ETradeOAuth instance and return the authorization URL."""
    oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)
    auth_url = oauth.get_request_token()
    return oauth, auth_url


def get_access_tokens(oauth, verifier_code: str) -> dict:
    """Exchange the verifier code for access tokens."""
    tokens = oauth.get_access_token(verifier_code)
    return tokens


def _prepare_psycopg_dsn(database_url: str) -> str:
    """Match scripts/watchlist_snapshot_to_postgres.py IPv4 pinning for Supabase/GitHub-style hosts."""
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
    if explicit:
        params["hostaddr"] = explicit
    elif want_v4 and host and not host.startswith("/"):
        try:
            infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
        except OSError:
            infos = []
        if infos:
            params["hostaddr"] = infos[0][4][0]
    if "hostaddr" not in params:
        return database_url
    return make_conninfo("", **params)


def fetch_latest_tokens_from_postgres() -> dict | None:
    """
    Load the most recently renewed access tokens from etrade_sessions
    (no date filter — E*Trade validity is checked via probe_etrade_tokens in the app).

    Returns {"oauth_token", "oauth_token_secret"} or None.
    """
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        return None
    try:
        import psycopg
    except ImportError:
        return None

    dsn = _prepare_psycopg_dsn(database_url)
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT access_token, access_token_secret
                    FROM etrade_sessions
                    ORDER BY last_renewed DESC NULLS LAST
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
    except Exception:
        return None
    if not row:
        return None
    tok, sec = row[0], row[1]
    if not tok or not sec:
        return None
    return {"oauth_token": str(tok), "oauth_token_secret": str(sec)}
