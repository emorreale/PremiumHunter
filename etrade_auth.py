import json
import os
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
