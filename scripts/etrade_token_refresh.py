#!/usr/bin/env python3
"""
Headless OAuth token refresh for E*Trade via Playwright + pyotp.

Performs the full OAuth 1.0 dance:
  1. Fetch request token from E*Trade API.
  2. Open the authorization URL in a headless Chromium browser.
  3. Log in with username / password.
  4. Handle TOTP 2FA if prompted.
  5. Accept the terms / authorize page.
  6. Scrape the verifier code.
  7. Exchange for access tokens.
  8. Write (token, secret) into the etrade_sessions Postgres table.

Required env:
  DATABASE_URL            — Postgres connection URI
  ETRADE_CONSUMER_KEY
  ETRADE_CONSUMER_SECRET
  ETRADE_USERNAME         — E*Trade login username
  ETRADE_PASSWORD         — E*Trade login password
  ETRADE_SANDBOX          — "true" or "false"

Optional:
  ETRADE_TOTP_SECRET      — base32 TOTP secret for 2FA (skip if account has no 2FA)
  DATABASE_FORCE_IPV4     — "1" to force IPv4 (same as watchlist sync)
  DATABASE_IPV4           — explicit IPv4 hostaddr
"""
from __future__ import annotations

import os
import socket
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")

_SQL_TS_CHICAGO_SEC = (
    "(date_trunc('second', timezone('America/Chicago', now())))::timestamp(0)"
)


def _prepare_psycopg_dsn(database_url: str) -> str:
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

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


def _ensure_sessions_table(conn) -> None:
    schema_path = Path(__file__).resolve().parent / "schema_watchlist_snapshots.sql"
    sql = schema_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _upsert_session(conn, token: str, secret: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT session_id FROM etrade_sessions
            WHERE access_token = %s AND access_token_secret = %s
            ORDER BY last_renewed DESC LIMIT 1
            """,
            (token, secret),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                f"UPDATE etrade_sessions SET last_renewed = {_SQL_TS_CHICAGO_SEC} WHERE session_id = %s",
                (row[0],),
            )
            conn.commit()
            return row[0]
        cur.execute(
            f"""
            INSERT INTO etrade_sessions (access_token, access_token_secret, last_renewed)
            VALUES (%s, %s, {_SQL_TS_CHICAGO_SEC}) RETURNING session_id
            """,
            (token, secret),
        )
        sid = cur.fetchone()[0]
    conn.commit()
    return sid


def _obtain_tokens() -> dict:
    """Full headless OAuth flow; returns {"oauth_token": ..., "oauth_token_secret": ...}."""
    import pyetrade
    from playwright.sync_api import sync_playwright

    consumer_key = os.environ["ETRADE_CONSUMER_KEY"]
    consumer_secret = os.environ["ETRADE_CONSUMER_SECRET"]
    username = os.environ["ETRADE_USERNAME"]
    password = os.environ["ETRADE_PASSWORD"]
    is_sandbox = os.environ.get("ETRADE_SANDBOX", "true").lower() == "true"
    totp_secret = (os.environ.get("ETRADE_TOTP_SECRET") or "").strip()

    oauth = pyetrade.ETradeOAuth(consumer_key, consumer_secret)
    auth_url = oauth.get_request_token()
    print(f"Authorization URL obtained (sandbox={is_sandbox})")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.goto(auth_url, wait_until="networkidle")

        # ── Login ────────────────────────────────────────────────────────
        # E*Trade uses input[name='USER'] and input[name='PASSWORD'] on the
        # OAuth authorize page. The logon button has id #logon_button or is
        # the submit named "Logon". We try multiple selectors for resilience.
        for sel in ("input[name='USER']", "#user_orig"):
            if page.locator(sel).count():
                page.locator(sel).fill(username)
                break

        for sel in ("input[name='PASSWORD']", "#txtPassword"):
            if page.locator(sel).count():
                page.locator(sel).fill(password)
                break

        for sel in ("#logon_button", "input[value='Logon']", "button:has-text('Log On')"):
            loc = page.locator(sel)
            if loc.count():
                loc.click()
                break

        page.wait_for_load_state("networkidle")
        time.sleep(2)

        # ── 2FA / TOTP ──────────────────────────────────────────────────
        if totp_secret:
            import pyotp

            totp = pyotp.TOTP(totp_secret)
            for sel in ("#otp_code", "input[name='otp_code']", "input[name='otpCode']",
                        "input[type='tel']", "input[placeholder*='code' i]"):
                loc = page.locator(sel)
                if loc.count():
                    loc.fill(totp.now())
                    break
            for sel in ("#submit_otp", "button:has-text('Submit')", "input[value='Submit']"):
                loc = page.locator(sel)
                if loc.count():
                    loc.click()
                    break
            page.wait_for_load_state("networkidle")
            time.sleep(2)

        # ── Accept terms ─────────────────────────────────────────────────
        for sel in ("input[value='Accept']", "button:has-text('Accept')", "#continueButton"):
            loc = page.locator(sel)
            if loc.count():
                loc.click()
                page.wait_for_load_state("networkidle")
                time.sleep(2)
                break

        # ── Scrape verifier code ─────────────────────────────────────────
        verifier = None

        # Most common: a text input holding the 5-6 digit verifier
        for sel in ("div > input[type='text']", "input[type='text']"):
            loc = page.locator(sel)
            if loc.count():
                val = (loc.first.input_value() or "").strip()
                if val and val.isalnum():
                    verifier = val
                    break

        # Fallback: look for a prominent text element with only digits/letters
        if not verifier:
            for sel in (".verifier-code", "#verifier", "code", "pre"):
                loc = page.locator(sel)
                if loc.count():
                    val = (loc.first.inner_text() or "").strip()
                    if val and val.isalnum() and len(val) <= 10:
                        verifier = val
                        break

        if not verifier:
            page.screenshot(path="/tmp/etrade_token_debug.png")
            final_url = page.url
            browser.close()
            print("ERROR: Could not find verifier code on the page.", file=sys.stderr)
            print("A debug screenshot was saved to /tmp/etrade_token_debug.png", file=sys.stderr)
            print("Page URL:", final_url, file=sys.stderr)
            sys.exit(1)

        browser.close()

    print(f"Verifier code obtained: {verifier[:2]}***")

    tokens = oauth.get_access_token(verifier)
    print("Access tokens obtained successfully.")
    return tokens


def main() -> int:
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 1

    for key in ("ETRADE_CONSUMER_KEY", "ETRADE_CONSUMER_SECRET", "ETRADE_USERNAME", "ETRADE_PASSWORD"):
        if not os.environ.get(key):
            print(f"{key} is required", file=sys.stderr)
            return 1

    import psycopg

    tokens = _obtain_tokens()
    tok = tokens["oauth_token"]
    sec = tokens["oauth_token_secret"]

    dsn = _prepare_psycopg_dsn(database_url)
    with psycopg.connect(dsn) as conn:
        _ensure_sessions_table(conn)
        sid = _upsert_session(conn, tok, sec)
        print(f"Tokens written to etrade_sessions (session_id={sid}).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
