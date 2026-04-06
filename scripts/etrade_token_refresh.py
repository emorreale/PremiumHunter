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
  ETRADE_TOTP_SECRET      — base32 TOTP secret for 2FA (skip if account has no 2FA).
                            For E*Trade VIP enrollment, mint a credential locally:
                            pip install -r requirements-etrade-tools.txt
                            python scripts/etrade_vipaccess_provision.py
  DATABASE_FORCE_IPV4     — "1" to force IPv4 (same as watchlist sync)
  DATABASE_IPV4           — explicit IPv4 hostaddr
  PLAYWRIGHT_HEADLESS     — set "false" to show the browser locally while debugging

GitHub Actions often cannot complete login if E*Trade shows CAPTCHA or blocks datacenter IPs;
use workflow_dispatch from a trusted network or refresh tokens from your machine if needed.
"""
from __future__ import annotations

import os
import re
import socket
import sys
import tempfile
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")


def _debug_png_path() -> Path:
    """CI: GITHUB_WORKSPACE so upload-artifact can collect; else OS temp dir."""
    ws = (os.environ.get("GITHUB_WORKSPACE") or "").strip()
    if ws:
        return Path(ws) / "etrade_token_debug.png"
    return Path(tempfile.gettempdir()) / "etrade_token_debug.png"


def _fail_browser(page, browser, *lines: str) -> None:
    path = _debug_png_path()
    try:
        page.screenshot(path=str(path), full_page=True)
    except Exception:
        pass
    try:
        browser.close()
    except Exception:
        pass
    for line in lines:
        print(line, file=sys.stderr)
    print(f"Debug screenshot: {path}", file=sys.stderr)
    sys.exit(1)


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


def _verifier_from_url(url: str) -> str | None:
    """Callback-style redirect includes oauth_verifier=…"""
    from urllib.parse import parse_qs, unquote, urlparse

    parsed = urlparse(url)
    for key in ("oauth_verifier", "verifier"):
        q = parse_qs(parsed.query)
        if key in q and q[key][0]:
            return unquote(q[key][0]).strip()
    if "oauth_verifier=" in url:
        part = url.split("oauth_verifier=", 1)[1].split("&")[0]
        return unquote(part).strip() or None
    return None


def _login_roots(page):
    """Main document plus child frames (E*Trade often embeds the form in an iframe)."""
    yield page
    for fr in page.frames:
        if fr != page.main_frame:
            yield fr


def _fill_first_visible(root, selectors: tuple[str, ...], value: str) -> bool:
    from playwright.sync_api import TimeoutError as PlaywrightTimeout

    for sel in selectors:
        loc = root.locator(sel).first
        try:
            loc.wait_for(state="visible", timeout=8000)
            loc.fill(value, timeout=5000)
            return True
        except PlaywrightTimeout:
            continue
    return False


def _fill_first_visible_tree(page, selectors: tuple[str, ...], value: str) -> bool:
    for root in _login_roots(page):
        if _fill_first_visible(root, selectors, value):
            return True
    return False


def _click_first_visible(root, selectors: tuple[str, ...]) -> bool:
    from playwright.sync_api import TimeoutError as PlaywrightTimeout

    for sel in selectors:
        loc = root.locator(sel).first
        try:
            loc.wait_for(state="visible", timeout=5000)
            loc.click(timeout=5000)
            return True
        except PlaywrightTimeout:
            continue
    return False


def _click_first_visible_tree(page, selectors: tuple[str, ...]) -> bool:
    for root in _login_roots(page):
        if _click_first_visible(root, selectors):
            return True
    return False


def _print_login_diagnostics(page, totp_secret: str) -> None:
    if not (totp_secret or "").strip():
        print(
            "HINT: ETRADE_TOTP_SECRET is unset. If E*Trade requires 2FA, add the base32 secret "
            "to GitHub Actions secrets or login will stay on the security step.",
            file=sys.stderr,
        )
    for sel in ("[role='alert']", ".error", ".message-error", ".alert-danger", ".alert", "#errorText"):
        try:
            loc = page.locator(sel).first
            if loc.count():
                t = loc.inner_text(timeout=2000).strip()
                if t:
                    print(f"On-page text ({sel}): {t[:900]}", file=sys.stderr)
        except Exception:
            continue


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
    headless = os.environ.get("PLAYWRIGHT_HEADLESS", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )

    oauth = pyetrade.ETradeOAuth(consumer_key, consumer_secret)
    auth_url = oauth.get_request_token()
    print(f"Authorization URL obtained (sandbox={is_sandbox})")

    if sys.platform == "win32":
        _ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
    else:
        _ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent=_ua,
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        page = context.new_page()
        page.goto(auth_url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(3000)

        # ── Login (main page + iframes) ──────────────────────────────────
        user_selectors = (
            "input[name='USER']",
            "#user_orig",
            "#userId",
            "input#userId",
            "input[name='userId']",
            "input[id*='user' i][type='text']",
            "input[autocomplete='username']",
        )
        pass_selectors = (
            "input[name='PASSWORD']",
            "#txtPassword",
            "input[type='password']",
            "input[autocomplete='current-password']",
        )
        logon_selectors = (
            "#logon_button",
            "input[value='Logon']",
            "input[type='submit'][value*='Log' i]",
            "button:has-text('Log On')",
            "button:has-text('Log on')",
            "button[type='submit']",
            "input[type='submit']",
        )

        if not _fill_first_visible_tree(page, user_selectors, username):
            _fail_browser(
                page,
                browser,
                "ERROR: Could not find username field (try iframe or E*Trade UI change).",
                f"Page URL: {page.url}",
            )
        if not _fill_first_visible_tree(page, pass_selectors, password):
            _fail_browser(page, browser, "ERROR: Could not find password field.", f"Page URL: {page.url}")

        prev_url = page.url
        if not _click_first_visible_tree(page, logon_selectors):
            _fail_browser(page, browser, "ERROR: Could not find Log On / submit control.", f"Page URL: {page.url}")

        def _wait_off_login_screen(timeout_s: float) -> None:
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                if "/etx/pxy/login" not in page.url:
                    return
                page.wait_for_timeout(400)

        _wait_off_login_screen(12.0)
        if "/etx/pxy/login" in page.url:
            try:
                page.keyboard.press("Enter")
            except Exception:
                pass
            page.wait_for_timeout(1500)
            _wait_off_login_screen(25.0)

        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(2000)

        if "/etx/pxy/login" in page.url or page.url == prev_url:
            _print_login_diagnostics(page, totp_secret)
            _fail_browser(
                page,
                browser,
                "ERROR: Still on E*Trade login — wrong GitHub secrets, CAPTCHA/bot block on datacenter IP, "
                "missing 2FA secret, or UI changed. Download the workflow artifact "
                "`etrade-token-refresh-debug` (screenshot).",
                f"Page URL: {page.url}",
            )

        # ── 2FA / TOTP ──────────────────────────────────────────────────
        if totp_secret:
            import pyotp

            totp = pyotp.TOTP(totp_secret)
            otp_selectors = (
                "#otp_code",
                "input[name='otp_code']",
                "input[name='otpCode']",
                "input[name='securityCode']",
                "input[inputmode='numeric']",
                "input[type='tel']",
                "input[placeholder*='code' i]",
            )
            otp_submit = (
                "#submit_otp",
                "button:has-text('Submit')",
                "button:has-text('Continue')",
                "input[value='Submit']",
                "button[type='submit']",
            )
            if _fill_first_visible_tree(page, otp_selectors, totp.now()):
                _click_first_visible_tree(page, otp_submit)
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(2500)

        # ── Accept / Authorize ───────────────────────────────────────────
        for sel in (
            "input[value='Accept']",
            "button:has-text('Accept')",
            "button:has-text('Approve')",
            "button:has-text('Allow')",
            "#continueButton",
            "input[value='Continue']",
            "button:has-text('Continue')",
        ):
            loc = page.locator(sel)
            if loc.count():
                try:
                    loc.first.click(timeout=5000)
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(2000)
                except Exception:
                    pass
                break

        # ── Verifier: URL param (callback) or page scrape ─────────────────
        verifier = _verifier_from_url(page.url)

        if not verifier:
            for sel in (
                "input[readonly][type='text']",
                "input[type='text'][readonly]",
                "div > input[type='text']",
                "input[type='text']",
            ):
                loc = page.locator(sel)
                n = loc.count()
                for i in range(min(n, 12)):
                    try:
                        val = (loc.nth(i).input_value() or "").strip()
                        if 4 <= len(val) <= 32 and re.fullmatch(r"[A-Za-z0-9]+", val):
                            verifier = val
                            break
                    except Exception:
                        continue
                if verifier:
                    break

        if not verifier:
            body = page.content()
            m = re.search(
                r"oauth_verifier=([A-Za-z0-9._~-]+)",
                body,
                re.I,
            ) or re.search(
                r"(?:verification|verifier)\s*(?:code)?[:\s]+([A-Za-z0-9]{4,32})",
                body,
                re.I,
            )
            if m:
                verifier = m.group(1).strip()

        if not verifier:
            for sel in (".verifier-code", "#verifier", "code", "pre"):
                loc = page.locator(sel)
                if loc.count():
                    val = (loc.first.inner_text() or "").strip()
                    if val and val.isalnum() and len(val) <= 32:
                        verifier = val
                        break

        if not verifier:
            _fail_browser(
                page,
                browser,
                "ERROR: Could not find verifier code on the page.",
                f"Page URL: {page.url}",
            )

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
