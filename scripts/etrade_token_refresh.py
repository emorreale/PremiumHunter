#!/usr/bin/env python3
"""
Headless OAuth token refresh for E*Trade via Playwright + pyotp.

Performs the full OAuth 1.0 dance:
  1. Fetch request token from E*Trade API.
  2. Open the OAuth host root (cookie warm-up), then the authorization URL in headless Chromium.
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
  ETRADE_SKIP_COOKIE_WARMUP — set "1" to skip the pre-authorize homepage visit
  ETRADE_HUMAN_DELAYS     — set "0" to skip random short pauses before fills/clicks
  ETRADE_USER_AGENT       — optional full UA string (otherwise a current Chrome desktop UA is used)
  ETRADE_LOCATOR_TIMEOUT_MS — max wait per selector when probing fields (default 2200; was 8000 and
                            multiplied by iframe count × selector count)
  ETRADE_FRAMES_FIRST      — set "1" to try child iframes before the main document (legacy order)

GitHub Actions often cannot complete login if E*Trade shows CAPTCHA or blocks datacenter IPs;
use workflow_dispatch from a trusted network or refresh tokens from your machine if needed.
"""
from __future__ import annotations

import os
import random
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

# Light "stealth": keep UA consistent with Chromium; avoid instant robotic input.
_STEALTH_INIT_JS = r"""
(() => {
  try {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
  } catch (e) {}
  try {
    if (!window.chrome) {
      window.chrome = { runtime: {} };
    }
  } catch (e) {}
})();
"""


def _human_delays_enabled() -> bool:
    return (os.environ.get("ETRADE_HUMAN_DELAYS") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _human_pause(root, lo_ms: int = 160, hi_ms: int = 620) -> None:
    if not _human_delays_enabled():
        return
    try:
        getattr(root, "page", root).wait_for_timeout(random.randint(lo_ms, hi_ms))
    except Exception:
        pass


def _locator_visible_timeout_ms() -> int:
    raw = (os.environ.get("ETRADE_LOCATOR_TIMEOUT_MS") or "").strip()
    if raw.isdigit():
        return max(400, min(20000, int(raw)))
    return 2200


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


def _warmup_etrade_origin(page, auth_url: str) -> None:
    """
    Load the OAuth host's origin (e.g. https://us.etrade.com/) before the authorize URL so the
    browser can receive first-party cookies some flows expect. Harmless if it does nothing.
    """
    from urllib.parse import urlparse

    if (os.environ.get("ETRADE_SKIP_COOKIE_WARMUP") or "").strip().lower() in ("1", "true", "yes"):
        return
    try:
        parsed = urlparse(auth_url)
        if not parsed.scheme or not parsed.netloc or "etrade" not in parsed.netloc.lower():
            return
        origin = f"{parsed.scheme}://{parsed.netloc}/"
        page.goto(origin, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(random.randint(2000, 3800) if _human_delays_enabled() else 2500)
    except Exception:
        pass


def _login_roots(page):
    """Main document first unless ETRADE_FRAMES_FIRST=1 (E*Trade login is usually full-page)."""
    frames_first = (os.environ.get("ETRADE_FRAMES_FIRST") or "").strip().lower() in ("1", "true", "yes")
    if frames_first:
        for fr in page.frames:
            if fr != page.main_frame:
                yield fr
        yield page
        return
    yield page
    for fr in page.frames:
        if fr != page.main_frame:
            yield fr


def _fill_first_visible(root, selectors: tuple[str, ...], value: str) -> bool:
    from playwright.sync_api import TimeoutError as PlaywrightTimeout

    vis_ms = _locator_visible_timeout_ms()
    for sel in selectors:
        loc = root.locator(sel).first
        try:
            loc.wait_for(state="visible", timeout=vis_ms)
            _human_pause(root)
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

    vis_ms = _locator_visible_timeout_ms()
    for sel in selectors:
        loc = root.locator(sel).first
        try:
            loc.wait_for(state="visible", timeout=vis_ms)
            _human_pause(root)
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


def _check_use_security_code_checkbox(root) -> None:
    """VIP / SYMC: same-page login often requires this before the security code field appears."""
    try:
        cb = root.get_by_role("checkbox", name=re.compile(r"security\s*code", re.I))
        if cb.count():
            first = cb.first
            if first.is_visible() and not first.is_checked():
                _human_pause(root, 200, 550)
                first.check(timeout=5000)
                return
    except Exception:
        pass
    try:
        lab = root.locator("label").filter(has_text=re.compile(r"use\s+security\s+code", re.I)).first
        if lab.count() and lab.is_visible():
            _human_pause(root, 200, 550)
            lab.click(timeout=5000)
    except Exception:
        pass


def _try_login_one_root(
    root,
    username: str,
    password: str,
    totp_secret: str,
    user_selectors: tuple[str, ...],
    pass_selectors: tuple[str, ...],
    otp_selectors: tuple[str, ...],
    logon_selectors: tuple[str, ...],
) -> bool:
    """
    Fill user + password in the *same* document/frame, optionally same-page TOTP, then Log on.
    Returns True if credentials were filled and submit was clicked in this root.
    """
    if not _fill_first_visible(root, user_selectors, username):
        return False
    if not _fill_first_visible(root, pass_selectors, password):
        return False
    if (totp_secret or "").strip():
        _check_use_security_code_checkbox(root)
        _gap = random.randint(700, 1400) if _human_delays_enabled() else 900
        getattr(root, "page", root).wait_for_timeout(_gap)
        import pyotp

        _fill_first_visible(root, otp_selectors, pyotp.TOTP(totp_secret.strip()).now())
    return _click_first_visible(root, logon_selectors)


def _etrade_authorize_hard_fail_hint(page) -> str | None:
    """
    E*Trade sometimes returns a minimal authorize page with only a yellow banner, e.g.
    'Due to a logon delay or other issue, your authentication could not be completed...'
    (no verifier). Return a short explanation for stderr, or None.
    """
    try:
        text = page.locator("body").inner_text(timeout=10000).lower()
    except Exception:
        try:
            text = (page.content() or "").lower()
        except Exception:
            return None
    if "authentication could not be completed" in text or "logon delay or other issue" in text:
        return (
            "E*Trade showed an authorize-page failure (often transient load-balancer / session timing). "
            "There is no oauth_verifier to scrape. Re-run the job; if it keeps failing from GitHub Actions, "
            "run workflow_dispatch from a trusted network or refresh tokens locally — datacenter IPs are "
            "often throttled."
        )
    return None


def _fail_if_etrade_authorize_error(page, browser) -> None:
    hint = _etrade_authorize_hard_fail_hint(page)
    if hint:
        _fail_browser(page, browser, f"ERROR: {hint}", f"Page URL: {page.url}")


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

    _ua = (os.environ.get("ETRADE_USER_AGENT") or "").strip()
    if not _ua:
        if sys.platform == "win32":
            _ua = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
            )
        else:
            _ua = (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
            )

    _launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--window-size=1920,1080",
    ]

    with sync_playwright() as pw:
        try:
            browser = pw.chromium.launch(
                headless=headless,
                args=_launch_args,
                ignore_default_args=["--enable-automation"],
            )
        except TypeError:
            browser = pw.chromium.launch(headless=headless, args=_launch_args)
        context = browser.new_context(
            user_agent=_ua,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/Chicago",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        context.add_init_script(_STEALTH_INIT_JS)
        page = context.new_page()
        _warmup_etrade_origin(page, auth_url)
        page.goto(auth_url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(random.randint(2200, 4200) if _human_delays_enabled() else 3000)

        # ── Login (same frame for user+pass; VIP = check "Use security code" + TOTP before Log on)
        user_selectors = (
            "input[name='USER']",
            "#user_orig",
            "#userId",
            "input#userId",
            "input[name='userId']",
            "input[id*='user' i][type='text']",
            "input[autocomplete='username']",
            "input[placeholder*='User ID' i]",
            "input[placeholder*='user id' i]",
            "input[aria-label*='User' i]",
        )
        pass_selectors = (
            "input[name='PASSWORD']",
            "#txtPassword",
            "input[type='password']",
            "input[autocomplete='current-password']",
            "input[placeholder*='Password' i]",
            "input[aria-label*='Password' i]",
        )
        otp_selectors = (
            "#otp_code",
            "input[name='otp_code']",
            "input[name='otpCode']",
            "input[name='securityCode']",
            "input[name*='security' i][type='text']",
            "input[id*='security' i]",
            "input[inputmode='numeric']",
            "input[type='tel']",
            "input[placeholder*='code' i]",
            "input[placeholder*='Security' i]",
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

        login_clicked = False
        for root in _login_roots(page):
            if _try_login_one_root(
                root,
                username,
                password,
                totp_secret,
                user_selectors,
                pass_selectors,
                otp_selectors,
                logon_selectors,
            ):
                login_clicked = True
                break

        if not login_clicked:
            _fail_browser(
                page,
                browser,
                "ERROR: Could not complete login in any frame (user+password+optional TOTP in same frame).",
                f"Page URL: {page.url}",
            )

        prev_url = page.url

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

        _fail_if_etrade_authorize_error(page, browser)

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

        # ── 2FA / TOTP (second step if not same-page as login) ─────────
        if totp_secret:
            import pyotp

            totp = pyotp.TOTP(totp_secret)
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
        _human_pause(page, 400, 900)
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
                    _human_pause(page, 250, 700)
                    loc.first.click(timeout=5000)
                    page.wait_for_load_state("domcontentloaded")
                    page.wait_for_timeout(
                        random.randint(1700, 2600) if _human_delays_enabled() else 2000
                    )
                except Exception:
                    pass
                break

        _fail_if_etrade_authorize_error(page, browser)

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
            _fail_if_etrade_authorize_error(page, browser)
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
