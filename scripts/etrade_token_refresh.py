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

GitHub-hosted runners cannot load us.etrade.com (WAF / datacenter IP). CI should
renew existing tokens via the OAuth API (after 6:00 PM ET). Full Playwright login
is for your machine:  python scripts/etrade_token_refresh.py
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


_RUN_T0 = time.time()


def _log_step(message: str) -> None:
    elapsed = time.time() - _RUN_T0
    print(f"[premiumhunter.etrade.refresh] +{elapsed:05.1f}s {message}")


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


def _load_latest_tokens(conn) -> tuple[str, str] | None:
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
    if not row or not row[0] or not row[1]:
        return None
    return str(row[0]), str(row[1])


def _save_pending_request(conn, request_token: str, request_secret: str, authorize_url: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etrade_oauth_pending
                (id, request_token, request_token_secret, authorize_url, created_at)
            VALUES (
                1, %s, %s, %s,
                (date_trunc('second', timezone('America/Chicago', now())))::timestamp(0)
            )
            ON CONFLICT (id) DO UPDATE SET
                request_token = EXCLUDED.request_token,
                request_token_secret = EXCLUDED.request_token_secret,
                authorize_url = EXCLUDED.authorize_url,
                created_at = EXCLUDED.created_at
            """,
            (request_token, request_secret, authorize_url),
        )
    conn.commit()


def _load_pending_request(conn) -> tuple[str, str] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT request_token, request_token_secret
            FROM etrade_oauth_pending
            WHERE id = 1
            """
        )
        row = cur.fetchone()
    if not row or not row[0] or not row[1]:
        return None
    return str(row[0]), str(row[1])


def _clear_pending_request(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM etrade_oauth_pending WHERE id = 1")
    conn.commit()


def _request_token_pair(oauth) -> tuple[str, str]:
    session = oauth.session
    token = getattr(session, "token", None) or {}
    key = token.get("oauth_token") if isinstance(token, dict) else None
    secret = token.get("oauth_token_secret") if isinstance(token, dict) else None
    if not key:
        key = getattr(session, "_client", None) and session._client.client.resource_owner_key
    if not secret:
        secret = getattr(session, "_client", None) and session._client.client.resource_owner_secret
    if not key or not secret:
        raise RuntimeError("Could not read OAuth request token from pyetrade session")
    return str(key), str(secret)


def _exchange_verifier(request_token: str, request_secret: str, verifier: str) -> dict:
    from requests_oauthlib import OAuth1Session

    consumer_key = os.environ["ETRADE_CONSUMER_KEY"]
    consumer_secret = os.environ["ETRADE_CONSUMER_SECRET"]
    session = OAuth1Session(
        consumer_key,
        client_secret=consumer_secret,
        resource_owner_key=request_token,
        resource_owner_secret=request_secret,
        verifier=verifier,
    )
    tokens = session.fetch_access_token("https://api.etrade.com/oauth/access_token")
    tok = tokens.get("oauth_token")
    sec = tokens.get("oauth_token_secret")
    if not tok or not sec:
        raise RuntimeError(f"Access token response missing fields: {list(tokens)}")
    return {"oauth_token": tok, "oauth_token_secret": sec}


def _emit_authorize_url(auth_url: str) -> None:
    print(auth_url, flush=True)
    summary = (os.environ.get("GITHUB_STEP_SUMMARY") or "").strip()
    if not summary:
        return
    with open(summary, "a", encoding="utf-8") as fh:
        fh.write("## E*Trade login URL\n\n")
        fh.write("1. Open this link **on your computer** (Chrome/Edge — not a GitHub server).\n\n")
        fh.write(f"{auth_url}\n\n")
        fh.write("2. Log in and copy the verification PIN.\n\n")
        fh.write(
            "3. GitHub → Actions → **Mint E*Trade token** → Run workflow. "
            "Paste the PIN into **verifier**. Do this within a few minutes.\n"
        )


def _renew_tokens_via_api(token: str, secret: str) -> bool:
    """E*Trade renew_access_token (no browser). Allowed after ~6:00 PM ET; fails if expired."""
    from pyetrade.authorization import ETradeAccessManager

    consumer_key = os.environ["ETRADE_CONSUMER_KEY"]
    consumer_secret = os.environ["ETRADE_CONSUMER_SECRET"]
    mgr = ETradeAccessManager(consumer_key, consumer_secret, token, secret)
    mgr.renew_access_token()
    return True


def _in_github_actions() -> bool:
    return (os.environ.get("GITHUB_ACTIONS") or "").strip().lower() == "true"


def _force_browser() -> bool:
    return (os.environ.get("ETRADE_FORCE_BROWSER") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _renew_only() -> bool:
    if "--renew-only" in sys.argv:
        return True
    return (os.environ.get("ETRADE_RENEW_ONLY") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


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


def _goto_resilient(page, url: str, *, timeout: int = 90000) -> None:
    """Navigate, retrying once. E*Trade's CDN often breaks HTTP/2 from GitHub-hosted IPs."""
    last_err: Exception | None = None
    for wait_until in ("domcontentloaded", "commit"):
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout)
            return
        except Exception as e:
            last_err = e
            msg = str(e)
            retryable = (
                "ERR_HTTP2_PROTOCOL_ERROR" in msg
                or "ERR_CONNECTION_RESET" in msg
                or "Timeout" in type(e).__name__
            )
            if not retryable:
                raise
            _log_step(f"Navigation failed ({wait_until}): {msg.splitlines()[0][:180]}")
    if last_err is not None:
        raise last_err


def _warmup_etrade_origin(page, auth_url: str) -> None:
    """
    Load the OAuth host's origin (e.g. https://us.etrade.com/) before the authorize URL so the
    browser can receive first-party cookies some flows expect. Harmless if it does nothing.
    """
    from urllib.parse import urlparse

    skip = (os.environ.get("ETRADE_SKIP_COOKIE_WARMUP") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if skip or _in_github_actions():
        return
    try:
        parsed = urlparse(auth_url)
        if not parsed.scheme or not parsed.netloc or "etrade" not in parsed.netloc.lower():
            return
        origin = f"{parsed.scheme}://{parsed.netloc}/"
        _goto_resilient(page, origin, timeout=60000)
        page.wait_for_timeout(random.randint(2000, 3800) if _human_delays_enabled() else 2500)
    except Exception as e:
        _log_step(f"Origin warmup skipped ({e})")


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
            loc.click(timeout=5000)
            loc.fill("", timeout=5000)
            # E*Trade's Angular form ignores Playwright fill(); keystrokes register.
            loc.press_sequentially(value, delay=35)
            try:
                loc.dispatch_event("input")
                loc.dispatch_event("change")
                loc.dispatch_event("blur")
            except Exception:
                pass
            return True
        except PlaywrightTimeout:
            continue
        except Exception:
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
    if (os.environ.get("PLAYWRIGHT_HEADLESS", "true").strip().lower() in ("0", "false", "no")):
        # Headed local run: let you click Log on (auto-click submits empty Angular state).
        return True
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


def _obtain_tokens_manual() -> dict:
    """Open the authorize URL in your real browser; you log in; paste the verifier PIN."""
    import webbrowser

    import pyetrade

    consumer_key = os.environ["ETRADE_CONSUMER_KEY"]
    consumer_secret = os.environ["ETRADE_CONSUMER_SECRET"]
    is_sandbox = os.environ.get("ETRADE_SANDBOX", "true").lower() == "true"

    _log_step("Requesting OAuth request token")
    oauth = pyetrade.ETradeOAuth(consumer_key, consumer_secret)
    auth_url = oauth.get_request_token()
    print(f"Authorization URL obtained (sandbox={is_sandbox})")
    print()
    print("E*Trade blocks automated Chrome (Playwright). Log in with your normal browser:")
    print()
    print(auth_url)
    print()
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass
    print("1. Log in on that page (User ID, password, 2FA as usual).")
    print("2. Accept / authorize the app if asked.")
    print("3. Copy the verification code (PIN) shown at the end.")
    print()
    verifier = input("Paste the verification code here, then press Enter: ").strip()
    if not verifier:
        raise SystemExit("No verification code entered.")
    _log_step("Exchanging verifier for access tokens")
    tokens = oauth.get_access_token(verifier)
    print("Access tokens obtained successfully.")
    return tokens


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

    _log_step("Requesting OAuth request token")
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

    # GitHub-hosted runners often get net::ERR_HTTP2_PROTOCOL_ERROR on us.etrade.com
    # (CDN/WAF vs Chromium HTTP/2). OAuth request_token still works; only the browser
    # navigation breaks. Force HTTP/1.1.
    _launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--window-size=1920,1080",
        "--disable-http2",
        "--disable-quic",
    ]

    _log_step(f"Launching Playwright Chromium (headless={headless})")
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
        _log_step("Warming E*Trade origin and opening authorize URL")
        _warmup_etrade_origin(page, auth_url)
        _goto_resilient(page, auth_url, timeout=90000)
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

        _log_step("Attempting login submit (user/password + optional same-page TOTP)")
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
        headed = os.environ.get("PLAYWRIGHT_HEADLESS", "true").strip().lower() in (
            "0",
            "false",
            "no",
        )
        if headed:
            _log_step(
                "User ID / password should be filled. Click Log on in the window "
                "(complete 2FA if asked). Waiting up to 5 minutes…"
            )
            print(
                "Chromium is open — click Log on yourself. This window waits for the "
                "authorize / verifier page.",
                flush=True,
            )
            wait_s = 300.0
        else:
            _log_step("Login submitted; waiting to leave /etx/pxy/login")
            wait_s = 37.0

        def _wait_off_login_screen(timeout_s: float) -> None:
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                if "/etx/pxy/login" not in page.url:
                    return
                if _verifier_from_url(page.url):
                    return
                page.wait_for_timeout(400)

        if not headed:
            _wait_off_login_screen(12.0)
            if "/etx/pxy/login" in page.url:
                try:
                    page.keyboard.press("Enter")
                except Exception:
                    pass
                page.wait_for_timeout(1500)
                _wait_off_login_screen(25.0)
        else:
            _wait_off_login_screen(wait_s)

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
        _log_step("Checking for second-step OTP challenge")
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
        _log_step("Checking for Accept/Authorize screen")
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
        _log_step("Searching for oauth_verifier in URL or page")
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

    _log_step("Exchanging verifier for access tokens")
    tokens = oauth.get_access_token(verifier)
    print("Access tokens obtained successfully.")
    return tokens


def _cli_verifier() -> str:
    if "--verifier" in sys.argv:
        i = sys.argv.index("--verifier")
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1].strip()
    return (os.environ.get("ETRADE_VERIFIER") or "").strip()


def _start_login() -> bool:
    if "--start-login" in sys.argv:
        return True
    return (os.environ.get("ETRADE_START_LOGIN") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def main() -> int:
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 1

    for key in ("ETRADE_CONSUMER_KEY", "ETRADE_CONSUMER_SECRET"):
        if not os.environ.get(key):
            print(f"{key} is required", file=sys.stderr)
            return 1

    import pyetrade
    import psycopg

    dsn = _prepare_psycopg_dsn(database_url)
    renew_only = _renew_only()
    verifier = _cli_verifier()
    start_login = _start_login()
    _log_step("Starting token refresh run")

    with psycopg.connect(dsn) as conn:
        _ensure_sessions_table(conn)

        if verifier:
            pending = _load_pending_request(conn)
            if not pending:
                print(
                    "No pending login. Run Mint E*Trade token first with verifier left empty.",
                    file=sys.stderr,
                )
                return 1
            _log_step("Exchanging verification PIN for access tokens")
            try:
                tokens = _exchange_verifier(pending[0], pending[1], verifier)
            except Exception as e:
                print(f"PIN exchange failed: {e}", file=sys.stderr)
                return 1
            sid = _upsert_session(conn, tokens["oauth_token"], tokens["oauth_token_secret"])
            _clear_pending_request(conn)
            print(f"Tokens written to etrade_sessions (session_id={sid}).")
            return 0

        if start_login:
            _log_step("Requesting OAuth request token")
            oauth = pyetrade.ETradeOAuth(
                os.environ["ETRADE_CONSUMER_KEY"],
                os.environ["ETRADE_CONSUMER_SECRET"],
            )
            auth_url = oauth.get_request_token()
            req_tok, req_sec = _request_token_pair(oauth)
            _save_pending_request(conn, req_tok, req_sec, auth_url)
            print("Authorization URL obtained. Open it in your browser, then re-run with the PIN.")
            _emit_authorize_url(auth_url)
            return 0

        existing = _load_latest_tokens(conn)
        if existing:
            tok, sec = existing
            _log_step("Attempting API token renew (no browser)")
            try:
                _renew_tokens_via_api(tok, sec)
            except Exception as e:
                _log_step(f"API renew failed: {e}")
            else:
                sid = _upsert_session(conn, tok, sec)
                print(f"Tokens renewed via API (session_id={sid}).")
                return 0

        if renew_only:
            print(
                "API renew failed and ETRADE_RENEW_ONLY is set. "
                "Run the Mint E*Trade token workflow (or this script locally).",
                file=sys.stderr,
            )
            return 1

        if _in_github_actions() and not _force_browser():
            if existing:
                print(
                    "GitHub-hosted runners cannot complete E*Trade login. "
                    "Use Actions → Mint E*Trade token (URL then PIN).",
                    file=sys.stderr,
                )
                return 0
            print(
                "No etrade_sessions tokens. Use Actions → Mint E*Trade token.",
                file=sys.stderr,
            )
            return 1

        if _force_browser():
            for key in ("ETRADE_USERNAME", "ETRADE_PASSWORD"):
                if not os.environ.get(key):
                    print(f"{key} is required for Playwright login", file=sys.stderr)
                    return 1
            tokens = _obtain_tokens()
        else:
            tokens = _obtain_tokens_manual()
        sid = _upsert_session(conn, tokens["oauth_token"], tokens["oauth_token_secret"])
        print(f"Tokens written to etrade_sessions (session_id={sid}).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
