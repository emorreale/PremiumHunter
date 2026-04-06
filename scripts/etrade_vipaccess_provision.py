#!/usr/bin/env python3
"""
Provision a Symantec VIP Access–style credential (SYMC/SYDC) for E*Trade 2FA.

E*Trade often enrolls “authenticator app” via VIP Access. The official app hides
the shared secret; python-vipaccess mints a compatible credential so you can:

  1. Register the Credential ID on E*Trade (same as with the phone app).
  2. Put the Base32 secret in GitHub Actions / .env as ETRADE_TOTP_SECRET for
     scripts/etrade_token_refresh.py (pyotp).

Install once (project venv recommended):

  pip install -r requirements-etrade-tools.txt

Run:

  python scripts/etrade_vipaccess_provision.py
  python scripts/etrade_vipaccess_provision.py --token-model SYDC

Optional:

  --show-code      After provisioning, print one 6-digit code for that NEW credential.
  --current-code   Do not provision. Print code + countdown from ETRADE_TOTP_SECRET in .env
                   (use this for the same token repeatedly; run provision only once).

Terms, availability, and correctness of third-party provisioning are between you
and Symantec/Broadcom; treat the secret like a password.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

_ROOT = Path(__file__).resolve().parent.parent


def _run_provision(token_model: str) -> str:
    cmd = [
        sys.executable,
        "-m",
        "vipaccess",
        "provision",
        "-p",
        "-t",
        token_model,
    ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    out = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    if proc.returncode != 0:
        print(proc.stdout or "", end="")
        print(proc.stderr or "", end="", file=sys.stderr)
        sys.exit(proc.returncode or 1)
    return out


def _parse_otpauth(block: str) -> tuple[str, str]:
    m = re.search(r"(otpauth://totp/[^\s]+)", block)
    if not m:
        print(
            "Could not find otpauth:// URI in vipaccess output. "
            "Is python-vipaccess installed? pip install -r requirements-etrade-tools.txt",
            file=sys.stderr,
        )
        sys.exit(1)
    uri = m.group(1).strip()
    parsed = urlparse(uri)
    qs = parse_qs(parsed.query)
    secrets = qs.get("secret")
    if not secrets:
        print("otpauth URI has no secret= parameter.", file=sys.stderr)
        sys.exit(1)
    secret = secrets[0].strip()
    path = unquote(parsed.path or "").lstrip("/")
    if ":" not in path:
        print(f"Unexpected otpauth path: {path!r}", file=sys.stderr)
        sys.exit(1)
    cred_id = path.split(":", 1)[-1].strip()
    if not cred_id or not secret:
        sys.exit(1)
    return cred_id, secret


def _expiry_line(block: str) -> str | None:
    for line in block.splitlines():
        if "expires" in line.lower() and "credential" in line.lower():
            return line.strip()
    return None


def _print_current_code_from_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        print("Install python-dotenv: pip install python-dotenv", file=sys.stderr)
        sys.exit(1)
    try:
        import pyotp
    except ImportError:
        print("Install pyotp: pip install -r requirements-etrade-tools.txt", file=sys.stderr)
        sys.exit(1)
    load_dotenv(_ROOT / ".env")
    secret = (os.environ.get("ETRADE_TOTP_SECRET") or "").strip()
    if not secret:
        print(
            "ETRADE_TOTP_SECRET is unset. Add it to .env after a single provision run, "
            "or run this script without --current-code once to mint a credential.",
            file=sys.stderr,
        )
        sys.exit(1)
    totp = pyotp.TOTP(secret)
    interval = totp.interval
    rem = interval - int(time.time()) % interval
    print(f"Current code: {totp.now()}")
    print(f"Next code in: {rem}s")


def main() -> None:
    p = argparse.ArgumentParser(description="Provision VIP credential for E*Trade + ETRADE_TOTP_SECRET.")
    p.add_argument(
        "-t",
        "--token-model",
        default="SYMC",
        metavar="MODEL",
        help='VIP model (default SYMC mobile; try SYDC if E*Trade rejects SYMC).',
    )
    p.add_argument(
        "--show-code",
        action="store_true",
        help="After provisioning only: print 6-digit TOTP for that new credential.",
    )
    p.add_argument(
        "--current-code",
        action="store_true",
        help="Skip provisioning; print code + seconds until rollover from ETRADE_TOTP_SECRET (.env).",
    )
    args = p.parse_args()

    if args.current_code:
        _print_current_code_from_env()
        return

    print(
        "Each run without --current-code creates a NEW Symantec credential. "
        "For login/enrollment timing practice use: python scripts/etrade_vipaccess_provision.py --current-code",
        file=sys.stderr,
    )
    print("Calling Symantec provisioning (python-vipaccess)...", file=sys.stderr)
    block = _run_provision(args.token_model)
    cred_id, secret = _parse_otpauth(block)
    exp = _expiry_line(block)

    print()
    print("--- E*Trade enrollment (browser) ---")
    print(f"  Credential ID: {cred_id}")
    print("  Use this ID when E*Trade asks for your VIP / Security ID credential.")
    if exp:
        print(f"  ({exp})")
    print()
    print("--- Automation (GitHub secret / .env) ---")
    print("  Name:  ETRADE_TOTP_SECRET")
    print(f"  Value: {secret}")
    print()
    print("  Do not commit this value. Rotate if it leaks.")

    if args.show_code:
        try:
            import pyotp
        except ImportError:
            print(
                "\nSkipped --show-code: pyotp not installed. "
                "pip install -r requirements-etrade-tools.txt",
                file=sys.stderr,
            )
            return
        code = pyotp.TOTP(secret).now()
        print()
        print("--- pyotp sanity check ---")
        print(f"  Current code: {code} (compare with official VIP Access for this Credential ID)")


if __name__ == "__main__":
    main()
