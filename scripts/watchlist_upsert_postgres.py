#!/usr/bin/env python3
"""
Upsert one row into watchlists (owner → symbols JSON array). Use this to store tickers in
Postgres so watchlist_snapshot_to_postgres.py can read them instead of WATCHLIST_JSON.

Required env:
  DATABASE_URL

Optional:
  --owner NAME     — primary key for the row to upsert (default "default"); the scan job merges symbols from every row.

Input (first match wins):
  1. --file PATH   — JSON array or { "tickers": [...] } / { "symbols": [...] }
  2. stdin         — if --file - or pipe JSON
  3. WATCHLIST_JSON env

Also honors DATABASE_FORCE_IPV4 / DATABASE_IPV4 like the snapshot script.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

from watchlist_db import normalize_watchlist_symbols, sync_watchlist_to_postgres

load_dotenv(_ROOT / ".env")


def _load_raw_json(args: argparse.Namespace) -> object:
    if args.file:
        if args.file == "-":
            raw_text = sys.stdin.read()
        else:
            p = Path(args.file)
            if not p.is_file():
                print(f"File not found: {args.file}", file=sys.stderr)
                sys.exit(1)
            raw_text = p.read_text(encoding="utf-8")
    else:
        raw_text = (os.environ.get("WATCHLIST_JSON") or "").strip()
        if not raw_text:
            print(
                "Provide --file PATH, pipe JSON to stdin with --file -, or set WATCHLIST_JSON.",
                file=sys.stderr,
            )
            sys.exit(1)
    return json.loads(raw_text)


def main() -> int:
    parser = argparse.ArgumentParser(description="Upsert watchlists row in Postgres.")
    parser.add_argument(
        "--file",
        "-f",
        metavar="PATH",
        help='JSON file, or "-" for stdin',
    )
    parser.add_argument(
        "--owner",
        "-o",
        default="default",
        help='watchlists.owner primary key (default "default")',
    )
    args = parser.parse_args()

    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 1

    owner = (args.owner or "default").strip() or "default"

    try:
        raw = _load_raw_json(args)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}", file=sys.stderr)
        return 1

    if not isinstance(raw, list) and not isinstance(raw, dict):
        print("Watchlist must be a JSON array or {tickers: [...]} / {symbols: [...]}", file=sys.stderr)
        return 1

    symbols = normalize_watchlist_symbols(raw)

    try:
        sync_watchlist_to_postgres(symbols, owner=owner)
    except Exception as e:
        print(f"Postgres upsert failed: {e}", file=sys.stderr)
        return 1

    print(f"Upserted watchlists row owner={owner!r} with {len(symbols)} symbol(s): {', '.join(symbols) or '(empty)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
