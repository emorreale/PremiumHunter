import datetime as dt

import pandas as pd
import pyetrade

from etrade_auth import CONSUMER_KEY, CONSUMER_SECRET, IS_SANDBOX


def create_market_session(tokens: dict):
    """Create an ETradeMarket session from access tokens."""
    return pyetrade.ETradeMarket(
        CONSUMER_KEY,
        CONSUMER_SECRET,
        tokens["oauth_token"],
        tokens["oauth_token_secret"],
        dev=IS_SANDBOX,
    )


def _as_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _first(d, *keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _quote_data_rows(resp: dict) -> list:
    root = _first(resp, "QuoteResponse", "quoteResponse") or {}
    rows = _first(root, "QuoteData", "quoteData")
    return _as_list(rows)


def _quote_row_symbol(row: dict) -> str:
    prod = _first(row, "Product", "product") or {}
    return str(_first(prod, "symbol", "Symbol") or "").upper()


def _last_trade_from_quote_row(row: dict):
    for block in (
        _first(row, "all", "All"),
        _first(row, "intraday", "Intraday"),
        _first(row, "fundamental", "Fundamental"),
    ):
        if isinstance(block, dict):
            v = _first(block, "lastTrade", "LastTrade")
            if v is not None and v != "":
                return v
    return None


def _quote_status(row: dict) -> str:
    return str(_first(row, "quoteStatus", "QuoteStatus") or "")


def get_quote(market, symbol: str) -> dict:
    """
    Return the QuoteData row for ``symbol`` from the E*Trade quote API.
    JSON responses use camelCase (quoteResponse, quoteData, all); XML-style
    PascalCase keys are accepted too.
    """
    resp = market.get_quote([symbol], resp_format="json")
    rows = _quote_data_rows(resp)
    want = symbol.upper().strip()
    matched = {}
    for row in rows:
        if _quote_row_symbol(row) == want:
            matched = row
            break
    if not matched and rows:
        matched = rows[0]
    import copy
    matched = copy.deepcopy(matched)
    matched["_raw_response"] = resp
    return matched


def get_last_trade_price(quote_row: dict):
    """Best-effort last trade from a QuoteData row; None if unknown."""
    if not quote_row:
        return None
    return _last_trade_from_quote_row(quote_row)


def get_equity_display_price(quote_row: dict) -> tuple:
    """
    Return (price, hint) for display. hint is None for last trade, or a short
    note if bid/ask midpoint was used.
    """
    if not quote_row:
        return None, None
    lt = _last_trade_from_quote_row(quote_row)
    if lt is not None:
        return lt, None
    for block in (
        _first(quote_row, "all", "All"),
        _first(quote_row, "intraday", "Intraday"),
    ):
        if not isinstance(block, dict):
            continue
        bid = _first(block, "bid", "Bid")
        ask = _first(block, "ask", "Ask")
        if bid is not None and ask is not None:
            try:
                return (float(bid) + float(ask)) / 2, "Mid of bid/ask (no last trade)"
            except (TypeError, ValueError):
                continue
    return None, None


def get_quote_status(quote_row: dict) -> str:
    return _quote_status(quote_row)


def get_expiry_dates(market, symbol: str) -> list[dict]:
    """Fetch option expiration entries (year/month/day) for a symbol."""
    resp = market.get_option_expire_date(symbol, resp_format="json")
    root = _first(resp, "OptionExpireDateResponse", "optionExpireDateResponse") or {}
    raw = _first(root, "expirationDates", "ExpirationDate")
    return _as_list(raw)


def _option_side(pair: dict, side: str):
    if side == "Call":
        return _first(pair, "Call", "call", "optioncall", "optionCall")
    return _first(pair, "Put", "put", "optionPut", "optionput")


def _option_greek(option: dict, greek_key: str, *alt_keys: str):
    if not isinstance(option, dict):
        return 0
    g = _first(option, "OptionGreeks", "optionGreeks")
    if isinstance(g, dict):
        for k in (greek_key, *alt_keys):
            v = g.get(k)
            if v is not None and v != "":
                return v
    v = option.get(greek_key)
    return v if v is not None else 0


def _option_iv(option: dict):
    return _option_greek(option, "iv", "IV")


def _option_pairs(resp: dict) -> list:
    root = _first(resp, "OptionChainResponse", "optionChainResponse") or {}
    raw = _first(root, "optionPairs", "OptionPair")
    return _as_list(raw)


def get_option_chain(
    market,
    symbol: str,
    expiry_date: dt.date = None,
    chain_type: str = None,
    no_of_strikes: int = None,
) -> pd.DataFrame:
    """
    Fetch the option chain for a symbol and return as a DataFrame.
    chain_type: 'CALL', 'PUT', or None for both.
    """
    resp = market.get_option_chains(
        symbol,
        expiry_date=expiry_date,
        chain_type=chain_type,
        no_of_strikes=no_of_strikes,
        resp_format="json",
    )

    pairs = _option_pairs(resp)

    rows = []
    for pair in pairs:
        for side in ("Call", "Put"):
            option = _option_side(pair, side)
            if not option:
                continue
            strike = _first(option, "strikePrice", "StrikePrice") or 0
            rows.append({
                "Type": side,
                "Symbol": option.get("symbol", "") or option.get("Symbol", ""),
                "Strike": strike,
                "Bid": option.get("bid", option.get("Bid", 0)),
                "Ask": option.get("ask", option.get("Ask", 0)),
                "Last": _first(option, "lastPrice", "LastPrice") or 0,
                "Volume": option.get("volume", option.get("Volume", 0)),
                "Open Interest": _first(option, "openInterest", "OpenInterest") or 0,
                "IV": _option_iv(option),
                "Gamma": _option_greek(option, "gamma", "Gamma"),
                "In The Money": option.get("inTheMoney", option.get("InTheMoney", "")),
            })

    return pd.DataFrame(rows)
