import datetime as dt
import pyetrade
import pandas as pd
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


def get_expiry_dates(market, symbol: str) -> list[dict]:
    """Fetch available option expiration dates for a symbol."""
    resp = market.get_option_expire_date(symbol, resp_format="json")
    dates = resp.get("OptionExpireDateResponse", {}).get("ExpirationDate", [])
    return dates


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

    pairs = (
        resp.get("OptionChainResponse", {})
        .get("OptionPair", [])
    )

    rows = []
    for pair in pairs:
        for side in ("Call", "Put"):
            option = pair.get(side)
            if not option:
                continue
            rows.append({
                "Type": side,
                "Symbol": option.get("symbol", ""),
                "Strike": option.get("strikePrice", 0),
                "Bid": option.get("bid", 0),
                "Ask": option.get("ask", 0),
                "Last": option.get("lastPrice", 0),
                "Volume": option.get("volume", 0),
                "Open Interest": option.get("openInterest", 0),
                "IV": option.get("iv", 0),
                "In The Money": option.get("inTheMoney", ""),
            })

    df = pd.DataFrame(rows)
    return df


def get_quote(market, symbol: str) -> dict:
    """Get a basic quote for the underlying symbol."""
    resp = market.get_quote([symbol], resp_format="json")
    quote_data = resp.get("QuoteResponse", {}).get("QuoteData", [])
    if quote_data:
        return quote_data[0]
    return {}
