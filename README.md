# PremiumHunter

A Streamlit app for hunting the best option premiums — built for Cash Secured Puts & Covered Calls using the E-Trade API.

## Prerequisites

- Python 3.10+
- An E-Trade brokerage account
- E-Trade API consumer key and secret ([request here](https://developer.etrade.com/getting-started))

## Setup

1. **Clone the repo**
   ```bash
   git clone <repo-url>
   cd PremiumHunter
   ```

2. **Create a virtual environment and install dependencies**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate        # Windows
   pip install -r requirements.txt
   ```

3. **Configure credentials**
   ```bash
   copy .env.example .env
   ```
   Edit `.env` and fill in your E-Trade consumer key and secret. Set `ETRADE_SANDBOX=True` for sandbox or `False` for live trading.

4. **Run the app**
   ```bash
   streamlit run app.py
   ```

5. **Authenticate** — Click "Connect to E-Trade" in the sidebar, follow the authorization link, and paste the verification code back into the app.

## Project Structure

```
PremiumHunter/
├── app.py              # Streamlit frontend
├── etrade_auth.py      # E-Trade OAuth authentication
├── etrade_market.py    # Market data & option chain fetching
├── requirements.txt    # Python dependencies
├── .env.example        # Credential template
└── .gitignore
```
