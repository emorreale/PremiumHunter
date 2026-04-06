-- Run once on your Postgres before the first sync.

-- 1. Tracks the E*Trade OAuth session used for each run.
CREATE TABLE IF NOT EXISTS etrade_sessions (
    id               SERIAL PRIMARY KEY,
    access_token     TEXT NOT NULL,
    access_token_secret TEXT NOT NULL,
    expires_at       TIMESTAMP,
    last_renewed     TIMESTAMP DEFAULT NOW()
);

-- 2. Raw option-chain data scanned per symbol + strike + expiry.
CREATE TABLE IF NOT EXISTS options_scans (
    scan_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol      VARCHAR(10) NOT NULL,
    strike      NUMERIC,
    expiry      DATE,
    dte         INTEGER,
    otm_pct     NUMERIC,
    mo_yield    NUMERIC,
    iv          NUMERIC,
    iv_rank     NUMERIC,
    gamma       NUMERIC,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_options_scans_symbol ON options_scans (symbol);
CREATE INDEX IF NOT EXISTS idx_options_scans_expiry ON options_scans (expiry);

-- 3. Computed Wheel Alpha results linked back to scanned rows.
CREATE TABLE IF NOT EXISTS wheel_alpha_results (
    result_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scan_id          UUID REFERENCES options_scans(scan_id),
    base_score       NUMERIC,
    gamma_tax_applied NUMERIC,
    final_alpha_score NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_wheel_alpha_results_score
    ON wheel_alpha_results (final_alpha_score DESC);
