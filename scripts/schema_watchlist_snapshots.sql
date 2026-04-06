-- Run once on your Postgres before the first sync.
-- Timestamps are wall time in America/Chicago, second precision (no fractional seconds).

-- 1. Tracks the E*Trade OAuth session used for each run.
CREATE TABLE IF NOT EXISTS etrade_sessions (
    session_id          SERIAL PRIMARY KEY,
    access_token        TEXT NOT NULL,
    access_token_secret TEXT NOT NULL,
    last_renewed        TIMESTAMP(0) DEFAULT (date_trunc('second', timezone('America/Chicago', now())))::timestamp(0)
);

-- 1b. Watchlist symbols per owner (GitHub scan merges every row’s symbols, deduped). Full overwrite on upsert.
CREATE TABLE IF NOT EXISTS watchlists (
    owner        VARCHAR(64) PRIMARY KEY,
    symbols      JSONB NOT NULL,
    updated_at   TIMESTAMP(0) DEFAULT (date_trunc('second', timezone('America/Chicago', now())))::timestamp(0)
);

-- 2. Raw option-chain data scanned per symbol + strike + expiry.
CREATE TABLE IF NOT EXISTS options_scans (
    session_id  INTEGER NOT NULL REFERENCES etrade_sessions(session_id),
    scan_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol      VARCHAR(10) NOT NULL,
    strategy    VARCHAR(32),
    strike      NUMERIC,
    underlying_price NUMERIC,
    expiry      DATE,
    dte         INTEGER,
    otm_pct     NUMERIC,
    mo_yield    NUMERIC,
    iv          NUMERIC,
    iv_rank     NUMERIC,
    earn_date   DATE,
    gamma       NUMERIC,
    wheel_alpha NUMERIC,
    create_ts   TIMESTAMP(0) DEFAULT (date_trunc('second', timezone('America/Chicago', now())))::timestamp(0)
);

CREATE INDEX IF NOT EXISTS idx_options_scans_session ON options_scans (session_id);
CREATE INDEX IF NOT EXISTS idx_options_scans_symbol ON options_scans (symbol);
CREATE INDEX IF NOT EXISTS idx_options_scans_expiry ON options_scans (expiry);
CREATE INDEX IF NOT EXISTS idx_options_scans_wheel_alpha ON options_scans (wheel_alpha DESC NULLS LAST);

-- ── Existing databases (idempotent where possible) ───────────────────────────
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'etrade_sessions'
          AND column_name = 'id'
    ) THEN
        ALTER TABLE etrade_sessions RENAME COLUMN id TO session_id;
    END IF;
END $$;

ALTER TABLE etrade_sessions DROP COLUMN IF EXISTS expires_at;

ALTER TABLE etrade_sessions ALTER COLUMN last_renewed TYPE TIMESTAMP(0);
ALTER TABLE etrade_sessions
    ALTER COLUMN last_renewed SET DEFAULT (date_trunc('second', timezone('America/Chicago', now())))::timestamp(0);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'options_scans'
          AND column_name = 'created_at'
    ) THEN
        ALTER TABLE options_scans RENAME COLUMN created_at TO create_ts;
    END IF;
END $$;

ALTER TABLE options_scans
    ALTER COLUMN create_ts TYPE TIMESTAMP(0),
    ALTER COLUMN create_ts SET DEFAULT (date_trunc('second', timezone('America/Chicago', now())))::timestamp(0);

ALTER TABLE options_scans ADD COLUMN IF NOT EXISTS strategy VARCHAR(32);
ALTER TABLE options_scans ADD COLUMN IF NOT EXISTS wheel_alpha NUMERIC;
ALTER TABLE options_scans ADD COLUMN IF NOT EXISTS session_id INTEGER;
ALTER TABLE options_scans ADD COLUMN IF NOT EXISTS underlying_price NUMERIC;
ALTER TABLE options_scans ADD COLUMN IF NOT EXISTS earn_date DATE;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'options_scans_session_id_fkey'
    ) THEN
        ALTER TABLE options_scans
            ADD CONSTRAINT options_scans_session_id_fkey
            FOREIGN KEY (session_id) REFERENCES etrade_sessions(session_id);
    END IF;
END $$;
