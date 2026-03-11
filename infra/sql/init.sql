CREATE TABLE IF NOT EXISTS strategy_signals (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    signal TEXT NOT NULL,
    confidence NUMERIC(5,2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sec_13f_dataset (
    id BIGSERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL UNIQUE,
    report_period DATE NOT NULL,
    source_url TEXT NOT NULL,
    load_status TEXT NOT NULL,
    loaded_at TIMESTAMPTZ,
    row_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_text TEXT
);

CREATE INDEX IF NOT EXISTS sec_13f_dataset_report_period_idx
    ON sec_13f_dataset (report_period);

CREATE TABLE IF NOT EXISTS sec_13f_filing (
    id BIGSERIAL PRIMARY KEY,
    dataset_id BIGINT NOT NULL REFERENCES sec_13f_dataset(id) ON DELETE CASCADE,
    accession_number TEXT NOT NULL UNIQUE,
    cik TEXT NOT NULL,
    manager_name TEXT NOT NULL,
    report_period DATE NOT NULL,
    filed_at DATE,
    submission_type TEXT NOT NULL,
    is_amendment BOOLEAN NOT NULL DEFAULT FALSE,
    other_manager_included BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS sec_13f_filing_report_period_cik_idx
    ON sec_13f_filing (report_period, cik);

CREATE INDEX IF NOT EXISTS sec_13f_filing_dataset_id_idx
    ON sec_13f_filing (dataset_id);

CREATE TABLE IF NOT EXISTS sec_13f_holding (
    id BIGSERIAL PRIMARY KEY,
    filing_id BIGINT NOT NULL REFERENCES sec_13f_filing(id) ON DELETE CASCADE,
    report_period DATE NOT NULL,
    cik TEXT NOT NULL,
    manager_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    issuer_name TEXT,
    class_title TEXT,
    value_thousands BIGINT,
    shares BIGINT,
    share_type TEXT,
    put_call TEXT,
    investment_discretion TEXT,
    voting_sole BIGINT,
    voting_shared BIGINT,
    voting_none BIGINT
);

CREATE INDEX IF NOT EXISTS sec_13f_holding_report_period_cusip_idx
    ON sec_13f_holding (report_period, cusip);

CREATE INDEX IF NOT EXISTS sec_13f_holding_cusip_cik_report_period_idx
    ON sec_13f_holding (cusip, cik, report_period);

CREATE INDEX IF NOT EXISTS sec_13f_holding_report_period_manager_name_idx
    ON sec_13f_holding (report_period, manager_name);

CREATE INDEX IF NOT EXISTS sec_13f_holding_filing_id_idx
    ON sec_13f_holding (filing_id);

CREATE TABLE IF NOT EXISTS sec_13f_symbol_map (
    symbol TEXT PRIMARY KEY,
    cusip TEXT NOT NULL,
    issuer_name TEXT,
    source TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS sec_13f_symbol_map_cusip_symbol_idx
    ON sec_13f_symbol_map (cusip, symbol);

CREATE TABLE IF NOT EXISTS sec_13f_recent_holding (
    id BIGSERIAL PRIMARY KEY,
    filing_id BIGINT NOT NULL REFERENCES sec_13f_filing(id) ON DELETE CASCADE,
    report_period DATE NOT NULL,
    cik TEXT NOT NULL,
    manager_name TEXT NOT NULL,
    cusip TEXT NOT NULL,
    issuer_name TEXT,
    class_title TEXT,
    value_thousands BIGINT,
    shares BIGINT,
    share_type TEXT,
    put_call TEXT,
    investment_discretion TEXT,
    voting_sole BIGINT,
    voting_shared BIGINT,
    voting_none BIGINT,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS sec_13f_recent_holding_cusip_report_period_idx
    ON sec_13f_recent_holding (cusip, report_period);

CREATE INDEX IF NOT EXISTS sec_13f_recent_holding_report_period_cik_idx
    ON sec_13f_recent_holding (report_period, cik);

CREATE INDEX IF NOT EXISTS sec_13f_recent_holding_filing_id_idx
    ON sec_13f_recent_holding (filing_id);
