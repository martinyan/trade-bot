INSERT INTO sec_13f_symbol_map (symbol, cusip, issuer_name, source, is_active, updated_at)
VALUES
    ('AAPL', '037833100', 'Apple Inc.', 'seed', TRUE, NOW()),
    ('MSFT', '594918104', 'Microsoft Corp.', 'seed', TRUE, NOW()),
    ('NVDA', '67066G104', 'NVIDIA Corp.', 'seed', TRUE, NOW()),
    ('AMZN', '023135106', 'Amazon.com, Inc.', 'seed', TRUE, NOW()),
    ('GOOGL', '02079K305', 'Alphabet Inc. Class A', 'seed', TRUE, NOW()),
    ('GOOG', '02079K107', 'Alphabet Inc. Class C', 'seed', TRUE, NOW()),
    ('META', '30303M102', 'Meta Platforms, Inc.', 'seed', TRUE, NOW()),
    ('TSLA', '88160R101', 'Tesla, Inc.', 'seed', TRUE, NOW()),
    ('AVGO', '11135F101', 'Broadcom Inc.', 'seed', TRUE, NOW()),
    ('AMD', '007903107', 'Advanced Micro Devices, Inc.', 'seed', TRUE, NOW()),
    ('NFLX', '64110L106', 'Netflix, Inc.', 'seed', TRUE, NOW()),
    ('JPM', '46625H100', 'JPMorgan Chase & Co.', 'seed', TRUE, NOW()),
    ('V', '92826C839', 'Visa Inc.', 'seed', TRUE, NOW()),
    ('MA', '57636Q104', 'Mastercard Inc.', 'seed', TRUE, NOW()),
    ('COST', '22160K105', 'Costco Wholesale Corp.', 'seed', TRUE, NOW()),
    ('PLTR', '69608A108', 'Palantir Technologies Inc.', 'seed', TRUE, NOW()),
    ('MU', '595112103', 'Micron Technology, Inc.', 'seed', TRUE, NOW()),
    ('BRK.B', '084670702', 'Berkshire Hathaway Inc. Class B', 'seed', TRUE, NOW())
ON CONFLICT (symbol)
DO UPDATE SET
    cusip = EXCLUDED.cusip,
    issuer_name = EXCLUDED.issuer_name,
    source = EXCLUDED.source,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();
