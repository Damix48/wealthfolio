-- Add justETF market data provider
INSERT OR IGNORE INTO market_data_providers (id, name, description, url, priority, enabled, logo_filename, last_synced_at, last_sync_status, last_sync_error)
VALUES ('JUST_ETF', 'justETF', 'European ETF prices, historical data, and profile information from justETF.com. No API key required.', 'https://www.justetf.com', 12, TRUE, 'just-etf.png', NULL, NULL, NULL);
