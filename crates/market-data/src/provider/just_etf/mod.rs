//! justETF market data provider.
//!
//! Fetches ETF quotes, historical prices, dividends, and profile data
//! from justETF.com using ISIN-based lookups. No API key required.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use log::debug;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, USER_AGENT};
use reqwest::Client;
use rust_decimal::Decimal;
use scraper::{Html, Selector};
use serde::Deserialize;
use std::time::Duration;

use crate::errors::MarketDataError;
use crate::models::{
    AssetProfile, DividendEvent, InstrumentKind, ProviderInstrument, Quote, QuoteContext,
    SearchResult,
};
use crate::provider::{MarketDataProvider, ProviderCapabilities, RateLimit};

const PROVIDER_ID: &str = "JUST_ETF";
const BASE_URL: &str = "https://www.justetf.com";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

// ---------------------------------------------------------------------------
// API response structs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RawValue {
    raw: f64,
    #[allow(dead_code)]
    localized: String,
}

#[derive(Debug, Deserialize)]
struct QuoteLowHigh {
    low: RawValue,
    high: RawValue,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuoteResponse {
    latest_quote: RawValue,
    latest_quote_date: String,
    #[allow(dead_code)]
    previous_quote: Option<RawValue>,
    #[allow(dead_code)]
    previous_quote_date: Option<String>,
    #[allow(dead_code)]
    dtd_prc: Option<RawValue>,
    #[allow(dead_code)]
    dtd_amt: Option<RawValue>,
    #[allow(dead_code)]
    quote_trading_venue: Option<String>,
    #[allow(dead_code)]
    quote_low_high: Option<QuoteLowHigh>,
}

#[derive(Debug, Deserialize)]
struct ChartPoint {
    date: String,
    value: RawValue,
}

#[derive(Debug, Deserialize)]
struct ChartFeature {
    #[serde(rename = "DIVIDENDS", default)]
    dividends: Vec<ChartPoint>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChartResponse {
    #[allow(dead_code)]
    latest_quote: Option<RawValue>,
    #[allow(dead_code)]
    latest_quote_date: Option<String>,
    series: Vec<ChartPoint>,
    #[allow(dead_code)]
    features: Option<ChartFeature>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScreenerItem {
    isin: String,
    name: String,
    #[allow(dead_code)]
    ticker: Option<String>,
    fund_currency: Option<String>,
    #[allow(dead_code)]
    domicile_country: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScreenerResponse {
    data: Vec<ScreenerItem>,
}

// ---------------------------------------------------------------------------
// Provider struct
// ---------------------------------------------------------------------------

pub struct JustEtfProvider {
    client: Client,
    ajax_client: Client,
}

impl JustEtfProvider {
    pub fn new() -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(UA));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .default_headers(headers)
            .build()
            .unwrap_or_else(|_| Client::new());

        let ajax_client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent(UA)
            .cookie_store(true)
            .build()
            .unwrap_or_else(|_| Client::new());

        Self { client, ajax_client }
    }

    fn extract_isin<'a>(&self, instrument: &'a ProviderInstrument) -> Option<&'a str> {
        match instrument {
            ProviderInstrument::EquitySymbol { symbol } => Some(symbol.as_ref()),
            _ => None,
        }
    }

    fn resolve_currency(&self, context: &QuoteContext) -> String {
        context
            .currency_hint
            .as_ref()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "EUR".to_string())
    }

    // -----------------------------------------------------------------------
    // Search / screener
    // -----------------------------------------------------------------------

    async fn extract_wicket_counter(&self) -> Result<String, MarketDataError> {
        let url = format!("{}/en/search.html?search=ETFS", BASE_URL);
        let resp = self.client.get(&url).send().await.map_err(|e| {
            MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Failed to load search page: {}", e),
            }
        })?;
        let html = resp.text().await.map_err(|e| MarketDataError::ProviderError {
            provider: PROVIDER_ID.to_string(),
            message: format!("Failed to read search page: {}", e),
        })?;

        let marker = "-1.0-container-tabsContentContainer-tabsContentRepeater-1-container-content-etfsTablePanel&search=ETFS&_wicket=1";

        if let Some(pos) = html.find(marker) {
            let before = &html[..pos];
            if let Some(digit_start) = before.rfind(|c: char| !c.is_ascii_digit()) {
                let counter = &before[digit_start + 1..];
                let counter: u64 = counter.parse().unwrap_or(0);
                return Ok(counter.to_string());
            }
        }

        Ok("0".to_string())
    }

    async fn search_etfs(
        &self,
        query: &str,
    ) -> Result<Vec<SearchResult>, MarketDataError> {
        let counter = self.extract_wicket_counter().await?;

        let url = format!(
            "{}/en/search.html?{}-1.0-container-tabsContentContainer-tabsContentRepeater-1-container-content-etfsTablePanel=&search=ETFS&_wicket=1",
            BASE_URL, counter
        );

        let etfs_params = format!("search=ETF&productGroup=epg-longOnly&ls=any&query={}", query);

        let form = [
            ("draw", "1"),
            ("start", "0"),
            ("length", "20"),
            ("lang", "en"),
            ("country", "DE"),
            ("universeType", "private"),
            ("defaultCurrency", "EUR"),
            ("etfsParams", &etfs_params),
        ];

        let resp = self
            .client
            .post(&url)
            .form(&form)
            .send()
            .await
            .map_err(|e| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Search request failed: {}", e),
            })?;

        if !resp.status().is_success() {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Search returned HTTP {}", resp.status()),
            });
        }

        let body: ScreenerResponse =
            resp.json()
                .await
                .map_err(|e| MarketDataError::ProviderError {
                    provider: PROVIDER_ID.to_string(),
                    message: format!("Search JSON parse error: {}", e),
                })?;

        let results: Vec<SearchResult> = body
            .data
            .into_iter()
            .map(|item| SearchResult {
                symbol: item.isin,
                name: item.name,
                exchange: String::new(),
                exchange_mic: None,
                exchange_name: None,
                asset_type: "ETF".to_string(),
                currency: item.fund_currency,
                score: None,
                data_source: Some(PROVIDER_ID.to_string()),
            })
            .collect();

        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Quote endpoint
    // -----------------------------------------------------------------------

    async fn fetch_quote(
        &self,
        isin: &str,
        currency: &str,
    ) -> Result<QuoteResponse, MarketDataError> {
        let url = format!(
            "{}/api/etfs/{}/quote?locale=en&currency={}",
            BASE_URL, isin, currency
        );

        debug!("justETF fetch_quote: {}", url);

        let resp = self.client.get(&url).send().await.map_err(|e| {
            MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP request failed: {}", e),
            }
        })?;

        if !resp.status().is_success() {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP {}", resp.status()),
            });
        }

        resp.json().await.map_err(|e| MarketDataError::ProviderError {
            provider: PROVIDER_ID.to_string(),
            message: format!("JSON parse error: {}", e),
        })
    }

    // -----------------------------------------------------------------------
    // History / performance-chart endpoint
    // -----------------------------------------------------------------------

    async fn fetch_chart(
        &self,
        isin: &str,
        currency: &str,
    ) -> Result<ChartResponse, MarketDataError> {
        let url = format!(
            "{}/api/etfs/{}/performance-chart?locale=en&currency={}&valuesType=MARKET_VALUE&reduceData=false&includeDividends=true&features=DIVIDENDS",
            BASE_URL, isin, currency
        );

        debug!("justETF fetch_chart: {}", url);

        let resp = self.client.get(&url).send().await.map_err(|e| {
            MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP request failed: {}", e),
            }
        })?;

        if !resp.status().is_success() {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP {}", resp.status()),
            });
        }

        resp.json().await.map_err(|e| MarketDataError::ProviderError {
            provider: PROVIDER_ID.to_string(),
            message: format!("JSON parse error: {}", e),
        })
    }

    // -----------------------------------------------------------------------
    // Profile (HTML scraping + AJAX)
    // -----------------------------------------------------------------------

    fn extract_profile_page_counter<'a>(&self, html: &'a str) -> &'a str {
        let marker = "-1.0-holdingsSection-sectors-loadMoreSectors";
        if let Some(pos) = html.find(marker) {
            let before = &html[..pos];
            if let Some(digit_start) = before.rfind(|c: char| !c.is_ascii_digit()) {
                return &before[digit_start + 1..];
            }
        }
        "0"
    }

    async fn fetch_ajax_full_table(
        &self,
        isin: &str,
        counter: &str,
        endpoint_suffix: &str,
    ) -> Result<Vec<(String, f64)>, MarketDataError> {
        let page_url = format!("{}/en/etf-profile.html?isin={}", BASE_URL, isin);
        let ajax_url = format!(
            "{}/en/etf-profile.html?{}-1.0-holdingsSection-{}&isin={}&_wicket=1",
            BASE_URL, counter, endpoint_suffix, isin
        );

        let base_url = format!("en/etf-profile.html?isin={}", isin);
        let resp = self
            .ajax_client
            .get(&ajax_url)
            .header("X-Requested-With", "XMLHttpRequest")
            .header("Wicket-Ajax", "true")
            .header("Wicket-Ajax-BaseURL", &base_url)
            .header(ACCEPT, "application/xml, text/xml, */*; q=0.01")
            .header(reqwest::header::REFERER, &page_url)
            .send()
            .await
            .map_err(|e| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("AJAX request failed: {}", e),
            })?;

        let xml = resp.text().await.map_err(|e| MarketDataError::ProviderError {
            provider: PROVIDER_ID.to_string(),
            message: format!("Failed to read AJAX response: {}", e),
        })?;

        Ok(self.parse_cdata_rows(&xml))
    }

    fn parse_cdata_rows(&self, xml: &str) -> Vec<(String, f64)> {
        let mut results = Vec::new();
        let mut rest = xml;

        while let Some(cdata_start) = rest.find("<![CDATA[") {
            let content_start = cdata_start + "<![CDATA[".len();
            let content = &rest[content_start..];
            if let Some(cdata_end) = content.find("]]>") {
                let cdata = &content[..cdata_end];

                let fragment = Html::parse_fragment(cdata);
                let row_sel = Selector::parse(
                    "tr[data-testid='etf-holdings_countries_row'], tr[data-testid='etf-holdings_sectors_row']"
                );
                let name_sel = Selector::parse(
                    "*[data-testid='tl_etf-holdings_countries_value_name'], *[data-testid='tl_etf-holdings_sectors_value_name']"
                );
                let pct_sel = Selector::parse(
                    "*[data-testid='tl_etf-holdings_countries_value_percentage'], *[data-testid='tl_etf-holdings_sectors_value_percentage']"
                );

                if let (Ok(row_s), Ok(name_s), Ok(pct_s)) = (&row_sel, &name_sel, &pct_sel) {
                    for row in fragment.select(row_s) {
                        let name = row.select(name_s).next().map(|el| {
                            el.text().collect::<Vec<_>>().join(" ").trim().to_string()
                        });
                        let pct_str = row.select(pct_s).next().map(|el| {
                            el.text().collect::<Vec<_>>().join(" ").trim().to_string()
                        });

                        if let (Some(name), Some(pct_str)) = (name, pct_str) {
                            if name.eq_ignore_ascii_case("Other") {
                                continue;
                            }
                            if let Ok(pct) = pct_str.trim_end_matches('%').replace(',', ".").parse::<f64>() {
                                results.push((name, pct / 100.0));
                            }
                        }
                    }
                }

                rest = &content[cdata_end + "]]>".len()..];
                continue;
            }
            break;
        }

        results
    }

    async fn fetch_profile_page(&self, isin: &str) -> Result<String, MarketDataError> {
        let url = format!("{}/en/etf-profile.html?isin={}", BASE_URL, isin);

        debug!("justETF fetch_profile_page: {}", url);

        let resp = self.ajax_client.get(&url).send().await.map_err(|e| {
            MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP request failed: {}", e),
            }
        })?;

        if !resp.status().is_success() {
            return Err(MarketDataError::SymbolNotFound(format!(
                "ETF profile page returned HTTP {} for ISIN {}",
                resp.status(),
                isin
            )));
        }

        resp.text().await.map_err(|e| MarketDataError::ProviderError {
            provider: PROVIDER_ID.to_string(),
            message: format!("Failed to read profile page: {}", e),
        })
    }

    async fn parse_profile_html(&self, html: &str, isin: &str) -> AssetProfile {
        let (name, description, country, dividend_yield, counter) = {
            let document = Html::parse_document(html);
            let name = self.extract_name(&document);
            let description = self.extract_description(&document);
            let country = self.extract_domicile(&document);
            let dividend_yield = self.extract_dividend_yield(&document);
            let counter = self.extract_profile_page_counter(html).to_string();
            (name, description, country, dividend_yield, counter)
        };

        let sectors = self.fetch_ajax_full_table(isin, &counter, "sectors-loadMoreSectors").await.ok();
        let regions = self.fetch_ajax_full_table(isin, &counter, "countries-loadMoreCountries").await.ok();

        let sectors_json = sectors.map(|s| {
            let items: Vec<serde_json::Value> = s
                .into_iter()
                .map(|(name, weight)| serde_json::json!({ "name": name, "weight": weight }))
                .collect();
            serde_json::to_string(&items).unwrap_or_default()
        });

        let regions_json = regions.map(|r| {
            let items: Vec<serde_json::Value> = r
                .into_iter()
                .map(|(name, weight)| serde_json::json!({ "name": name, "weight": weight }))
                .collect();
            serde_json::to_string(&items).unwrap_or_default()
        });

        AssetProfile {
            source: Some(PROVIDER_ID.to_string()),
            name,
            quote_type: Some("ETF".to_string()),
            description,
            country,
            countries: regions_json,
            sectors: sectors_json,
            isin: Some(isin.to_string()),
            dividend_yield,
            ..Default::default()
        }
    }

    fn extract_name(&self, document: &Html) -> Option<String> {
        let selector = Selector::parse("h1").ok()?;
        document
            .select(&selector)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" ").trim().to_string())
    }

    fn extract_description(&self, document: &Html) -> Option<String> {
        let label_selector =
            Selector::parse("*[data-testid='etf-quote-section_description-label']").ok()?;
        if let Some(el) = document.select(&label_selector).next() {
            let text: String = el.text().collect::<Vec<_>>().join(" ").trim().to_string();
            if !text.is_empty() {
                return Some(text);
            }
        }

        let selector = Selector::parse("meta[name='description']").ok()?;
        if let Some(el) = document.select(&selector).next() {
            return el.value().attr("content").map(|s| s.trim().to_string());
        }
        None
    }

    fn extract_domicile(&self, document: &Html) -> Option<String> {
        let selector = Selector::parse(
            "tr[data-testid='etf-basics_row_domicile-country'] td.val"
        ).ok()?;
        document
            .select(&selector)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" ").trim().to_string())
    }

    fn extract_dividend_yield(&self, document: &Html) -> Option<f64> {
        let selector = Selector::parse(
            "tr[data-testid='etf-basics_row_distribution-policy'] td.val"
        ).ok()?;
        let policy = document
            .select(&selector)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" ").trim().to_lowercase())?;

        if policy.contains("accumulating") {
            return None;
        }

        let yield_selector = Selector::parse(
            "tr[data-testid='etf-basics_row_current-dividend-yield'] td.val"
        ).ok()?;
        if let Some(el) = document.select(&yield_selector).next() {
            let pct_str: String = el.text().collect();
            let pct = pct_str
                .trim()
                .trim_end_matches('%')
                .replace(',', ".")
                .parse::<f64>()
                .ok()?;
            return Some(pct / 100.0);
        }
        None
    }
}

impl Default for JustEtfProvider {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// MarketDataProvider impl
// ---------------------------------------------------------------------------

#[async_trait]
impl MarketDataProvider for JustEtfProvider {
    fn id(&self) -> &'static str {
        PROVIDER_ID
    }

    fn priority(&self) -> u8 {
        9
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            instrument_kinds: &[InstrumentKind::Equity],
            coverage: crate::models::Coverage::global_best_effort(),
            supports_latest: true,
            supports_historical: true,
            supports_search: true,
            supports_profile: true,
            supports_dividends: true,
        }
    }

    fn rate_limit(&self) -> RateLimit {
        RateLimit {
            requests_per_minute: 30,
            max_concurrency: 3,
            min_delay: Duration::from_millis(500),
        }
    }

    async fn get_latest_quote(
        &self,
        context: &QuoteContext,
        instrument: ProviderInstrument,
    ) -> Result<Quote, MarketDataError> {
        let isin = self
            .extract_isin(&instrument)
            .ok_or_else(|| MarketDataError::UnsupportedAssetType(format!("{:?}", instrument)))?;

        let currency = self.resolve_currency(context);
        let response = self.fetch_quote(isin, &currency).await?;

        let close = Decimal::try_from(response.latest_quote.raw).map_err(|_| {
            MarketDataError::ValidationFailed {
                message: format!(
                    "Failed to convert quote price {} to decimal",
                    response.latest_quote.raw
                ),
            }
        })?;

        let high = response.quote_low_high.as_ref().and_then(|lh| {
            Decimal::try_from(lh.high.raw).ok()
        });
        let low = response.quote_low_high.as_ref().and_then(|lh| {
            Decimal::try_from(lh.low.raw).ok()
        });

        let timestamp = NaiveDate::parse_from_str(&response.latest_quote_date, "%Y-%m-%d")
            .ok()
            .and_then(|d| {
                d.and_hms_opt(0, 0, 0)
                    .map(|dt| Utc.from_utc_datetime(&dt))
            })
            .unwrap_or_else(Utc::now);

        Ok(Quote {
            timestamp,
            open: None,
            high,
            low,
            close,
            volume: None,
            currency,
            source: PROVIDER_ID.to_string(),
        })
    }

    async fn get_historical_quotes(
        &self,
        context: &QuoteContext,
        instrument: ProviderInstrument,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Quote>, MarketDataError> {
        let isin = self
            .extract_isin(&instrument)
            .ok_or_else(|| MarketDataError::UnsupportedAssetType(format!("{:?}", instrument)))?;

        let currency = self.resolve_currency(context);
        let response = self.fetch_chart(isin, &currency).await?;

        let mut quotes: Vec<Quote> = response
            .series
            .iter()
            .filter_map(|point| {
                let date = NaiveDate::parse_from_str(&point.date, "%Y-%m-%d").ok()?;
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .map(|dt| Utc.from_utc_datetime(&dt))?;

                if dt < start || dt > end {
                    return None;
                }

                let close = Decimal::try_from(point.value.raw).ok()?;

                Some(Quote {
                    timestamp: dt,
                    open: None,
                    high: None,
                    low: None,
                    close,
                    volume: None,
                    currency: currency.clone(),
                    source: PROVIDER_ID.to_string(),
                })
            })
            .collect();

        // The chart endpoint only provides data through the previous trading day.
        // Fetch the latest quote endpoint separately so the sync captures
        // today's / the most recent available price.
        match self.fetch_quote(isin, &currency).await {
            Ok(latest) => {
                if let Ok(date) =
                    NaiveDate::parse_from_str(&latest.latest_quote_date, "%Y-%m-%d")
                {
                    if let Some(dt) = date
                        .and_hms_opt(0, 0, 0)
                        .map(|dt| Utc.from_utc_datetime(&dt))
                    {
                        if dt >= start
                            && dt <= end
                            && !quotes.iter().any(|q| q.timestamp.date_naive() == date)
                        {
                            if let Ok(close) =
                                Decimal::try_from(latest.latest_quote.raw)
                            {
                                let high = latest.quote_low_high.as_ref().and_then(|lh| {
                                    Decimal::try_from(lh.high.raw).ok()
                                });
                                let low = latest.quote_low_high.as_ref().and_then(|lh| {
                                    Decimal::try_from(lh.low.raw).ok()
                                });

                                quotes.push(Quote {
                                    timestamp: dt,
                                    open: None,
                                    high,
                                    low,
                                    close,
                                    volume: None,
                                    currency,
                                    source: PROVIDER_ID.to_string(),
                                });
                            }
                        }
                    }
                }
            }
            Err(e) => {
                debug!("justETF fetch_quote for latest in historical: {}", e);
            }
        }

        if quotes.is_empty() && !response.series.is_empty() {
            return Err(MarketDataError::NoDataForRange);
        }

        Ok(quotes)
    }

    async fn get_dividends(
        &self,
        _context: &QuoteContext,
        instrument: ProviderInstrument,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<DividendEvent>, MarketDataError> {
        let isin = self
            .extract_isin(&instrument)
            .ok_or_else(|| MarketDataError::NotSupported {
                operation: "dividends".to_string(),
                provider: self.id().to_string(),
            })?;

        let currency = "EUR";
        let response = self.fetch_chart(isin, currency).await?;

        let dividends = match response.features {
            Some(features) => features.dividends,
            None => return Ok(vec![]),
        };

        let events: Vec<DividendEvent> = dividends
            .iter()
            .filter_map(|d| {
                let date = NaiveDate::parse_from_str(&d.date, "%Y-%m-%d").ok()?;
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .map(|dt| Utc.from_utc_datetime(&dt))?;

                if dt < start || dt > end {
                    return None;
                }

                Some(DividendEvent {
                    amount: d.value.raw,
                    date: dt.timestamp(),
                })
            })
            .collect();

        Ok(events)
    }

    async fn get_profile(&self, symbol: &str) -> Result<AssetProfile, MarketDataError> {
        let html = self.fetch_profile_page(symbol).await?;
        Ok(self.parse_profile_html(&html, symbol).await)
    }

    async fn search(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError> {
        self.search_etfs(query).await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a string looks like an ISIN (2 uppercase letters + 10 alphanumeric).
pub fn looks_like_isin(s: &str) -> bool {
    s.len() == 12
        && s[..2].chars().all(|c| c.is_ascii_uppercase())
        && s[2..].chars().all(|c| c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::sync::Arc;

    use crate::models::InstrumentId;

    #[test]
    fn test_looks_like_isin() {
        assert!(looks_like_isin("IE00B4L5Y983"));
        assert!(looks_like_isin("DE0007164600"));
        assert!(looks_like_isin("US912810TH14"));
        assert!(!looks_like_isin("AAPL"));
        assert!(!looks_like_isin("123456789012"));
        assert!(!looks_like_isin("IE00B4L5Y98")); // too short
        assert!(!looks_like_isin("IE00B4L5Y9834")); // too long
    }

    #[test]
    fn test_extract_isin() {
        let provider = JustEtfProvider::new();
        let instrument = ProviderInstrument::EquitySymbol {
            symbol: Arc::from("IE00B4L5Y983"),
        };
        assert_eq!(
            provider.extract_isin(&instrument),
            Some("IE00B4L5Y983")
        );
    }

    #[test]
    fn test_resolve_currency_default() {
        let provider = JustEtfProvider::new();
        let context = QuoteContext {
            instrument: InstrumentId::Equity {
                ticker: Arc::from("IE00B4L5Y983"),
                mic: Some(Cow::Borrowed("XETR")),
            },
            identifiers: Default::default(),
            overrides: None,
            currency_hint: None,
            preferred_provider: None,
            bond_metadata: None,
            custom_provider_code: None,
        };
        assert_eq!(provider.resolve_currency(&context), "EUR");
    }

    #[test]
    fn test_resolve_currency_from_hint() {
        let provider = JustEtfProvider::new();
        let context = QuoteContext {
            instrument: InstrumentId::Equity {
                ticker: Arc::from("IE00B4L5Y983"),
                mic: Some(Cow::Borrowed("XETR")),
            },
            identifiers: Default::default(),
            overrides: None,
            currency_hint: Some(Cow::Borrowed("USD")),
            preferred_provider: None,
            bond_metadata: None,
            custom_provider_code: None,
        };
        assert_eq!(provider.resolve_currency(&context), "USD");
    }

    #[tokio::test]
    #[ignore = "requires network — run with cargo test -- --ignored just_etf_live"]
    async fn test_live_etf_profile() {
        let provider = JustEtfProvider::new();
        let isin = "GB00BJYDH287";

        println!("=== justETF profile for ISIN {} ===\n", isin);

        let profile = provider.get_profile(isin).await.unwrap();
        println!("source:       {:?}", profile.source);
        println!("name:         {:?}", profile.name);
        println!("quote_type:   {:?}", profile.quote_type);
        println!("description:  {:?}", profile.description.as_deref().map(|d| &d[..d.len().min(200)]));
        println!("country:      {:?}", profile.country);
        println!("dividend_yield: {:?}", profile.dividend_yield);
        println!("isin:         {:?}", profile.isin);

        if let Some(ref s) = profile.sectors {
            println!("\n--- Sectors ---");
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                println!("{}", serde_json::to_string_pretty(&parsed).unwrap());
            } else {
                println!("  (raw) {}", s);
            }
        } else {
            println!("\n--- Sectors: NONE ---");
        }

        if let Some(ref a) = profile.countries {
            println!("\n--- Countries ---");
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(a) {
                println!("{}", serde_json::to_string_pretty(&parsed).unwrap());
            } else {
                println!("  (raw) {}", a);
            }
        } else {
            println!("\n--- Countries: NONE ---");
        }

        println!("\n=== justETF quote for ISIN {} ===\n", isin);
        let quote = provider.fetch_quote(isin, "EUR").await.unwrap();
        println!("latest_quote: {}", quote.latest_quote.raw);
        println!("date:         {}", quote.latest_quote_date);
        println!("venue:        {:?}", quote.quote_trading_venue);

        println!("\n=== justETF dividends for ISIN {} (last 5) ===\n", isin);
        let chart = provider.fetch_chart(isin, "EUR").await.unwrap();
        if let Some(ref feats) = chart.features {
            for d in feats.dividends.iter().rev().take(5) {
                println!("  {} -> {}", d.date, d.value.raw);
            }
        }

        assert!(profile.isin.as_deref() == Some(isin));
        assert!(profile.name.is_some());
        assert!(profile.sectors.is_some(), "Sectors should be populated");
    }
}
