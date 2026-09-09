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

        Self {
            client,
            ajax_client,
        }
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
        let resp =
            self.client
                .get(&url)
                .send()
                .await
                .map_err(|e| MarketDataError::ProviderError {
                    provider: PROVIDER_ID.to_string(),
                    message: format!("Failed to load search page: {}", e),
                })?;
        let html = resp
            .text()
            .await
            .map_err(|e| MarketDataError::ProviderError {
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

    async fn search_etfs(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError> {
        let counter = self.extract_wicket_counter().await?;

        let url = format!(
            "{}/en/search.html?{}-1.0-container-tabsContentContainer-tabsContentRepeater-1-container-content-etfsTablePanel=&search=ETFS&_wicket=1",
            BASE_URL, counter
        );

        let etfs_params = format!(
            "search=ETF&productGroup=epg-longOnly&ls=any&query={}",
            query
        );

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

        let resp =
            self.client
                .get(&url)
                .send()
                .await
                .map_err(|e| MarketDataError::ProviderError {
                    provider: PROVIDER_ID.to_string(),
                    message: format!("HTTP request failed: {}", e),
                })?;

        if !resp.status().is_success() {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP {}", resp.status()),
            });
        }

        resp.json()
            .await
            .map_err(|e| MarketDataError::ProviderError {
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

        let resp =
            self.client
                .get(&url)
                .send()
                .await
                .map_err(|e| MarketDataError::ProviderError {
                    provider: PROVIDER_ID.to_string(),
                    message: format!("HTTP request failed: {}", e),
                })?;

        if !resp.status().is_success() {
            return Err(MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("HTTP {}", resp.status()),
            });
        }

        resp.json()
            .await
            .map_err(|e| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("JSON parse error: {}", e),
            })
    }

    // -----------------------------------------------------------------------
    // Profile (HTML scraping + AJAX)
    // -----------------------------------------------------------------------

    /// Wicket counter for the load-more endpoints, or `None` when the page has
    /// no holdings table.
    ///
    /// justETF renders no load-more control for a fund without a country or
    /// sector table, and calling the endpoint anyway answers from whatever the
    /// shared session loaded last — another fund's weights.
    fn extract_profile_page_counter<'a>(&self, html: &'a str) -> Option<&'a str> {
        let marker = "-1.0-holdingsSection-sectors-loadMoreSectors";
        let before = &html[..html.find(marker)?];
        let digit_start = before.rfind(|c: char| !c.is_ascii_digit())?;
        Some(&before[digit_start + 1..])
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

        let xml = resp
            .text()
            .await
            .map_err(|e| MarketDataError::ProviderError {
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
                    results.extend(Self::collect_rows(&fragment, row_s, name_s, pct_s));
                }

                rest = &content[cdata_end + "]]>".len()..];
                continue;
            }
            break;
        }

        results
    }

    /// Rows a table renders in the page itself: the whole table when justETF
    /// offers no load-more endpoint for it, its first page otherwise.
    fn parse_page_rows(&self, document: &Html, table: &str) -> Vec<(String, f64)> {
        let Ok(rows) = Selector::parse(&format!("tr[data-testid='etf-holdings_{table}_row']"))
        else {
            return Vec::new();
        };
        let Ok(names) = Selector::parse(&format!(
            "*[data-testid='tl_etf-holdings_{table}_value_name']"
        )) else {
            return Vec::new();
        };
        let Ok(percentages) = Selector::parse(&format!(
            "*[data-testid='tl_etf-holdings_{table}_value_percentage']"
        )) else {
            return Vec::new();
        };

        Self::collect_rows(document, &rows, &names, &percentages)
    }

    fn collect_rows(
        scope: &Html,
        rows: &Selector,
        names: &Selector,
        percentages: &Selector,
    ) -> Vec<(String, f64)> {
        let mut results = Vec::new();

        for row in scope.select(rows) {
            let name = row
                .select(names)
                .next()
                .map(|el| el.text().collect::<Vec<_>>().join(" ").trim().to_string());
            let pct_str = row
                .select(percentages)
                .next()
                .map(|el| el.text().collect::<Vec<_>>().join(" ").trim().to_string());

            if let (Some(name), Some(pct_str)) = (name, pct_str) {
                if name.eq_ignore_ascii_case("Other") {
                    continue;
                }
                if let Ok(pct) = pct_str
                    .trim_end_matches('%')
                    .replace(',', ".")
                    .parse::<f64>()
                {
                    results.push((name, pct / 100.0));
                }
            }
        }

        results
    }

    /// Complete weights for one table, falling back to the rows the page
    /// rendered when there is no load-more endpoint to ask.
    async fn load_full_table(
        &self,
        isin: &str,
        counter: Option<&str>,
        endpoint_suffix: &str,
        page_rows: Vec<(String, f64)>,
    ) -> Vec<(String, f64)> {
        let Some(counter) = counter else {
            return page_rows;
        };

        match self
            .fetch_ajax_full_table(isin, counter, endpoint_suffix)
            .await
        {
            Ok(rows) if !rows.is_empty() => rows,
            _ => page_rows,
        }
    }

    fn weights_json(rows: Vec<(String, f64)>) -> String {
        let items: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|(name, weight)| serde_json::json!({ "name": name, "weight": weight }))
            .collect();
        serde_json::to_string(&items).unwrap_or_default()
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

        resp.text()
            .await
            .map_err(|e| MarketDataError::ProviderError {
                provider: PROVIDER_ID.to_string(),
                message: format!("Failed to read profile page: {}", e),
            })
    }

    async fn parse_profile_html(&self, html: &str, isin: &str) -> AssetProfile {
        let (name, description, country, dividend_yield, counter, page_sectors, page_regions) = {
            let document = Html::parse_document(html);
            let name = self.extract_name(&document);
            let description = self.extract_description(&document);
            let country = self.extract_domicile(&document);
            let dividend_yield = self.extract_dividend_yield(&document);
            let counter = self.extract_profile_page_counter(html);
            let page_sectors = self.parse_page_rows(&document, "sectors");
            let page_regions = self.parse_page_rows(&document, "countries");
            (
                name,
                description,
                country,
                dividend_yield,
                counter,
                page_sectors,
                page_regions,
            )
        };

        let sectors = self
            .load_full_table(isin, counter, "sectors-loadMoreSectors", page_sectors)
            .await;
        let regions = self
            .load_full_table(isin, counter, "countries-loadMoreCountries", page_regions)
            .await;

        let sectors_json = Some(Self::weights_json(sectors));
        let regions_json = Some(Self::weights_json(regions));

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
        let selector =
            Selector::parse("tr[data-testid='etf-basics_row_domicile-country'] td.val").ok()?;
        document
            .select(&selector)
            .next()
            .map(|el| el.text().collect::<Vec<_>>().join(" ").trim().to_string())
    }

    fn extract_dividend_yield(&self, document: &Html) -> Option<f64> {
        let selector =
            Selector::parse("tr[data-testid='etf-basics_row_distribution-policy'] td.val").ok()?;
        let policy = document.select(&selector).next().map(|el| {
            el.text()
                .collect::<Vec<_>>()
                .join(" ")
                .trim()
                .to_lowercase()
        })?;

        if policy.contains("accumulating") {
            return None;
        }

        let yield_selector =
            Selector::parse("tr[data-testid='etf-basics_row_current-dividend-yield'] td.val")
                .ok()?;
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

        let high = response
            .quote_low_high
            .as_ref()
            .and_then(|lh| Decimal::try_from(lh.high.raw).ok());
        let low = response
            .quote_low_high
            .as_ref()
            .and_then(|lh| Decimal::try_from(lh.low.raw).ok());

        let timestamp = NaiveDate::parse_from_str(&response.latest_quote_date, "%Y-%m-%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0).map(|dt| Utc.from_utc_datetime(&dt)))
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
                if let Ok(date) = NaiveDate::parse_from_str(&latest.latest_quote_date, "%Y-%m-%d") {
                    if let Some(dt) = date
                        .and_hms_opt(0, 0, 0)
                        .map(|dt| Utc.from_utc_datetime(&dt))
                    {
                        if dt >= start
                            && dt <= end
                            && !quotes.iter().any(|q| q.timestamp.date_naive() == date)
                        {
                            if let Ok(close) = Decimal::try_from(latest.latest_quote.raw) {
                                let high = latest
                                    .quote_low_high
                                    .as_ref()
                                    .and_then(|lh| Decimal::try_from(lh.high.raw).ok());
                                let low = latest
                                    .quote_low_high
                                    .as_ref()
                                    .and_then(|lh| Decimal::try_from(lh.low.raw).ok());

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
        assert_eq!(provider.extract_isin(&instrument), Some("IE00B4L5Y983"));
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
        println!(
            "description:  {:?}",
            profile
                .description
                .as_deref()
                .map(|d| &d[..d.len().min(200)])
        );
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

    const PAGE_WITH_HOLDINGS: &str = r#"<html><body>
<h1>iShares Core MSCI World UCITS ETF USD (Acc)</h1>
<table data-testid="etf-holdings_countries_table"><tbody>
<tr data-testid="etf-holdings_countries_row"><td data-testid="tl_etf-holdings_countries_value_name">United States</td><td><span data-testid="tl_etf-holdings_countries_value_percentage">70.32%</span></td></tr>
<tr data-testid="etf-holdings_countries_row"><td data-testid="tl_etf-holdings_countries_value_name">Other</td><td><span data-testid="tl_etf-holdings_countries_value_percentage">5.00%</span></td></tr>
</tbody></table>
<table data-testid="etf-holdings_sectors_table"><tbody>
<tr data-testid="etf-holdings_sectors_row"><td data-testid="tl_etf-holdings_sectors_value_name">Technology</td><td><span data-testid="tl_etf-holdings_sectors_value_percentage">35.71%</span></td></tr>
</tbody></table>
<a href="/en/etf-profile.html?3-1.0-holdingsSection-sectors-loadMoreSectors&amp;isin=IE00B4L5Y983&amp;_wicket=1">Load more</a>
</body></html>"#;

    /// justETF renders no country or sector table for a single-asset ETC.
    const PAGE_WITHOUT_HOLDINGS: &str =
        r#"<html><body><h1>WisdomTree Physical Bitcoin</h1></body></html>"#;

    fn rounded(rows: &[(String, f64)]) -> Vec<(String, f64)> {
        rows.iter()
            .map(|(name, weight)| (name.clone(), (weight * 10_000.0).round() / 10_000.0))
            .collect()
    }

    #[test]
    fn parses_the_rows_a_page_renders_without_mixing_the_tables() {
        let provider = JustEtfProvider::new();
        let document = Html::parse_document(PAGE_WITH_HOLDINGS);

        let countries = provider.parse_page_rows(&document, "countries");
        let sectors = provider.parse_page_rows(&document, "sectors");

        // The "Other" row is dropped, exactly as in the load-more response.
        assert_eq!(
            rounded(&countries),
            vec![("United States".to_string(), 0.7032)]
        );
        assert_eq!(rounded(&sectors), vec![("Technology".to_string(), 0.3571)]);
    }

    #[test]
    fn the_load_more_counter_is_absent_without_a_holdings_table() {
        let provider = JustEtfProvider::new();

        assert_eq!(
            provider.extract_profile_page_counter(PAGE_WITH_HOLDINGS),
            Some("3")
        );
        assert_eq!(
            provider.extract_profile_page_counter(PAGE_WITHOUT_HOLDINGS),
            None
        );
    }

    #[tokio::test]
    async fn a_page_without_a_counter_uses_its_own_rows_and_fetches_nothing() {
        let provider = JustEtfProvider::new();
        let rendered = vec![("Technology".to_string(), 0.3571)];

        assert_eq!(
            provider
                .load_full_table(
                    "IE00B4L5Y983",
                    None,
                    "sectors-loadMoreSectors",
                    rendered.clone()
                )
                .await,
            rendered
        );
        // The regression: a fund with no table must never reach the load-more
        // endpoint, which answers with whatever the session loaded last.
        assert!(provider
            .load_full_table("GB00BJYDH287", None, "sectors-loadMoreSectors", Vec::new())
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn a_profile_without_tables_reports_empty_weights_instead_of_another_funds() {
        let provider = JustEtfProvider::new();

        let profile = provider
            .parse_profile_html(PAGE_WITHOUT_HOLDINGS, "GB00BJYDH287")
            .await;

        assert_eq!(profile.countries.as_deref(), Some("[]"));
        assert_eq!(profile.sectors.as_deref(), Some("[]"));
    }
}
