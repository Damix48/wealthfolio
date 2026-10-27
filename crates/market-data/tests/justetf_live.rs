//! Opt-in upstream smoke test; never reads portfolio data or credentials.
#![allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
use wealthfolio_market_data::{JustEtfProvider, MarketDataProvider};

/// ISINs under review; each must be an ETF listed on justETF.
const ISINS: [&str; 4] = [
    "IE0006WW1TQ4",
    "GB00BJYDH287",
    "IE00B4L5Y983",
    "LU0290358497",
];

/// Prints a table's row count and total weight; the parser skips justETF's
/// "Other" row, so a complete table sums to slightly under 100%.
fn summarize(raw: &Option<String>) -> String {
    let rows: Vec<serde_json::Value> = raw
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();
    let total: f64 = rows
        .iter()
        .map(|row| row["weight"].as_f64().unwrap_or_default())
        .sum();
    let top: Vec<String> = rows
        .iter()
        .take(3)
        .map(|row| {
            format!(
                "{} {:.2}%",
                row["name"].as_str().unwrap_or("?"),
                row["weight"].as_f64().unwrap_or_default() * 100.0
            )
        })
        .collect();
    format!(
        "{} rows, sum {:.2}% | {}",
        rows.len(),
        total * 100.0,
        top.join(", ")
    )
}

#[tokio::test]
#[ignore = "requires live justETF HTTPS access; run explicitly to inspect provider data"]
async fn justetf_profiles_for_review() {
    tokio::time::timeout(std::time::Duration::from_secs(180), async {
        let shared = JustEtfProvider::new();
        for isin in ISINS {
            println!("\n================ {isin} ================");
            let fresh = JustEtfProvider::new();
            match fresh.get_profile(isin).await {
                Ok(profile) => {
                    let warm = shared.get_profile(isin).await.unwrap();
                    println!("name:             {:?}", profile.name);
                    println!("domicile country: {:?}", profile.country);
                    println!("fresh countries:  {}", summarize(&profile.countries));
                    println!("fresh sectors:    {}", summarize(&profile.sectors));
                    println!("warm  countries:  {}", summarize(&warm.countries));
                    println!("warm  sectors:    {}", summarize(&warm.sectors));
                    println!("fresh countries json: {:?}", profile.countries);
                    println!("fresh sectors json:   {:?}", profile.sectors);
                }
                Err(error) => println!("ERROR: {error}"),
            }
        }
    })
    .await
    .expect("probe timed out");
}

/// Compares a shared session against a fresh one per ISIN to tell an ETF-specific
/// partial payload apart from a stale/cross-ETF response.
#[tokio::test]
#[ignore = "requires live justETF HTTPS access; run explicitly to diagnose stale payloads"]
async fn justetf_profile_weights_are_session_scoped() {
    fn top(raw: &Option<String>) -> String {
        summarize(raw)
    }

    tokio::time::timeout(std::time::Duration::from_secs(180), async {
        let shared = JustEtfProvider::new();
        for isin in ISINS {
            let profile = shared.get_profile(isin).await.unwrap();
            println!(
                "[shared session] {isin} countries: {}",
                top(&profile.countries)
            );
            println!(
                "[shared session] {isin} sectors:   {}",
                top(&profile.sectors)
            );
        }

        for isin in ISINS {
            let fresh = JustEtfProvider::new();
            let profile = fresh.get_profile(isin).await.unwrap();
            println!(
                "[fresh session]  {isin} countries: {}",
                top(&profile.countries)
            );
            println!(
                "[fresh session]  {isin} sectors:   {}",
                top(&profile.sectors)
            );
        }
    })
    .await
    .expect("probe timed out");
}
