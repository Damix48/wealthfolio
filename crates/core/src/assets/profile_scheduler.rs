use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use log::{error, info, warn};

use super::assets_model::QuoteMode;
use super::assets_traits::AssetServiceTrait;

pub async fn run_periodic_profile_enrichment(
    asset_service: Arc<dyn AssetServiceTrait>,
    initial_delay: Duration,
    interval: Duration,
) {
    tokio::time::sleep(initial_delay).await;
    info!(
        "Periodic profile enrichment started (interval: {}h)",
        interval.as_secs() / 3600
    );

    loop {
        let assets = match asset_service.get_assets() {
            Ok(a) => a,
            Err(e) => {
                error!(
                    "Periodic profile enrichment: failed to get assets: {}",
                    e
                );
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        let enrichment_ids: Vec<String> = assets
            .into_iter()
            .filter(|a| a.is_active && a.quote_mode == QuoteMode::Market)
            .map(|a| a.id)
            .collect();

        info!(
            "Periodic profile enrichment: processing {} assets",
            enrichment_ids.len()
        );

        let results: Vec<(String, Result<(), String>)> = stream::iter(enrichment_ids)
            .map(|asset_id| {
                let svc = Arc::clone(&asset_service);
                async move {
                    match svc.enrich_asset_profile(&asset_id).await {
                        Ok(_) => {
                            info!("Periodic profile enrichment: enriched {}", asset_id);
                            (asset_id, Ok(()))
                        }
                        Err(e) => {
                            warn!(
                                "Periodic profile enrichment: failed {}: {}",
                                asset_id, e
                            );
                            (asset_id, Err(e.to_string()))
                        }
                    }
                }
            })
            .buffer_unordered(5)
            .collect()
            .await;

        let enriched = results.iter().filter(|(_, r)| r.is_ok()).count();
        let failed = results.iter().filter(|(_, r)| r.is_err()).count();
        info!(
            "Periodic profile enrichment cycle complete: {} enriched, {} failed",
            enriched, failed
        );

        tokio::time::sleep(interval).await;
    }
}
