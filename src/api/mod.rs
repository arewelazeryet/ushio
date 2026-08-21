pub mod aggregates;
pub mod changelogs;
pub mod models;

use std::fmt::Display;

use crate::{database::impls::BucketedResponse, state::SharedState};
use apply::Apply as _;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::get,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct UserIdDistributionEntry {
    stable: u32,
    lazer: u32,
    both: u32,
    bucket: u32,
}

impl From<BucketedResponse> for UserIdDistributionEntry {
    fn from(value: BucketedResponse) -> Self {
        Self {
            stable: value.stable as u32,
            lazer: value.lazer as u32,
            both: value.both as u32,
            bucket: value.bucket_floor as u32,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BucketTimeRange {
    Day,
    Week,
    Month,
}

impl Display for BucketTimeRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            BucketTimeRange::Day => "day",
            BucketTimeRange::Week => "week",
            BucketTimeRange::Month => "month",
        };
        write!(f, "{}", string)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct BucketQuery {
    range: BucketTimeRange,
}

async fn get_unique_users(
    Query(range): Query<BucketQuery>,
    State(state): State<SharedState>,
) -> Result<Json<Vec<UserIdDistributionEntry>>, (StatusCode, String)> {
    state
        .get_unique_users(range.range)
        .await
        .inspect_err(|e| {
            tracing::warn!(
                "Failed to return daily unique users: {:?}",
                e.chain().map(|e| e.to_string()).collect::<Vec<_>>()
            )
        })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .apply(Json)
        .apply(Ok)
}

async fn get_unique_scores(
    Query(range): Query<BucketQuery>,
    State(state): State<SharedState>,
) -> Result<Json<Vec<UserIdDistributionEntry>>, (StatusCode, String)> {
    state
        .get_unique_scores(range.range)
        .await
        .inspect_err(|e| {
            tracing::warn!(
                "Failed to return daily unique scores: {:?}",
                e.chain().map(|e| e.to_string()).collect::<Vec<_>>()
            )
        })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .apply(Json)
        .apply(Ok)
}

pub(crate) fn router() -> Router<SharedState> {
    Router::new()
        .route("/distribution", get(get_unique_users))
        .route("/distribution/scores", get(get_unique_scores))
}
