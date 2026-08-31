use color_eyre::{Result, eyre::Context};
use redis::{AsyncCommands, JsonAsyncCommands};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use time::{OffsetDateTime, Time};

use crate::{
    api::{
        BucketTimeRange, UserIdDistributionEntry,
        models::{PointLineResponse, ScoreAggregateResponse, SinglePointResponse},
    },
    database::{Database, models::BucketSize},
};

pub(crate) struct AppState {
    database: Database,
    cache: redis::Client,
}

fn parse_json_root<T: DeserializeOwned>(value: &str, key: &str) -> Result<T> {
    let mut parsed: Vec<T> = serde_json::from_str(value)?;
    parsed.pop().ok_or_else(|| {
        color_eyre::eyre::eyre!("cache entry {} was missing its JSON root value", key)
    })
}

macro_rules! cache_json_pair {
    (
        $suffix:ident,
        key = $key:expr,
        ty = $ty:ty,
        ttl = $ttl:expr,
        refresh => |$this:ident| $($refresh:tt)+
    ) => {
        pastey::paste! {
            pub async fn [<set_ $suffix>](&self, value: &$ty) -> Result<()> {
                let payload = serde_json::to_value(value)?;
                let _: () = self.cache().await.json_set($key, "$", &payload).await?;
                let _: bool = self.cache().await.expire($key, $ttl).await?;

                tracing::debug!(key = $key, ttl = $ttl, "Updated cache entry");
                Ok(())
            }

            pub async fn [<refresh_ $suffix>](&self) -> Result<$ty> {
                tracing::info!(key = $key, "Attempting to refresh cache entry");
                let $this = self;
                let value = { $($refresh)+ };
                $this.[<set_ $suffix>](&value).await?;
                tracing::info!(key = $key, "Refreshed cache entry");
                Ok(value)
            }

            pub async fn [<get_ $suffix>](&self) -> Result<$ty> {
                let ttl: i64 = self.cache().await.ttl($key).await?;

                if ttl <= 0 {
                    tracing::debug!(key = $key, ttl, "Cache entry expired or missing");
                    return self.[<refresh_ $suffix>]().await;
                }

                let serialized: String = self.cache().await.json_get($key, "$").await?;
                let value: $ty = parse_json_root(&serialized, $key)?;

                tracing::info!(key = $key, expires_in = ttl, "Fetched cache entry");
                Ok(value)
            }
        }
    };
    (
        $suffix:ident,
        key = $key:expr,
        ty = $ty:ty,
        ttl = $ttl:expr
    ) => {
        paste! {
            pub async fn [<set_ $suffix>](&self, value: &$ty) -> Result<()> {
                let payload = serde_json::to_value(value)?;
                let _: () = self.cache().await.json_set($key, "$", &payload).await?;
                let _: bool = self.cache().await.expire($key, $ttl).await?;

                tracing::debug!(key = $key, ttl = $ttl, "Updated cache entry");
                Ok(())
            }

            pub async fn [<get_ $suffix>](&self) -> Result<$ty> {
                let ttl: i64 = self.cache().await.ttl($key).await?;

                if ttl <= 0 {
                    tracing::debug!(key = $key, ttl, "Cache entry expired or missing");
                    return Err(color_eyre::eyre::eyre!(
                        "cache entry {} expired without a refresh function",
                        $key
                    ));
                }

                let serialized: String = self.cache().await.json_get($key, "$").await?;
                let value: $ty = parse_json_root(&serialized, $key)?;

                tracing::info!(key = $key, expires_in = ttl, "Fetched cache entry");
                Ok(value)
            }

            pub async fn [<refresh_ $suffix>](&self) -> Result<$ty> {
                tracing::info!(key = $key, "Attempting to refresh cache entry");
                let $this = self;
                let value = { $($refresh)+ };
                $this.[<set_ $suffix>](&value).await?;
                tracing::info!(key = $key, "Refreshed cache entry");
                Ok(value)
            }
        }
    };

}

impl AppState {
    pub async fn new_shared() -> Result<SharedState> {
        let db = Database::new(&std::env::var("DATABASE_URL")?).await?;

        let redis = redis::Client::open(std::env::var("CACHE_URL")?)?;

        let app_state = AppState {
            database: db,
            cache: redis,
        };
        Ok(Arc::new(app_state))
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub async fn cache(&self) -> redis::aio::MultiplexedConnection {
        self.cache.get_multiplexed_async_connection().await.unwrap()
    }

    pub async fn get_unique_users(
        &self,
        bucket_range: BucketTimeRange,
    ) -> Result<Vec<UserIdDistributionEntry>> {
        match bucket_range {
            BucketTimeRange::Day => self.get_daily_aggregate().await,
            BucketTimeRange::Week => self.get_weekly_aggregate().await,
            BucketTimeRange::Month => self.get_monthly_aggregate().await,
        }
    }

    pub async fn get_unique_scores(
        &self,
        bucket_range: BucketTimeRange,
    ) -> Result<Vec<UserIdDistributionEntry>> {
        match bucket_range {
            BucketTimeRange::Day => self.get_daily_scores().await,
            BucketTimeRange::Week => self.get_weekly_scores().await,
            BucketTimeRange::Month => self.get_monthly_scores().await,
        }
    }

    cache_json_pair!(
        latest_changelog,
        key = "athena:changelogs:changelog:latest",
        ty = SinglePointResponse,
        ttl = 60,
        refresh => |server| {
            server.database().get_latest().await?.into()
        }
    );

    cache_json_pair!(
        peak_user_count,
        key = "athena:changelogs:peak:users",
        ty = SinglePointResponse,
        ttl = 300,
        refresh => |server| server.database().get_user_count_peak().await?.into()
    );

    cache_json_pair!(
        peak_user_ratio,
        key = "athena:changelogs:peak:ratio",
        ty = SinglePointResponse,
        ttl = 300,
        refresh => |server| server.database().get_user_ratio_peak().await?.into()
    );

    cache_json_pair!(
        peak_user_percentile,
        key = "athena:changelogs:peak:percentile",
        ty = SinglePointResponse,
        ttl = 300,
        refresh => |server| server.database().get_user_highest_percentile_peak().await?.into()
    );

    cache_json_pair!(
        day_user_graph,
        key = "athena:changelogs:graph:day",
        ty = PointLineResponse,
        ttl = 300,
        refresh => |server| server.database().get_past_day().await?.into()
    );

    cache_json_pair!(
        history_user_graph,
        key = "athena:changelogs:graph:history",
        ty = PointLineResponse,
        ttl = 300,
        refresh => |server| server.database().get_history(BucketSize::Day).await?.into()
    );

    cache_json_pair!(
        daily_aggregate,
        key = "athena:unique_users_by_id:daily",
        ty = Vec<UserIdDistributionEntry>,
        ttl = 86400,
        refresh => |server| {
            server.database().get_unique_buckets(BucketTimeRange::Day).await?.into_iter().map(UserIdDistributionEntry::from).collect()
        }
    );

    cache_json_pair!(
        weekly_aggregate,
        key = "athena:unique_users_by_id:weekly",
        ty = Vec<UserIdDistributionEntry>,
        ttl = 86400,
        refresh => |server| {
            server.database().get_unique_buckets(BucketTimeRange::Week).await?.into_iter().map(UserIdDistributionEntry::from).collect()
        }
    );
    cache_json_pair!(
        monthly_aggregate,
        key = "athena:unique_users_by_id:monthly",
        ty = Vec<UserIdDistributionEntry>,
        ttl = 604800,
        refresh => |server| {
            server.database().get_unique_buckets(BucketTimeRange::Month).await?.into_iter().map(UserIdDistributionEntry::from).collect()
        }
    );

    cache_json_pair!(
        daily_scores,
        key = "athena:unique_scores:daily",
        ty = Vec<UserIdDistributionEntry>,
        ttl = 86400,
        refresh => |server| {
            server.database().get_bucketed_scores(BucketTimeRange::Day).await?.into_iter().map(UserIdDistributionEntry::from).collect()
        }
    );

    cache_json_pair!(
        weekly_scores,
        key = "athena:unique_scores:weekly",
        ty = Vec<UserIdDistributionEntry>,
        ttl = 86400,
        refresh => |server| {
            server.database().get_bucketed_scores(BucketTimeRange::Week).await?.into_iter().map(UserIdDistributionEntry::from).collect()
        }
    );
    cache_json_pair!(
        monthly_scores,
        key = "athena:unique_scores:monthly",
        ty = Vec<UserIdDistributionEntry>,
        ttl = 604800,
        refresh => |server| {
            server.database().get_bucketed_scores(BucketTimeRange::Month).await?.into_iter().map(UserIdDistributionEntry::from).collect()
        }
    );

    pub async fn set_daily_historic_graphs(&self, value: &[ScoreAggregateResponse]) -> Result<()> {
        let payload = serde_json::to_value(value)?;
        tracing::debug!(key = "athena:daily_graph", json = ?payload, "Setting value");

        let now = OffsetDateTime::now_utc();
        let tomorrow = (now + time::Duration::days(1)).replace_time(Time::from_hms(1, 0, 0)?);
        let _: () = self
            .cache()
            .await
            .json_set("athena:daily_graph", "$", &payload)
            .await
            .wrap_err("Failed to set value of 'athena:daily_graph'")?;
        let is_set: bool = self
            .cache()
            .await
            .expire_at("athena:daily_graph", tomorrow.unix_timestamp())
            .await
            .wrap_err("Failed to set TTL of 'athena:daily_graph'")?;

        tracing::info!(
            ttl_set = is_set,
            key = "athena:daily_graph",
            "Updated daily graphs"
        );

        Ok(())
    }

    pub async fn get_daily_historic_graphs(&self) -> Result<Vec<ScoreAggregateResponse>> {
        let span = tracing::debug_span!("get_daily_historic_graphs", key = "athena:daily_graph");
        tracing::debug!(parent: &span, "Getting daily historic graphs");
        let ttl: i64 = self
            .cache()
            .await
            .ttl("athena:daily_graph")
            .await
            .inspect_err(|e| tracing::warn!(parent: &span, "Failed to get TTL: {e}"))
            .wrap_err("Failed to get TTL of 'athena:daily_graph'")?;

        let mut graph = vec![];

        if ttl <= 0 {
            tracing::debug!(key = "athena:daily_graph", ttl, "Cache entry expired");
            graph = self
                .database()
                .get_daily_historic_graphs()
                .await
                .wrap_err("Failed to get 'athena:daily_graph'")?
                .iter()
                .map(|v| ScoreAggregateResponse::from(v))
                .collect();
            tracing::debug!(parent: &span, "Got value from cache");

            self.set_daily_historic_graphs(&graph)
                .await
                .wrap_err("Failed to set 'athena:daily_graph'")?;
            tracing::debug!(parent: &span, "Set value to cache");
        } else {
            let serialized: String = self
                .cache()
                .await
                .json_get("athena:daily_graph", "$")
                .await
                .wrap_err("Failed to re-get 'athena:daily_graph'")?;
            graph = parse_json_root(&serialized, "athena:daily_graph")
                .wrap_err("Failed to parse 'athena:daily_graph'")?;
            tracing::debug!(key = "athena:daily_graph", ttl, "Cache hit");
        }

        Ok(graph)
    }
}

pub(crate) type SharedState = Arc<AppState>;

#[cfg(test)]
mod tests {
    use time::{Date, OffsetDateTime, Time};

    #[test]
    fn test_offsets() {
        let baseline = OffsetDateTime::from_unix_timestamp(1782675578).unwrap();
        assert_eq!(
            baseline,
            OffsetDateTime::new_utc(
                Date::from_calendar_date(2026, time::Month::June, 28).unwrap(),
                Time::from_hms(19, 39, 38).unwrap()
            )
        );

        let tomorrow = baseline
            .clone()
            .replace_day(baseline.day() + 1)
            .unwrap()
            .replace_time(Time::from_hms(1, 0, 0).unwrap());

        assert_eq!(
            tomorrow,
            OffsetDateTime::new_utc(
                Date::from_calendar_date(2026, time::Month::June, 29).unwrap(),
                Time::from_hms(1, 0, 0).unwrap()
            )
        )
    }
}
