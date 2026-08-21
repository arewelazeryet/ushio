use std::time::{SystemTime, UNIX_EPOCH};

use color_eyre::eyre::{Context, Result};
use metrics::{counter, gauge, histogram};
use sqlx::{query, query_as};
use time::OffsetDateTime;

use crate::{
    api::BucketTimeRange,
    database::{Database, models::DatabaseScore},
};

pub struct ScoreDistributionResponse {
    pub stable: i64,
    pub lazer: i64,
}

#[derive(sqlx::FromRow)]
pub struct BucketedResponse {
    pub bucket_floor: i64,
    pub stable: i64,
    pub lazer: i64,
    pub both: i64,
}

pub struct LatestScore {
    pub ended_at: OffsetDateTime,
    pub id: i64,
}

impl Database {
    /// Fetch score distribution for all scores in datetime range
    pub async fn get_score_distribution_in_range(
        &self,
        from: OffsetDateTime,
        to: OffsetDateTime,
    ) -> Result<ScoreDistributionResponse> {
        let result = query_as!(
            ScoreDistributionResponse,
            r#"
            SELECT
                COUNT(*) FILTER (WHERE lazer = true)  AS "lazer!",
                COUNT(*) FILTER (WHERE lazer = false) AS "stable!"
            FROM scores
            WHERE ended_at > $1 AND ended_at < $2
            "#,
            from,
            to
        )
        .fetch_one(&*self)
        .await
        .wrap_err("Failed to fetch score distribution");

        result
    }

    pub async fn get_last_inserted_score(&self) -> Result<LatestScore> {
        let result = query_as!(
            LatestScore,
            r#"SELECT id, ended_at FROM scores
            WHERE ended_at >= NOW() - INTERVAL '12 hours'
            ORDER BY id DESC LIMIT 1"#
        )
        .fetch_one(&*self)
        .await
        .wrap_err("Failed to fetch last score");

        result
    }

    pub async fn get_unique_buckets(
        &self,
        bucket_range: BucketTimeRange,
    ) -> Result<Vec<BucketedResponse>> {
        tracing::info!(time_range = ?bucket_range, "Getting unique buckets");
        counter!(format!(
            "athena.database.get_{}_unique_users.query_count",
            bucket_range
        ))
        .increment(1);

        let mut trans = self.begin_with_chunkwise_aggregation_disabled().await?;

        let mut builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new("");
        builder.push(
            r#"
            SELECT
                (user_id / 2000000) * 2000000 AS "bucket_floor",
                COUNT(*) FILTER (WHERE has_stable AND NOT has_lazer) AS "stable",
                COUNT(*) FILTER (WHERE has_lazer AND NOT has_stable) AS "lazer",
                COUNT(*) FILTER (WHERE has_lazer AND has_stable) AS "both"
            FROM (
                SELECT
                    user_id,
                    BOOL_OR(lazer) AS has_lazer,
                    BOOL_OR(NOT lazer) AS has_stable
                FROM scores
                WHERE ended_at >= NOW() - INTERVAL "#,
        );
        match bucket_range {
            BucketTimeRange::Day => {
                builder.push("'1 day'");
            }
            BucketTimeRange::Week => {
                builder.push("'7 days'");
            }
            BucketTimeRange::Month => {
                builder.push("'30 days'");
            }
        }
        builder.push(
            r#"
                GROUP BY user_id
            ) u
            GROUP BY "bucket_floor"
            ORDER BY "bucket_floor" ASC;
        "#,
        );

        let query = builder.build_query_as::<BucketedResponse>();
        let result = query
            .fetch_all(&mut *trans)
            .await
            .wrap_err("Failed to get monthly unique users");

        trans.commit().await?;

        tracing::info!(time_range = ?bucket_range, "Got unique buckets");

        result
    }

    pub async fn get_bucketed_scores(
        &self,
        bucket_range: BucketTimeRange,
    ) -> Result<Vec<BucketedResponse>> {
        tracing::info!(time_range = ?bucket_range, "Getting unique buckets");
        counter!(format!(
            "athena.database.get_{}_unique_scores.query_count",
            bucket_range
        ))
        .increment(1);

        let mut trans = self.begin_with_chunkwise_aggregation_disabled().await?;

        let mut builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new("");
        builder.push(
            r#"
            SELECT
                ((user_id / 2000000) * 2000000)::INT8 AS "bucket_floor",
                (COUNT(*) FILTER (WHERE lazer))::INT8 AS "lazer",
                (COUNT(*) FILTER (WHERE NOT lazer))::INT8 AS "stable",
                0::INT8 AS "both"
            FROM scores
            WHERE ended_at >= NOW() - INTERVAL "#,
        );
        match bucket_range {
            BucketTimeRange::Day => {
                builder.push("'1 day'");
            }
            BucketTimeRange::Week => {
                builder.push("'7 days'");
            }
            BucketTimeRange::Month => {
                builder.push("'30 days'");
            }
        }
        builder.push(
            r#"
            GROUP BY "bucket_floor"
            ORDER BY "bucket_floor" ASC;
        "#,
        );

        let query = builder.build_query_as::<BucketedResponse>();
        let result = query
            .fetch_all(&mut *trans)
            .await
            .wrap_err("Failed to get monthly unique users");

        trans.commit().await?;

        tracing::info!(time_range = ?bucket_range, "Got unique buckets");

        result
    }
}
