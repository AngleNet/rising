// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Context;
use async_trait::async_trait;

use crate::source::fetcher::{FetcherProperties, FetcherSplit};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

/// Enumerator for the REST API fetcher source.
///
/// If `fetcher.params.sql` is configured, connects to the external database,
/// runs the SQL query, and produces one split per result row. Each row's columns
/// are serialized as JSON and stored in `FetcherSplit::template_params`, making
/// them available as Jinja2 template variables in the URL/body/headers.
///
/// If no SQL params are configured, produces a single split.
#[derive(Debug, Clone)]
pub struct FetcherSplitEnumerator {
    params_sql: Option<String>,
    params_connection: Option<String>,
    /// Cached splits from the last enumeration.
    cached_splits: Vec<FetcherSplit>,
}

#[async_trait]
impl SplitEnumerator for FetcherSplitEnumerator {
    type Properties = FetcherProperties;
    type Split = FetcherSplit;

    async fn new(
        properties: FetcherProperties,
        _context: SourceEnumeratorContextRef,
    ) -> crate::error::ConnectorResult<Self> {
        let mut enumerator = Self {
            params_sql: properties.params_sql.clone(),
            params_connection: properties.params_connection.clone(),
            cached_splits: Vec::new(),
        };
        // Pre-populate the splits on creation.
        enumerator.cached_splits = enumerator.enumerate_splits().await?;
        Ok(enumerator)
    }

    async fn list_splits(&mut self) -> crate::error::ConnectorResult<Vec<FetcherSplit>> {
        // Re-enumerate to pick up any changes in the external DB.
        self.cached_splits = self.enumerate_splits().await?;
        Ok(self.cached_splits.clone())
    }
}

impl FetcherSplitEnumerator {
    /// Build splits by optionally querying the external database.
    async fn enumerate_splits(&self) -> crate::error::ConnectorResult<Vec<FetcherSplit>> {
        match (&self.params_sql, &self.params_connection) {
            (Some(sql), Some(conn_str)) => self.splits_from_sql(sql, conn_str).await,
            (None, None) => {
                // Single split, no template params.
                Ok(vec![FetcherSplit::new(0, 1, None, None)])
            }
            _ => Err(anyhow::anyhow!(
                "fetcher.params.sql and fetcher.params.connection must both be set or both be unset"
            )
            .into()),
        }
    }

    /// Connect to an external Postgres database, run the SQL query, and produce
    /// one split per result row.
    async fn splits_from_sql(
        &self,
        sql: &str,
        conn_str: &str,
    ) -> crate::error::ConnectorResult<Vec<FetcherSplit>> {
        use serde_json::{Map, Value};

        let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls)
            .await
            .context("failed to connect to external database for fetcher params")?;

        // Spawn the connection handler.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "fetcher params database connection error");
            }
        });

        let rows = client
            .query(sql, &[])
            .await
            .context("failed to execute fetcher.params.sql")?;

        if rows.is_empty() {
            tracing::warn!(
                "fetcher.params.sql returned no rows, producing a single split with no template params"
            );
            return Ok(vec![FetcherSplit::new(0, 1, None, None)]);
        }

        let split_num = rows.len() as i32;
        let mut splits = Vec::with_capacity(rows.len());

        for (idx, row) in rows.iter().enumerate() {
            let columns = row.columns();
            let mut params = Map::new();
            for col in columns {
                let name = col.name().to_owned();
                // Convert common Postgres types to JSON values.
                let value: Value = if let Ok(v) = row.try_get::<_, String>(col.name()) {
                    Value::String(v)
                } else if let Ok(v) = row.try_get::<_, i64>(col.name()) {
                    Value::Number(v.into())
                } else if let Ok(v) = row.try_get::<_, i32>(col.name()) {
                    Value::Number(v.into())
                } else if let Ok(v) = row.try_get::<_, bool>(col.name()) {
                    Value::Bool(v)
                } else if let Ok(v) = row.try_get::<_, f64>(col.name()) {
                    serde_json::Number::from_f64(v)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                } else {
                    // Fallback: try to get as string representation.
                    Value::Null
                };
                params.insert(name, value);
            }

            let params_json = serde_json::to_string(&params)
                .context("failed to serialize fetcher template params")?;

            splits.push(FetcherSplit::new(
                idx as i32,
                split_num,
                None,
                Some(params_json),
            ));
        }

        Ok(splits)
    }
}
