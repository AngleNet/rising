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

use std::collections::BTreeMap;

use anyhow::Context;
use async_trait::async_trait;
use futures_async_stream::try_stream;

use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::data_gen_util::spawn_data_generation_stream;
use crate::source::fetcher::split::FetcherSplit;
use crate::source::fetcher::{FetcherProperties, PaginationMode};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader, into_chunk_stream,
};

/// Split reader for the REST API fetcher source.
///
/// Periodically polls a REST API endpoint (scheduled by cron or simple interval),
/// handles pagination (offset or cursor-based), applies field-to-column mappings,
/// and produces `SourceMessage` payloads containing JSON bytes for the downstream parser.
pub struct FetcherSplitReader {
    properties: FetcherProperties,
    split_id: SplitId,
    events_so_far: u64,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    /// Pre-rendered URL (after Jinja2 template substitution with split params).
    rendered_url: String,
    /// Pre-rendered body (after Jinja2 template substitution).
    rendered_body: Option<String>,
    /// Resolved HTTP headers.
    resolved_headers: BTreeMap<String, String>,
}

#[async_trait]
impl SplitReader for FetcherSplitReader {
    type Properties = FetcherProperties;
    type Split = FetcherSplit;

    async fn new(
        properties: FetcherProperties,
        splits: Vec<FetcherSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        debug_assert!(splits.len() == 1);
        let split = splits.into_iter().next().unwrap();
        let split_id = split.id();

        let mut events_so_far = 0u64;
        if let Some(offset) = split.start_offset {
            events_so_far = offset + 1;
        }

        // Parse template params from the split (if any).
        let template_ctx: serde_json::Value = match &split.template_params {
            Some(params) => serde_json::from_str(params)
                .context("failed to parse fetcher split template_params")?,
            None => serde_json::Value::Object(Default::default()),
        };

        // Render URL template.
        let rendered_url = render_template(&properties.url, &template_ctx)
            .context("failed to render fetcher.url template")?;

        // Render body template if present.
        let rendered_body = match &properties.body {
            Some(body) => Some(
                render_template(body, &template_ctx)
                    .context("failed to render fetcher.body template")?,
            ),
            None => None,
        };

        // Resolve headers (also render any template vars in header values).
        let mut resolved_headers = BTreeMap::new();
        if let Some(headers) = &properties.headers {
            for (k, v) in headers {
                let rendered_v = render_template(v, &template_ctx)
                    .context("failed to render fetcher header template")?;
                resolved_headers.insert(k.clone(), rendered_v);
            }
        }

        // Validate scheduling: exactly one of cron or interval must be set.
        if properties.cron.is_none() && properties.poll_interval_seconds.is_none() {
            return Err(anyhow::anyhow!(
                "either fetcher.cron or fetcher.poll.interval.seconds must be set"
            )
            .into());
        }
        if properties.cron.is_some() && properties.poll_interval_seconds.is_some() {
            return Err(anyhow::anyhow!(
                "fetcher.cron and fetcher.poll.interval.seconds are mutually exclusive"
            )
            .into());
        }

        // Validate pagination config.
        if properties.pagination_mode == PaginationMode::Cursor
            && properties.pagination_cursor_path.is_none()
        {
            return Err(anyhow::anyhow!(
                "fetcher.pagination.cursor.path is required when pagination mode is 'cursor'"
            )
            .into());
        }

        Ok(Self {
            properties,
            split_id,
            events_so_far,
            parser_config,
            source_ctx,
            rendered_url,
            rendered_body,
            resolved_headers,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        const BUFFER_SIZE: usize = 4;
        let parser_config = self.parser_config.clone();
        let source_ctx = self.source_ctx.clone();
        into_chunk_stream(
            spawn_data_generation_stream(self.into_data_stream(), BUFFER_SIZE),
            parser_config,
            source_ctx,
        )
    }
}

impl FetcherSplitReader {
    /// Produce a stream of `Vec<SourceMessage>` by periodically polling the REST API.
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(mut self) {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("failed to build HTTP client")?;

        // Build the scheduler.
        let scheduler = Scheduler::new(
            self.properties.cron.as_deref(),
            self.properties.poll_interval_seconds,
        )?;

        loop {
            // Wait for the next scheduled tick.
            scheduler.wait_next_tick().await;

            // Fetch all pages and produce messages.
            let messages = self
                .fetch_all_pages(&client)
                .await
                .context("fetcher poll cycle failed")?;

            if !messages.is_empty() {
                yield messages;
            }
        }
    }

    /// Fetch all pages from the API (handling pagination) and return source messages.
    async fn fetch_all_pages(
        &mut self,
        client: &reqwest::Client,
    ) -> ConnectorResult<Vec<SourceMessage>> {
        let mut all_messages = Vec::new();
        let mut page_offset: u64 = 0;
        let mut cursor: Option<String> = None;

        loop {
            // Build the request URL with pagination params.
            let url = self.build_paginated_url(
                &self.rendered_url.clone(),
                page_offset,
                cursor.as_deref(),
            )?;

            // Build the HTTP request.
            let method: reqwest::Method = self
                .properties
                .method
                .parse()
                .context("invalid HTTP method")?;

            let mut req = client.request(method, &url);

            // Add headers.
            for (k, v) in &self.resolved_headers {
                req = req.header(k, v);
            }

            // Add body if present.
            if let Some(body) = &self.rendered_body {
                req = req.body(body.clone());
                // Default content-type for body.
                if !self.resolved_headers.contains_key("Content-Type")
                    && !self.resolved_headers.contains_key("content-type")
                {
                    req = req.header("Content-Type", "application/json");
                }
            }

            // Execute the request.
            let response = req.send().await.context("HTTP request failed")?;

            let status = response.status();
            if !status.is_success() {
                let body_text = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("HTTP {} from {}: {}", status, url, body_text).into());
            }

            let body_bytes = response
                .bytes()
                .await
                .context("failed to read response body")?;
            let body_json: serde_json::Value =
                serde_json::from_slice(&body_bytes).context("response is not valid JSON")?;

            // Extract the data array using the configured JSON pointer.
            let data_items = self.extract_data_items(&body_json)?;

            if data_items.is_empty() {
                // No more data on this page — stop pagination.
                break;
            }

            // Convert items to SourceMessages with field mapping applied.
            for item in &data_items {
                let mapped_item = self.apply_field_mappings(item);
                let payload =
                    serde_json::to_vec(&mapped_item).context("failed to serialize mapped item")?;

                all_messages.push(SourceMessage {
                    key: None,
                    payload: Some(payload),
                    offset: self.events_so_far.to_string(),
                    split_id: self.split_id.clone(),
                    meta: crate::source::SourceMeta::Empty,
                });
                self.events_so_far += 1;
            }

            // Handle pagination to decide whether to fetch the next page.
            match self.properties.pagination_mode {
                PaginationMode::None => break,
                PaginationMode::Offset => {
                    let page_size = self
                        .properties
                        .pagination_page_size
                        .unwrap_or(data_items.len() as u64);
                    if (data_items.len() as u64) < page_size {
                        // Last page (fewer results than page_size).
                        break;
                    }
                    page_offset += page_size;
                }
                PaginationMode::Cursor => {
                    let cursor_path = self
                        .properties
                        .pagination_cursor_path
                        .as_deref()
                        .expect("cursor path validated in new()");
                    match body_json.pointer(cursor_path) {
                        Some(serde_json::Value::String(next)) if !next.is_empty() => {
                            cursor = Some(next.clone());
                        }
                        Some(serde_json::Value::Null) | None => {
                            // No next cursor — done.
                            break;
                        }
                        Some(other) => {
                            // Cursor value is not a string — try to_string.
                            let s = other.to_string().trim_matches('"').to_owned();
                            if s.is_empty() || s == "null" {
                                break;
                            }
                            cursor = Some(s);
                        }
                    }
                }
            }
        }

        Ok(all_messages)
    }

    /// Build the URL with pagination query parameters appended.
    fn build_paginated_url(
        &self,
        base_url: &str,
        offset: u64,
        cursor: Option<&str>,
    ) -> ConnectorResult<String> {
        match self.properties.pagination_mode {
            PaginationMode::None => Ok(base_url.to_owned()),
            PaginationMode::Offset => {
                let page_size = self.properties.pagination_page_size.unwrap_or(100);
                let separator = if base_url.contains('?') { "&" } else { "?" };
                Ok(format!(
                    "{}{}{}={}&{}={}",
                    base_url,
                    separator,
                    self.properties.pagination_offset_param,
                    offset,
                    self.properties.pagination_limit_param,
                    page_size,
                ))
            }
            PaginationMode::Cursor => {
                if let Some(cursor_val) = cursor {
                    let separator = if base_url.contains('?') { "&" } else { "?" };
                    Ok(format!(
                        "{}{}{}={}",
                        base_url, separator, self.properties.pagination_cursor_param, cursor_val,
                    ))
                } else {
                    // First page — no cursor parameter.
                    Ok(base_url.to_owned())
                }
            }
        }
    }

    /// Extract the data items array from the JSON response using the configured path.
    fn extract_data_items(
        &self,
        body: &serde_json::Value,
    ) -> ConnectorResult<Vec<serde_json::Value>> {
        let target = if let Some(path) = &self.properties.response_data_path {
            body.pointer(path).ok_or_else(|| {
                anyhow::anyhow!("JSON pointer '{}' not found in API response", path)
            })?
        } else {
            body
        };

        match target {
            serde_json::Value::Array(arr) => Ok(arr.clone()),
            // Single object — treat as a one-element array.
            serde_json::Value::Object(_) => Ok(vec![target.clone()]),
            _ => Err(anyhow::anyhow!(
                "expected JSON array or object at data path, got: {}",
                target
            )
            .into()),
        }
    }

    /// Apply field-to-column name mappings to a JSON value.
    /// Renames keys in the top-level JSON object according to `fetcher.field.mappings`.
    fn apply_field_mappings(&self, item: &serde_json::Value) -> serde_json::Value {
        let mappings = match &self.properties.field_mappings {
            Some(m) if !m.is_empty() => m,
            _ => return item.clone(),
        };

        match item {
            serde_json::Value::Object(obj) => {
                let mut new_obj = serde_json::Map::with_capacity(obj.len());
                for (key, value) in obj {
                    let mapped_key = mappings.get(key).unwrap_or(key);
                    new_obj.insert(mapped_key.clone(), value.clone());
                }
                serde_json::Value::Object(new_obj)
            }
            _ => item.clone(),
        }
    }
}

/// Simple scheduling abstraction supporting cron expressions and fixed intervals.
struct Scheduler {
    kind: SchedulerKind,
}

enum SchedulerKind {
    /// Cron-based scheduling using the `croner` crate.
    Cron(croner::Cron),
    /// Fixed-interval scheduling.
    Interval(tokio::time::Interval),
}

impl Scheduler {
    fn new(cron_expr: Option<&str>, interval_seconds: Option<u64>) -> ConnectorResult<Self> {
        if let Some(expr) = cron_expr {
            let cron = croner::Cron::new(expr)
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid cron expression '{}': {}", expr, e))?;
            Ok(Self {
                kind: SchedulerKind::Cron(cron),
            })
        } else {
            let secs = interval_seconds.unwrap_or(60);
            let interval = tokio::time::interval(std::time::Duration::from_secs(secs));
            Ok(Self {
                kind: SchedulerKind::Interval(interval),
            })
        }
    }

    async fn wait_next_tick(&self) {
        match &self.kind {
            SchedulerKind::Cron(cron) => {
                let now = chrono::Utc::now();
                match cron.find_next_occurrence(&now, false) {
                    Ok(next) => {
                        let duration = (next - now).to_std().unwrap_or_default();
                        tokio::time::sleep(duration).await;
                    }
                    Err(e) => {
                        // No next occurrence — sleep forever (shouldn't happen with valid cron).
                        tracing::warn!(error = %e, "cron expression has no next occurrence, sleeping indefinitely");
                        std::future::pending::<()>().await;
                    }
                }
            }
            SchedulerKind::Interval(interval) => {
                // We need interior mutability for Interval::tick().
                // Since Scheduler is consumed by the stream, we handle this via
                // the non-mutable sleep-based approach for fixed intervals too.
                let period = interval.period();
                tokio::time::sleep(period).await;
            }
        }
    }
}

/// Render a Jinja2 template string with the given context values.
fn render_template(template: &str, context: &serde_json::Value) -> Result<String, anyhow::Error> {
    // Fast path: if there are no template markers, return as-is.
    if !template.contains("{{") && !template.contains("{%") {
        return Ok(template.to_owned());
    }

    let env = minijinja::Environment::new();
    let rendered = env
        .render_str(template, context)
        .map_err(|e| anyhow::anyhow!("template rendering failed: {}", e))?;
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_template_no_vars() {
        let result =
            render_template("https://api.example.com/users", &serde_json::json!({})).unwrap();
        assert_eq!(result, "https://api.example.com/users");
    }

    #[test]
    fn test_render_template_with_vars() {
        let ctx = serde_json::json!({"tenant_id": "abc123", "page": 1});
        let result = render_template(
            "https://api.example.com/{{ tenant_id }}/data?p={{ page }}",
            &ctx,
        )
        .unwrap();
        assert_eq!(result, "https://api.example.com/abc123/data?p=1");
    }

    #[test]
    fn test_apply_field_mappings() {
        let props = FetcherProperties {
            url: String::new(),
            method: "GET".into(),
            headers: None,
            body: None,
            cron: None,
            poll_interval_seconds: Some(60),
            response_data_path: None,
            field_mappings: Some(
                [
                    ("user_id".to_owned(), "id".to_owned()),
                    ("created_at".to_owned(), "signup_date".to_owned()),
                ]
                .into_iter()
                .collect(),
            ),
            pagination_mode: PaginationMode::None,
            pagination_page_size: None,
            pagination_offset_param: "offset".into(),
            pagination_limit_param: "limit".into(),
            pagination_cursor_path: None,
            pagination_cursor_param: "cursor".into(),
            params_sql: None,
            params_connection: None,
        };

        let reader = FetcherSplitReader {
            properties: props,
            split_id: "1-0".into(),
            events_so_far: 0,
            parser_config: ParserConfig::default(),
            source_ctx: crate::source::SourceContext::dummy().into(),
            rendered_url: String::new(),
            rendered_body: None,
            resolved_headers: BTreeMap::new(),
        };

        let input = serde_json::json!({
            "user_id": 42,
            "created_at": "2024-01-01",
            "email": "test@example.com"
        });

        let output = reader.apply_field_mappings(&input);
        assert_eq!(output["id"], 42);
        assert_eq!(output["signup_date"], "2024-01-01");
        assert_eq!(output["email"], "test@example.com");
        assert!(output.get("user_id").is_none());
        assert!(output.get("created_at").is_none());
    }

    #[test]
    fn test_extract_data_items_array() {
        let props = FetcherProperties {
            url: String::new(),
            method: "GET".into(),
            headers: None,
            body: None,
            cron: None,
            poll_interval_seconds: Some(60),
            response_data_path: Some("/data/items".to_owned()),
            field_mappings: None,
            pagination_mode: PaginationMode::None,
            pagination_page_size: None,
            pagination_offset_param: "offset".into(),
            pagination_limit_param: "limit".into(),
            pagination_cursor_path: None,
            pagination_cursor_param: "cursor".into(),
            params_sql: None,
            params_connection: None,
        };

        let reader = FetcherSplitReader {
            properties: props,
            split_id: "1-0".into(),
            events_so_far: 0,
            parser_config: ParserConfig::default(),
            source_ctx: crate::source::SourceContext::dummy().into(),
            rendered_url: String::new(),
            rendered_body: None,
            resolved_headers: BTreeMap::new(),
        };

        let body = serde_json::json!({
            "data": {
                "items": [
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"},
                ]
            }
        });

        let items = reader.extract_data_items(&body).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["id"], 1);
        assert_eq!(items[1]["name"], "Bob");
    }

    #[test]
    fn test_build_paginated_url_offset() {
        let props = FetcherProperties {
            url: String::new(),
            method: "GET".into(),
            headers: None,
            body: None,
            cron: None,
            poll_interval_seconds: Some(60),
            response_data_path: None,
            field_mappings: None,
            pagination_mode: PaginationMode::Offset,
            pagination_page_size: Some(50),
            pagination_offset_param: "offset".into(),
            pagination_limit_param: "limit".into(),
            pagination_cursor_path: None,
            pagination_cursor_param: "cursor".into(),
            params_sql: None,
            params_connection: None,
        };

        let reader = FetcherSplitReader {
            properties: props,
            split_id: "1-0".into(),
            events_so_far: 0,
            parser_config: ParserConfig::default(),
            source_ctx: crate::source::SourceContext::dummy().into(),
            rendered_url: String::new(),
            rendered_body: None,
            resolved_headers: BTreeMap::new(),
        };

        let url = reader
            .build_paginated_url("https://api.example.com/data", 100, None)
            .unwrap();
        assert_eq!(url, "https://api.example.com/data?offset=100&limit=50");

        let url = reader
            .build_paginated_url("https://api.example.com/data?foo=bar", 0, None)
            .unwrap();
        assert_eq!(
            url,
            "https://api.example.com/data?foo=bar&offset=0&limit=50"
        );
    }

    #[test]
    fn test_build_paginated_url_cursor() {
        let props = FetcherProperties {
            url: String::new(),
            method: "GET".into(),
            headers: None,
            body: None,
            cron: None,
            poll_interval_seconds: Some(60),
            response_data_path: None,
            field_mappings: None,
            pagination_mode: PaginationMode::Cursor,
            pagination_page_size: None,
            pagination_offset_param: "offset".into(),
            pagination_limit_param: "limit".into(),
            pagination_cursor_path: Some("/meta/next".to_owned()),
            pagination_cursor_param: "cursor".into(),
            params_sql: None,
            params_connection: None,
        };

        let reader = FetcherSplitReader {
            properties: props,
            split_id: "1-0".into(),
            events_so_far: 0,
            parser_config: ParserConfig::default(),
            source_ctx: crate::source::SourceContext::dummy().into(),
            rendered_url: String::new(),
            rendered_body: None,
            resolved_headers: BTreeMap::new(),
        };

        // First page: no cursor.
        let url = reader
            .build_paginated_url("https://api.example.com/data", 0, None)
            .unwrap();
        assert_eq!(url, "https://api.example.com/data");

        // Subsequent page: cursor=abc.
        let url = reader
            .build_paginated_url("https://api.example.com/data", 0, Some("abc"))
            .unwrap();
        assert_eq!(url, "https://api.example.com/data?cursor=abc");
    }
}
