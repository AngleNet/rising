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

pub mod enumerator;
pub mod source;
pub mod split;

use std::collections::{BTreeMap, HashMap};

pub use enumerator::*;
use serde::Deserialize;
use serde_with::json::JsonString;
use serde_with::serde_as;
pub use source::*;
pub use split::*;

use crate::deserialize_optional_u64_from_string;
use crate::enforce_secret::EnforceSecret;
use crate::source::SourceProperties;

pub const FETCHER_CONNECTOR: &str = "fetcher";

/// Pagination mode for the REST API fetcher.
#[derive(Clone, Debug, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PaginationMode {
    /// No pagination — fetch a single page.
    #[default]
    None,
    /// Offset/limit pagination: increment offset by page_size each page.
    Offset,
    /// Cursor-based pagination: extract a next-cursor token from the response.
    Cursor,
}

impl crate::with_options::WithOptions for PaginationMode {
    fn assert_receiver_is_with_options(&self) {}
}

/// Properties for the REST API fetcher source connector.
///
/// Example usage:
/// ```sql
/// CREATE SOURCE api_data (
///     id INT,
///     name VARCHAR,
///     signup_date TIMESTAMPTZ
/// ) WITH (
///     connector = 'fetcher',
///     fetcher.url = 'https://api.example.com/users',
///     fetcher.method = 'GET',
///     fetcher.headers = '{"Authorization": "Bearer xxx"}',
///     fetcher.cron = '*/5 * * * *',
///     fetcher.response.data.path = '/data/items',
///     fetcher.field.mappings = '{"user_id": "id", "created_at": "signup_date"}',
///     fetcher.pagination.mode = 'offset',
///     fetcher.pagination.page.size = '100',
/// ) FORMAT PLAIN ENCODE JSON;
/// ```
#[serde_as]
#[derive(Clone, Debug, Deserialize, with_options::WithOptions)]
pub struct FetcherProperties {
    /// The REST API URL. May contain Jinja2 template variables like `{{ var_name }}`
    /// that are rendered using SQL query results from `fetcher.params.sql`.
    #[serde(rename = "fetcher.url")]
    pub url: String,

    /// HTTP method: GET, POST, PUT, etc. Default: GET.
    #[serde(rename = "fetcher.method", default = "default_method")]
    pub method: String,

    /// HTTP headers as a JSON object string, e.g. `{"Authorization": "Bearer token"}`.
    /// Supports API key, Bearer token, Basic auth via standard HTTP headers.
    #[serde(rename = "fetcher.headers", default)]
    #[serde_as(as = "Option<JsonString>")]
    pub headers: Option<BTreeMap<String, String>>,

    /// HTTP request body template (for POST/PUT). Supports Jinja2 template variables.
    #[serde(rename = "fetcher.body", default)]
    pub body: Option<String>,

    /// Cron expression for scheduling fetches, e.g. `"*/5 * * * *"` (every 5 min).
    /// Supports standard 5-field cron syntax and extended 6-field (with seconds).
    /// Mutually exclusive with `fetcher.poll.interval.seconds`.
    #[serde(rename = "fetcher.cron", default)]
    pub cron: Option<String>,

    /// Simple polling interval in seconds. Default: 60.
    /// Mutually exclusive with `fetcher.cron`.
    #[serde(
        rename = "fetcher.poll.interval.seconds",
        default,
        deserialize_with = "deserialize_optional_u64_from_string"
    )]
    pub poll_interval_seconds: Option<u64>,

    /// JSON pointer (RFC 6901) to the data array in the API response.
    /// E.g. `/data/items` extracts `response["data"]["items"]`.
    /// If not specified, the entire response is treated as the data
    /// (must be a JSON array or single object).
    #[serde(rename = "fetcher.response.data.path", default)]
    pub response_data_path: Option<String>,

    /// Field-to-column mapping as a JSON object string.
    /// Keys are the API response field names, values are the target RisingWave column names.
    /// E.g. `{"user_id": "id", "created_at": "signup_date"}` renames `user_id` → `id`.
    /// Unmapped fields keep their original names.
    #[serde(rename = "fetcher.field.mappings", default)]
    #[serde_as(as = "Option<JsonString>")]
    pub field_mappings: Option<BTreeMap<String, String>>,

    // -- Pagination options --
    /// Pagination mode: `none`, `offset`, or `cursor`. Default: `none`.
    #[serde(rename = "fetcher.pagination.mode", default)]
    pub pagination_mode: PaginationMode,

    /// Page size for offset pagination or max items per page for cursor pagination.
    #[serde(
        rename = "fetcher.pagination.page.size",
        default,
        deserialize_with = "deserialize_optional_u64_from_string"
    )]
    pub pagination_page_size: Option<u64>,

    /// Query parameter name for the offset value in offset pagination. Default: `offset`.
    #[serde(
        rename = "fetcher.pagination.offset.param",
        default = "default_offset_param"
    )]
    pub pagination_offset_param: String,

    /// Query parameter name for the limit/page_size in offset pagination. Default: `limit`.
    #[serde(
        rename = "fetcher.pagination.limit.param",
        default = "default_limit_param"
    )]
    pub pagination_limit_param: String,

    /// JSON pointer to the next-cursor token in the API response for cursor pagination.
    /// E.g. `/meta/next_cursor`.
    #[serde(rename = "fetcher.pagination.cursor.path", default)]
    pub pagination_cursor_path: Option<String>,

    /// Query parameter name for the cursor value in cursor pagination. Default: `cursor`.
    #[serde(
        rename = "fetcher.pagination.cursor.param",
        default = "default_cursor_param"
    )]
    pub pagination_cursor_param: String,

    // -- External SQL parameter options --
    /// SQL query to run against an external database. Each result row produces
    /// a separate split whose columns are available as Jinja2 template variables
    /// in `fetcher.url`, `fetcher.body`, and `fetcher.headers`.
    #[serde(rename = "fetcher.params.sql", default)]
    pub params_sql: Option<String>,

    /// Connection string for the external database used by `fetcher.params.sql`.
    /// Supports `postgres://...` and `mysql://...` schemes.
    #[serde(rename = "fetcher.params.connection", default)]
    pub params_connection: Option<String>,

    /// Whether to include split template parameters (from `fetcher.params.sql` rows)
    /// in each emitted JSON object.
    ///
    /// Useful for preserving request context fields like `code` / `exchange` that may
    /// not be present in the API response payload.
    /// Response payload fields take precedence on key conflicts.
    #[serde(rename = "fetcher.include.params", default)]
    pub include_params: bool,

    /// Maximum number of poll cycles to perform before stopping.
    /// If not set, the fetcher polls indefinitely.
    /// Useful for testing or one-shot ingestion scenarios.
    #[serde(
        rename = "fetcher.max.poll.count",
        default,
        deserialize_with = "deserialize_optional_u64_from_string"
    )]
    pub max_poll_count: Option<u64>,

    /// Number of concurrent HTTP requests per poll cycle for offset pagination.
    /// When set to N > 1, the fetcher issues up to N page requests in parallel
    /// during each poll cycle (offset mode only; cursor mode is inherently sequential).
    /// Default: 1 (sequential fetching).
    #[serde(
        rename = "fetcher.concurrency",
        default,
        deserialize_with = "deserialize_optional_u64_from_string"
    )]
    pub concurrency: Option<u64>,
}

impl EnforceSecret for FetcherProperties {}

impl SourceProperties for FetcherProperties {
    type Split = FetcherSplit;
    type SplitEnumerator = FetcherSplitEnumerator;
    type SplitReader = FetcherSplitReader;

    const SOURCE_NAME: &'static str = FETCHER_CONNECTOR;
}

impl crate::source::UnknownFields for FetcherProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

fn default_method() -> String {
    "GET".to_owned()
}

fn default_offset_param() -> String {
    "offset".to_owned()
}

fn default_limit_param() -> String {
    "limit".to_owned()
}

fn default_cursor_param() -> String {
    "cursor".to_owned()
}
