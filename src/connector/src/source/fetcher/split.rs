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

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::SplitId;
use crate::source::base::SplitMetaData;

/// Split metadata for the REST API fetcher source.
///
/// Each split represents an independent API polling task. When `fetcher.params.sql`
/// is configured, each SQL result row becomes a separate split with its own
/// template parameters.
#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq, Hash)]
pub struct FetcherSplit {
    /// Index of this split (0-based).
    pub split_index: i32,
    /// Total number of splits.
    pub split_num: i32,
    /// Monotonic offset counter tracking the last successfully produced row.
    /// Used for checkpointing and resumption.
    pub start_offset: Option<u64>,
    /// JSON-serialized template variable bindings for this split, derived from
    /// the external SQL query result row. `None` if no SQL params are configured.
    pub template_params: Option<String>,
}

impl SplitMetaData for FetcherSplit {
    fn id(&self) -> SplitId {
        format!("{}-{}", self.split_num, self.split_index).into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.start_offset = Some(last_seen_offset.as_str().parse::<u64>().unwrap());
        Ok(())
    }
}

impl FetcherSplit {
    pub fn new(
        split_index: i32,
        split_num: i32,
        start_offset: Option<u64>,
        template_params: Option<String>,
    ) -> Self {
        Self {
            split_index,
            split_num,
            start_offset,
            template_params,
        }
    }
}
