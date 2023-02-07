use chrono::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "op", content = "d", rename_all = "camelCase")]
pub enum Query {
    Messages {
        conversation_id: String,
        take: i8,
        after_sent_at: DateTime<Utc>,
    },
}
