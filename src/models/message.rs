use chrono::prelude::*;
use serde::Serialize;

#[derive(Serialize)]
pub struct Message {
    pub content: String,
    pub sent_at: DateTime<Utc>,
    pub from_chooser: bool,
}
