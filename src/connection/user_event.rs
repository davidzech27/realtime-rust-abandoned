use chrono::prelude::*;
use serde::{Deserialize, Serialize};

use crate::connection::error::UnsupportedFormatError;

#[derive(Deserialize, Serialize)]
#[serde(tag = "op", content = "d", rename_all = "camelCase")]
pub enum UserEvent {
    Chosen {
        conversation_id: String,
        content: String,
        sent_at: DateTime<Utc>,
    },
    Message {
        conversation_id: String,
        content: String,
        sent_at: DateTime<Utc>,
    },
    ChooseePresence {
        conversation_id: String,
        leaving: bool,
        occurred_at: DateTime<Utc>,
    },
}

impl UserEvent {
    pub fn to_vec(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, UnsupportedFormatError> {
        Ok(serde_json::from_slice::<Self>(slice)?)
    }
}
