use serde::Serialize;

use crate::models::message::Message;

#[derive(Serialize)]
#[serde(tag = "op", content = "d", rename_all = "camelCase")]
pub enum Response {
    Error(String),
    Messages {
        conversation_id: String,
        messages: Vec<Message>,
    },
}

impl Response {
    pub fn to_message(&self) -> tungstenite::Message {
        tungstenite::Message::Text(serde_json::to_string(self).unwrap())
    }
}
