use serde::Serialize;

use super::UserEvent;
use crate::connection::error::UnsupportedFormatError;

#[derive(Serialize)]
pub struct Notification(pub UserEvent);

impl Notification {
    pub fn from(raw_nats_message: nats::asynk::Message) -> Result<Self, UnsupportedFormatError> {
        Ok(Self(UserEvent::from_slice(&raw_nats_message.data)?))
    }

    pub fn to_message(&self) -> tungstenite::Message {
        tungstenite::Message::Text(serde_json::to_string(self).unwrap())
    }
}
