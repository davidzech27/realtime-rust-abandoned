use super::user_event::UserEvent;

pub struct NatsMessage {
    pub to_username_hash: String,
    pub user_event: UserEvent,
}

impl NatsMessage {
    pub fn subject(&self) -> &str {
        &self.to_username_hash
    }

    pub fn data(&self) -> Vec<u8> {
        self.user_event.to_vec()
    }
}
