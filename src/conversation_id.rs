use chrono::prelude::*;

use crate::hash;
pub struct ConversationId {
    inner: String,
}

#[derive(PartialEq)]
pub enum ConversationRole {
    Chooser,
    Choosee,
    NotInConversation,
}

// for added security could append secret string to username before hashing

impl ConversationId {
    pub fn new(chooser_username: String, choosee_username: String) -> Self {
        let chooser_hash = hash::base64_encoded_md5_hash_with_secret(chooser_username);

        let choosee_hash = hash::base64_encoded_md5_hash_with_secret(choosee_username);

        let now: DateTime<Utc> = DateTime::default();

        let time_segment = (now.year() % 100).to_string() // basically an hour id
            + &now.month().to_string()
            + &now.day().to_string()
            + &now.hour().to_string();

        ConversationId {
            inner: chooser_hash + &choosee_hash + &time_segment,
        }
    }

    pub fn from(string: String) -> Self {
        Self { inner: string }
    }

    pub fn to_string(&self) -> String {
        self.inner.clone()
    }

    pub fn get_role_of_username(&self, username: &str) -> ConversationRole {
        let chooser_hash = self.get_chooser_hash();

        let choosee_hash = self.get_choosee_hash();

        let username_hash = hash::base64_encoded_md5_hash_with_secret(username.to_owned());

        if chooser_hash == username_hash {
            ConversationRole::Chooser
        } else if choosee_hash == username_hash {
            ConversationRole::Choosee
        } else {
            ConversationRole::NotInConversation
        }
    }

    pub fn get_chooser_hash(&self) -> &str {
        &self.inner[0..22]
    }

    pub fn get_choosee_hash(&self) -> &str {
        &self.inner[22..44]
    }
}
