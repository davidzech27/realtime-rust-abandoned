use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(tag = "op", content = "d", rename_all = "camelCase")]
pub enum Mutation {
    Choose {
        content: String,
        choosee_username: String,
    },
    Send {
        content: String,
        conversation_id: String,
    },
    RegisterPresenceChoosee {
        conversation_id: String,
        leaving: bool,
    },
}
