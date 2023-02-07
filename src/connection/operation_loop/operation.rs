use serde::{Deserialize, Serialize};

use super::{mutation::Mutation, query::Query};
use crate::connection::error::UnsupportedFormatError;

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub enum Operation {
    Query(Query),
    Mutation(Mutation),
}

impl Operation {
    pub fn from_str(str: &str) -> Result<Self, UnsupportedFormatError> {
        Ok(serde_json::from_str(str)?)
    }
}
