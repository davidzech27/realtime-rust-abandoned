use scylla::{
    cql_to_rust::FromCqlVal,
    macros::{FromUserType, IntoUserType},
};

#[derive(FromUserType, IntoUserType, Clone)]
pub struct FriendProfile {
    pub username: String,
    pub name: String,
    pub friendship_started_on: scylla::frame::value::Timestamp,
}
