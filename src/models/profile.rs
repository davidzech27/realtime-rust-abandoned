use scylla::{
    cql_to_rust::FromCqlVal,
    macros::{FromUserType, IntoUserType},
};

#[derive(FromUserType, IntoUserType, Clone)]
pub struct Profile {
    pub username: String,
    pub name: String,
}
