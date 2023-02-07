use base64::{engine::general_purpose, Engine as _};
use std::env;

pub fn base64_encoded_md5_hash_with_secret(input: String) -> String {
    general_purpose::STANDARD
        .encode(&md5::compute(input + &env::var("CONVERSATION_ID_SECRET").unwrap()).0)[0..22]
        .to_owned()
}
