use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use tungstenite::handshake::server::Request;

pub struct JWTAuth {
    decoding_key: DecodingKey,
    validation: Validation,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessTokenPayload {
    pub phone_number: i64,
    pub username: String,
}

impl JWTAuth {
    pub fn new(access_token_secret: &str) -> Self {
        let access_token_secret = access_token_secret.as_bytes();

        Self {
            decoding_key: DecodingKey::from_secret(access_token_secret),
            validation: Validation::new(Algorithm::HS256),
        }
    }

    pub fn veryify_req(&self, req: &Request) -> Result<AccessTokenPayload, ()> {
        jsonwebtoken::decode::<AccessTokenPayload>(
            req.headers()
                .get("Authorization")
                .ok_or(())?
                .to_str()
                .map_err(|_| ())?
                .strip_prefix("Bearer ")
                .ok_or(())?,
            &self.decoding_key,
            &self.validation,
        )
        .map_err(|_| ())
        .map(|token_data| token_data.claims)
    }
}
