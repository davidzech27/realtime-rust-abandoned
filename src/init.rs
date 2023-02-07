use crate::db::Database;
use std::{env, sync::Arc};

pub struct Init {
    pub db: Arc<Database>,
    pub nc: Arc<nats::asynk::Connection>,
    pub port: u16,
    pub access_token_secret: String,
}

impl Init {
    pub async fn init() -> Self {
        dotenv::dotenv().expect("Failed to load .env");

        tracing_subscriber::fmt::init();

        let db = Database::build(
            &env::var("SCYLLA_URL").expect("Must set SCYLLA_URL environment variable"),
            &env::var("SCYLLA_USERNAME").expect("Must set SCYLLA_USERNAME environment variable"),
            &env::var("SCYLLA_PASSWORD").expect("Must set SCYLLA_PASSWORD environment variable"),
            "zap",
        )
        .await
        .expect("Failed to connect to scylla cluster");

        let nc = nats::asynk::Options::with_credentials(
            env::var("NATS_CRED_PATH").expect("Must set NATS_CRED_PATH environment variable"),
        )
        .connect(env::var("NATS_URL").expect("Must set NATS_URL environment variable"))
        .await
        .expect("Failed to connect to nats server");

        env::var("CONVERSATION_ID_SECRET")
            .expect("Must set CONVERSATION_ID_SECRET environment variable");

        Self {
            db: Arc::new(db),
            nc: Arc::new(nc),
            port: env::var("PORT")
                .expect("Must set PORT environment variable")
                .parse()
                .expect("PORT environment variable could not be parsed to integer"),
            access_token_secret: env::var("ACCESS_TOKEN_SECRET")
                .expect("Must set ACCESS_TOKEN_SECRET environment variable"),
        }
    }
}
