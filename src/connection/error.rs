use thiserror::Error;
use tungstenite::Message;

use crate::db::DatabaseError;

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("{0}")]
    Fatal(FatalConnectionError),
    #[error("{0}")]
    NonFatal(NonFatalConnectionError),
}

#[derive(Error, Debug)]
pub enum FatalConnectionError {
    #[error("Websocket error: {0}")]
    WebSocketError(#[from] tungstenite::Error),
    #[error("Unexpected close frame: {close_frame}")]
    UnexpectedClose { close_frame: String },
    #[error("Nats error while attempting to subscribe: {0}")]
    NatsSubscribeError(#[from] std::io::Error),
    #[error("Nats subscription terminated unexpectedly")]
    UnexpectedNatsSubscriptionTerminate,
    #[error("Received unsupported protocol: {0}")]
    UnsupportedProtocol(Message),
    #[error("Forbidden error: {0}")]
    Forbidden(&'static str),
}

#[derive(Error, Debug)]
#[error("{0}")]
pub struct UnsupportedFormatError(#[from] serde_json::Error);

#[derive(Error, Debug)]
pub enum NonFatalConnectionError {
    #[error("Database error: {0}")]
    DatabaseError(DatabaseError),
    #[error("Received unexpected message format: {0}")]
    UnsupportedFormat(#[from] UnsupportedFormatError), // non fatal error because this mainly serves as an indicator that the websocket client may have been implemented incorrectly
    #[error("Nats error while attempting to publish: {0}")]
    NatsPublishError(#[from] std::io::Error),
}
