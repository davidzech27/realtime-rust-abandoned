use futures_util::StreamExt;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::WebSocketStream;

use crate::db::Database;
use crate::hash;

use error::FatalConnectionError;
use notification_loop::NotificationLoop;
use operation_loop::OperationLoop;

// handles connection and closing it but caller handles printing error

// only unwrap when stringifying struct

mod error;
mod nats_message;
mod notification_loop;
mod operation_loop;
mod user_event;

pub struct Connection {
    pub websocket: WebSocketStream<TcpStream>,
    pub db: Arc<Database>,
    pub nc: Arc<nats::asynk::Connection>,
    pub phone_number: i64,
    pub username: String,
}

impl Connection {
    pub async fn handle(self) -> Result<(), FatalConnectionError> {
        let (user_tx, user_rx) = self.websocket.split();
        let user_tx = Arc::new(Mutex::new(user_tx));

        let (result_tx, mut result_rx) = mpsc::channel::<Result<(), FatalConnectionError>>(1);
        let result_tx_clone = result_tx.clone();

        let (notification_loop_cancel_tx, notification_loop_cancel_rx) = mpsc::channel::<()>(1);
        let (operation_loop_cancel_tx, operation_loop_cancel_rx) = mpsc::channel::<()>(1);

        let notification_loop = NotificationLoop {
            user_tx: user_tx.clone(),
            nc: self.nc.clone(),
            username_hash: hash::base64_encoded_md5_hash_with_secret(self.username.clone()),
        };

        let operation_loop = OperationLoop {
            user_rx,
            user_tx,
            db: self.db,
            nc: self.nc,
            username: self.username,
        };

        tokio::task::spawn(async move {
            let result = notification_loop.handle(notification_loop_cancel_rx).await;

            let _ = operation_loop_cancel_tx.send(()).await; // will return error if other task completed first because sender will have been dropped, so we'll ignore this error

            let _ = result_tx.send(result).await; // same as above ^^^
        });

        tokio::task::spawn(async move {
            let result = operation_loop.handle(operation_loop_cancel_rx).await;

            let _ = notification_loop_cancel_tx.send(()).await;

            let _ = result_tx_clone.send(result).await;
        });

        result_rx.recv().await.unwrap() // senders won't drop until after sending to this channel
    }
}
