use futures_util::{stream::SplitSink, SinkExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message;

use super::error::FatalConnectionError;
use super::nats_message::NatsMessage;
use super::user_event::UserEvent;
use notification::Notification;

mod notification;

pub struct NotificationLoop {
    pub user_tx: Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
    pub nc: Arc<nats::asynk::Connection>,
    pub username_hash: String,
}

impl NotificationLoop {
    pub async fn handle(
        mut self,
        mut cancel_rx: mpsc::Receiver<()>,
    ) -> Result<(), FatalConnectionError> {
        let message_sub = self.nc.subscribe(&self.username_hash).await?;

        while let Some(nats_message) = tokio::select! {
            next = message_sub.next() => next,
            _ = cancel_rx.recv() => return Ok(()),
        } {
            match Notification::from(nats_message) {
                Ok(Notification(user_event)) => {
                    self.handle_user_event(user_event).await?;
                }
                Err(err) => {
                    warn!("Invalid nats message received: {}", err);

                    continue;
                }
            }
        }

        Err(FatalConnectionError::UnexpectedNatsSubscriptionTerminate) // will only get to this when message_sub returns none. this line won't run if nc_loop is canceled
    }

    pub async fn handle_user_event(&mut self, data: UserEvent) -> Result<(), FatalConnectionError> {
        self.user_tx
            .lock()
            .await
            .send(Message::Text(data.to_string()))
            .await?;

        Ok(())
    }
}
