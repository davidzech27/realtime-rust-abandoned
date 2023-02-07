use chrono::prelude::*;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    Mutex,
};
use tokio_tungstenite::WebSocketStream;
use tungstenite::{protocol::frame::coding::CloseCode, Message};

use super::{
    error::{ConnectionError, FatalConnectionError, NonFatalConnectionError},
    nats_message::NatsMessage,
    user_event::UserEvent,
};
use crate::{
    conversation_id::{ConversationId, ConversationRole},
    db::Database,
};
use mutation::Mutation;
use operation::Operation;
use query::Query;
use response::Response;

mod mutation;
mod operation;
mod query;
mod response;

pub struct OperationLoop {
    pub user_rx: SplitStream<WebSocketStream<TcpStream>>,
    pub user_tx: Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
    pub db: Arc<Database>,
    pub nc: Arc<nats::asynk::Connection>,
    pub username: String,
}

impl OperationLoop {
    pub async fn handle(
        mut self,
        mut cancel_rx: mpsc::Receiver<()>,
    ) -> Result<(), FatalConnectionError> {
        let (err_tx, mut err_rx) = mpsc::unbounded_channel::<ConnectionError>(); // unbounded because theoretically many ws sends could fail at once

        'operation_loop: while let Some(message) = tokio::select! {
            next = self.user_rx.next() => next,
            _ = cancel_rx.recv() => {
                return Ok(());
            }
            err = err_rx.recv() => {
                let err = err.expect("err_tx should not have dropped until after the select loop finishes");

                match err {
                    ConnectionError::Fatal(err) => {
                        return Err(err);
                    }
                    ConnectionError::NonFatal(err) => {
                        warn!("Non fatal error: {}", err);
                    }
                };

                continue 'operation_loop;
            }
        } {
            let message = message?;

            match message {
                Message::Text(message) => match Operation::from_str(&message) {
                    Ok(user_operation) => {
                        let err_tx = err_tx.clone();

                        self.handle_operation(user_operation, err_tx);
                    }
                    Err(err) => {
                        let _ = err_tx.send(ConnectionError::NonFatal(
                            NonFatalConnectionError::UnsupportedFormat(err),
                        )); // no way for err_rx to be dropped if this is running

                        continue;
                    }
                },
                Message::Close(close_frame) => {
                    if let Some(close_frame) = close_frame {
                        match close_frame.code {
                            CloseCode::Normal | CloseCode::Away => {
                                return Ok(());
                            }
                            _ => {
                                return Err(FatalConnectionError::UnexpectedClose {
                                    close_frame: close_frame.to_string(),
                                })
                            }
                        }
                    }

                    return Ok(());
                }
                _ => {
                    return Err(FatalConnectionError::UnsupportedProtocol(message));
                }
            }
        }

        Ok(()) // not sure if this code will ever be reached
    }

    fn handle_operation(
        &self,
        user_operation: Operation,
        err_tx: UnboundedSender<ConnectionError>,
    ) {
        match user_operation {
            Operation::Query(query) => match query {
                Query::Messages {
                    conversation_id,
                    take,
                    after_sent_at,
                } => {
                    let conversation_id = ConversationId::from(conversation_id);

                    if conversation_id.get_role_of_username(&self.username)
                        == ConversationRole::NotInConversation
                    {
                        let _ =
                            err_tx.send(ConnectionError::Fatal(FatalConnectionError::Forbidden(
                                "User attempted to get messages in conversation not belonging to",
                            )));
                        return;
                    }

                    let db = self.db.clone();
                    let user_tx = self.user_tx.clone();

                    tokio::task::spawn(async move {
                        match db
                            .get_messages(&conversation_id.to_string(), take, after_sent_at)
                            .await
                        {
                            Ok(messages) => {
                                let response = Response::Messages {
                                    conversation_id: conversation_id.to_string(),
                                    messages,
                                };

                                if let Err(err) =
                                    user_tx.lock().await.send(response.to_message()).await
                                {
                                    let _ = err_tx.send(ConnectionError::Fatal(
                                        FatalConnectionError::WebSocketError(err),
                                    )); // ignoring error because loop could've already closed
                                }
                            }
                            Err(err) => {
                                let _ = err_tx.send(ConnectionError::NonFatal(
                                    NonFatalConnectionError::DatabaseError(err),
                                ));

                                if let Err(err) = user_tx
                                    .lock()
                                    .await
                                    .send(
                                        Response::Error(
                                            "Failed to get messages for this conversation"
                                                .to_owned(),
                                        )
                                        .to_message(),
                                    )
                                    .await
                                {
                                    let _ = err_tx.send(ConnectionError::Fatal(
                                        FatalConnectionError::WebSocketError(err),
                                    ));
                                }
                            }
                        }
                    });
                }
            },
            Operation::Mutation(mutation) => match mutation {
                Mutation::Choose {
                    content,
                    choosee_username,
                } => {
                    let conversation_id =
                        ConversationId::new(self.username.clone(), choosee_username.clone());

                    let user_event = UserEvent::Chosen {
                        conversation_id: conversation_id.to_string(),
                        content: content.clone(),
                        sent_at: DateTime::<Utc>::default(),
                    };

                    let nats_message = NatsMessage {
                        to_username_hash: conversation_id.get_choosee_hash().to_owned(),
                        user_event,
                    };

                    let nc = self.nc.clone();
                    let err_tx_clone = err_tx.clone();

                    tokio::task::spawn(async move {
                        if let Err(err) = nc
                            .publish(nats_message.subject(), nats_message.data())
                            .await
                        {
                            let _ = err_tx_clone.send(ConnectionError::NonFatal(
                                // err_rx could potentially be dropped because this is running in task and after an await, so unfortunately error will not get logged, but not really worth doing anything about because of how unlikely it is
                                NonFatalConnectionError::NatsPublishError(err),
                            ));
                        }
                    });

                    let db = self.db.clone();
                    let username = self.username.clone();
                    let conversation_id_string = conversation_id.to_string();
                    let err_tx_clone = err_tx.clone();

                    tokio::task::spawn(async move {
                        if let Err(err) = db
                            .new_conversation(&username, &choosee_username, &conversation_id_string)
                            .await
                        {
                            let _ = err_tx_clone.send(ConnectionError::NonFatal(
                                NonFatalConnectionError::DatabaseError(err),
                            ));
                        }
                    });

                    let db = self.db.clone();
                    let conversation_id_string = conversation_id.to_string();

                    tokio::task::spawn(async move {
                        if let Err(err) = db
                            .new_message(&conversation_id_string, &content, true)
                            .await
                        {
                            let _ = err_tx.send(ConnectionError::NonFatal(
                                NonFatalConnectionError::DatabaseError(err),
                            ));
                        }
                    });
                }
                Mutation::Send {
                    content,
                    conversation_id,
                } => {
                    let conversation_id = ConversationId::from(conversation_id);

                    let (to_username_hash, from_chooser) =
                        match conversation_id.get_role_of_username(&self.username) {
                            ConversationRole::Chooser => {
                                (conversation_id.get_choosee_hash().to_owned(), true)
                            }
                            ConversationRole::Choosee => {
                                (conversation_id.get_chooser_hash().to_owned(), false)
                            }
                            ConversationRole::NotInConversation => {
                                let _ = err_tx
                                .send(ConnectionError::Fatal(FatalConnectionError::Forbidden(
                                "User attempted to send message to conversation not belonging to",
                            )));

                                return;
                            }
                        };

                    let user_event = UserEvent::Message {
                        conversation_id: conversation_id.to_string(),
                        content: content.clone(),
                        sent_at: DateTime::<Utc>::default(),
                    };

                    let nats_message = NatsMessage {
                        to_username_hash,
                        user_event,
                    };

                    let nc = self.nc.clone();
                    let err_tx_clone = err_tx.clone();

                    tokio::task::spawn(async move {
                        if let Err(err) = nc
                            .publish(nats_message.subject(), nats_message.data())
                            .await
                        {
                            let _ = err_tx_clone.send(ConnectionError::NonFatal(
                                NonFatalConnectionError::NatsPublishError(err),
                            ));
                        }
                    });

                    let db = self.db.clone();

                    tokio::task::spawn(async move {
                        if let Err(err) = db
                            .new_message(&conversation_id.to_string(), &content, from_chooser)
                            .await
                        {
                            let _ = err_tx.send(ConnectionError::NonFatal(
                                NonFatalConnectionError::DatabaseError(err),
                            ));
                        }
                    });
                }
                Mutation::RegisterPresenceChoosee {
                    conversation_id,
                    leaving,
                } => {
                    let conversation_id = ConversationId::from(conversation_id);

                    let role_in_conversation = conversation_id.get_role_of_username(&self.username);

                    if role_in_conversation == ConversationRole::NotInConversation
                        || role_in_conversation == ConversationRole::Chooser
                    {
                        let _ = err_tx.send(ConnectionError::Fatal(FatalConnectionError::Forbidden("User attempted to register choosee presence in conversation not not a choosee of")));

                        return;
                    }

                    todo!();
                    // db.update_choosee_last_presence_at(choosee_username, created_at);
                }
            },
        }
    }
}
