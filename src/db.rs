use chrono::{prelude::*, Duration};
use futures_util::FutureExt;
use scylla::prepared_statement::PreparedStatement;
use std::sync::Arc;
use thiserror::Error;

use crate::models::{friend_profile::FriendProfile, message::Message, profile::Profile};

const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

pub struct Database {
    db: Arc<scylla::Session>,
    new_conversation_query: PreparedStatement,
    new_message_query: PreparedStatement,
    update_choosee_last_presence_at_query: PreparedStatement,
    get_messages_query: PreparedStatement,
    add_friend_request_on_sender_query: PreparedStatement,
    add_friend_request_on_receiver_query: PreparedStatement,
    get_friends_of_user_query: PreparedStatement,
    remove_friend_request_on_sender_query: PreparedStatement,
    remove_friend_request_on_receiver_query: PreparedStatement,
    add_friend_query: PreparedStatement,
    add_friends_of_friends_query: PreparedStatement,
    remove_friend_query: PreparedStatement,
    remove_friends_of_friends_query: PreparedStatement,
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct DatabaseError(String);

impl Database {
    pub async fn build(
        known_node_hostname: &str,
        username: &str,
        password: &str,
        keyspace: &str,
    ) -> Result<Self, scylla::transport::errors::NewSessionError> {
        let db = Arc::new(
            scylla::SessionBuilder::new()
                .known_node(known_node_hostname)
                .user(username, password)
                .use_keyspace(keyspace, true)
                .build()
                .await?,
        );

        let new_conversation_query = Self::prepare_new_conversation_query(&db).await;

        let new_message_query = Self::prepare_new_message_query(&db).await;

        let update_choosee_last_presence_at_query =
            Self::prepare_update_choosee_last_presence_at_query(&db).await;

        let get_messages_query = Self::prepare_get_messages_query(&db).await;

        let add_friend_request_on_sender_query =
            Self::prepare_add_friend_request_on_sender_query(&db).await;

        let get_friends_of_user_query = Self::prepare_get_friends_of_user_query(&db).await;

        let add_friend_request_on_receiver_query =
            Self::prepare_add_friend_request_on_receiver_query(&db).await;

        let remove_friend_request_on_sender_query =
            Self::prepare_remove_friend_request_on_sender_query(&db).await;

        let remove_friend_request_on_receiver_query =
            Self::prepare_remove_friend_request_on_receiver_query(&db).await;

        let add_friend_query = Self::prepare_add_friend_query(&db).await;

        let add_friends_of_friends_query = Self::prepare_add_friends_of_friends_query(&db).await;

        let remove_friend_query = Self::prepare_remove_friend_query(&db).await;

        let remove_friends_of_friends_query =
            Self::prepare_remove_friends_of_friends_query(&db).await;

        Ok(Database {
            db,
            new_conversation_query,
            new_message_query,
            update_choosee_last_presence_at_query,
            get_messages_query,
            add_friend_request_on_sender_query,
            add_friend_request_on_receiver_query,
            get_friends_of_user_query,
            remove_friend_request_on_sender_query,
            remove_friend_request_on_receiver_query,
            add_friend_query,
            add_friends_of_friends_query,
            remove_friend_query,
            remove_friends_of_friends_query,
        })
    }

    async fn prepare_new_conversation_query(db: &scylla::Session) -> PreparedStatement {
        let mut new_conversation_query = db.prepare("INSERT INTO conversation (chooser_username, choosee_username, chooser_name, choosee_name, id, created_at) values (?, ?, ?, ?, ?, ?)").await.expect("New conversation prepared query failed");
        new_conversation_query.set_is_idempotent(true);
        new_conversation_query
    }

    pub async fn new_conversation(
        &self,
        chooser_username: &str,
        choosee_username: &str,
        chooser_name: &str,
        choosee_name: &str,
        conversation_id: &str,
    ) -> Result<(), DatabaseError> {
        self.db
            .execute(
                &self.new_conversation_query,
                (
                    chooser_username,
                    choosee_username,
                    chooser_name,
                    choosee_name,
                    conversation_id.to_string(),
                    Self::current_timestamp(),
                ),
            )
            .await
            .map(|_| ())
            .map_err(|err| DatabaseError(format!("Error creating new conversation: {}", err)))
    }

    async fn prepare_new_message_query(db: &scylla::Session) -> PreparedStatement {
        let mut get_messages_query = db
            .prepare(
                "INSERT INTO conversation (conversation_id, content, sent_at, from_chooser) VALUES (?, ?, ?, ?)",
            )
            .await
            .expect("Get messages prepared query failed");
        get_messages_query.set_is_idempotent(true);
        get_messages_query
    }

    pub async fn new_message(
        &self,
        conversation_id: &str,
        content: &str,
        from_chooser: bool,
    ) -> Result<(), DatabaseError> {
        self.db
            .execute(
                &self.new_message_query,
                (
                    conversation_id,
                    content,
                    Self::current_timestamp(),
                    from_chooser,
                ),
            )
            .await
            .map(|_| ())
            .map_err(|err| DatabaseError(format!("Error creating new message: {}", err)))
    }

    async fn prepare_update_choosee_last_presence_at_query(
        db: &scylla::Session,
    ) -> PreparedStatement {
        let mut update_choosee_last_presence_at_query = db
            .prepare("INSERT INTO choosee_presence (conversation_id, occurred_at, leaving, chooser_username) VALUES (?, ?, ?, ?)")
            .await
            .expect("Update choosee last presence prepared query failed");
        update_choosee_last_presence_at_query.set_is_idempotent(true);
        update_choosee_last_presence_at_query
    }

    pub async fn update_choosee_last_presence_at(
        &self,
        conversation_id: &str,
        occurred_at: DateTime<Utc>,
        leaving: bool,
        chooser_username: &str,
    ) -> Result<(), DatabaseError> {
        self.db
            .execute(
                &self.update_choosee_last_presence_at_query,
                (
                    conversation_id,
                    Self::timestamp_from_datetime(occurred_at),
                    leaving,
                    chooser_username,
                ),
            )
            .await
            .map(|_| ())
            .map_err(|err| {
                DatabaseError(format!("Error updating choosee_last_presence_at: {}", err))
            })
    }

    async fn prepare_get_messages_query(db: &scylla::Session) -> PreparedStatement {
        let mut get_messages_query = db
            .prepare(
                "SELECT content, sent_at, from_chooser FROM message WHERE conversation_id = ? AND sent_at > ? LIMIT ?",
            )
            .await
            .expect("Get messages prepared query failed");
        get_messages_query.set_is_idempotent(true);
        get_messages_query
    }

    pub async fn get_messages(
        &self,
        conversation_id: &str,
        take: i8,
        after_sent_at: DateTime<Utc>,
    ) -> Result<Vec<Message>, DatabaseError> {
        let mut message_vec = Vec::<Message>::new();

        for row in self
            .db
            .execute(
                &self.get_messages_query,
                (
                    conversation_id,
                    Self::timestamp_from_datetime(after_sent_at),
                    take,
                ),
            )
            .await
            .map_err(|err| DatabaseError(format!("Error getting messages: {}", err)))?
            .rows_typed_or_empty::<(String, Duration, bool)>()
        {
            let row =
                row.map_err(|err| DatabaseError(format!("Error getting messages: {}", err)))?;

            message_vec.push(Message {
                content: row.0,
                sent_at: Self::datetime_from_timestamp(row.1),
                from_chooser: row.2,
            });
        }

        Ok(message_vec)
    }

    async fn prepare_add_friend_request_on_sender_query(db: &scylla::Session) -> PreparedStatement {
        let mut add_friend_request_on_sender_query = db.prepare("UPDATE user SET friend_requests_sent = friend_requests_sent + { ? } WHERE username = ?").await.expect("Add friend request on sender prepared query failed");
        add_friend_request_on_sender_query.set_is_idempotent(true);
        add_friend_request_on_sender_query
    }

    async fn prepare_add_friend_request_on_receiver_query(
        db: &scylla::Session,
    ) -> PreparedStatement {
        let mut add_friend_request_on_receiver_query = db.prepare("UPDATE user SET friend_requests_received = friend_requests_received + { ? } WHERE username = ?").await.expect("Add friend request on sender prepared query failed");
        add_friend_request_on_receiver_query.set_is_idempotent(true);
        add_friend_request_on_receiver_query
    }

    pub async fn create_friend_request(
        &self,
        sender: Profile,
        receiver: Profile,
    ) -> Result<(), DatabaseError> {
        let sender_username_clone = sender.username.clone();
        let receiver_username_clone = receiver.username.clone();

        let (sender_result, receiver_result) = tokio::join!(
            self.db.execute(
                &self.add_friend_request_on_sender_query,
                (receiver, sender_username_clone),
            ),
            self.db.execute(
                &self.add_friend_request_on_receiver_query,
                (sender, receiver_username_clone),
            )
        );

        sender_result.map_err(|err| {
            DatabaseError(format!(
                "Error adding friend requestee username to requester: {}",
                err
            ))
        })?;

        receiver_result.map_err(|err| {
            DatabaseError(format!(
                "Error adding friend requester username to requestee: {}",
                err
            ))
        })?;

        Ok(())
    }

    async fn prepare_remove_friend_request_on_sender_query(
        db: &scylla::Session,
    ) -> PreparedStatement {
        let mut remove_friend_request_on_sender_query = db.prepare("UPDATE user SET friend_requests_sent = friend_requests_sent - { ? } WHERE username = ?").await.expect("Remove friend request on sender prepared query failed");
        remove_friend_request_on_sender_query.set_is_idempotent(true);
        remove_friend_request_on_sender_query
    }

    async fn prepare_remove_friend_request_on_receiver_query(
        db: &scylla::Session,
    ) -> PreparedStatement {
        let mut remove_friend_request_on_receiver_query = db.prepare("UPDATE user SET friend_requests_received = friend_requests_received - { ? } WHERE username = ?").await.expect("Remove friend request on sender prepared query failed");
        remove_friend_request_on_receiver_query.set_is_idempotent(true);
        remove_friend_request_on_receiver_query
    }

    pub async fn delete_friend_request(
        &self,
        sender: Profile,
        receiver: Profile,
    ) -> Result<(), DatabaseError> {
        let sender_username_clone = sender.username.clone();
        let receiver_username_clone = receiver.username.clone();

        let (sender_result, receiver_result) = tokio::join!(
            self.db.execute(
                &self.remove_friend_request_on_sender_query,
                (receiver, sender_username_clone),
            ),
            self.db.execute(
                &self.remove_friend_request_on_receiver_query,
                (sender, receiver_username_clone),
            )
        );

        sender_result.map_err(|err| {
            DatabaseError(format!(
                "Error removing friend requestee username from requester: {}",
                err
            ))
        })?;

        receiver_result.map_err(|err| {
            DatabaseError(format!(
                "Error removing friend requester username from requestee: {}",
                err
            ))
        })?;

        Ok(())
    }

    async fn prepare_get_friends_of_user_query(db: &scylla::Session) -> PreparedStatement {
        let mut get_friends_of_user_query = db
            .prepare("SELECT friends FROM user WHERE username = ?")
            .await
            .expect("Get friends of user prepared query failed");
        get_friends_of_user_query.set_is_idempotent(true);
        get_friends_of_user_query
    }

    pub async fn get_friends(&self, username: &str) -> Result<Vec<FriendProfile>, DatabaseError> {
        let mut friend_vec = Vec::<FriendProfile>::new();

        for row in self
            .db
            .execute(&self.get_friends_of_user_query, (username,))
            .await
            .map_err(|err| DatabaseError(format!("Error get friends of user: {}", err)))?
            .rows_typed_or_empty::<(FriendProfile,)>()
        {
            let row =
                row.map_err(|err| DatabaseError(format!("Error get friends of user: {}", err)))?;

            friend_vec.push(row.0);
        }

        Ok(friend_vec)
    }

    async fn prepare_add_friend_query(db: &scylla::Session) -> PreparedStatement {
        let mut add_friend_query = db
            .prepare("UPDATE user SET friends = friends + ? WHERE username = ?")
            .await
            .expect("Add friend prepared query failed");
        add_friend_query.set_is_idempotent(true);
        add_friend_query
    }

    async fn prepare_add_friends_of_friends_query(db: &scylla::Session) -> PreparedStatement {
        let mut add_friends_of_friends_query = db
            .prepare(
                "UPDATE user SET friends_of_friends = friends_of_friends + ? WHERE username = ?",
            )
            .await
            .expect("Add friends of friends prepared query failed");
        add_friends_of_friends_query.set_is_idempotent(true);
        add_friends_of_friends_query
    }

    pub async fn create_friendship(
        &self,
        sender: Profile,
        receiver: Profile,
        receiver_friends: Vec<Profile>,
    ) -> Result<(), DatabaseError> {
        let db = self.db.clone();
        let add_friends_of_friends_query = self.add_friends_of_friends_query.clone();
        let receiver_friends_clone = receiver_friends.clone();
        let sender_username_clone = sender.username.clone();

        tokio::spawn(async move {
            db.execute(
                &add_friends_of_friends_query,
                (receiver_friends_clone, sender_username_clone),
            )
            .await
        });

        for receiver_friend in receiver_friends.iter() {
            let db = self.db.clone();
            let add_friends_of_friends_query = self.add_friends_of_friends_query.clone();
            let sender_clone = sender.clone();
            let receiver_friend_username = receiver_friend.username.to_owned();

            tokio::spawn(async move {
                db.execute(
                    &add_friends_of_friends_query,
                    (vec![sender_clone], receiver_friend_username),
                )
                .await
            });
        }

        let db = self.db.clone();

        let add_friends_of_friends_query = self.add_friends_of_friends_query.clone();
        let get_friends_of_user_query = self.get_friends_of_user_query.clone();

        let sender_clone = sender.clone();
        let receiver_clone = receiver.clone();

        tokio::spawn(async move {
            match db
                .execute(&get_friends_of_user_query, (&sender_clone.username,))
                .await
            {
                Ok(sender_friends) => {
                    let sender_friends = sender_friends
                        .rows_typed_or_empty::<(FriendProfile,)>()
                        .filter_map(|row| {
                            row.ok().map(|row| Profile {
                                username: row.0.username,
                                name: row.0.name,
                            })
                        })
                        .collect::<Vec<_>>();

                    let db_clone = db.clone();
                    let add_friends_of_friends_query_clone = add_friends_of_friends_query.clone();

                    let sender_friends_clone = sender_friends.clone();
                    let receiver_username = receiver_clone.username.clone();

                    tokio::spawn(async move {
                        db_clone
                            .execute(
                                &add_friends_of_friends_query_clone,
                                (sender_friends_clone, receiver_username),
                            )
                            .await
                    });

                    for sender_friend in sender_friends.iter() {
                        let db = db.clone();
                        let add_friends_of_friends_query = add_friends_of_friends_query.clone();

                        let reciever = receiver_clone.clone();
                        let sender_friend = sender_friend.clone();

                        tokio::spawn(async move {
                            let _ = db
                                .execute(
                                    &add_friends_of_friends_query,
                                    (vec![reciever], sender_friend),
                                )
                                .await;
                        });
                    }
                }
                Err(_) => return,
            }
        });

        let sender_clone = sender.clone();
        let receiver_clone = receiver.clone();

        let results = tokio::join!(
            self.delete_friend_request(sender, receiver),
            self.db.execute(
                &self.add_friend_query,
                (&sender_clone, &receiver_clone.username)
            ),
            self.db.execute(
                &self.add_friend_query,
                (&receiver_clone, &sender_clone.username)
            ),
        );

        results.0?;

        results.1.map_err(|err| {
            DatabaseError(format!(
                "Error adding sender username to receiver's friends: {}",
                err
            ))
        })?;

        results.2.map_err(|err| {
            DatabaseError(format!(
                "Error adding receiver username to sender's friends: {}",
                err
            ))
        })?;

        Ok(())
    }

    async fn prepare_remove_friend_query(db: &scylla::Session) -> PreparedStatement {
        let mut remove_friend_query = db
            .prepare("UPDATE user SET friends = friends - ? WHERE username = ?")
            .await
            .expect("Remove friend prepared query failed");
        remove_friend_query.set_is_idempotent(true);
        remove_friend_query
    }

    async fn prepare_remove_friends_of_friends_query(db: &scylla::Session) -> PreparedStatement {
        let mut remove_friends_of_friends_query = db
            .prepare(
                "UPDATE user SET friends_of_friends = friends_of_friends - ? WHERE username IN ?",
            )
            .await
            .expect("Add friends of friends prepared query failed");
        remove_friends_of_friends_query.set_is_idempotent(true);
        remove_friends_of_friends_query
    }

    async fn delete_friendship(
        &self,
        deleter: Profile,
        other: Profile,
        deleter_friends: Vec<Profile>,
    ) -> Result<(), DatabaseError> {
        let db = self.db.clone();
        let add_friends_of_friends_query = self.add_friends_of_friends_query.clone();
        let deleter_friends_clone = deleter_friends.clone();
        let other_username_clone = other.username.clone();

        tokio::spawn(async move {
            db.execute(
                &add_friends_of_friends_query,
                (deleter_friends_clone, other_username_clone),
            )
            .await
        });

        for deleter_friend in deleter_friends.iter() {
            let db = self.db.clone();
            let remove_friends_of_friends_query = self.remove_friends_of_friends_query.clone();
            let other_clone = other.clone();
            let deleter_friend_username = deleter_friend.username.to_owned();

            tokio::spawn(async move {
                db.execute(
                    &remove_friends_of_friends_query,
                    (vec![other_clone], deleter_friend_username),
                )
                .await
            });
        }

        let db = self.db.clone();

        let remove_friends_of_friends_query = self.remove_friends_of_friends_query.clone();
        let get_friends_of_user_query = self.get_friends_of_user_query.clone();

        let deleter_clone = deleter.clone();
        let other_clone = other.clone();

        tokio::spawn(async move {
            match db
                .execute(&get_friends_of_user_query, (&other_clone.username,))
                .await
            {
                Ok(other_friends) => {
                    let other_friends = other_friends
                        .rows_typed_or_empty::<(FriendProfile,)>()
                        .filter_map(|row| {
                            row.ok().map(|row| Profile {
                                username: row.0.username,
                                name: row.0.name,
                            })
                        })
                        .collect::<Vec<_>>();

                    let db_clone = db.clone();
                    let remove_friends_of_friends_query_clone =
                        remove_friends_of_friends_query.clone();

                    let other_friends_clone = other_friends.clone();
                    let deleter_username = deleter_clone.username.clone();

                    tokio::spawn(async move {
                        db_clone
                            .execute(
                                &remove_friends_of_friends_query_clone,
                                (other_friends_clone, deleter_username),
                            )
                            .await
                    });

                    for other_friend in other_friends.iter() {
                        let db = db.clone();
                        let add_friends_of_friends_query = add_friends_of_friends_query.clone();

                        let deleter = deleter_clone.clone();
                        let other_friend = other_friend.clone();

                        tokio::spawn(async move {
                            let _ = db
                                .execute(
                                    &add_friends_of_friends_query,
                                    (vec![reciever], sender_friend),
                                )
                                .await;
                        });
                    }
                }
                Err(_) => return,
            }
        });

        let sender_clone = sender.clone();
        let receiver_clone = receiver.clone();

        let results = tokio::join!(
            self.delete_friend_request(sender, receiver),
            self.db.execute(
                &self.add_friend_query,
                (&sender_clone, &receiver_clone.username)
            ),
            self.db.execute(
                &self.add_friend_query,
                (&receiver_clone, &sender_clone.username)
            ),
        );

        results.0?;

        results.1.map_err(|err| {
            DatabaseError(format!(
                "Error adding sender username to receiver's friends: {}",
                err
            ))
        })?;

        results.2.map_err(|err| {
            DatabaseError(format!(
                "Error adding receiver username to sender's friends: {}",
                err
            ))
        })?;

        Ok(())
    }

    async fn prepare_get_friends_of_friends_query(db: &scylla::Session) -> PreparedStatement {
        let mut get_friends_of_friends_query = db
            .prepare("SELECT friends_of_friends FROM user WHERE username = ?")
            .await
            .expect("Get friends of friends prepared query failed");
        get_friends_of_friends_query.set_is_idempotent(true);
        get_friends_of_friends_query
    }

    fn current_timestamp() -> scylla::frame::value::Timestamp {
        scylla::frame::value::Timestamp(Duration::milliseconds(
            DateTime::<Utc>::default().timestamp_millis(),
        ))
    }

    fn timestamp_from_datetime(datetime: DateTime<Utc>) -> scylla::frame::value::Timestamp {
        scylla::frame::value::Timestamp(Duration::milliseconds(datetime.timestamp_millis()))
    }

    fn datetime_from_timestamp(timestamp: Duration) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_millis(timestamp.num_milliseconds())
                .expect("Timestamp out of range"),
            Utc,
        )
    }
}
