use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tungstenite::http::{Request, Response, StatusCode};
extern crate tracing_subscriber;
#[macro_use]
extern crate tracing;

use auth::{AccessTokenPayload, JWTAuth};
use connection::Connection;
use init::Init;

mod auth;
mod connection;
mod conversation_id;
mod db;
mod hash;
mod init;
mod models;

// todo - try to eliminated clones and unwraps and make every error logged

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let Init {
        db,
        nc,
        port,
        access_token_secret,
    } = Init::init().await;

    let server_addr = SocketAddr::from(([127, 0, 0, 1], port));

    let server = TcpListener::bind(server_addr)
        .await
        .expect("Failed to bind");

    info!(
        "Listening on {}",
        server
            .local_addr()
            .expect("Error getting address server is listening on")
    );

    let jwt_auth = Arc::new(JWTAuth::new(&access_token_secret));

    loop {
        let db = db.clone();
        let nc = nc.clone();

        let jwt_auth = jwt_auth.clone();

        match server.accept().await {
            Ok((stream, _addr)) => {
                tokio::task::spawn(async move {
                    let mut access_token_payload: Option<AccessTokenPayload> = None;

                    match tokio_tungstenite::accept_hdr_async(
                        stream,
                        |req: &Request<()>, mut res: Response<()>| {
                            return match jwt_auth.veryify_req(req) {
                                Ok(payload) => {
                                    access_token_payload = Some(payload);

                                    Ok(res)
                                }
                                Err(_) => {
                                    *res.status_mut() = StatusCode::UNAUTHORIZED;

                                    Err(Response::from_parts(
                                        res.into_parts().0,
                                        Some("Valid access token required".to_owned()),
                                    ))
                                }
                            };
                        },
                    )
                    .await
                    {
                        Ok(websocket) => {
                            let access_token_payload = access_token_payload.expect("This error should not happen because access_token_payload should be set if websocket handshake is successful");

                            let username = access_token_payload.username.clone();

                            let conn = Connection {
                                websocket,
                                db,
                                nc,
                                phone_number: access_token_payload.phone_number,
                                username,
                            };

                            if let Err(fatal_connection_error) = conn.handle().await {
                                error!("Error during websocket connection for user with username {}: {}", access_token_payload.username,  fatal_connection_error);
                            };
                        }
                        Err(err) => {
                            error!("Error during websocket handshake: {}", err);
                        }
                    }
                });
            }
            Err(_) => {
                error!("Error accepting tcp connection");
                continue;
            }
        }
    }
}
