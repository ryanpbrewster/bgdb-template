use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use clap::Parser;
use serde::Deserialize;
use sqlite_async::{
    backgroundb::{self, DatabaseClient},
    Item,
};
use std::{future::IntoFuture, path::PathBuf};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, help = "Path to the database file")]
    database: PathBuf,

    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,
}

#[derive(Deserialize)]
struct ValuePayload {
    value: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let db_client = backgroundb::spawn(backgroundb::open(args.database)?);

    // Build the axum application with routes
    let app = Router::new()
        .route("/items", get(get_all_items))
        .route("/items/:key", get(get_item).put(put_item))
        .with_state(db_client.clone());

    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    tracing::info!("listening on {}", args.addr);

    tokio::spawn(axum::serve(listener, app).into_future());

    tokio::signal::ctrl_c().await?;

    db_client.shutdown().await?;

    Ok(())
}

async fn get_all_items(
    State(db_client): State<DatabaseClient>,
) -> Result<impl IntoResponse, StatusCode> {
    match db_client.get_all_items().await {
        Ok(items) => Ok(Json(items)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_item(
    Path(key): Path<String>,
    State(db_client): State<DatabaseClient>,
) -> Result<impl IntoResponse, StatusCode> {
    match db_client.get_item(key).await {
        Ok(Some(item)) => Ok((StatusCode::OK, Json(item))),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn put_item(
    Path(key): Path<String>,
    State(db_client): State<DatabaseClient>,
    Json(ValuePayload { value }): Json<ValuePayload>,
) -> Result<impl IntoResponse, StatusCode> {
    match db_client.put_item(Item { key, value }).await {
        Ok(_) => Ok(StatusCode::CREATED),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
