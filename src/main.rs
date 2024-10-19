use anyhow::Context;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use clap::Parser;
use rusqlite::Connection;
use serde::Deserialize;
use sqlite_async::{
    backgroundb::{self, DatabaseClient},
    Item,
};
use std::path::PathBuf;

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
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    // Parse command-line arguments
    let args = Args::parse();

    let db_client = {
        // Open the SQLite database
        let conn = Connection::open(args.database)?;
        // Ensure the "items" table exists
        conn.execute(
            "CREATE TABLE IF NOT EXISTS items (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            [],
        )
        .context("Failed to create table")?;
        backgroundb::spawn(conn)
    };

    // Build the axum application with routes
    let app = Router::new()
        .route("/items", get(get_all_items))
        .route("/items/:key", get(get_item).put(put_item))
        .with_state(db_client);

    // Start the server
    tracing::info!("Listening on {}", args.addr);
    let listener = tokio::net::TcpListener::bind(args.addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// Handler to get all items
async fn get_all_items(
    State(db_client): State<DatabaseClient>,
) -> Result<impl IntoResponse, StatusCode> {
    match db_client.get_all_items().await {
        Ok(items) => Ok(Json(items)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// Handler to get a single item by key
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

// Handler to insert or update an item
async fn put_item(
    Path(key): Path<String>,
    State(db_client): State<DatabaseClient>,
    Json(payload): Json<ValuePayload>,
) -> Result<impl IntoResponse, StatusCode> {
    let item = Item {
        key,
        value: payload.value,
    };

    match db_client.put_item(item).await {
        Ok(_) => Ok(StatusCode::CREATED),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
