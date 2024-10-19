use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use clap::Parser;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, help = "Path to the database file")]
    database: PathBuf,

    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Item {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct ValuePayload {
    value: String,
}

enum DbRequest {
    GetAll {
        respond_to: oneshot::Sender<Result<Vec<Item>, String>>,
    },
    GetItem {
        key: String,
        respond_to: oneshot::Sender<Result<Option<Item>, String>>,
    },
    PutItem {
        item: Item,
        respond_to: oneshot::Sender<Result<(), String>>,
    },
}
impl std::fmt::Debug for DbRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GetAll { .. } => f.debug_struct("GetAll").finish(),
            Self::GetItem { key, .. } => f.debug_struct("GetItem").field("key", key).finish(),
            Self::PutItem { item, .. } => f.debug_struct("PutItem").field("item", item).finish(),
        }
    }
}

// Database client struct
#[derive(Clone)]
struct DatabaseClient {
    db_tx: mpsc::Sender<DbRequest>,
}

impl DatabaseClient {
    pub fn new(db_tx: mpsc::Sender<DbRequest>) -> Self {
        Self { db_tx }
    }

    pub async fn get_all_items(&self) -> Result<Vec<Item>, String> {
        let (respond_to, response) = oneshot::channel();

        self.db_tx
            .send(DbRequest::GetAll { respond_to })
            .await
            .map_err(|e| e.to_string())?;

        response.await.map_err(|e| e.to_string())?
    }

    pub async fn get_item(&self, key: String) -> Result<Option<Item>, String> {
        let (respond_to, response) = oneshot::channel();

        self.db_tx
            .send(DbRequest::GetItem { key, respond_to })
            .await
            .map_err(|e| e.to_string())?;

        response.await.map_err(|e| e.to_string())?
    }

    pub async fn put_item(&self, item: Item) -> Result<(), String> {
        let (respond_to, response) = oneshot::channel();

        self.db_tx
            .send(DbRequest::PutItem { item, respond_to })
            .await
            .map_err(|e| e.to_string())?;

        response.await.map_err(|e| e.to_string())?
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    // Parse command-line arguments
    let args = Args::parse();

    // Set up the channel for database communication
    let (db_tx, db_rx) = mpsc::channel::<DbRequest>(32);

    let _db_thread = {
        // Open the SQLite database
        let conn = Connection::open(args.database)?;

        // Ensure the "items" table exists
        conn.execute(
            "CREATE TABLE IF NOT EXISTS items (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            [],
        )
        .expect("Failed to create table");

        std::thread::spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(database_thread(conn, db_rx))
        })
    };

    // Create the database client
    let db_client = DatabaseClient::new(db_tx);

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

// This is an abomination: an async function that does a ton of blocking I/O.
// This should only be run in a dedicated runtime.
async fn database_thread(conn: Connection, mut db_rx: mpsc::Receiver<DbRequest>) {
    // Listen for database requests
    while let Some(request) = db_rx.recv().await {
        tracing::debug!(?request, "recv");
        match request {
            DbRequest::GetAll { respond_to } => {
                let result = get_all_items_db(&conn);
                let _ = respond_to.send(result);
            }
            DbRequest::GetItem { key, respond_to } => {
                let result = get_item_db(&conn, key);
                let _ = respond_to.send(result);
            }
            DbRequest::PutItem { item, respond_to } => {
                let result = put_item_db(&conn, item);
                let _ = respond_to.send(result);
            }
        }
    }
}

// Database operation functions
fn get_all_items_db(conn: &Connection) -> Result<Vec<Item>, String> {
    let mut stmt = conn
        .prepare("SELECT key, value FROM items")
        .map_err(|e| e.to_string())?;
    let item_iter = stmt
        .query_map([], |row| {
            Ok(Item {
                key: row.get(0)?,
                value: row.get(1)?,
            })
        })
        .map_err(|e| e.to_string())?;

    let mut items = Vec::new();
    for item in item_iter {
        items.push(item.map_err(|e| e.to_string())?);
    }
    Ok(items)
}

fn get_item_db(conn: &Connection, key: String) -> Result<Option<Item>, String> {
    let mut stmt = conn
        .prepare("SELECT value FROM items WHERE key = ?1")
        .map_err(|e| e.to_string())?;
    let result = stmt
        .query_row([key.clone()], |row| row.get::<_, String>(0))
        .optional()
        .map_err(|e| e.to_string())?;

    Ok(result.map(|value| Item { key, value }))
}

fn put_item_db(conn: &Connection, item: Item) -> Result<(), String> {
    conn.execute(
        "INSERT INTO items (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![item.key, item.value],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}
