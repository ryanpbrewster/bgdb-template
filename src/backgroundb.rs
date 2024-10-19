use std::{path::PathBuf, time::{Duration, Instant}};

use anyhow::{bail, Context};
use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::{mpsc, oneshot};

use crate::Item;

pub fn open(path: PathBuf) -> anyhow::Result<Connection> {
    let conn = Connection::open(path)?;
    // Ensure the "items" table exists
    conn.execute(
        "CREATE TABLE IF NOT EXISTS items (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        [],
    )
    .context("Failed to create table")?;
    Ok(conn)
}
pub fn spawn(conn: Connection) -> DatabaseClient {
    let (db_tx, db_rx) = mpsc::channel::<DbRequest>(32);
    std::thread::spawn(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(database_thread(conn, db_rx))
    });
    DatabaseClient { db_tx }
}

#[derive(Clone)]
pub struct DatabaseClient {
    db_tx: mpsc::Sender<DbRequest>,
}

enum DbRequest {
    BurnCpu {
        duration: Duration,
        respond_to: oneshot::Sender<anyhow::Result<()>>,
    },
    GetAll {
        respond_to: oneshot::Sender<anyhow::Result<Vec<Item>>>,
    },
    GetItem {
        key: String,
        respond_to: oneshot::Sender<anyhow::Result<Option<Item>>>,
    },
    PutItem {
        item: Item,
        respond_to: oneshot::Sender<anyhow::Result<()>>,
    },
    Shutdown {
        respond_to: oneshot::Sender<anyhow::Result<()>>,
    },
}
impl std::fmt::Debug for DbRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BurnCpu { duration: time, .. } => f.debug_struct("BurnCpu").field("time", time).finish(),
            Self::GetAll { .. } => f.debug_struct("GetAll").finish(),
            Self::GetItem { key, .. } => f.debug_struct("GetItem").field("key", key).finish(),
            Self::PutItem { item, .. } => f.debug_struct("PutItem").field("item", item).finish(),
            Self::Shutdown { .. } => f.debug_struct("Shutdown").finish(),
        }
    }
}

impl DatabaseClient {
    pub async fn burn_cpu(&self, duration: Duration) -> anyhow::Result<()> {
        let (respond_to, response) = oneshot::channel();

        self.db_tx.send(DbRequest::BurnCpu { duration, respond_to }).await?;

        response.await?
    }
    pub async fn get_all_items(&self) -> anyhow::Result<Vec<Item>> {
        let (respond_to, response) = oneshot::channel();

        self.db_tx.send(DbRequest::GetAll { respond_to }).await?;

        response.await?
    }

    pub async fn get_item(&self, key: String) -> anyhow::Result<Option<Item>> {
        let (respond_to, response) = oneshot::channel();

        self.db_tx
            .send(DbRequest::GetItem { key, respond_to })
            .await?;

        response.await?
    }

    pub async fn put_item(&self, item: Item) -> anyhow::Result<()> {
        let (respond_to, response) = oneshot::channel();

        self.db_tx
            .send(DbRequest::PutItem { item, respond_to })
            .await?;

        response.await?
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let (respond_to, response) = oneshot::channel();

        self.db_tx.send(DbRequest::Shutdown { respond_to }).await?;

        response.await?
    }
}

// This is an abomination: an async function that does a ton of blocking I/O.
// This should only be run in a dedicated runtime.
async fn database_thread(conn: Connection, mut db_rx: mpsc::Receiver<DbRequest>) {
    // Listen for database requests
    while let Some(request) = db_rx.recv().await {
        tracing::debug!(?request, "recv");
        match request {
            DbRequest::BurnCpu { duration, respond_to } => {
                let end = Instant::now() + duration;
                while Instant::now() < end {
                }
                let _ = respond_to.send(Ok(()));
            }
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
            DbRequest::Shutdown { respond_to } => {
                let _ = respond_to.send(shutdown(conn));
                break;
            }
        }
    }
}

// Database operation functions
fn get_all_items_db(conn: &Connection) -> anyhow::Result<Vec<Item>> {
    let mut stmt = conn.prepare("SELECT key, value FROM items")?;
    let item_iter = stmt.query_map([], |row| {
        Ok(Item {
            key: row.get(0)?,
            value: row.get(1)?,
        })
    })?;

    let mut items = Vec::new();
    for item in item_iter {
        items.push(item?);
    }
    Ok(items)
}

fn get_item_db(conn: &Connection, key: String) -> anyhow::Result<Option<Item>> {
    let mut stmt = conn.prepare("SELECT value FROM items WHERE key = ?1")?;
    let result = stmt
        .query_row([key.clone()], |row| row.get::<_, String>(0))
        .optional()?;

    Ok(result.map(|value| Item { key, value }))
}

fn put_item_db(conn: &Connection, item: Item) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO items (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![item.key, item.value],
    )?;
    Ok(())
}

fn shutdown(conn: Connection) -> anyhow::Result<()> {
    match conn.close() {
        Ok(_) => {
            tracing::info!("closed db connection");
            Ok(())
        }
        Err((_, err)) => {
            tracing::error!(?err, "failed to close db connection");
            bail!(err)
        }
    }
}
