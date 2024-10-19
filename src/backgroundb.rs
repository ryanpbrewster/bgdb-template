use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::mpsc;

use crate::{DbRequest, Item};

// This is an abomination: an async function that does a ton of blocking I/O.
// This should only be run in a dedicated runtime.
pub async fn database_thread(conn: Connection, mut db_rx: mpsc::Receiver<DbRequest>) {
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
