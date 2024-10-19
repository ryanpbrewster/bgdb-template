use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

pub mod backgroundb;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Item {
    pub key: String,
    pub value: String,
}
pub enum DbRequest {
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
pub struct DatabaseClient {
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
