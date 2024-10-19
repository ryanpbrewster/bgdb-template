use serde::{Deserialize, Serialize};

pub mod backgroundb;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Item {
    pub key: String,
    pub value: String,
}
