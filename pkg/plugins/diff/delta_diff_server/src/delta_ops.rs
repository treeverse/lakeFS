use std::collections::HashMap;

use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use serde_json::{Map, Value};
use tonic::{Code, Status};

use crate::differ::OperationType;
use crate::differ::TablePath;

pub(crate) async fn get_delta_table(config: &HashMap<String, String>, repo: &str, table_path: TablePath) -> Result<DeltaTable, Status> {
    let path = create_s3_path(repo, table_path);
    return match create_table_with_config(config, path).await {
        Ok(table) => {
            Ok(table)
        },
        Err(err) => {
            return Err(Status::new(Code::NotFound, format!("{:?}", err)));
        }
    };
}

fn create_s3_path(repo: &str, table_path: TablePath) -> String {
    format!("s3://{}/{}/{}", repo, table_path.r#ref, table_path.path)
}

async fn create_table_with_config(config: &HashMap<String, String>, path: String) -> Result<DeltaTable, DeltaTableError> {
    let cloned_config = config.clone();
    let builder = DeltaTableBuilder::from_uri(path)
        .with_storage_options(cloned_config);

    builder.load().await
}

pub(crate) async fn history(delta: &mut DeltaTable, limit: Option<usize>) -> Result<Vec<Map<String, Value>>, Status> {
    return match delta.history(limit).await {
        Ok(vec) => {
            Ok(vec)
        },
        Err(err) => {
            return Err(Status::new(Code::NotFound, format!("{:?}", err)))
        }
    };
}

pub struct OpTypes {
    types: HashMap<String, i32>
}

impl OpTypes {
    pub fn new() -> Self {
        let mut hm: HashMap<String, i32> = HashMap::new();
        hm.insert(String::from("WRITE"), OperationType::Update as i32);
        hm.insert(String::from("INSERT"), OperationType::Update as i32);
        hm.insert(String::from("DELETE"), OperationType::Delete as i32);
        hm.insert(String::from("CREATE TABLE AS SELECT"), OperationType::Create as i32);
        hm.insert(String::from("REPLACE TABLE AS SELECT"), OperationType::Update as i32);
        hm.insert(String::from("COPY INTO"), OperationType::Update as i32);
        hm.insert(String::from("STREAMING UPDATE"), OperationType::Update as i32);
        hm.insert(String::from("TRUNCATE"), OperationType::Delete as i32);
        hm.insert(String::from("MERGE"), OperationType::Update as i32);
        hm.insert(String::from("UPDATE"), OperationType::Update as i32);
        hm.insert(String::from("FSCK"), OperationType::Delete as i32);
        hm.insert(String::from("CONVERT"), OperationType::Create as i32);
        hm.insert(String::from("OPTIMIZE"), OperationType::Update as i32);
        hm.insert(String::from("RESTORE"), OperationType::Update as i32);
        hm.insert(String::from("VACUUM"), OperationType::Delete as i32);
        Self{
            types: hm,
        }
    }

    pub fn get_type(&self, op: &str) -> Option<i32> {
        self.types.get(op).copied()
    }
}
