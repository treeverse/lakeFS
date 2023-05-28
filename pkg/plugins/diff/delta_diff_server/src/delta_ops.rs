use std::collections::HashMap;
use lazy_static::lazy_static;

use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use deltalake::action::CommitInfo;
use tonic::{Code, Status};

use crate::differ::OperationType;
use crate::differ::TablePath;

pub(crate) async fn get_delta_table(config: &HashMap<String, String>, repo: &str, table_path: &TablePath) -> Result<DeltaTable, Status> {
    if repo.is_empty() || table_path.path.is_empty() || table_path.r#ref.is_empty() {
        return Err(Status::new(Code::InvalidArgument, "Missing path or namespace info"))
    }
    let path = create_s3_path(repo, table_path);
    match create_table_with_config(config, path).await {
        Ok(table) => {
            Ok(table)
        },
        Err(err) => {
            return Err(convert_delta_error(err));
        }
    }
}

fn create_s3_path(repo: &str, table_path: &TablePath) -> String {
    format!("s3://{}/{}/{}", repo, table_path.r#ref, table_path.path)
}

async fn create_table_with_config(config: &HashMap<String, String>, path: String) -> Result<DeltaTable, DeltaTableError> {
    let cloned_config = config.clone();
    let builder = DeltaTableBuilder::from_uri(path)
        .with_storage_options(cloned_config);

    builder.load().await
}

pub(crate) async fn history(delta: &mut DeltaTable, limit: Option<usize>) -> Result<Vec<CommitInfo>, Status> {
    match delta.history(limit).await {
        Ok(vec) => {
            Ok(vec)
        },
        Err(err) => {
            return Err(convert_delta_error(err))
        }
    }
}

lazy_static! {
    pub static ref OP_TYPES: HashMap<&'static str, i32> = {
        let mut hm = HashMap::new();
        hm.insert("WRITE", OperationType::Update as i32);
        hm.insert("INSERT", OperationType::Update as i32);
        hm.insert("DELETE", OperationType::Delete as i32);
        hm.insert("CREATE TABLE AS SELECT", OperationType::Create as i32);
        hm.insert("REPLACE TABLE AS SELECT", OperationType::Update as i32);
        hm.insert("COPY INTO", OperationType::Update as i32);
        hm.insert("STREAMING UPDATE", OperationType::Update as i32);
        hm.insert("TRUNCATE", OperationType::Delete as i32);
        hm.insert("MERGE", OperationType::Update as i32);
        hm.insert("UPDATE", OperationType::Update as i32);
        hm.insert("FSCK", OperationType::Delete as i32);
        hm.insert("CONVERT", OperationType::Create as i32);
        hm.insert("OPTIMIZE", OperationType::Update as i32);
        hm.insert("RESTORE", OperationType::Update as i32);
        hm.insert("VACUUM", OperationType::Delete as i32);
        hm
    };
}

fn convert_delta_error(e: DeltaTableError) -> Status {
    match e {
        DeltaTableError::LoadCheckpoint { .. } | DeltaTableError::MissingDataFile { .. } |
        DeltaTableError::NotATable(_) | DeltaTableError::NoMetadata => {
            Status::new(Code::NotFound, format!("{:?}", e))
        }
        DeltaTableError::NoSchema => {
            Status::new(Code::FailedPrecondition, format!("{:?}", e))
        }
        DeltaTableError::VersionAlreadyExists(_) => {
            Status::new(Code::AlreadyExists, format!("{:?}", e))
        }
        _ => Status::new(Code::Unknown, format!("{:?}", e))
    }
}
