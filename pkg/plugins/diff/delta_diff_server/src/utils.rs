use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use deltalake::action::CommitInfo;
use deltalake::builder::s3_storage_options;

use deltalake::DeltaDataTypeVersion;
use prost_types::Timestamp;

use crate::delta_ops::OP_TYPES;
use crate::differ::{GatewayConfig, TableOperation};

#[derive(Debug)]
pub enum TableOperationsError {
    MissingKey(String),
    UnknownOperation(String),
}

impl Display for TableOperationsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableOperationsError::MissingKey(key) => write!(f, "Key '{}' is missing", key),
            TableOperationsError::UnknownOperation(op) => write!(f, "Unknown operation: '{}'", op),
        }
    }
}

const OPERATION_PARAMETERS_KEY: &str = "operationParameters";
const OPERATION_KEY: &str = "operation";
const TIMESTAMP_KEY: &str = "timestamp";

pub(crate) fn construct_storage_config(config: GatewayConfig) -> HashMap<String, String> {
    let mut s3_config: HashMap<String, String> = HashMap::new();
    s3_config.insert(s3_storage_options::AWS_ACCESS_KEY_ID.to_string(), config.key);
    s3_config.insert(s3_storage_options::AWS_ENDPOINT_URL.to_string(), config.endpoint);
    s3_config.insert(s3_storage_options::AWS_S3_ADDRESSING_STYLE.to_string(), "path".to_string());
    s3_config.insert(s3_storage_options::AWS_SECRET_ACCESS_KEY.to_string(), config.secret);
    s3_config.insert(s3_storage_options::AWS_STORAGE_ALLOW_HTTP.to_string(), "true".to_string());
    s3_config.insert(s3_storage_options::AWS_REGION.to_string(), "us-east-1".to_string());
    s3_config
}

pub(crate) fn construct_table_op(commit_info: &CommitInfo, version: DeltaDataTypeVersion) -> Result<TableOperation, TableOperationsError> {
    let op_params = match &commit_info.operation_parameters {
        None => return Err(TableOperationsError::MissingKey(OPERATION_PARAMETERS_KEY.to_string())),
        Some(params) => params
    };
    let mut op_content_hash: HashMap<String, String> = HashMap::new();
    for (k, v) in op_params {
        let k_clone = k.clone();
        op_content_hash.insert(k_clone, v.to_string());
    }

    let op_name = match &commit_info.operation {
        None => return Err(TableOperationsError::MissingKey(OPERATION_KEY.to_string())),
        Some(op) => (*op).as_str()
    };

    let op_type = match OP_TYPES.get(op_name) {
        None => return Err(TableOperationsError::UnknownOperation(op_name.to_string())),
        Some(t) => *t
    };

    let seconds = match &commit_info.timestamp {
        None => return Err(TableOperationsError::MissingKey(TIMESTAMP_KEY.to_string())),
        Some(ts) => *ts
    };

    let mut ts = Timestamp::default();
    ts.seconds = seconds;
    Ok(TableOperation{
        id: (version as u32).to_string(),
        timestamp: Option::from(ts),
        operation: op_name.to_string(),
        content: op_content_hash,
        operation_type: i32::from(op_type),
    })
}
