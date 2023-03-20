use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use deltalake::DeltaDataTypeVersion;
use prost_types::Timestamp;
use serde_json::{Map, Value};

use crate::delta_ops::OP_TYPES;
use crate::differ::{GatewayConfig, TableOperation};

#[derive(Debug)]
pub enum TableOperationsError {
    MissingKey(String),
    UnexpectedType(String),
    UnknownOperation(String),
}

impl Display for TableOperationsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableOperationsError::MissingKey(key) => write!(f, "Key '{}' is missing", key),
            TableOperationsError::UnexpectedType(key) => write!(f, "Unexpected type while trying to cast value for key '{}'", key),
            TableOperationsError::UnknownOperation(op) => write!(f, "Unknown operation: '{}'", op),
        }
    }
}

const OPERATION_PARAMETERS_KEY: &str = "operationParameters";
const OPERATION_KEY: &str = "operation";
const TIMESTAMP_KEY: &str = "timestamp";

pub(crate) fn construct_storage_config(config: GatewayConfig) -> HashMap<String, String> {
    let mut s3_config: HashMap<String, String> = HashMap::new();
    let mut endpoint = config.endpoint;
    if endpoint.contains("0.0.0.0:") {
        endpoint = endpoint.replace("0.0.0.0:", "localhost:");
    }
    s3_config.insert("AWS_ACCESS_KEY_ID".to_string(), config.key);
    s3_config.insert("AWS_ENDPOINT_URL".to_string(), endpoint);
    s3_config.insert("AWS_S3_ADDRESSING_STYLE".to_string(), "path".to_string());
    s3_config.insert("AWS_SECRET_ACCESS_KEY".to_string(), config.secret);
    s3_config.insert("AWS_STORAGE_ALLOW_HTTP".to_string(), "true".to_string());
    s3_config
}

pub(crate) fn construct_table_op(commit_info: &Map<String, Value>, version: DeltaDataTypeVersion) -> Result<TableOperation, TableOperationsError> {
    let op_params = match commit_info.get(OPERATION_PARAMETERS_KEY) {
        None => return Err(TableOperationsError::MissingKey(OPERATION_PARAMETERS_KEY.to_string())),
        Some(params) => params
    };
    let op_params_map = match op_params.as_object() {
        None => return Err(TableOperationsError::UnexpectedType(OPERATION_PARAMETERS_KEY.to_string())),
        Some(param_map) => param_map
    };
    let mut op_content_hash: HashMap<String, String> = HashMap::new();
    for (k, v) in op_params_map {
        let k_clone = k.clone();
        op_content_hash.insert(k_clone, v.to_string());
    }

    let op_name = match commit_info.get(OPERATION_KEY){
        None => return Err(TableOperationsError::MissingKey(OPERATION_KEY.to_string())),
        Some(op) => match op.as_str() {
            None => return Err(TableOperationsError::UnexpectedType(OPERATION_KEY.to_string())),
            Some(op_str) => op_str
        }
    };

    let op_type = match OP_TYPES.get(op_name) {
        None => return Err(TableOperationsError::UnknownOperation(op_name.to_string())),
        Some(t) => *t
    };

    let seconds = match commit_info.get(TIMESTAMP_KEY) {
        None => return Err(TableOperationsError::MissingKey(TIMESTAMP_KEY.to_string())),
        Some(ts) => match ts.as_i64() {
            None => return Err(TableOperationsError::UnexpectedType(TIMESTAMP_KEY.to_string())),
            Some(ts_i64) => ts_i64
        }
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
