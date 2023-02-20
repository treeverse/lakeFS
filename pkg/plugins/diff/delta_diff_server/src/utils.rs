use std::collections::HashMap;
use std::str::FromStr;

use deltalake::DeltaDataTypeVersion;
use prost_types::Timestamp;
use serde_json::{Map, Value};

use crate::delta_ops;
use crate::differ::{GatewayConfig, TableOperation};

pub(crate) fn construct_storage_config(config: GatewayConfig) -> HashMap<String, String> {
    let mut s3_config: HashMap<String, String> = HashMap::new();
    s3_config.insert("AWS_ACCESS_KEY_ID".to_string(), config.key);
    s3_config.insert("AWS_ENDPOINT_URL".to_string(), config.endpoint);
    s3_config.insert("AWS_S3_ADDRESSING_STYLE".to_string(), "path".to_string());
    s3_config.insert("AWS_SECRET_ACCESS_KEY".to_string(), config.secret);
    s3_config.insert("AWS_STORAGE_ALLOW_HTTP".to_string(), "true".to_string());
    s3_config
}

pub(crate) fn construct_table_op(commit_info: &Map<String, Value>, version: DeltaDataTypeVersion, ops: &delta_ops::OpTypes) -> TableOperation {
    let op_params = commit_info.get("operationParameters").unwrap();
    let op_params_map = op_params.as_object().unwrap();
    let mut op_content_hash: HashMap<String, String> = HashMap::new();
    for (k, v) in op_params_map {
        let k_clone = k.clone();
        op_content_hash.insert(k_clone, v.to_string());
    }

    let op_name = commit_info.get("operation").unwrap().as_str().unwrap().to_string();
    let op_type = ops.get_type(&op_name).unwrap();
    TableOperation{
        id: (version as u32).to_string(),
        timestamp: Option::from(
            Timestamp::from_str(&commit_info.get("timestamp").unwrap().to_string()).unwrap()
        ),
        operation: op_name,
        content: op_content_hash,
        operation_type: op_type,
    }
}
