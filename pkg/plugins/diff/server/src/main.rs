use std::collections::HashMap;
use std::fmt::Error;
use std::io::{self, Write};
use std::net::SocketAddr;

use deltalake::DeltaDataTypeVersion;
use deltalake::builder::DeltaTableBuilder;
use deltalake::delta::{DeltaTable, DeltaTableError};
use serde_json::{Map, Value};
use tonic::{Code, Request, Response, Status, transport::Server};

use differ::{Diff, differ_server::{Differ, DifferServer}, DiffPaths, DiffRequest, DiffResponse, GatewayConfig};

pub mod differ {
    include!("diff.rs");
}

#[derive(Debug, Default)]
pub struct DifferService {}

#[tonic::async_trait]
impl Differ for DifferService {
    async fn diff(&self, request: Request<DiffRequest>) -> Result<Response<DiffResponse>, Status> {
        let r = request.into_inner();
        let s3_gateway_config_req: GatewayConfig = r.gateway_config.unwrap();
        let s3_config_map: HashMap<String, String> = construct_storage_config(s3_gateway_config_req);

        let ps: DiffPaths = match r.paths {
            Some(ps) => ps,
            None => return Ok(Response::new(DiffResponse { diffs: vec![] }))
        };
        let left_table_path: String = ps.left_path;
        let right_table_path: String = ps.right_path;
        eprintln!("Pre left table run");
        let mut left_table: DeltaTable = match create_table_with_config(&s3_config_map, left_table_path).await {
            Ok(table) => {
                table
            },
            Err(err) => {
                return Err(Status::new(Code::NotFound, format!("{:?}", err)));
            }
        };
        eprintln!("Pre right table run");
        let mut right_table: DeltaTable = match create_table_with_config(&s3_config_map, right_table_path).await {
            Ok(table) => {
                table
            },
            Err(err) => {
                return Err(Status::new(Code::NotFound, format!("{:?}", err)));
            }
        };
        eprintln!("Left table version: {}", left_table.version());
        eprintln!("Right table version: {}", right_table.version());

        let left_table_history = history(&mut left_table, None);
        let mut left_table_history_v = match left_table_history.await {
            Ok(vec) => {
                vec
            },
            Err(err) => {
                return Err(Status::new(Code::NotFound, format!("{:?}", err)))
            }
        };
        let right_table_history = history(&mut right_table, None);
        let mut right_table_history_v = match right_table_history.await {
            Ok(vec) => {
                vec
            },
            Err(err) => {
                return Err(Status::new(Code::NotFound, format!("{:?}", err)))
            }
        };

        let ans: Vec<Diff> = compare(&mut left_table_history_v, left_table.version(),
                                     &mut right_table_history_v, right_table.version()).unwrap();
        return Ok(Response::new(DiffResponse { diffs: ans }))
    }
}

fn construct_storage_config(config: GatewayConfig) -> HashMap<String, String> {
    let mut s3_config: HashMap<String, String> = HashMap::new();
    s3_config.insert("AWS_ACCESS_KEY_ID".to_string(), config.key);
    s3_config.insert("AWS_ENDPOINT_URL".to_string(), config.endpoint);
    s3_config.insert("AWS_S3_ADDRESSING_STYLE".to_string(), "path".to_string());
    s3_config.insert("AWS_SECRET_ACCESS_KEY".to_string(), config.secret);
    s3_config.insert("AWS_STORAGE_ALLOW_HTTP".to_string(), "true".to_string());
    s3_config
}

async fn create_table_with_config(config: &HashMap<String, String>, path: String) -> Result<DeltaTable, DeltaTableError> {
    let cloned_config = config.clone();
    let builder = DeltaTableBuilder::from_uri(path)
        .with_storage_options(cloned_config);

    builder.load().await
}

async fn history(delta: &mut DeltaTable, limit: Option<usize>) -> Result<Vec<Map<String, Value>>, DeltaTableError> {
    delta.history(limit).await
}

impl std::hash::Hash for Diff {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.version.hash(state);
        self.description.hash(state);
        self.content.hasher();
        self.timestamp.hash(state);
    }
}

fn compare(left_table_vec: &mut Vec<Map<String, Value>>,
           left_table_version: DeltaDataTypeVersion,
           right_table_vec: &mut Vec<Map<String, Value>>,
           right_table_version: DeltaDataTypeVersion,) -> Result<Vec<Diff>, Error> {
    eprintln!("LEFT VECTOR:\n{:?}", left_table_vec);
    eprintln!("RIGHT VECTOR:\n{:?}", right_table_vec);
    let left_earliest_version = left_table_version + 1 - i64::try_from(left_table_vec.len()).unwrap();
    let right_earliest_version = right_table_version + 1 - i64::try_from(right_table_vec.len()).unwrap();
    // The lower version of the two:
    let lower_limit = if left_earliest_version > right_earliest_version {left_earliest_version} else { right_earliest_version };
    let mut diff_list: Vec<Diff> = vec![];
    let left_version_bigger = left_table_version > right_table_version;
    let mut curr_version = left_table_version;
    left_table_vec.reverse();
    right_table_vec.reverse();
    let mut left_commit_slice = &left_table_vec[..];
    let mut right_commit_slice = &right_table_vec[..];
    if left_version_bigger {
        let left_iter = left_table_vec.iter();
        for commit_info in left_iter {
            if curr_version == right_table_version {
                left_commit_slice = &left_table_vec[(left_table_version - right_table_version) as usize..];
                break;
            }
            match commit_info {
                _operation_content => {
                    let curr_op_params = commit_info.get("operationParameters").unwrap();
                    let curr_op_params_map = curr_op_params.as_object().unwrap();
                    let mut operation_content_hash: HashMap<String, String> = HashMap::new();
                    for (k, v) in curr_op_params_map {
                        let k_clone = k.clone();
                        operation_content_hash.insert(k_clone, v.to_string());
                    }
                    let d = Diff{
                        version: (curr_version as u32).to_string(),
                        timestamp: commit_info.get("timestamp").unwrap().as_i64().unwrap(),
                        description: commit_info.get("operation").unwrap().as_str().unwrap().to_string(),
                        content: operation_content_hash,
                    };
                    diff_list.push(d)
                }
            }
            curr_version -= 1;
        }
    } else {
        right_commit_slice = &right_table_vec[(right_table_version - left_table_version) as usize..];
    }

    let mut i: usize = 0; // iterating over the vector while 'curr_version' is the real version of the
    while i < left_commit_slice.len() && i < right_commit_slice.len() && curr_version >= lower_limit {
        let left_commit_info = left_commit_slice.get(i).unwrap();
        let left_curr_op_params = left_commit_info.get("operationParameters").unwrap();
        let left_curr_op_params_map = left_curr_op_params.as_object().unwrap();
        let mut left_operation_content_hash: HashMap<String, String> = HashMap::new();
        for (k, v) in left_curr_op_params_map {
            let k_clone = k.clone();
            left_operation_content_hash.insert(k_clone, v.to_string());
        }
        let l_diff = Diff{
            version: (curr_version as u32).to_string(),
            timestamp: left_commit_info.get("timestamp").unwrap().as_i64().unwrap(),
            description: left_commit_info.get("operation").unwrap().as_str().unwrap().to_string(),
            content: left_operation_content_hash,
        };

        let right_commit_info = right_commit_slice.get(i).unwrap();
        let right_curr_op_params = right_commit_info.get("operationParameters").unwrap();
        let right_curr_op_params_map = right_curr_op_params.as_object().unwrap();
        let mut right_operation_content_hash: HashMap<String, String> = HashMap::new();
        for (k, v) in right_curr_op_params_map {
            let k_clone = k.clone();
            right_operation_content_hash.insert(k_clone, v.to_string());
        }
        let r_diff = Diff{
            version: (curr_version as u32).to_string(),
            timestamp: right_commit_info.get("timestamp").unwrap().as_i64().unwrap(),
            description: right_commit_info.get("operation").unwrap().as_str().unwrap().to_string(),
            content: right_operation_content_hash,
        };

        if r_diff == l_diff {
            return Ok(diff_list);
        } else {
            diff_list.push(l_diff);
        }

        curr_version -= 1;
        i += 1;
    }
    Ok(diff_list)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = "127.0.0.1:1234".parse().unwrap();
    let differ_service = DifferService::default();
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<DifferServer<DifferService>>()
        .await;

    let serve = Server::builder()
        .add_service(health_service)
        .add_service(DifferServer::new(differ_service))
        .serve(address);

    // Communicating to the go-plugin application client
    println!("1|1|tcp|{}|grpc", address.to_string());
    io::stdout().flush().unwrap();

    serve.await?;

    Ok(())
}
