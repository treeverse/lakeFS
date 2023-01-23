use std::collections::HashMap;
use std::fmt::Error;
use std::io::{self, Write};
use std::net::SocketAddr;
use config::{Config, File};

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
        let mut left_table: DeltaTable = match create_table_with_config(&s3_config_map, left_table_path).await {
            Ok(table) => {
                table
            },
            Err(err) => {
                return Err(Status::new(Code::NotFound, format!("{:?}", err)));
            }
        };
        let mut right_table: DeltaTable = match create_table_with_config(&s3_config_map, right_table_path).await {
            Ok(table) => {
                table
            },
            Err(err) => {
                return Err(Status::new(Code::NotFound, format!("{:?}", err)));
            }
        };

        let mut left_table_history = match history(&mut left_table, None).await {
            Ok(vec) => {
                vec
            },
            Err(err) => {
                return Err(Status::new(Code::NotFound, format!("{:?}", err)))
            }
        };
        let mut right_table_history = match history(&mut right_table, None).await {
            Ok(vec) => {
                vec
            },
            Err(err) => {
                return Err(Status::new(Code::NotFound, format!("{:?}", err)))
            }
        };

        let ans: Vec<Diff> = compare(&mut left_table_history, left_table.version(),
                                     &mut right_table_history, right_table.version()).unwrap();
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
    let left_earliest_version = left_table_version + 1 - i64::try_from(left_table_vec.len()).unwrap();
    let right_earliest_version = right_table_version + 1 - i64::try_from(right_table_vec.len()).unwrap();
    // The lower version of the two:
    let lower_limit = if right_earliest_version > left_earliest_version { right_earliest_version } else { left_earliest_version };
    let mut diff_list: Vec<Diff> = vec![];
    let mut curr_version = right_table_version;
    left_table_vec.reverse();
    right_table_vec.reverse();
    let mut left_commit_slice = &left_table_vec[..];
    let mut right_commit_slice = &right_table_vec[..];
    if right_table_version > left_table_version {
        let right_iter = right_table_vec.iter();
        for commit_info in right_iter {
            if curr_version == left_table_version {
                right_commit_slice = &right_table_vec[(right_table_version - left_table_version) as usize..];
                break;
            }
            match commit_info {
                _operation_content => {
                    let d = construct_diff(commit_info, curr_version);
                    diff_list.push(d)
                }
            }
            curr_version -= 1;
        }
    } else {
        left_commit_slice = &left_table_vec[(left_table_version - right_table_version) as usize..];
    }

    compare_slices(left_commit_slice, right_commit_slice, curr_version, lower_limit, diff_list)
}

fn compare_slices(left_commit_slice: &[Map<String, Value>],
            right_commit_slice: &[Map<String, Value>],
            mut curr_version: DeltaDataTypeVersion,
            lower_limit: DeltaDataTypeVersion,
            mut diff_list: Vec<Diff>) -> Result<Vec<Diff>, Error> {
    let mut i: usize = 0; // iterating over the vector while 'curr_version' is the real version of the
    while i < left_commit_slice.len() && i < right_commit_slice.len() && curr_version >= lower_limit {
        let left_commit_info = left_commit_slice.get(i).unwrap();
        let right_commit_info = right_commit_slice.get(i).unwrap();
        let l_diff = construct_diff(left_commit_info, curr_version);
        let r_diff = construct_diff(right_commit_info, curr_version);

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

fn construct_diff(commit_info: &Map<String, Value>, version: DeltaDataTypeVersion) -> Diff {
    let op_params = commit_info.get("operationParameters").unwrap();
    let op_params_map = op_params.as_object().unwrap();
    let mut op_content_hash: HashMap<String, String> = HashMap::new();
    for (k, v) in op_params_map {
        let k_clone = k.clone();
        op_content_hash.insert(k_clone, v.to_string());
    }
    Diff{
        version: (version as u32).to_string(),
        timestamp: commit_info.get("timestamp").unwrap().as_i64().unwrap(),
        description: commit_info.get("operation").unwrap().as_str().unwrap().to_string(),
        content: op_content_hash,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config();
    let port: &str = get_config(&config, "port","1234");
    let version: &str = get_config(&config, "version", "1");
    let address: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
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
    println!("1|{}|tcp|{}|grpc", version, address.to_string());
    io::stdout().flush().unwrap();

    serve.await?;

    Ok(())
}

fn load_config() -> HashMap<String, String> {
    let settings = match Config::builder()
        .add_source(File::with_name("src/config.yml"))
        .build() {
        Ok(config) => {
            config
        }
        Err(_) => {
            Config::default()
        }
    };
    settings
        .try_deserialize::<HashMap<String, String>>()
        .unwrap()
}

fn get_config<'a>(config: &'a HashMap<String, String>, name: &'a str, default: &'a str) -> &'a str {
    match config.get(name) {
        None => {
            default
        }
        Some(port) => {
            port.as_str()
        }
    }
}
