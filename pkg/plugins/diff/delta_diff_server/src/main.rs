use std::collections::HashMap;
use std::fmt::Error;
use std::io::{self, Write};
use std::net::SocketAddr;

use deltalake::delta::DeltaTable;
use deltalake::DeltaDataTypeVersion;
use portpicker;
use serde_json::{Map, Value};
use tonic::{Code, Request, Response, Status, transport::Server};

use differ::{DiffRequest, DiffResponse, GatewayConfig, table_differ_server::{TableDiffer, TableDifferServer}};

use crate::delta_ops::OpTypes;
use crate::differ::{DiffProps, HistoryRequest, HistoryResponse, TableOperation, TablePath};

mod delta_ops;
mod utils;

pub mod differ {
    include!("diff.rs");
}

impl std::hash::Hash for TableOperation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.operation.hash(state);
        self.operation_type.hash(state);
        self.content.hasher();
        self.timestamp.hash(state);
    }
}

#[derive(Debug, Default)]
pub struct DifferService {}

#[tonic::async_trait]
impl TableDiffer for DifferService {
    async fn table_diff(&self, request: Request<DiffRequest>) -> Result<Response<DiffResponse>, Status> {
        let r = request.into_inner();
        let s3_gateway_config_req: GatewayConfig = r.gateway_config.unwrap();
        let s3_config_map: HashMap<String, String> = utils::construct_storage_config(s3_gateway_config_req);

        let diff_props: DiffProps = r.props.unwrap();

        let left_table_path: TablePath = diff_props.left_table_path.unwrap();
        let right_table_path: TablePath = diff_props.right_table_path.unwrap();
        // let base_table_path: Option<TablePath> = diff_props.base_table_path;
        let left_table_res: Result<DeltaTable, Status> =
            delta_ops::get_delta_table(&s3_config_map, &diff_props.repo, left_table_path).await;
        let right_table_res: Result<DeltaTable, Status> =
            delta_ops::get_delta_table(&s3_config_map, &diff_props.repo, right_table_path).await;

        // Both tables not found
        if right_table_res.is_err() && left_table_res.is_err() {
            return Err(Status::new(Code::NotFound,
                                   format!("{:?}, {:?}", left_table_res.unwrap_err(),
                                           right_table_res.unwrap_err())
            ));
        }
        // If the table exists on the left ref but doesn't exist on the right ref -> table dropped.
        if right_table_res.is_err() && left_table_res.is_ok() {
            return Ok(Response::new(DiffResponse { entries: vec![], diff_type: differ::DiffType::Dropped as i32 }));
        }
        let mut right_table = right_table_res?;

        // If the table exists on the right, but doesn't exist on the left -> table created.
        if left_table_res.is_err() {
            let hist = delta_ops::history(&mut right_table, None).await?;
            let ans = reverse_and_create_table_ops(hist, right_table.version());
            return Ok(Response::new(DiffResponse { entries: ans, diff_type: differ::DiffType::Created as i32 }))
        }
        let mut left_table= left_table_res?;

        // todo: if base table exists, use it's version to start the comparison instead of going from most recent to oldest.
        // let mut base_table: Option<DeltaTable> = None;
        // if let Some(table_path) = base_table_path {
        //     base_table = Option::from(delta_ops::get_delta_table(&s3_config_map, &diff_props.repo, table_path).await?);
        // }

        let left_table_history = delta_ops::history(&mut left_table, None).await?;
        let right_table_history = delta_ops::history(&mut right_table, None).await?;

        let mut left_history_version = HistoryAndVersion{ history: left_table_history, version: left_table.version() };
        let mut right_history_version = HistoryAndVersion{ history: right_table_history, version: right_table.version() };

        let ans = compare(&mut left_history_version,  &mut right_history_version).unwrap();
        return Ok(Response::new(DiffResponse { entries: ans, diff_type: differ::DiffType::Changed as i32 }))
    }

    async fn show_history(&self, _request: Request<HistoryRequest>) -> Result<Response<HistoryResponse>, Status> {
        todo!()
    }
}

struct HistoryAndVersion {
    pub history: Vec<Map<String, Value>>,
    pub version: DeltaDataTypeVersion,
}

impl HistoryAndVersion {
    fn get_earliest_version(&self) -> i64 {
        self.version + 1 - i64::try_from(self.history.len()).unwrap()
    }

    fn get_lower_limit(&self, other: &HistoryAndVersion) -> i64 {
        let self_early = self.get_earliest_version();
        let other_early = self.get_earliest_version();
        if self_early > other.version { self_early } else { other_early }
    }
}

fn reverse_and_create_table_ops(mut hist: Vec<Map<String, Value>>, table_version: DeltaDataTypeVersion) -> Vec<TableOperation> {
    hist.reverse();
    let ops = OpTypes::new();
    let mut ans: Vec<TableOperation> = Vec::with_capacity(hist.len());
    for m in hist {
        let table_op = utils::construct_table_op(&m, table_version, &ops);
        ans.push(table_op);
    }
    return ans;
}

fn compare(left_history_version: &mut HistoryAndVersion,
           right_history_version: &mut HistoryAndVersion,) -> Result<Vec<TableOperation>, Error> {
    // The lower limit of the two (earliest common version):
    let lower_limit = right_history_version.get_lower_limit(&left_history_version);
    let mut table_op_list: Vec<TableOperation> = vec![];

    let right_table_version = right_history_version.version;
    let left_table_version = left_history_version.version;
    let mut curr_version = right_table_version;

    // Run through the commits from newest to oldest.
    left_history_version.history.reverse();
    right_history_version.history.reverse();
    let mut left_commit_slice = &left_history_version.history[..];
    let mut right_commit_slice = &right_history_version.history[..];
    let ops = OpTypes::new();

    // If the right table's version is bigger than the left table's version, add all commits until reaching the same version as the left table.
    if right_table_version > left_table_version {
        let right_iter = right_history_version.history.iter();
        for commit_info in right_iter {
            if curr_version == left_table_version {
                right_commit_slice = &right_history_version.history[(right_table_version - left_table_version) as usize..];
                break;
            }
            match commit_info {
                _operation_content => {
                    let d = utils::construct_table_op(commit_info, curr_version, &ops);
                    table_op_list.push(d)
                }
            }
            curr_version -= 1;
        }
    } else { // If left table's version is bigger, start running over the left log from the right table's version.
        left_commit_slice = &left_history_version.history[(left_table_version - right_table_version) as usize..];
    }
    compare_table_slices(left_commit_slice, right_commit_slice, curr_version, lower_limit, table_op_list)
}

fn compare_table_slices(left_commit_slice: &[Map<String, Value>],
                        right_commit_slice: &[Map<String, Value>],
                        mut curr_version: DeltaDataTypeVersion,
                        lower_limit: DeltaDataTypeVersion,
                        mut table_op_list: Vec<TableOperation>) -> Result<Vec<TableOperation>, Error> {
    let ops = OpTypes::new();
    let mut i: usize = 0; // iterating through the vector while 'curr_version' is the real version of the
    while i < left_commit_slice.len() && i < right_commit_slice.len() && curr_version >= lower_limit {
        let left_commit_info = left_commit_slice.get(i).unwrap();
        let right_commit_info = right_commit_slice.get(i).unwrap();
        let l_op = utils::construct_table_op(left_commit_info, curr_version, &ops);
        let r_op = utils::construct_table_op(right_commit_info, curr_version, &ops);

        if r_op == l_op {
            return Ok(table_op_list);
        } else {
            table_op_list.push(l_op);
        }
        curr_version -= 1;
        i += 1;
    }
    Ok(table_op_list)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let version: &str = "1";
    let port: u16 = portpicker::pick_unused_port().expect("No free ports");
    let address: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let differ_service = DifferService::default();
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<TableDifferServer<DifferService>>()
        .await;

    let serve = Server::builder()
        .add_service(health_service)
        .add_service(TableDifferServer::new(differ_service))
        .serve(address);

    // Communicating to the go-plugin application client (handshake)
    println!("1|{}|tcp|{}|grpc", version, address.to_string());
    io::stdout().flush().unwrap();

    serve.await?;

    Ok(())
}

