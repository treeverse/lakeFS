use std::cmp::max;
use std::collections::HashMap;
use std::io::{self, Write};
use std::net::SocketAddr;

use deltalake::delta::DeltaTable;
use deltalake::DeltaDataTypeVersion;
use futures::join;
use portpicker;
use serde_json::{Map, Value};
use tonic::{Code, Request, Response, Status, transport::Server};

use differ::{DiffRequest, DiffResponse, GatewayConfig, table_differ_server::{TableDiffer, TableDifferServer}};

use crate::differ::{DiffProps, DiffType, HistoryRequest, HistoryResponse, TableOperation, TablePath};

mod delta_ops;
mod utils;

pub mod differ {
    include!("diff.rs");
}

// The std::hash::Hash implementation is used to compare two table operations (commits)
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
    /*
    table_diff will compare two states of a given table over two refs in the following way:
    1. If the destination table (the one which the source table branch is compared to) isn't found
       then it's assumed that the table has been created, and all of its Delta Log will be returned as an answer.
    2. If the source table (the one which the user asked to compare to some destination table) isn't found
       then it's assumed that the table has been dropped.
    3. If both tables are found:
        A. If the right table's version is bigger than the left table's version: add all versions of the right tables that are bigger than
           the left table to the returned answer.
        B. Mark the latest common version as the start of the comparison algorithm.
        C. Mark the earliest common version as the end of the comparison algorithm.
        D. Compare the versions (commits) of the tables starting from the version found on step 'B':
            a. If the "commit info"s (Delta's commits) are different, add the right table's commit info to the result.
            b. Else, a common commit was found and the diff is complete.
     */
    async fn table_diff(&self, request: Request<DiffRequest>) -> Result<Response<DiffResponse>, Status> {
        let r = request.into_inner();
        let s3_gateway_config_req: GatewayConfig = r.gateway_config.expect("Missing S3 compatible gateway configurations");
        let diff_props: DiffProps = r.props.expect("Missing diff properties");
        let left_table_path: TablePath = diff_props.left_table_path.expect("Missing left table's path");
        let right_table_path: TablePath = diff_props.right_table_path.expect("Missing right table's path");
        let s3_config_map: HashMap<String, String> = utils::construct_storage_config(s3_gateway_config_req);
        let left_table_res =
            delta_ops::get_delta_table(&s3_config_map, &diff_props.repo, &left_table_path);
        let right_table_res =
            delta_ops::get_delta_table(&s3_config_map, &diff_props.repo, &right_table_path);

        // Get the base path, or return a default table path (with all fields initialized to their default type values)
        let base_path = diff_props.base_table_path.unwrap_or(TablePath{ r#ref: "".to_string(), path: "".to_string() });
        let base_table_res =
            delta_ops::get_delta_table(&s3_config_map, &diff_props.repo, &base_path);

        let (mut left_table_res, right_table_res, base_table_res) =
            join!(left_table_res, right_table_res, base_table_res);

        if right_table_res.is_err() || left_table_res.is_err() {
            match get_diff_type(&left_table_res, &right_table_res, &base_table_res).await {
                Ok(diff_type) => {
                    match diff_type {
                        DiffType::Dropped => {
                            return Ok(Response::new(DiffResponse { entries: vec![], diff_type: i32::from(DiffType::Dropped) }))
                        }
                        DiffType::Created => {
                            let mut right_table = right_table_res.unwrap();
                            let hist = delta_ops::history(&mut right_table, None).await?;
                            let ans = show_history(hist, right_table.version())?;
                            return Ok(Response::new(DiffResponse { entries: ans, diff_type: i32::from(DiffType::Created) }))
                        }
                        DiffType::Changed => {
                            left_table_res = base_table_res;
                        }
                    }
                }
                Err(e) => return Err(e)
            }
        }

        let mut right_table = right_table_res?;
        let mut left_table= left_table_res?;
        return run_diff(&mut left_table, &mut right_table).await;
    }

    async fn show_history(&self, _request: Request<HistoryRequest>) -> Result<Response<HistoryResponse>, Status> {
        todo!()
    }
}

async fn get_diff_type(left_table_res: &Result<DeltaTable, Status>,
                       right_table_res: &Result<DeltaTable, Status>,
                       base_table_res: &Result<DeltaTable, Status>) -> Result<DiffType, Status> {
    return match (left_table_res, right_table_res) {
        (Ok(_), Err(status)) => {
            // right table wasn't found, but it exists on the base: table dropped
            if matches!(status.code(), Code::NotFound) && base_table_res.is_ok() {
                Ok(DiffType::Dropped)
            } else {
                Err(status.clone())
            }
        },
        (Err(status), Ok(_)) => {
            // left table wasn't found, and it doesn't exist on the base: table created
            if matches!(status.code(), Code::NotFound) {
                if base_table_res.is_err() {
                    if matches!(base_table_res.as_ref().unwrap_err().code(), Code::NotFound | Code::InvalidArgument) { // didn't exist in base ref or no base ref provided
                        Ok(DiffType::Created)
                    } else { // There was other kind of error with the base branch
                        Err(base_table_res.as_ref().unwrap_err().clone())
                    }
                } else { // The table existed on the base branch
                    Ok(DiffType::Changed)
                }
            } else {
                Err(left_table_res.as_ref().unwrap_err().clone())
            }
        },
        // If both are erroneous, return one of the return codes
        _ => Err(left_table_res.as_ref().unwrap_err().clone())
    }
}

async fn run_diff(left_table: &mut DeltaTable, right_table: &mut DeltaTable) -> Result<Response<DiffResponse>, Status> {
    let left_table_history = delta_ops::history(left_table, None);
    let right_table_history = delta_ops::history(right_table, None);

    let (left_table_history, right_table_history) = join!(left_table_history, right_table_history);

    let mut left_history_version = HistoryAndVersion{ history: left_table_history?, version: left_table.version() };
    let mut right_history_version = HistoryAndVersion{ history: right_table_history?, version: right_table.version() };
    let diff = compare(&mut left_history_version,  &mut right_history_version).unwrap();
    return Ok(Response::new(DiffResponse { entries: diff, diff_type: i32::from(DiffType::Changed) }))
}

struct HistoryAndVersion {
    pub history: Vec<Map<String, Value>>,
    pub version: DeltaDataTypeVersion,
}

impl HistoryAndVersion {
    // For instance: table version is 51, history length is 10 (due to compaction)-> returned early version = 42
    fn get_earliest_version(&self) -> i64 {
        self.version + 1 - i64::try_from(self.history.len()).unwrap()
    }

    fn get_common_available_ancestor(&self, other: &HistoryAndVersion) -> i64 {
        let self_early = self.get_earliest_version();
        let other_early = other.get_earliest_version();
        max(self_early, other_early)
    }
}

fn show_history(mut hist: Vec<Map<String, Value>>, mut table_version: DeltaDataTypeVersion) -> Result<Vec<TableOperation>, Status> {
    hist.reverse();
    let mut ans: Vec<TableOperation> = Vec::with_capacity(hist.len());
    for commit in hist {
        let table_op = construct_table_op(&commit, table_version)?;
        ans.push(table_op);
        table_version -= 1;
    }
    Ok(ans)
}

fn compare(left_history_version: &mut HistoryAndVersion,
           right_history_version: &mut HistoryAndVersion) -> Result<Vec<TableOperation>, Status> {
    // The lower limit of the two (earliest common version):
    let common_ancestor = right_history_version.get_common_available_ancestor(&left_history_version);
    let mut table_op_list: Vec<TableOperation> = vec![];

    let right_table_version = right_history_version.version;
    let left_table_version = left_history_version.version;
    let mut curr_version = right_table_version;

    // Run through the commits from newest to oldest.
    left_history_version.history.reverse();
    right_history_version.history.reverse();
    let mut left_commit_slice = &left_history_version.history[..];
    let mut right_commit_slice = &right_history_version.history[..];

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
                    let d = construct_table_op(commit_info, curr_version)?;
                    table_op_list.push(d)
                }
            }
            curr_version -= 1;
        }
    } else { // If left table's version is bigger, start running over the left log from the right table's version.
        left_commit_slice = &left_history_version.history[(left_table_version - right_table_version) as usize..];
    }
    compare_table_slices(left_commit_slice, right_commit_slice, curr_version, common_ancestor, table_op_list)
}

fn compare_table_slices(left_commit_slice: &[Map<String, Value>],
                        right_commit_slice: &[Map<String, Value>],
                        mut curr_version: DeltaDataTypeVersion,
                        common_ancestor: DeltaDataTypeVersion,
                        mut table_op_list: Vec<TableOperation>) -> Result<Vec<TableOperation>, Status> {
    let mut i: usize = 0; // iterating through the vector while 'curr_version' is the version of the current commit
    while i < left_commit_slice.len() && i < right_commit_slice.len() && curr_version >= common_ancestor {
        let left_commit_info = left_commit_slice.get(i).unwrap();
        let right_commit_info = right_commit_slice.get(i).unwrap();
        let l_op = construct_table_op(left_commit_info, curr_version)?;
        let r_op = construct_table_op(right_commit_info, curr_version)?;

        if r_op == l_op {
            return Ok(table_op_list);
        } else {
            table_op_list.push(r_op);
        }
        curr_version -= 1;
        i += 1;
    }
    Ok(table_op_list)
}

fn construct_table_op(commit_info: &Map<String, Value>, version: DeltaDataTypeVersion) -> Result<TableOperation, Status> {
    return match utils::construct_table_op(commit_info, version) {
        Ok(table_ops) => Ok(table_ops),
        Err(e) => Err(Status::new(Code::Aborted, format!("Creating operation history aborted due to:\n{:?}", e)))
    };
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

