/*
 * lakeFS API
 *
 * lakeFS HTTP API
 *
 * The version of the OpenAPI document: 1.0.0
 * Contact: services@treeverse.io
 * Generated by: https://openapi-generator.tech
 */

use crate::models;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommitRecordCreation {
    /// id of the commit record
    #[serde(rename = "commit_id")]
    pub commit_id: String,
    /// version of the commit record
    #[serde(rename = "version")]
    pub version: i32,
    /// committer of the commit record
    #[serde(rename = "committer")]
    pub committer: String,
    /// message of the commit record
    #[serde(rename = "message")]
    pub message: String,
    /// metarange_id of the commit record
    #[serde(rename = "metarange_id")]
    pub metarange_id: String,
    /// Unix Epoch in seconds
    #[serde(rename = "creation_date")]
    pub creation_date: i64,
    /// parents of the commit record
    #[serde(rename = "parents")]
    pub parents: Vec<String>,
    /// metadata of the commit record
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
    /// generation of the commit record
    #[serde(rename = "generation")]
    pub generation: i64,
    #[serde(rename = "force", skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,
}

impl CommitRecordCreation {
    pub fn new(commit_id: String, version: i32, committer: String, message: String, metarange_id: String, creation_date: i64, parents: Vec<String>, generation: i64) -> CommitRecordCreation {
        CommitRecordCreation {
            commit_id,
            version,
            committer,
            message,
            metarange_id,
            creation_date,
            parents,
            metadata: None,
            generation,
            force: None,
        }
    }
}
