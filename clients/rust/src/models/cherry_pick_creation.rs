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
pub struct CherryPickCreation {
    /// the commit to cherry-pick, given by a ref
    #[serde(rename = "ref")]
    pub r#ref: String,
    /// When cherry-picking a merge commit, the parent number (starting from 1) with which to perform the diff. The default branch is parent 1. 
    #[serde(rename = "parent_number", skip_serializing_if = "Option::is_none")]
    pub parent_number: Option<i32>,
    #[serde(rename = "force", skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,
}

impl CherryPickCreation {
    pub fn new(r#ref: String) -> CherryPickCreation {
        CherryPickCreation {
            r#ref,
            parent_number: None,
            force: None,
        }
    }
}
