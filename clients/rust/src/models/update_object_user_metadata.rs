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
pub struct UpdateObjectUserMetadata {
    #[serde(rename = "set")]
    pub set: std::collections::HashMap<String, String>,
}

impl UpdateObjectUserMetadata {
    pub fn new(set: std::collections::HashMap<String, String>) -> UpdateObjectUserMetadata {
        UpdateObjectUserMetadata {
            set,
        }
    }
}

