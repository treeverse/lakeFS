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
pub struct UploadPartFrom {
    /// The physical address (of the entire intended object) returned from createPresignMultipartUpload. 
    #[serde(rename = "physical_address")]
    pub physical_address: String,
}

impl UploadPartFrom {
    pub fn new(physical_address: String) -> UploadPartFrom {
        UploadPartFrom {
            physical_address,
        }
    }
}

