/*
 * lakeFS API
 *
 * lakeFS HTTP API
 *
 * The version of the OpenAPI document: 1.0.0
 * Contact: services@treeverse.io
 * Generated by: https://openapi-generator.tech
 */


use reqwest;

use crate::{apis::ResponseContent, models};
use super::{Error, configuration};


/// struct for typed errors of method [`create_pull_request`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CreatePullRequestError {
    Status400(models::Error),
    Status401(models::Error),
    Status403(models::Error),
    Status404(models::Error),
    Status409(models::Error),
    Status429(),
    DefaultResponse(models::Error),
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`get_pull_request`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetPullRequestError {
    Status400(models::Error),
    Status401(models::Error),
    Status404(models::Error),
    Status429(),
    DefaultResponse(models::Error),
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`list_pull_requests`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ListPullRequestsError {
    Status401(models::Error),
    Status404(models::Error),
    Status429(),
    DefaultResponse(models::Error),
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`merge_pull_request`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MergePullRequestError {
    Status400(models::Error),
    Status401(models::Error),
    Status403(models::Error),
    Status404(models::Error),
    Status409(models::MergeResult),
    Status412(models::Error),
    Status429(),
    DefaultResponse(models::Error),
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`update_pull_request`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UpdatePullRequestError {
    Status400(models::Error),
    Status401(models::Error),
    Status403(models::Error),
    Status404(models::Error),
    Status429(),
    DefaultResponse(models::Error),
    UnknownValue(serde_json::Value),
}


pub async fn create_pull_request(configuration: &configuration::Configuration, repository: &str, pull_request_creation: models::PullRequestCreation) -> Result<models::PullRequestCreationResponse, Error<CreatePullRequestError>> {
    let local_var_configuration = configuration;

    let local_var_client = &local_var_configuration.client;

    let local_var_uri_str = format!("{}/repositories/{repository}/pulls", local_var_configuration.base_path, repository=crate::apis::urlencode(repository));
    let mut local_var_req_builder = local_var_client.request(reqwest::Method::POST, local_var_uri_str.as_str());

    if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
        local_var_req_builder = local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(ref local_var_auth_conf) = local_var_configuration.basic_auth {
        local_var_req_builder = local_var_req_builder.basic_auth(local_var_auth_conf.0.to_owned(), local_var_auth_conf.1.to_owned());
    };
    if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
        local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
    };
    local_var_req_builder = local_var_req_builder.json(&pull_request_creation);

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
        serde_json::from_str(&local_var_content).map_err(Error::from)
    } else {
        let local_var_entity: Option<CreatePullRequestError> = serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent { status: local_var_status, content: local_var_content, entity: local_var_entity };
        Err(Error::ResponseError(local_var_error))
    }
}

pub async fn get_pull_request(configuration: &configuration::Configuration, repository: &str, pull_request: &str) -> Result<models::PullRequest, Error<GetPullRequestError>> {
    let local_var_configuration = configuration;

    let local_var_client = &local_var_configuration.client;

    let local_var_uri_str = format!("{}/repositories/{repository}/pulls/{pull_request}", local_var_configuration.base_path, repository=crate::apis::urlencode(repository), pull_request=crate::apis::urlencode(pull_request));
    let mut local_var_req_builder = local_var_client.request(reqwest::Method::GET, local_var_uri_str.as_str());

    if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
        local_var_req_builder = local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(ref local_var_auth_conf) = local_var_configuration.basic_auth {
        local_var_req_builder = local_var_req_builder.basic_auth(local_var_auth_conf.0.to_owned(), local_var_auth_conf.1.to_owned());
    };
    if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
        local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
    };

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
        serde_json::from_str(&local_var_content).map_err(Error::from)
    } else {
        let local_var_entity: Option<GetPullRequestError> = serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent { status: local_var_status, content: local_var_content, entity: local_var_entity };
        Err(Error::ResponseError(local_var_error))
    }
}

pub async fn list_pull_requests(configuration: &configuration::Configuration, repository: &str, prefix: Option<&str>, after: Option<&str>, amount: Option<i32>, status: Option<&str>) -> Result<models::PullRequestsList, Error<ListPullRequestsError>> {
    let local_var_configuration = configuration;

    let local_var_client = &local_var_configuration.client;

    let local_var_uri_str = format!("{}/repositories/{repository}/pulls", local_var_configuration.base_path, repository=crate::apis::urlencode(repository));
    let mut local_var_req_builder = local_var_client.request(reqwest::Method::GET, local_var_uri_str.as_str());

    if let Some(ref local_var_str) = prefix {
        local_var_req_builder = local_var_req_builder.query(&[("prefix", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = after {
        local_var_req_builder = local_var_req_builder.query(&[("after", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = amount {
        local_var_req_builder = local_var_req_builder.query(&[("amount", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_str) = status {
        local_var_req_builder = local_var_req_builder.query(&[("status", &local_var_str.to_string())]);
    }
    if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
        local_var_req_builder = local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(ref local_var_auth_conf) = local_var_configuration.basic_auth {
        local_var_req_builder = local_var_req_builder.basic_auth(local_var_auth_conf.0.to_owned(), local_var_auth_conf.1.to_owned());
    };
    if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
        local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
    };

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
        serde_json::from_str(&local_var_content).map_err(Error::from)
    } else {
        let local_var_entity: Option<ListPullRequestsError> = serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent { status: local_var_status, content: local_var_content, entity: local_var_entity };
        Err(Error::ResponseError(local_var_error))
    }
}

pub async fn merge_pull_request(configuration: &configuration::Configuration, repository: &str, pull_request: &str) -> Result<models::MergeResult, Error<MergePullRequestError>> {
    let local_var_configuration = configuration;

    let local_var_client = &local_var_configuration.client;

    let local_var_uri_str = format!("{}/repositories/{repository}/pulls/{pull_request}/merge", local_var_configuration.base_path, repository=crate::apis::urlencode(repository), pull_request=crate::apis::urlencode(pull_request));
    let mut local_var_req_builder = local_var_client.request(reqwest::Method::PUT, local_var_uri_str.as_str());

    if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
        local_var_req_builder = local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(ref local_var_auth_conf) = local_var_configuration.basic_auth {
        local_var_req_builder = local_var_req_builder.basic_auth(local_var_auth_conf.0.to_owned(), local_var_auth_conf.1.to_owned());
    };
    if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
        local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
    };

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
        serde_json::from_str(&local_var_content).map_err(Error::from)
    } else {
        let local_var_entity: Option<MergePullRequestError> = serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent { status: local_var_status, content: local_var_content, entity: local_var_entity };
        Err(Error::ResponseError(local_var_error))
    }
}

pub async fn update_pull_request(configuration: &configuration::Configuration, repository: &str, pull_request: &str, pull_request_basic: models::PullRequestBasic) -> Result<(), Error<UpdatePullRequestError>> {
    let local_var_configuration = configuration;

    let local_var_client = &local_var_configuration.client;

    let local_var_uri_str = format!("{}/repositories/{repository}/pulls/{pull_request}", local_var_configuration.base_path, repository=crate::apis::urlencode(repository), pull_request=crate::apis::urlencode(pull_request));
    let mut local_var_req_builder = local_var_client.request(reqwest::Method::PATCH, local_var_uri_str.as_str());

    if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
        local_var_req_builder = local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(ref local_var_auth_conf) = local_var_configuration.basic_auth {
        local_var_req_builder = local_var_req_builder.basic_auth(local_var_auth_conf.0.to_owned(), local_var_auth_conf.1.to_owned());
    };
    if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
        local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
    };
    local_var_req_builder = local_var_req_builder.json(&pull_request_basic);

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
        Ok(())
    } else {
        let local_var_entity: Option<UpdatePullRequestError> = serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent { status: local_var_status, content: local_var_content, entity: local_var_entity };
        Err(Error::ResponseError(local_var_error))
    }
}

