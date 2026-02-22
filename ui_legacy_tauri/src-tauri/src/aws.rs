use aws_config::meta::region::RegionProviderChain;
use aws_sdk_apigatewayv2::Client as ApiGatewayV2Client;
use aws_sdk_ec2::Client as Ec2Client;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_s3::Client as S3Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug, thiserror::Error)]
pub enum AwsCommanderError {
    #[error("unknown service: {0}")]
    UnknownService(String),
    #[error("invalid payload: {0}")]
    InvalidPayload(String),
    #[error("operation not implemented for service: {0}")]
    NotImplemented(String),
    #[error("aws sdk error: {0}")]
    AwsSdk(String),
    #[error("aws cli error: {0}")]
    AwsCli(String),
    #[error("resource not found")]
    NotFound,
}

pub type AwsCommanderResult<T> = Result<T, AwsCommanderError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSummary {
    pub name: String,
    pub default_region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PanelContext {
    pub profile_name: String,
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub id: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRef {
    pub id: String,
    pub name: String,
    pub service: String,
    pub region: String,
    pub arn: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcePage {
    pub items: Vec<ResourceRef>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceDetail {
    pub resource: ResourceRef,
    pub detail: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationResult {
    pub ok: bool,
    pub message: String,
    pub changed_resources: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyPlan {
    pub service: String,
    pub source_id: String,
    pub suggested_target_name: String,
    pub normalized_spec: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteCopyInput {
    pub copy_plan: CopyPlan,
    pub target_name: Option<String>,
    pub overwrite: bool,
}

pub fn list_profiles() -> Vec<ProfileSummary> {
    let mut names = BTreeSet::new();
    let mut config_regions = std::collections::HashMap::new();

    if let Some(cfg_path) = aws_file("config") {
        if let Ok(content) = fs::read_to_string(cfg_path) {
            for line in content.lines() {
                let line = line.trim();
                if line.starts_with('[') && line.ends_with(']') {
                    let section = line.trim_start_matches('[').trim_end_matches(']');
                    let profile = if let Some(rest) = section.strip_prefix("profile ") {
                        rest.trim().to_string()
                    } else {
                        section.to_string()
                    };
                    if !profile.is_empty() {
                        names.insert(profile.clone());
                    }
                }
            }
            let mut current_profile: Option<String> = None;
            for raw in content.lines() {
                let line = raw.trim();
                if line.starts_with('[') && line.ends_with(']') {
                    let section = line.trim_start_matches('[').trim_end_matches(']');
                    let profile = if let Some(rest) = section.strip_prefix("profile ") {
                        rest.trim().to_string()
                    } else {
                        section.to_string()
                    };
                    current_profile = if profile.is_empty() { None } else { Some(profile) };
                    continue;
                }
                if let Some(profile) = &current_profile {
                    if let Some((key, value)) = line.split_once('=') {
                        if key.trim() == "region" {
                            config_regions.insert(profile.clone(), value.trim().to_string());
                        }
                    }
                }
            }
        }
    }

    if let Some(creds_path) = aws_file("credentials") {
        if let Ok(content) = fs::read_to_string(creds_path) {
            for line in content.lines() {
                let line = line.trim();
                if line.starts_with('[') && line.ends_with(']') {
                    let section = line.trim_start_matches('[').trim_end_matches(']').trim();
                    if !section.is_empty() {
                        names.insert(section.to_string());
                    }
                }
            }
        }
    }

    if names.is_empty() {
        names.insert("default".to_string());
    }

    names
        .into_iter()
        .map(|name| ProfileSummary {
            default_region: config_regions.get(&name).cloned(),
            name,
        })
        .collect()
}

pub fn list_regions(profile_name: &str) -> Vec<String> {
    let mut regions = vec![
        "us-east-1".to_string(),
        "us-east-2".to_string(),
        "us-west-1".to_string(),
        "us-west-2".to_string(),
        "eu-west-1".to_string(),
        "eu-central-1".to_string(),
        "ap-southeast-1".to_string(),
    ];
    if let Some(default_region) = list_profiles()
        .into_iter()
        .find(|p| p.name.eq_ignore_ascii_case(profile_name))
        .and_then(|p| p.default_region)
    {
        if let Some(index) = regions.iter().position(|r| r == &default_region) {
            regions.swap(0, index);
        } else {
            regions.insert(0, default_region);
        }
    }
    regions
}

pub async fn list_services(ctx: &PanelContext) -> AwsCommanderResult<Vec<ServiceInfo>> {
    let candidates = [
        ("s3", "S3"),
        ("lambda", "Lambda"),
        ("vpc", "VPC"),
        ("apigateway", "API Gateway"),
    ];

    let mut services = Vec::new();
    for (id, label) in candidates {
        if let Ok(page) = list_resources(ctx, id, None).await {
            if !page.items.is_empty() {
                services.push(ServiceInfo {
                    id: id.to_string(),
                    label: label.to_string(),
                });
            }
        }
    }

    if services.is_empty() {
        services = candidates
            .iter()
            .map(|(id, label)| ServiceInfo {
                id: id.to_string(),
                label: label.to_string(),
            })
            .collect();
    }

    Ok(services)
}

pub async fn list_resources(
    ctx: &PanelContext,
    service: &str,
    cursor: Option<String>,
) -> AwsCommanderResult<ResourcePage> {
    match service {
        "s3" => list_s3_buckets(ctx).await,
        "lambda" => list_lambda_functions(ctx, cursor).await,
        "vpc" => list_vpcs(ctx).await,
        "apigateway" => list_apis(ctx, cursor).await,
        _ => Err(AwsCommanderError::UnknownService(service.to_string())),
    }
}

pub async fn get_resource(
    ctx: &PanelContext,
    service: &str,
    resource_id: &str,
) -> AwsCommanderResult<ResourceDetail> {
    match service {
        "s3" => get_s3_bucket(ctx, resource_id).await,
        "lambda" => get_lambda_function(ctx, resource_id).await,
        "vpc" => get_vpc(ctx, resource_id).await,
        "apigateway" => get_api(ctx, resource_id).await,
        _ => Err(AwsCommanderError::UnknownService(service.to_string())),
    }
}

pub async fn create_resource(
    ctx: &PanelContext,
    service: &str,
    payload: Value,
) -> AwsCommanderResult<OperationResult> {
    match service {
        "s3" => {
            let name = payload
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| AwsCommanderError::InvalidPayload("missing field `name`".to_string()))?;
            let cfg = aws_config_for(ctx).await;
            let client = S3Client::new(&cfg);
            client
                .create_bucket()
                .bucket(name)
                .send()
                .await
                .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;
            Ok(OperationResult {
                ok: true,
                message: format!("Bucket `{name}` created"),
                changed_resources: vec![name.to_string()],
                warnings: vec![],
            })
        }
        _ => Err(AwsCommanderError::NotImplemented(service.to_string())),
    }
}

pub async fn update_resource(
    _ctx: &PanelContext,
    service: &str,
    _resource_id: &str,
    _payload: Value,
) -> AwsCommanderResult<OperationResult> {
    Err(AwsCommanderError::NotImplemented(service.to_string()))
}

pub async fn delete_resource(
    ctx: &PanelContext,
    service: &str,
    resource_id: &str,
) -> AwsCommanderResult<OperationResult> {
    match service {
        "s3" => {
            let cfg = aws_config_for(ctx).await;
            let client = S3Client::new(&cfg);
            client
                .delete_bucket()
                .bucket(resource_id)
                .send()
                .await
                .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;
            Ok(OperationResult {
                ok: true,
                message: format!("Bucket `{resource_id}` deleted"),
                changed_resources: vec![resource_id.to_string()],
                warnings: vec![],
            })
        }
        "lambda" => {
            let cfg = aws_config_for(ctx).await;
            let client = LambdaClient::new(&cfg);
            client
                .delete_function()
                .function_name(resource_id)
                .send()
                .await
                .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;
            Ok(OperationResult {
                ok: true,
                message: format!("Lambda `{resource_id}` deleted"),
                changed_resources: vec![resource_id.to_string()],
                warnings: vec![],
            })
        }
        _ => Err(AwsCommanderError::NotImplemented(service.to_string())),
    }
}

pub async fn prepare_copy(
    ctx: &PanelContext,
    service: &str,
    source_id: &str,
) -> AwsCommanderResult<CopyPlan> {
    match service {
        "s3" => Ok(CopyPlan {
            service: service.to_string(),
            source_id: source_id.to_string(),
            suggested_target_name: source_id.to_string(),
            normalized_spec: json!({ "bucket_name": source_id }),
        }),
        "lambda" => {
            let detail = get_lambda_function(ctx, source_id).await?;
            let function_name = detail.resource.name;
            Ok(CopyPlan {
                service: service.to_string(),
                source_id: source_id.to_string(),
                suggested_target_name: function_name.clone(),
                normalized_spec: json!({
                    "function_name": function_name,
                    "warning": "Lambda copy requires deployment package artifact and is not fully automated yet"
                }),
            })
        }
        _ => Err(AwsCommanderError::NotImplemented(service.to_string())),
    }
}

pub async fn execute_copy(
    ctx: &PanelContext,
    input: ExecuteCopyInput,
) -> AwsCommanderResult<OperationResult> {
    match input.copy_plan.service.as_str() {
        "s3" => {
            let target_name = input
                .target_name
                .unwrap_or_else(|| input.copy_plan.suggested_target_name.clone());
            let cfg = aws_config_for(ctx).await;
            let client = S3Client::new(&cfg);

            if !input.overwrite {
                let exists = client.head_bucket().bucket(&target_name).send().await.is_ok();
                if exists {
                    return Err(AwsCommanderError::InvalidPayload(format!(
                        "target bucket `{target_name}` exists; choose a new name or set overwrite"
                    )));
                }
            }

            client
                .create_bucket()
                .bucket(&target_name)
                .send()
                .await
                .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;

            Ok(OperationResult {
                ok: true,
                message: format!("Copied S3 bucket template to `{target_name}`"),
                changed_resources: vec![target_name],
                warnings: vec!["Only bucket shell is created; objects are not copied in this MVP".to_string()],
            })
        }
        "lambda" => Err(AwsCommanderError::NotImplemented(
            "lambda copy execution pending deployment package support".to_string(),
        )),
        service => Err(AwsCommanderError::NotImplemented(service.to_string())),
    }
}

async fn list_s3_buckets(ctx: &PanelContext) -> AwsCommanderResult<ResourcePage> {
    let cfg = aws_config_for(ctx).await;
    let client = S3Client::new(&cfg);
    let out = match client.list_buckets().send().await {
        Ok(out) => out,
        Err(err) => {
            let sdk_message = format!("{err:?}");
            if is_dispatch_failure(&sdk_message) {
                return list_s3_buckets_via_cli(ctx).map_err(|cli_err| {
                    AwsCommanderError::AwsSdk(format!(
                        "{sdk_message}. CLI fallback also failed: {cli_err}"
                    ))
                });
            }
            return Err(AwsCommanderError::AwsSdk(format!(
                "{sdk_message}. Hint: verify network and run `aws sts get-caller-identity --profile {}`",
                ctx.profile_name
            )));
        }
    };

    let mut items = Vec::new();
    for bucket in out.buckets() {
        if let Some(name) = bucket.name() {
            items.push(ResourceRef {
                id: name.to_string(),
                name: name.to_string(),
                service: "s3".to_string(),
                region: ctx.region.clone(),
                arn: Some(format!("arn:aws:s3:::{name}")),
                status: None,
            });
        }
    }

    Ok(ResourcePage {
        items,
        next_cursor: None,
    })
}

async fn list_lambda_functions(
    ctx: &PanelContext,
    cursor: Option<String>,
) -> AwsCommanderResult<ResourcePage> {
    let cfg = aws_config_for(ctx).await;
    let client = LambdaClient::new(&cfg);
    let mut req = client.list_functions();
    if let Some(marker) = cursor {
        req = req.marker(marker);
    }
    let out = req
        .send()
        .await
        .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;

    let mut items = Vec::new();
    for fn_item in out.functions() {
        if let Some(name) = fn_item.function_name() {
            items.push(ResourceRef {
                id: name.to_string(),
                name: name.to_string(),
                service: "lambda".to_string(),
                region: ctx.region.clone(),
                arn: fn_item.function_arn().map(ToString::to_string),
                status: fn_item.state().map(|s| s.as_str().to_string()),
            });
        }
    }

    Ok(ResourcePage {
        items,
        next_cursor: out.next_marker().map(ToString::to_string),
    })
}

async fn list_vpcs(ctx: &PanelContext) -> AwsCommanderResult<ResourcePage> {
    let cfg = aws_config_for(ctx).await;
    let client = Ec2Client::new(&cfg);
    let out = client
        .describe_vpcs()
        .send()
        .await
        .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;

    let mut items = Vec::new();
    for vpc in out.vpcs() {
        if let Some(id) = vpc.vpc_id() {
            items.push(ResourceRef {
                id: id.to_string(),
                name: id.to_string(),
                service: "vpc".to_string(),
                region: ctx.region.clone(),
                arn: None,
                status: vpc.state().map(|s| s.as_str().to_string()),
            });
        }
    }

    Ok(ResourcePage {
        items,
        next_cursor: None,
    })
}

async fn list_apis(ctx: &PanelContext, cursor: Option<String>) -> AwsCommanderResult<ResourcePage> {
    let cfg = aws_config_for(ctx).await;
    let client = ApiGatewayV2Client::new(&cfg);
    let mut req = client.get_apis();
    if let Some(token) = cursor {
        req = req.next_token(token);
    }

    let out = req
        .send()
        .await
        .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;

    let mut items = Vec::new();
    for api in out.items() {
        if let Some(id) = api.api_id() {
            let name = api.name().unwrap_or(id);
            items.push(ResourceRef {
                id: id.to_string(),
                name: name.to_string(),
                service: "apigateway".to_string(),
                region: ctx.region.clone(),
                arn: api.api_endpoint().map(ToString::to_string),
                status: api.protocol_type().map(|p| p.as_str().to_string()),
            });
        }
    }

    Ok(ResourcePage {
        items,
        next_cursor: out.next_token().map(ToString::to_string),
    })
}

async fn get_s3_bucket(ctx: &PanelContext, bucket_name: &str) -> AwsCommanderResult<ResourceDetail> {
    let cfg = aws_config_for(ctx).await;
    let client = S3Client::new(&cfg);

    let location = match client.get_bucket_location().bucket(bucket_name).send().await {
        Ok(out) => Some(out.location_constraint().map(|l| l.as_str().to_string())),
        Err(err) => {
            let sdk_message = format!("{err:?}");
            if is_dispatch_failure(&sdk_message) {
                let via_cli = run_aws_cli_json(
                    &ctx.profile_name,
                    &ctx.region,
                    &["s3api", "get-bucket-location", "--bucket", bucket_name],
                )?;
                let region = via_cli
                    .get("LocationConstraint")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                Some(region)
            } else {
                return Err(AwsCommanderError::AwsSdk(format!(
                    "{sdk_message}. Hint: verify network and run `aws sts get-caller-identity --profile {}`",
                    ctx.profile_name
                )));
            }
        }
    };

    Ok(ResourceDetail {
        resource: ResourceRef {
            id: bucket_name.to_string(),
            name: bucket_name.to_string(),
            service: "s3".to_string(),
            region: ctx.region.clone(),
            arn: Some(format!("arn:aws:s3:::{bucket_name}")),
            status: None,
        },
        detail: json!({
            "bucket": bucket_name,
            "location": location.flatten(),
        }),
    })
}

async fn get_lambda_function(
    ctx: &PanelContext,
    function_name: &str,
) -> AwsCommanderResult<ResourceDetail> {
    let cfg = aws_config_for(ctx).await;
    let client = LambdaClient::new(&cfg);

    let out = client
        .get_function()
        .function_name(function_name)
        .send()
        .await
        .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;

    let conf = out.configuration().ok_or(AwsCommanderError::NotFound)?;
    Ok(ResourceDetail {
        resource: ResourceRef {
            id: conf.function_name().unwrap_or(function_name).to_string(),
            name: conf.function_name().unwrap_or(function_name).to_string(),
            service: "lambda".to_string(),
            region: ctx.region.clone(),
            arn: conf.function_arn().map(ToString::to_string),
            status: conf.state().map(|s| s.as_str().to_string()),
        },
        detail: json!({
            "runtime": conf.runtime().map(|r| r.as_str()),
            "handler": conf.handler(),
            "role": conf.role(),
            "timeout": conf.timeout(),
            "memory_size": conf.memory_size(),
            "description": conf.description(),
            "last_modified": conf.last_modified(),
        }),
    })
}

async fn get_vpc(ctx: &PanelContext, vpc_id: &str) -> AwsCommanderResult<ResourceDetail> {
    let cfg = aws_config_for(ctx).await;
    let client = Ec2Client::new(&cfg);

    let out = client
        .describe_vpcs()
        .vpc_ids(vpc_id)
        .send()
        .await
        .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;

    let vpc = out.vpcs().first().ok_or(AwsCommanderError::NotFound)?;
    Ok(ResourceDetail {
        resource: ResourceRef {
            id: vpc_id.to_string(),
            name: vpc_id.to_string(),
            service: "vpc".to_string(),
            region: ctx.region.clone(),
            arn: None,
            status: vpc.state().map(|s| s.as_str().to_string()),
        },
        detail: json!({
            "cidr": vpc.cidr_block(),
            "is_default": vpc.is_default(),
            "state": vpc.state().map(|s| s.as_str()),
            "owner_id": vpc.owner_id(),
        }),
    })
}

async fn get_api(ctx: &PanelContext, api_id: &str) -> AwsCommanderResult<ResourceDetail> {
    let cfg = aws_config_for(ctx).await;
    let client = ApiGatewayV2Client::new(&cfg);

    let out = client
        .get_api()
        .api_id(api_id)
        .send()
        .await
        .map_err(|e| AwsCommanderError::AwsSdk(e.to_string()))?;

    let name = out.name().unwrap_or(api_id);
    Ok(ResourceDetail {
        resource: ResourceRef {
            id: api_id.to_string(),
            name: name.to_string(),
            service: "apigateway".to_string(),
            region: ctx.region.clone(),
            arn: out.api_endpoint().map(ToString::to_string),
            status: out.protocol_type().map(|s| s.as_str().to_string()),
        },
        detail: json!({
            "api_endpoint": out.api_endpoint(),
            "protocol_type": out.protocol_type().map(|p| p.as_str()),
            "route_selection_expression": out.route_selection_expression(),
            "version": out.version(),
            "created_date": out.created_date().map(|d| d.to_string()),
        }),
    })
}

async fn aws_config_for(ctx: &PanelContext) -> aws_config::SdkConfig {
    let resolved_profile = resolve_profile_name(&ctx.profile_name);
    let region_provider = RegionProviderChain::first_try(Some(aws_config::Region::new(ctx.region.clone())))
        .or_default_provider();

    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .profile_name(resolved_profile)
        .region(region_provider)
        .load()
        .await
}

fn aws_file(name: &str) -> Option<PathBuf> {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)?;
    Some(home.join(".aws").join(name))
}

fn list_s3_buckets_via_cli(ctx: &PanelContext) -> AwsCommanderResult<ResourcePage> {
    let output = run_aws_cli_json(
        &ctx.profile_name,
        &ctx.region,
        &["s3api", "list-buckets", "--output", "json"],
    )?;

    let mut items = Vec::new();
    for bucket in output
        .get("Buckets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(name) = bucket.get("Name").and_then(Value::as_str) {
            items.push(ResourceRef {
                id: name.to_string(),
                name: name.to_string(),
                service: "s3".to_string(),
                region: ctx.region.clone(),
                arn: Some(format!("arn:aws:s3:::{name}")),
                status: Some("cli-fallback".to_string()),
            });
        }
    }

    Ok(ResourcePage {
        items,
        next_cursor: None,
    })
}

fn run_aws_cli_json(profile: &str, region: &str, args: &[&str]) -> AwsCommanderResult<Value> {
    let resolved_profile = resolve_profile_name(profile);
    let mut cmd = Command::new("aws");
    cmd.env("AWS_PAGER", "")
        .arg("--profile")
        .arg(resolved_profile)
        .arg("--region")
        .arg(region);
    for arg in args {
        cmd.arg(arg);
    }
    let output = cmd
        .output()
        .map_err(|e| AwsCommanderError::AwsCli(e.to_string()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(AwsCommanderError::AwsCli(stderr));
    }
    serde_json::from_slice::<Value>(&output.stdout)
        .map_err(|e| AwsCommanderError::AwsCli(e.to_string()))
}

fn is_dispatch_failure(message: &str) -> bool {
    let lower = message.to_lowercase();
    lower.contains("dispatchfailure")
        || lower.contains("dispatch failure")
        || lower.contains("connectorerror")
        || lower.contains("httptimeouterror")
}

fn resolve_profile_name(input: &str) -> String {
    for profile in list_profiles() {
        if profile.name.eq_ignore_ascii_case(input) {
            return profile.name;
        }
    }
    input.to_string()
}
