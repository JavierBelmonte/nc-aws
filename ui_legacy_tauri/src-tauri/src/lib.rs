mod aws;

use aws::{
    AwsCommanderError, CopyPlan, ExecuteCopyInput, OperationResult, PanelContext, ProfileSummary,
    ResourceDetail, ResourcePage, ServiceInfo,
};
use serde_json::Value;

fn err_to_string(err: AwsCommanderError) -> String {
    err.to_string()
}

#[tauri::command]
fn list_profiles() -> Vec<ProfileSummary> {
    eprintln!("[aws-commander] list_profiles()");
    aws::list_profiles()
}

#[tauri::command]
fn list_regions(profile_name: String) -> Vec<String> {
    eprintln!("[aws-commander] list_regions(profile={profile_name})");
    aws::list_regions(&profile_name)
}

#[tauri::command]
async fn list_services(panel_ctx: PanelContext) -> Result<Vec<ServiceInfo>, String> {
    eprintln!(
        "[aws-commander] list_services(profile={}, region={})",
        panel_ctx.profile_name, panel_ctx.region
    );
    aws::list_services(&panel_ctx).await.map_err(err_to_string)
}

#[tauri::command]
async fn list_resources(
    panel_ctx: PanelContext,
    service: String,
    cursor: Option<String>,
) -> Result<ResourcePage, String> {
    eprintln!(
        "[aws-commander] list_resources(profile={}, region={}, service={}, cursor={})",
        panel_ctx.profile_name,
        panel_ctx.region,
        service,
        cursor.as_deref().unwrap_or("-")
    );
    aws::list_resources(&panel_ctx, &service, cursor)
        .await
        .map_err(err_to_string)
}

#[tauri::command]
async fn get_resource(
    panel_ctx: PanelContext,
    service: String,
    resource_id: String,
) -> Result<ResourceDetail, String> {
    aws::get_resource(&panel_ctx, &service, &resource_id)
        .await
        .map_err(err_to_string)
}

#[tauri::command]
async fn create_resource(
    panel_ctx: PanelContext,
    service: String,
    payload: Value,
) -> Result<OperationResult, String> {
    aws::create_resource(&panel_ctx, &service, payload)
        .await
        .map_err(err_to_string)
}

#[tauri::command]
async fn update_resource(
    panel_ctx: PanelContext,
    service: String,
    resource_id: String,
    payload: Value,
) -> Result<OperationResult, String> {
    aws::update_resource(&panel_ctx, &service, &resource_id, payload)
        .await
        .map_err(err_to_string)
}

#[tauri::command]
async fn delete_resource(
    panel_ctx: PanelContext,
    service: String,
    resource_id: String,
) -> Result<OperationResult, String> {
    aws::delete_resource(&panel_ctx, &service, &resource_id)
        .await
        .map_err(err_to_string)
}

#[tauri::command]
async fn prepare_copy(
    source_ctx: PanelContext,
    service: String,
    resource_id: String,
) -> Result<CopyPlan, String> {
    aws::prepare_copy(&source_ctx, &service, &resource_id)
        .await
        .map_err(err_to_string)
}

#[tauri::command]
async fn execute_copy(
    target_ctx: PanelContext,
    input: ExecuteCopyInput,
) -> Result<OperationResult, String> {
    aws::execute_copy(&target_ctx, input)
        .await
        .map_err(err_to_string)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_profiles,
            list_regions,
            list_services,
            list_resources,
            get_resource,
            create_resource,
            update_resource,
            delete_resource,
            prepare_copy,
            execute_copy,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
