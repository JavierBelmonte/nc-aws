import { invoke } from '@tauri-apps/api/core'
import type {
  CopyPlan,
  ExecuteCopyInput,
  OperationResult,
  PanelContext,
  ProfileSummary,
  ResourceDetail,
  ResourcePage,
  ServiceInfo,
} from './types'

const FALLBACK_REGIONS = [
  'us-east-1',
  'us-east-2',
  'us-west-1',
  'us-west-2',
  'eu-west-1',
  'eu-central-1',
]

function isTauriRuntime(): boolean {
  if (typeof window === 'undefined') {
    return false
  }
  return '__TAURI_INTERNALS__' in (window as unknown as Record<string, unknown>)
}

export async function listProfiles(): Promise<ProfileSummary[]> {
  if (!isTauriRuntime()) {
    return [{ name: 'default', default_region: 'us-east-1' }]
  }
  return invoke<ProfileSummary[]>('list_profiles')
}

export async function listRegions(profileName: string): Promise<string[]> {
  if (!isTauriRuntime()) {
    return FALLBACK_REGIONS
  }
  return invoke<string[]>('list_regions', { profileName })
}

export async function listServices(panelCtx: PanelContext): Promise<ServiceInfo[]> {
  if (!isTauriRuntime()) {
    return [
      { id: 's3', label: 'S3' },
      { id: 'lambda', label: 'Lambda' },
      { id: 'vpc', label: 'VPC' },
      { id: 'apigateway', label: 'API Gateway' },
    ]
  }
  return invoke<ServiceInfo[]>('list_services', { panelCtx })
}

export async function listResources(
  panelCtx: PanelContext,
  service: string,
  cursor?: string | null,
): Promise<ResourcePage> {
  if (!isTauriRuntime()) {
    return { items: [], next_cursor: null }
  }
  return invoke<ResourcePage>('list_resources', {
    panelCtx,
    service,
    cursor: cursor ?? null,
  })
}

export async function getResource(
  panelCtx: PanelContext,
  service: string,
  resourceId: string,
): Promise<ResourceDetail> {
  if (!isTauriRuntime()) {
    throw new Error('Resource detail is only available in Tauri runtime')
  }
  return invoke<ResourceDetail>('get_resource', {
    panelCtx,
    service,
    resourceId,
  })
}

export async function createResource(
  panelCtx: PanelContext,
  service: string,
  payload: Record<string, unknown>,
): Promise<OperationResult> {
  if (!isTauriRuntime()) {
    throw new Error('Create operation is only available in Tauri runtime')
  }
  return invoke<OperationResult>('create_resource', { panelCtx, service, payload })
}

export async function updateResource(
  panelCtx: PanelContext,
  service: string,
  resourceId: string,
  payload: Record<string, unknown>,
): Promise<OperationResult> {
  if (!isTauriRuntime()) {
    throw new Error('Update operation is only available in Tauri runtime')
  }
  return invoke<OperationResult>('update_resource', {
    panelCtx,
    service,
    resourceId,
    payload,
  })
}

export async function deleteResource(
  panelCtx: PanelContext,
  service: string,
  resourceId: string,
): Promise<OperationResult> {
  if (!isTauriRuntime()) {
    throw new Error('Delete operation is only available in Tauri runtime')
  }
  return invoke<OperationResult>('delete_resource', {
    panelCtx,
    service,
    resourceId,
  })
}

export async function prepareCopy(
  sourceCtx: PanelContext,
  service: string,
  resourceId: string,
): Promise<CopyPlan> {
  if (!isTauriRuntime()) {
    throw new Error('Copy is only available in Tauri runtime')
  }
  return invoke<CopyPlan>('prepare_copy', {
    sourceCtx,
    service,
    resourceId,
  })
}

export async function executeCopy(
  targetCtx: PanelContext,
  input: ExecuteCopyInput,
): Promise<OperationResult> {
  if (!isTauriRuntime()) {
    throw new Error('Copy is only available in Tauri runtime')
  }
  return invoke<OperationResult>('execute_copy', {
    targetCtx,
    input,
  })
}

export async function closeWindow(): Promise<void> {
  if (!isTauriRuntime()) {
    return
  }
  const { getCurrentWindow } = await import('@tauri-apps/api/window')
  await getCurrentWindow().close()
}
