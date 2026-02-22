export type ProfileSummary = {
  name: string
  default_region?: string | null
}

export type PanelContext = {
  profile_name: string
  region: string
}

export type ServiceInfo = {
  id: string
  label: string
}

export type ResourceRef = {
  id: string
  name: string
  service: string
  region: string
  arn?: string | null
  status?: string | null
}

export type ResourcePage = {
  items: ResourceRef[]
  next_cursor?: string | null
}

export type ResourceDetail = {
  resource: ResourceRef
  detail: Record<string, unknown>
}

export type OperationResult = {
  ok: boolean
  message: string
  changed_resources: string[]
  warnings: string[]
}

export type CopyPlan = {
  service: string
  source_id: string
  suggested_target_name: string
  normalized_spec: Record<string, unknown>
}

export type ExecuteCopyInput = {
  copy_plan: CopyPlan
  target_name?: string | null
  overwrite: boolean
}
