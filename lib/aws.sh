#!/usr/bin/env bash

set -u

AWS_DEFAULT_REGIONS=(
  us-east-1
  us-east-2
  us-west-1
  us-west-2
  eu-west-1
  eu-central-1
  sa-east-1
)

aws_profile_list() {
  aws configure list-profiles 2>/dev/null | awk 'NF > 0'
}

lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

aws_resolve_profile_case() {
  local desired="$1"
  local desired_lc
  desired_lc="$(lower "$desired")"
  local p
  while IFS= read -r p; do
    if [[ "$(lower "$p")" == "$desired_lc" ]]; then
      printf '%s\n' "$p"
      return 0
    fi
  done < <(aws_profile_list)

  printf '%s\n' "$desired"
}

aws_profile_region() {
  local profile="$1"
  local region
  region="$(aws configure get region --profile "$profile" 2>/dev/null || true)"
  if [[ -n "$region" ]]; then
    printf '%s\n' "$region"
  else
    printf '%s\n' "us-east-1"
  fi
}

aws_regions_for_profile() {
  local profile="$1"
  local preferred
  preferred="$(aws_profile_region "$profile")"

  printf '%s\n' "$preferred"
  local r
  for r in "${AWS_DEFAULT_REGIONS[@]}"; do
    if [[ "$r" != "$preferred" ]]; then
      printf '%s\n' "$r"
    fi
  done
}

aws_list_resources() {
  local profile="$1"
  local region="$2"
  local service="$3"

  case "$service" in
    s3)
      aws --profile "$profile" --region "$region" s3api list-buckets \
        --query 'Buckets[].Name' --output text 2>&1 | tr '\t' '\n'
      ;;
    lambda)
      aws --profile "$profile" --region "$region" lambda list-functions \
        --query 'Functions[].FunctionName' --output text 2>&1 | tr '\t' '\n'
      ;;
    vpc)
      aws --profile "$profile" --region "$region" ec2 describe-vpcs \
        --query 'Vpcs[].VpcId' --output text 2>&1 | tr '\t' '\n'
      ;;
    apigateway)
      aws --profile "$profile" --region "$region" apigatewayv2 get-apis \
        --query 'Items[].ApiId' --output text 2>&1 | tr '\t' '\n'
      ;;
    *)
      printf 'Unsupported service: %s\n' "$service"
      return 2
      ;;
  esac
}

aws_get_resource_detail() {
  local profile="$1"
  local region="$2"
  local service="$3"
  local id="$4"

  case "$service" in
    s3)
      aws --profile "$profile" --region "$region" s3api get-bucket-location \
        --bucket "$id" --output json
      ;;
    lambda)
      aws --profile "$profile" --region "$region" lambda get-function \
        --function-name "$id" --output json
      ;;
    vpc)
      aws --profile "$profile" --region "$region" ec2 describe-vpcs \
        --vpc-ids "$id" --output json
      ;;
    apigateway)
      aws --profile "$profile" --region "$region" apigatewayv2 get-api \
        --api-id "$id" --output json
      ;;
    *)
      printf 'Unsupported service: %s\n' "$service"
      return 2
      ;;
  esac
}

aws_create_resource() {
  local profile="$1"
  local region="$2"
  local service="$3"
  local name="$4"

  case "$service" in
    s3)
      aws --profile "$profile" --region "$region" s3api create-bucket \
        --bucket "$name" --output json
      ;;
    *)
      printf 'Create not implemented for %s\n' "$service"
      return 2
      ;;
  esac
}

aws_delete_resource() {
  local profile="$1"
  local region="$2"
  local service="$3"
  local id="$4"

  case "$service" in
    s3)
      aws --profile "$profile" --region "$region" s3api delete-bucket \
        --bucket "$id" --output json
      ;;
    lambda)
      aws --profile "$profile" --region "$region" lambda delete-function \
        --function-name "$id" --output json
      ;;
    *)
      printf 'Delete not implemented for %s\n' "$service"
      return 2
      ;;
  esac
}
