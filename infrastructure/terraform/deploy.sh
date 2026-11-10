#!/usr/bin/env bash
# Stage-Driven Terraform Deployment Script for LakeForge
# Usage: ./deploy.sh [stage] [apply|plan|destroy]
set -euo pipefail

STAGE="${1:-dev}"
ACTION="${2:-plan}"

if [[ ! "$STAGE" =~ ^(dev|test|staging|prod)$ ]]; then
  echo "Error: Invalid stage '$STAGE'. Supported: dev, test, staging, prod"
  exit 1
fi

TFVARS_FILE="environments/${STAGE}.tfvars"
if [[ ! -f "$TFVARS_FILE" ]]; then
  echo "Error: Variables file '$TFVARS_FILE' not found"
  exit 1
fi

echo "======================================================================"
echo " LakeForge Terraform Deployment: [${STAGE^^}] - Action: ${ACTION^^}"
echo "======================================================================"

terraform init -upgrade
terraform "${ACTION}" -var-file="${TFVARS_FILE}"
