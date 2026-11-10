# Stage-Driven Terraform Deployment Script for LakeForge (PowerShell)
# Usage: .\deploy.ps1 -Stage dev -Action plan
param (
    [ValidateSet("dev", "test", "staging", "prod")]
    [string]$Stage = "dev",

    [ValidateSet("plan", "apply", "destroy")]
    [string]$Action = "plan"
)

$ErrorActionPreference = "Stop"

$tfvars = "environments/$Stage.tfvars"
if (-not (Test-Path $tfvars)) {
    Write-Error "Variables file '$tfvars' not found."
    exit 1
}

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host " LakeForge Terraform Deployment: [$($Stage.ToUpper())] - Action: $($Action.ToUpper())" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

terraform init -upgrade
terraform $Action -var-file="$tfvars"
