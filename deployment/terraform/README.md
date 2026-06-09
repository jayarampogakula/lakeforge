# Infrastructure Provisioning with Terraform

This directory contains Terraform scripts to automate the deployment of Unity Catalog assets, compute resources, and ETL pipeline workflows on Databricks.

## Prerequisites
- Terraform v1.0+ installed
- Databricks Workspace host URL and Personal Access Token (PAT)

## Instructions

### 1. Initialize Terraform
```bash
terraform init
```

### 2. Configure Variables
Create a file named `terraform.tfvars` with your workspace details:
```hcl
databricks_host  = "https://adb-xxxx.azuredatabricks.net"
databricks_token = "dapi-your-personal-access-token"
environment      = "dev"
```

### 3. Deploy Assets
Generate the plan and apply:
```bash
terraform plan
terraform apply
```

This will automatically:
1. Create the `lakeforge_dev` catalog.
2. Create schemas: `bronze`, `silver`, and `gold`.
3. Provision an auto-scaling compute cluster.
4. Deploy the 5-task medallion workflow linking Bronze Ingestion -> Silver Transformation -> Gold Summary Mart.
