variable "databricks_host" {
  type        = string
  description = "The URL of the Databricks workspace (e.g. https://adb-xxx.azuredatabricks.net)"
}

variable "databricks_token" {
  type        = string
  description = "The Databricks Personal Access Token (PAT)"
  sensitive   = true
}

variable "environment" {
  type        = string
  description = "The target environment: dev, staging, or prod"
  default     = "dev"
}
