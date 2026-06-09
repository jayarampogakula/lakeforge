# Terraform Configuration for LakeForge Databricks Infrastructure

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# Create Unity Catalog Catalogs
resource "databricks_catalog" "lakeforge" {
  name           = "lakeforge_${var.environment}"
  comment        = "Lakehouse catalog for LakeForge in ${var.environment} environment"
  properties     = {
    purpose = "etl"
  }
}

# Create Bronze, Silver, Gold Schemas
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.lakeforge.id
  name         = "bronze"
  comment      = "Raw raw-zone schema"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.lakeforge.id
  name         = "silver"
  comment      = "Cleansed and conformed schema"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.lakeforge.id
  name         = "gold"
  comment      = "Aggregated business metrics marts"
}

# Dev Cluster
resource "databricks_cluster" "shared_autoscaling" {
  cluster_name            = "lakeforge-compute-${var.environment}"
  spark_version           = "14.3.x-scala2.12"
  node_type_id            = "i3.xlarge"
  autotermination_minutes = 20
  
  autoscale {
    min_workers = 1
    max_workers = 4
  }

  spark_conf = {
    "spark.databricks.delta.preview.enabled" = "true"
  }
}

# Databricks Job to run the Medallion Pipeline
resource "databricks_job" "medallion_pipeline" {
  name = "lakeforge-medallion-pipeline-${var.environment}"

  job_cluster {
    job_cluster_key = "lakeforge_job_compute"
    new_cluster {
      spark_version = "14.3.x-scala2.12"
      node_type_id  = "i3.xlarge"
      num_workers   = 2
    }
  }

  task {
    task_key = "bronze_customers_ingestion"
    job_cluster_key = "lakeforge_job_compute"

    notebook_task {
      notebook_path = "/lakeforge/pipelines/Bronze_Generic_Pipeline"
      base_parameters = {
        config_path     = "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/config/pipeline_config.json"
        source_system   = "erp"
        pipeline_name   = "customers"
        load_type       = "full"
        environment     = var.environment
      }
    }
  }

  task {
    task_key = "bronze_transactions_ingestion"
    job_cluster_key = "lakeforge_job_compute"

    notebook_task {
      notebook_path = "/lakeforge/pipelines/Bronze_Generic_Pipeline"
      base_parameters = {
        config_path     = "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/config/pipeline_config.json"
        source_system   = "erp"
        pipeline_name   = "transactions"
        load_type       = "full"
        environment     = var.environment
      }
    }
  }

  task {
    task_key = "silver_customers_transform"
    depends_on {
      task_key = "bronze_customers_ingestion"
    }
    job_cluster_key = "lakeforge_job_compute"

    notebook_task {
      notebook_path = "/lakeforge/pipelines/Silver_Generic_Pipeline"
      base_parameters = {
        config_path     = "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/config/pipeline_config.json"
        source_table    = "customers"
        target_table    = "customers_clean"
        environment     = var.environment
        use_scd_type2   = "true"
      }
    }
  }

  task {
    task_key = "silver_transactions_transform"
    depends_on {
      task_key = "bronze_transactions_ingestion"
    }
    job_cluster_key = "lakeforge_job_compute"

    notebook_task {
      notebook_path = "/lakeforge/pipelines/Silver_Generic_Pipeline"
      base_parameters = {
        config_path     = "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/config/pipeline_config.json"
        source_table    = "transactions"
        target_table    = "transactions_clean"
        environment     = var.environment
        use_scd_type2   = "false"
      }
    }
  }

  task {
    task_key = "gold_customer_metrics"
    depends_on {
      task_key = "silver_customers_transform"
    }
    depends_on {
      task_key = "silver_transactions_transform"
    }
    job_cluster_key = "lakeforge_job_compute"

    notebook_task {
      notebook_path = "/lakeforge/pipelines/GOLD_Aggregation_Pipeline"
      base_parameters = {
        config_path     = "/Workspace/Users/jayarampogakula@gmail.com/lakeforge/config/pipeline_config.json"
        business_domain = "sales"
        target_table    = "customer_summary"
        environment     = var.environment
      }
    }
  }
}
