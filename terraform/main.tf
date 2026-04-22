/**
 * MAIN INFRASTRUCTURE CONFIGURATION
 * This file manages the core resources for SupplyChain360, including 
 * the Snowflake data warehouse structure and S3 raw storage.
 */

# --- DATABASE & SCHEMA SECTION ---

# Primary database for the SupplyChain platform
resource "snowflake_database" "main" {
  name    = var.database_name
  comment = "Centralized repository for SupplyChain360 operational data" 
}

# Bronze Layer: Stores data exactly as it arrived from S3
resource "snowflake_schema" "raw" {
  name     = "RAW"
  database = snowflake_database.main.name
}

# Gold Layer: Final modeled data for analytics and reporting
resource "snowflake_schema" "analytics" {
  name     = "ANALYTICS"
  database = snowflake_database.main.name
}

# --- COMPUTE SECTION ---

# Dedicated warehouse for ingestion and dbt transformations
resource "snowflake_warehouse" "main" {
  name           = "SUPPLYCHAIN_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60 # Saves cost by suspending after 1 minute of inactivity
  auto_resume    = true
}

# --- STORAGE SECTION ---

# S3 Bucket for raw data ingestion (Parquet format)
resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.project_name}-raw-${var.environment}"
  tags   = local.common_tags
}

# Security: Block all public access to the raw data
resource "aws_s3_bucket_public_access_block" "raw_data_protection" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}