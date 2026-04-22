/**
 * PROVIDERS CONFIGURATION
 * Consolidates all required providers and the remote backend state.
 * Using a single state file ensures resource dependencies are tracked correctly.
 */

terraform {
  # Remote Backend: Stores state in S3
  backend "s3" {
    bucket  = "supplychain360-tfstate-dev"
    key     = "terraform/supplychain360/terraform.tfstate"
    region  = "eu-west-2"
    profile = "terraform"
    encrypt = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.87" # Modern version supporting latest resources
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

# --- AWS Provider ---
provider "aws" {
  region  = var.region
  profile = var.profile
}

# --- Snowflake Provider (Primary) ---
# Used for general DB and Warehouse creation
provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  role              = var.snowflake_role
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(pathexpand(var.snowflake_private_key_path))
}

# --- Snowflake Provider (UserAdmin Alias) ---
# Specific role required to manage users and roles safely
provider "snowflake" {
  alias             = "useradmin"
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  role              = "USERADMIN"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(pathexpand(var.snowflake_private_key_path))
}