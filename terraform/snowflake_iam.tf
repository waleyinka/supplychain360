/**
 * ACCESS CONTROL CONFIGURATION
 * To manage Snowflake roles and the User that Airflow/dbt would use.
 */

# Create a functional role for the automation pipeline
resource "snowflake_account_role" "pipeline_role" {
  name    = "SUPPLYCHAIN_PIPELINE_ROLE"
  comment = "Role used by Airflow and dbt for automated tasks"
}

# Generate a new RSA Key pair for secure, passwordless authentication
resource "tls_private_key" "pipeline_key" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

# Create the service user for the pipeline
resource "snowflake_user" "pipeline_user" {
  name              = "PIPELINE_SVC_USER"
  default_role      = snowflake_account_role.pipeline_role.name
  default_warehouse = snowflake_warehouse.main.name
  # Strip headers from PEM for Snowflake public key format
  rsa_public_key    = substr(tls_private_key.pipeline_key.public_key_pem, 27, 398)
}

# Grant usage privileges (Database & Warehouse)
resource "snowflake_grant_privileges_to_account_role" "warehouse_access" {
  privileges        = ["USAGE", "OPERATE"]
  account_role_name = snowflake_account_role.pipeline_role.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.main.name
  }
}