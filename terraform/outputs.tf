output "pipeline_user_name" {
  value = snowflake_user.pipeline_user.name
}

output "pipeline_role_name" {
  value = snowflake_account_role.pipeline_role.name
}

output "warehouse_name" {
  value = snowflake_warehouse.main.name
}

output "database_name" {
  value = snowflake_database.main.name
}

output "raw_schema_name" {
  value = snowflake_schema.raw.name
}

output "analytics_schema_name" {
  value = snowflake_schema.analytics.name
}

output "pipeline_user_public_key" {
  value = tls_private_key.pipeline_user_key.public_key_pem
}