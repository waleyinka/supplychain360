# =================== AWS S3 ================= 
variable "region" {
    description = "AWS Region for the infrastructure"
    type        = string
    default     = "eu-west-2"
}

variable "profile" {
    description = "AWS CLI profile"
    type        = string
    default     = "terraform"
}

variable "project_name" {
    description = "Name of the project"
    type        = string
    default     = "supplychain360"
}

variable "environment" {
    description = "Environment name"
    type        = string
    default     = "dev"
}



# =================== Snowflake ================= 
variable "snowflake_account_name" {
  type = string
  default = "ii89680"
}

variable "snowflake_organization_name" {
  type = string
  default = "vmrdrdk"
}

variable "snowflake_user" {
  type = string
  default = "TERRAFORM_SVC"
}

variable "snowflake_role" {
  type    = string
  default = "SYSADMIN"
}

variable "snowflake_private_key_path" {
  type = string
  default = "~/.ssh/snowflake_tf_snow_key.p8"
}

variable "database_name" {
  type    = string
  default = "SUPPLYCHAIN360"
}

variable "raw_schema_name" {
  type    = string
  default = "RAW"
}

variable "analytics_schema_name" {
  type    = string
  default = "ANALYTICS"
}

variable "warehouse_name" {
  type    = string
  default = "SUPPLYCHAIN360_WH"
}

variable "warehouse_size" {
  type    = string
  default = "SMALL"
}