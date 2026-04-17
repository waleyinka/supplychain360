locals {
  common_tags = {
    Environment = var.environment
    Team        = "DEC Launchpad"
    Project     = var.project_name
  }
}