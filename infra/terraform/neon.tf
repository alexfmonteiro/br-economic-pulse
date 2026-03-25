# -----------------------------------------------------------------------------
# Neon — Serverless Postgres for metadata, insights, and query logs
# -----------------------------------------------------------------------------

resource "neon_project" "main" {
  name                      = var.neon_project_name
  region_id                 = var.neon_region
  history_retention_seconds = 21600 # 6 hours (free tier)

  default_endpoint_settings {
    autoscaling_limit_min_cu = 0.25
    autoscaling_limit_max_cu = 2
    suspend_timeout_seconds  = 0
  }
}

resource "neon_database" "app" {
  project_id = neon_project.main.id
  branch_id  = neon_project.main.default_branch_id
  name       = var.neon_database_name
  owner_name = "neondb_owner"
}
