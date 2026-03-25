# -----------------------------------------------------------------------------
# Outputs — All sensitive values marked accordingly
#
# Usage: terraform output -json | jq '.r2_bucket_name.value'
# These values map to the env vars in .env.example.
# -----------------------------------------------------------------------------

# Cloudflare R2
output "r2_bucket_name" {
  description = "R2 bucket name (R2_BUCKET_NAME)"
  value       = cloudflare_r2_bucket.data.name
}

# Neon Postgres
output "database_url" {
  description = "Neon Postgres connection string (DATABASE_URL)"
  value       = neon_project.main.connection_uri
  sensitive   = true
}

output "neon_project_id" {
  description = "Neon project ID for console access"
  value       = neon_project.main.id
}

# Upstash Redis
output "upstash_redis_url" {
  description = "Upstash Redis REST URL (UPSTASH_REDIS_URL)"
  value       = upstash_redis_database.cache.endpoint
  sensitive   = true
}

output "upstash_redis_token" {
  description = "Upstash Redis REST token (UPSTASH_REDIS_TOKEN)"
  value       = upstash_redis_database.cache.password
  sensitive   = true
}

# Sentry
output "sentry_dsn_api" {
  description = "Sentry DSN for API project (SENTRY_DSN_API)"
  value       = sentry_key.api.dsn["public"]
  sensitive   = true
}

output "sentry_dsn_frontend" {
  description = "Sentry DSN for frontend project (SENTRY_DSN_FRONTEND)"
  value       = sentry_key.frontend.dsn["public"]
  sensitive   = true
}
