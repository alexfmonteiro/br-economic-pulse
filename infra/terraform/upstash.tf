# -----------------------------------------------------------------------------
# Upstash — Serverless Redis for rate limiting and caching
# -----------------------------------------------------------------------------

resource "upstash_redis_database" "cache" {
  database_name  = "br_economic_pulse"
  region         = "global"
  primary_region = "us-east-1"
  tls            = true
  eviction       = false # Matches current setting
}
