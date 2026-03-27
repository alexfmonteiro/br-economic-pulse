# -----------------------------------------------------------------------------
# Veredas — Terraform Configuration (Phase 1: v1.5)
#
# Manages: Cloudflare R2, Neon Postgres, Upstash Redis, Sentry
# See ADR-007 for phased adoption strategy.
# Railway and Vercel are managed via UI until Phase 2 (v2).
# -----------------------------------------------------------------------------

provider "cloudflare" {
  api_token = var.cloudflare_api_token
}

provider "neon" {
  api_key = var.neon_api_key
}

provider "upstash" {
  email   = var.upstash_email
  api_key = var.upstash_api_key
}

provider "sentry" {
  token = var.sentry_token
}
