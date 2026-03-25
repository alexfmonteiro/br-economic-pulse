# -----------------------------------------------------------------------------
# Cloudflare
# -----------------------------------------------------------------------------

variable "cloudflare_api_token" {
  description = "Cloudflare API token with R2 read/write permissions"
  type        = string
  sensitive   = true
}

variable "cloudflare_account_id" {
  description = "Cloudflare account ID"
  type        = string
  sensitive   = true
}

variable "r2_bucket_name" {
  description = "Name of the R2 bucket for pipeline data"
  type        = string
  default     = "br-economic-pulse-data"
}

# -----------------------------------------------------------------------------
# Neon
# -----------------------------------------------------------------------------

variable "neon_api_key" {
  description = "Neon API key from console.neon.tech"
  type        = string
  sensitive   = true
}

variable "neon_project_name" {
  description = "Neon project name"
  type        = string
  default     = "br_economic_pulse"
}

variable "neon_database_name" {
  description = "Neon database name"
  type        = string
  default     = "neondb"
}

variable "neon_region" {
  description = "Neon region (aws-us-east-1, aws-us-west-2, aws-eu-central-1)"
  type        = string
  default     = "aws-us-east-1"
}

# -----------------------------------------------------------------------------
# Upstash
# -----------------------------------------------------------------------------

variable "upstash_email" {
  description = "Upstash account email"
  type        = string
  sensitive   = true
}

variable "upstash_api_key" {
  description = "Upstash API key from console.upstash.com"
  type        = string
  sensitive   = true
}

variable "upstash_region" {
  description = "Upstash Redis region (us-east-1, eu-west-1, ap-southeast-1)"
  type        = string
  default     = "us-east-1"
}

# -----------------------------------------------------------------------------
# Sentry
# -----------------------------------------------------------------------------

variable "sentry_token" {
  description = "Sentry internal integration auth token"
  type        = string
  sensitive   = true
}

variable "sentry_organization" {
  description = "Sentry organization slug"
  type        = string
}

variable "sentry_team" {
  description = "Sentry team slug (Settings → Teams)"
  type        = string
}

# -----------------------------------------------------------------------------
# General
# -----------------------------------------------------------------------------

variable "environment" {
  description = "Environment name (production, staging)"
  type        = string
  default     = "production"

  validation {
    condition     = contains(["production", "staging"], var.environment)
    error_message = "Environment must be 'production' or 'staging'."
  }
}
