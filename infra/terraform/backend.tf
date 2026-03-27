# -----------------------------------------------------------------------------
# State backend — local for v1.5 (single developer)
#
# Migrate to remote backend when team grows beyond one contributor:
#
#   terraform {
#     backend "s3" {
#       bucket   = "veredas-tfstate"
#       key      = "terraform.tfstate"
#       region   = "auto"
#       endpoint = "https://<account_id>.r2.cloudflarestorage.com"
#       encrypt  = true
#
#       skip_credentials_validation = true
#       skip_metadata_api_check     = true
#       skip_region_validation      = true
#       skip_requesting_account_id  = true
#     }
#   }
#
# See ADR-007 for the migration strategy.
# -----------------------------------------------------------------------------

terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}
