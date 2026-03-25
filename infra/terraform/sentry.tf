# -----------------------------------------------------------------------------
# Sentry — Error tracking and performance monitoring
# -----------------------------------------------------------------------------

resource "sentry_project" "api" {
  organization = var.sentry_organization
  teams        = [var.sentry_team]
  name         = "br-economic-pulse-api"
  slug         = "br-economic-pulse-api"
  platform     = "python-fastapi"
}

resource "sentry_project" "frontend" {
  organization = var.sentry_organization
  teams        = [var.sentry_team]
  name         = "br-economic-pulse-frontend"
  slug         = "br-economic-pulse-frontend"
  platform     = "javascript-react"
}

resource "sentry_key" "api" {
  organization = var.sentry_organization
  project      = sentry_project.api.slug
  name         = "Default"
}

resource "sentry_key" "frontend" {
  organization = var.sentry_organization
  project      = sentry_project.frontend.slug
  name         = "Default"
}
