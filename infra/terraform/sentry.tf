# -----------------------------------------------------------------------------
# Sentry — Error tracking and performance monitoring
# -----------------------------------------------------------------------------

resource "sentry_project" "api" {
  organization = var.sentry_organization
  teams        = [var.sentry_team]
  name         = "veredas-api"
  slug         = "veredas-api"
  platform     = "python-fastapi"
}

resource "sentry_project" "frontend" {
  organization = var.sentry_organization
  teams        = [var.sentry_team]
  name         = "veredas-frontend"
  slug         = "veredas-frontend"
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
