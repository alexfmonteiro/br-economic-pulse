# ADR-007: Infrastructure as Code with Terraform

## Status
Accepted

## Date
2026-03-23

## Context
The project uses six providers (Railway, Vercel, Cloudflare R2, Neon, Upstash,
Sentry) chosen for their free tiers (ADR-006). During v1, all infrastructure
was provisioned manually through each provider's UI or dashboard. This was
deliberate — the priority was shipping the product, not codifying
infrastructure. However, manual provisioning creates several risks as the
project matures:

- **Drift**: UI changes are not tracked. There is no audit trail of who changed
  what or when. A misconfigured CORS rule or deleted volume has no rollback.
- **Bus factor**: Only the person who clicked through the UIs knows the exact
  configuration. Onboarding a second contributor requires a walkthrough of six
  dashboards.
- **Reproducibility**: Standing up a staging environment or replicating the
  project for another domain (see ROADMAP §11, Dark Factory) requires repeating
  the entire manual setup from the Execution Playbook Phase 0.
- **Migration friction**: ADR-006 anticipates consolidating to GCP or AWS when
  revenue justifies it. Without IaC, migration is a manual re-provisioning
  exercise rather than a provider swap.

The SPEC references Terraform throughout but contains a contradiction: section 4
says "managed in Terraform from day one" while the IaC section says "Terraform
for v1.5+. Use Railway/Vercel UI for v1." The Execution Playbook confirms the
latter — all Phase 0 setup is manual.

### Provider landscape

All six providers have Terraform providers available:

| Provider | Terraform Provider | Maintainer | Maturity |
|---|---|---|---|
| Cloudflare R2 | `cloudflare/cloudflare` | Official (Cloudflare) | Stable |
| Neon Postgres | `kislerdm/neon` | Community | Production-ready, caution on upgrades |
| Upstash Redis | `upstash/upstash` | Official (Upstash) | Stable |
| Sentry | `jianyuan/sentry` | Community (Sentry-sponsored) | Stable |
| Vercel | `vercel/vercel` | Official (Vercel) | Stable |
| Railway | `terraform-community-providers/railway` | Community | Production-ready |

## Decision
Adopt Terraform in two phases, aligned with the project roadmap:

### Phase 1 — v1.5 (post-launch hardening)

Codify the four providers with stable, low-risk Terraform providers:

1. **Cloudflare R2** — bucket with prefix-based organization
   (bronze/silver/gold/quality)
2. **Neon** — Postgres project and database
3. **Upstash** — Redis instance
4. **Sentry** — API and frontend projects with client keys

These resources are stateful but rarely change after initial creation. Importing
existing resources into Terraform state is safe and non-disruptive.

**State backend**: Local (`terraform.tfstate`) for v1.5. A single developer
does not need remote state locking. Migrate to an S3-compatible remote backend
(Cloudflare R2 or AWS S3) when the team grows beyond one contributor.

### Phase 2 — v2 (when deploying staging or second contributor joins)

Add the two deployment-coupled providers:

5. **Vercel** — project, environment variables, domain
6. **Railway** — service, persistent volume, environment variables

These are deferred because:
- Their Terraform providers manage deployment lifecycle, not just resource
  provisioning. Misconfiguration can cause downtime.
- Railway's community provider requires more testing against the project's
  specific setup (persistent volumes, deploy webhooks).
- The benefit of codifying deployment config is highest when standing up a
  second environment (staging) or onboarding a contributor.

### What Terraform does NOT manage

- **GitHub Actions workflows** — already version-controlled as YAML in `.github/`
- **GitHub repository settings** — managed via GitHub UI (branch protection,
  secrets). Codify with the `integrations/github` provider only if managing
  multiple repos.
- **DNS/domain routing** — managed via Vercel/Cloudflare dashboards until
  custom domain setup is needed.
- **Anthropic API keys** — created manually, rotated manually. No Terraform
  provider exists.

## Consequences

### Positive
- All infrastructure configuration is version-controlled and reviewable
- Standing up a new environment (staging, dark factory clone) is
  `terraform apply` rather than a 7-step manual process
- Future cloud consolidation (ADR-006) is a provider swap in Terraform, not
  a manual re-provisioning exercise
- Import of existing v1 resources is non-destructive — Terraform adopts
  management of resources that already exist
- The phased approach avoids blocking v1 launch on IaC completeness

### Negative
- Adds Terraform as a required tool for infrastructure changes (minor — single
  developer for now)
- Community providers (Neon, Railway) may lag behind provider API changes
- State file must be protected — contains sensitive outputs (database URLs,
  API keys). Local state is gitignored; remote state (v2+) requires encryption
- Importing existing resources requires careful `terraform import` commands
  to avoid accidental recreation

### Risks
- **Neon provider stability**: The community provider is not officially
  maintained by Neon. Pin the provider version and avoid `terraform init
  -upgrade` in CI to prevent unintended resource replacements.
- **Railway provider gaps**: The community provider may not support all Railway
  features (e.g., volume resize, custom domains). Fall back to UI for
  unsupported operations.
