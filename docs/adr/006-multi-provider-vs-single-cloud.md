# ADR-006: Multi-Provider Free Tier over Single Cloud

## Status
Accepted

## Context
The project needs hosting for the API (Railway), frontend (Vercel), object
storage (Cloudflare R2), relational database (Neon Postgres), and caching
(Upstash Redis). We could consolidate onto a single cloud provider (AWS or
GCP) or use best-of-breed free tiers from multiple providers.

Single cloud (AWS) estimated cost: ~$40-65/month. Single cloud (GCP):
~$30-50/month. Multi-provider free tiers: under $15/month.

## Decision
Use multi-provider free tiers for v1. Each provider was chosen for its
generous free tier. Infrastructure is provisioned manually via provider UIs for
v1 and codified in Terraform from v1.5 onward (see ADR-007), making future
consolidation a configuration change rather than a rewrite.

## Consequences
- Total running cost under $15/month — sustainable for a portfolio project
- Each service is best-in-class for its function (Railway for containers,
  R2 for object storage with zero egress, Neon for serverless Postgres)
- More providers to manage — more dashboards, more credentials, more
  potential points of failure
- Terraform abstraction means migration to AWS/GCP is a provider swap,
  not an architecture change
- Revisit when monthly revenue exceeds the cost delta (~$30-50/month)
