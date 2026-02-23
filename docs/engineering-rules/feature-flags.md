# Rule: Feature Flags for Risky Changes

## Objective
Use feature flags to reduce release risk, enable gradual rollout, and allow rollback without redeploy.

## Scope
Use this rule for:
- high-impact user behavior changes,
- risky refactors,
- production-path changes with operational risk.

Do not use for:
- trivial fixes,
- non-runtime/internal-only changes.

## Prerequisites
- A flag mechanism exists (service or local config provider).
- Observability is available (logs + metrics).
- Each flag has an owner.
- Deploy process supports per-environment activation.

## Required Metadata
Every flag must define:
- `name`
- `owner`
- `created_at`
- `expires_at`
- `environments`
- `type` (`release`, `experiment`, `ops_kill_switch`)
- `risk_level`
- `default_value`

## Defaults (Auto-apply if unanswered)
- `expires_at`: 30 days from creation.
- Rollout plan: `5% -> 25% -> 50% -> 100%`, one step every 24h if stable.
- Rollback trigger:
  - error rate > 20% over baseline, or
  - p95 latency > 30% over baseline, or
  - any critical incident.

## Lifecycle
1. Create flag OFF by default.
2. Deploy code supporting both ON/OFF.
3. Validate in non-prod.
4. Progressive rollout in prod.
5. Monitor metrics and logs.
6. Rollback by disabling flag if trigger condition is met.
7. Remove old path and delete flag before/at `expires_at`.

## Anti-patterns
- Flags without expiration.
- Permanent release flags.
- Multiple overlapping flags in one critical path.
- Missing monitoring for flag impact.

