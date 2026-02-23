# Feature Flag Template

## Identity
- Name:
- Owner:
- Type (`release|experiment|ops_kill_switch`):
- Risk level (`low|medium|high`):

## Timing
- Created at:
- Expires at: (default: +30 days)

## Scope
- Environments:
- Affected components:
- User segment / traffic scope:

## Defaults
- Default value: `false` (recommended)
- Rollout plan (default): `5% -> 25% -> 50% -> 100%` every 24h if stable
- Rollback conditions:
  - error rate > 20% baseline
  - p95 latency > 30% baseline
  - critical incident

## Observability
- Metrics to monitor:
- Log fields:
- Dashboard link:
- Alert link:

## Checklist
- [ ] OFF path implemented and tested
- [ ] ON path implemented and tested
- [ ] Owner assigned
- [ ] Expiration date set
- [ ] Rollout plan approved
- [ ] Rollback plan documented
- [ ] Cleanup task created

