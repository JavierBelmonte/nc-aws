# AGENTS

<!--
## Feature Flags Rule (Commented: enable when you want)

- Rule file: `docs/engineering-rules/feature-flags.md`
- Template file: `docs/templates/feature-flag-template.md`
- Inventory file: `docs/ops/feature-flags-inventory.md`

Default behavior for all questions:
- If user does not provide values, apply recommended defaults automatically.

Required defaults:
- expiry: 30 days
- rollout: 5% -> 25% -> 50% -> 100% (every 24h if metrics are stable)
- rollback trigger:
  - error rate > 20% over baseline, or
  - p95 latency > 30% over baseline, or
  - critical incident appears
-->

