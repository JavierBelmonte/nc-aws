# Feature Flags Inventory

Track all active flags and cleanup status.

| name | owner | type | created_at | expires_at | envs | default | status | cleanup_ticket |
|---|---|---|---|---|---|---|---|---|
| example.copy.lambda.auto_role | platform | release | 2026-02-23 | 2026-03-25 | dev,stage,prod | false | active | TKT-000 |

## Rules
- No active flag without `expires_at`.
- Expired flags must be removed in the next iteration.
- Keep this inventory updated on each rollout/change.

