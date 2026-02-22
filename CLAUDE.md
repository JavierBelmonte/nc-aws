# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**nc-aws** is a Norton Commander-style dual-panel TUI for managing AWS resources across profiles and regions. Written in Go using the BubbleTea framework.

## Build & Run

```bash
go run ./cmd/nc-aws        # Run directly
./bin/nc-aws               # Run via wrapper script
go build ./...             # Build binary
```

No Makefile. Standard Go tooling applies (`go vet`, `go test ./...`).

## Architecture

The entire application lives in a single file: `cmd/nc-aws/main.go`. This is intentional.

### BubbleTea Elm Pattern

The app follows BubbleTea's Model-Update-View pattern:
- `model` struct — all application state
- `Update(msg tea.Msg)` — handles all events (keyboard input, window resize, async AWS responses)
- `View()` — renders current screen state
- `Init()` — starts initial data loading

Async AWS operations are launched as goroutines and communicate results back via custom message types (e.g., `loadedPanelMsg`, `opMsg`, `copyProgressMsg`, `lambdaCopyProgressMsg`). These are picked up by `Update()` on the next tick.

### Panel State & Navigation Levels

Two independent `panelState` structs (`model.left`, `model.right`) each track their own navigation depth:

```
levelProfiles → levelRegions → levelServices → levelResources
```

Profile entries display account ID from a cached STS call: `profilename (123456789012)`.

Each panel has its own `Profile`, `Region`, `Service`, cursor position, marked items, and loading/error state. Tab switches the active panel.

### Application Modes

`model.mode` controls what keyboard input does:

```
normal | input | view | confirm | conflict | lambdaConflict | grantRegistry
```

### Supported AWS Services & Operations

- **S3**: browse buckets/prefixes/objects, copy/move with per-object progress, delete object/bucket-full, create bucket, cross-account grant. Handles keys with spaces/accents in CopySource. Conflict prompt: overwrite all / skip existing / cancel.
- **Lambda**: list functions (with scroll), copy between accounts/regions with per-function progress, delete single or bulk. Conflict prompt: overwrite all / skip existing. On copy: creates/updates function (code + config). Handles `iam:PassRole` automatically (auto-grant, auto-create role in destination, fallback to alternate role if role cannot be assumed by Lambda).
- **VPC**: list EC2 VPCs.
- **API Gateway v2**: list APIs.

### Selection Behavior (space)

- Short press: toggle current item.
- Hold ~3s: select all selectable items in the panel.

### Grants Registry (`a` key)

`a` opens the Grants Registry — a list of all resources auto-created by the app (S3 bucket policy statements, IAM policies, IAM roles). Inside the registry, `u` revokes/deletes the selected entry, including cleanup of associated roles and policies. Outside the registry, `u` just instructs the user to open `a` first.

### Session & Registry Persistence

- `~/.nc-aws-session.json` — panel state saved on every navigation change via `persistContext()`, restored on startup.
- `~/.nc-aws-policy-registry.json` — all auto-created grants, IAM policies, and roles, used for cleanup/revocation.

### Cross-Account Operations

- **S3**: adds bucket policy statements granting access to the source principal.
- **Lambda**: auto-attaches IAM role policies and records `AutoGrantedRoles`/`DstPrincipalArn` in function tags. Auto-creates destination role if missing, with fallback if the role can't be assumed by Lambda.
- Everything created is recorded in the registry.

### AWS Client Pattern

No singleton clients — a fresh AWS config/client is created per operation using `config.LoadDefaultConfig()` with the relevant profile and region. Profile names are resolved case-insensitively via `resolveProfileCase()`. STS account IDs are cached in-memory per profile.

## Key Shortcuts

| Key | Action |
|-----|--------|
| `Tab` | Switch panel |
| `j`/`k`, arrows | Move cursor |
| `Enter` | Drill down |
| `h`, `Backspace` | Go up one level |
| `i` | Reset to profiles |
| `g` | Refresh |
| `Space` | Mark item / hold to select all |
| `c` | Copy |
| `m` | Move |
| `d` | Delete (single or bulk) |
| `n` | Create S3 bucket |
| `v` | View detail |
| `a` | Open Grants Registry |
| `u` | Revoke selected grant (inside registry) |
| `e` | Show last error |
| `l` | View log history |
| `q` / `Ctrl+C` | Quit |

## Key Data Types

```go
// Per-panel navigation state
type panelState struct { Level, Profile, Region, Service, S3Bucket, S3Prefix, Resources, Marked, Cursor, ... }

// Generic item in a panel list
type resourceItem struct { Label, Value, Kind string }
```

## Legacy Code (not used by Go app)

- `bin/nc-aws.bash_legacy` — original Bash implementation
- `ui_legacy_tauri/` — abandoned React + Tauri + Rust desktop UI
- `lib/aws.sh` — Bash AWS CLI helper library
