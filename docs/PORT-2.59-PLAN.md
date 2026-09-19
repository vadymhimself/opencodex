# Port: fork lane onto upstream 2.59.0

**Goal.** Run upstream 2.59.0 on both hosts with every fork change preserved, proven by gates
below, with no regression to the quota fixes and no unplanned gateway downtime.

Base: `upstream/main` = 134c92a01 (2.59.0). Fork lane: `origin/main` = 9ff58e185 (2.43.0 + 8
commits, of which f47552136 is 116 files / +16,985). Branch: `port/lane-onto-2.59`.

Direction is deliberate: start from 2.59 and re-apply the lane file by file, committing per
group, instead of one giant merge. A merge leaves an unresolvable 51-file index for days; this
way every step is committable, testable, and revertable.

## Definition of done

1. `bunx tsc --noEmit` clean.
2. Lane symbols present in `src/`: `canReplayAnthropicSource`,
   `isExactCanonicalAnthropicMessagesUrl`, `comboTargetAcceptsAnthropicSource`,
   `customWindowAppliesToModel`.
3. Per-domain suites green (full-suite run hangs on `responses-state`; run per domain with a
   150s per-file alarm, never as one job).
4. Fork tests ported and passing, notably `claude-messages-endpoint`, `combos`,
   `server-combo-failover-e2e`, `routing-analytics`, `usage-*`.
5. Warm-pair cache-miss rate on the staged host within 2pp of the 2.43 baseline (7.7-8.1%),
   measured by the group-by-(conversation, model, provider), pairs <300s, miss when
   `cacheReadInputTokens < 0.5 x (prev read + prev write)` test. Never compare read share
   across windows: it tracks conversation depth and account switches, not gateway behaviour.
6. Burn checks clean on the staged host: zero doomed sends (real send -> 429/502/529), zero
   `503` with no attempts.

## Safety rules

- 2.43 stays deployed until every gate passes. Rollback tarball: `bitkyc08-opencodex-2.43.0.tgz`,
  tags `gw-known-good-2.43.0` (f47552136) and `gw-deployed-2.43.0-quota` (origin/main).
- Install and restart in ONE command per host: a changed package tree under a running gateway
  makes it answer `restart_required` to everything.
- Stage on the Mac mini first, watch one hour, then the laptop. Never both at once.
- Account switches invalidate Anthropic prompt caches (cache is per key), so never change the
  active account during a measurement window.
- No `git push --force` to `main`. The port lands as a PR.

## Phases

- **P0 inventory** (done): 16 src files + 20 test files + 2 scripts carry lane deltas.
- **P1 one-to-one files.** Files upstream kept in place: `adapters/anthropic.ts`,
  `combos/{resolve,failover}.ts`, `oauth/anthropic-routing.ts`, `claude/outbound.ts`,
  `server/{relay,request-log,sse-frame-buffer}.ts`, `routing/{analytics,health}.ts`,
  `usage/{log,summary}.ts`, `images/loop.ts`, `web-search/loop.ts`, `router.ts`.
- **P2 re-homed files.** `server/responses/core.ts`: upstream cut it 7,220 -> 210 lines and split
  the body into ~20 modules (`core-combo.ts`, `adapter-dispatch.ts`, `request-prepare.ts`, ...).
  Our 116 hunks there include 81 inside `handleResponsesInner`; each must be re-applied in the
  module that now owns that code. Then `server/claude-messages.ts` (21 upstream commits) and
  `server/responses/combo-stream-preflight.ts`.
- **P3 tests.** Port fork tests, adapting to moved modules.
- **P4 gates.** Items 1-4 above, plus `scripts/claude-cache-count-parity-eval.ts`.
- **P5 staged deploy.** Mini, one hour of gates 5-6, then laptop.

## Log

- 2026-09-19: branch created on 2.59.0; inventory complete.
