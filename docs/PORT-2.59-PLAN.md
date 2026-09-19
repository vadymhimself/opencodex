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

## Deliberate behaviour deviations

Recorded so the port's shape is auditable; each is a case where upstream's version was taken
over the fork's and the behaviour is NOT identical.

1. **Reasoning envelope text is no longer replayed as thinking text** (`src/claude/outbound.ts`).
   The fork's `emitReasoningEnvelope` exposed `envelope.txt` back to the caller as thinking
   content. 2.59 decodes envelopes budget-aware (`decodeReasoningEnvelope`) and deliberately does
   not surface hidden envelope text. Upstream taken; the fork helper is dropped. Affects what a
   caller sees in reasoning blocks, not routing, quota or caching.
2. **`onRetry` replaced by the send-budget API** (`src/adapters/base.ts`): upstream's
   `sendBudget` / `onPhysicalSend` / `onRecoveryWithheld` counts every physical send including an
   adapter's inner retries, which the fork hook could not see.
3. **Lenient usage parsing dropped** (`src/adapters/anthropic.ts`): invalid upstream usage is now
   unreported rather than counted as zero.
4. **The 60s Anthropic account cooldown cap is gone**: 2.59 treats a server `Retry-After` as
   authoritative and caps local fallbacks at ten minutes. The fork's exhaustion branch is kept, so
   a proven-empty window still takes the ten-minute ceiling instead of the 60s default.
5. **Bare `error` events are no longer classified in `captureTerminalHttpStatus`**
   (`src/server/request-log.ts`). The fork classified them from `json.error`, pre-setting
   `terminalHttpStatus = 502`; 2.59 decides a bare error's status downstream via
   `bareErrorStatus`, and the fork branch shadowed it, making every bare error look like a
   transport failure. Upstream taken. Two fork tests that pinned the old policy are dropped.
6. **`max_output_tokens` is stripped again on non-canonical forward gateways**
   (`src/adapters/openai-responses.ts`): upstream's `stripCanonicalForwardSamplingParams`
   supersedes the fork's canonical-only stripping. Affects forward-mode gateways only.

## Fork behaviour kept over upstream

The reverse of the list above: places where 2.59 changed behaviour and the fork's version was
retained on purpose. All bias toward "unknown" rather than a confident wrong number, which is the
correct direction for a cost and quota report.

1. **`computeEntryCost` fails closed when any attempt is unpriced** — a partially priced request
   reports no cost rather than an understated one.
2. **Unpriced dominates priced in request counts** (`src/usage/summary.ts`).
3. **`cacheTokensFromUsage` routes through `normalizeCostTokens`**, so contradictory cache
   telemetry stays unavailable instead of being averaged into a plausible-looking number.
4. **`usageAttributions` guards on `entry.attempts === undefined`**, not `!entry.attempts?.length`.
   A row whose attempts are all non-physical (locally answered, `sendCount: 0`) must not fall back
   to its stale parent usage: upstream's own rule — never emit the attempts AND the request total —
   holds strictly harder this way, since an all-hidden row emits neither.
5. **`onTransportDispatch`** is kept alongside upstream's `nativeControl` / `beforeContinuation`
   hooks, in the fork's positional slot because a reserve-dispatch test passes it positionally.
6. **The shared `mergeUsage`** from `empty-completion-guard.ts` is used in `terminal-guard.ts`
   instead of upstream's private duplicate: it carries `contextTotalTokens` and
   `anthropicServerToolUse`.

## Inherited upstream failures (baseline, 2026-09-19)

Gate 3 cannot mean "everything green": upstream 2.59.0 does not pass its own suite on this
platform. Verified by copying `upstream/main`'s unmodified test files into /tmp, repointing only
their import paths, and running them against this tree — both reproduce, and the combo list is
byte-identical to the pre-port baseline.

- `tests/server/server-combo-failover-e2e.test.ts`: 162 pass / **12 fail** upstream.
  Failing: model-scoped image 400 after 429; optional `reasoning.effort` 400 -> 200 (both stream
  modes); optional `user` 400 -> 200 (both stream modes); target that cannot hold input plus
  requested output; body-confirmed quota inside a 5xx cooling for its reset window; passthrough
  failed usage captured once; context overflow advancing while exhausted targets return sanitized
  status; model-lifecycle 410; monthly quota then Orca free-prompt cap; provider-specific
  prompt-too-long 400.
- `tests/web-search/web-search-timeout-contract.test.ts`: 6 pass / **1 fail** upstream
  (`a never-settling 429 body cancel cannot block rotation under the cumulative header deadline`).

- `tests/images/*` (SSRF / artifact-download lane): 65 pass / **50 fail** — `callXaiImages` (20),
  `downloadImageToArtifact` scheme enforcement (12), `pinnedHttpsGet` (6), `pruneOldArtifacts` (5)
  and 7 others. Both the test files and the modules they exercise (`xai-client.ts`,
  `artifacts.ts`, `pinned-https-get.ts`) are byte-identical to upstream; the fork touched only
  `images/loop.ts` in that lane, and these tests do not import it.

**Gate 3 therefore reads: no failure outside this inherited set.** A new failure is port damage;
one of these is upstream's own. Re-measure the baseline if upstream is bumped again.
