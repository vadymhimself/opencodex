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
- **Install into the tree that actually runs, not the one on PATH.** The laptop carries TWO
  2.43.0 installs. `launchd` (`com.opencodex.proxy.plist`) hardcodes `/opt/homebrew/bin/ocx`, so
  the supervised gateway is always the homebrew tree; `which ocx` resolves to an nvm tree last
  written Sep 18. They are not identical -- homebrew has the ten-minute ceiling
  (`QUOTA_CAP_COOLDOWN_MS = 10 * 60_000`), nvm still has `MAX_COOLDOWN_MS`. An `npm i -g` off
  PATH installs 2.59 where nothing executes it, exits 0, and leaves 2.43 serving. Resolve the
  prefix from the live process, verify by readback (`ocx --version` plus `ocx health`), and check
  the Mini for the same split before deploying there.
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
  1. Resolve the install prefix from the LIVE process, never PATH (see the two-tree hazard above).
  2. Install and restart in one command; prove it by readback -- `ocx --version` AND `ocx health`.
     The install exit code is not evidence: an install into a tree nothing runs also exits 0.
  3. THEN load the burn guard on that host: `launchctl load -w <plist>`, 10-minute poll. It is
     staged unloaded on both hosts. Loading it before a deploy is pointless and not harmless --
     a trip would reinstall 2.43 over 2.43 and restart a healthy gateway for nothing.
  4. Watch one hour. Roll back on a TRIP; investigate, do not auto-act, on an ALERT.

### Why the guard splits the laptop's zero-attempt failures by signature

The laptop's 2.43 ring carries ~6.4% zero-attempt failures, every one of them
`requestedModel: combo/waterfall` + `No available targets for combo: waterfall`. The fault is
INTERMITTENT, and that was established the hard way: it went quiet at 09-19 11:53 for 7.3h and
156 clean requests, which I read as fixed and reported as fixed. It then recurred at
**09-19 19:32:49, on 2.43, with nothing deployed**. Earlier gaps in the same ring run to 24.0h
and 841 rows.

So a post-quiet baseline would record 0% for a live fault and the next recurrence would roll back
2.59 for something 2.59 did not cause. The guard therefore trips on the RESIDUAL -- zero-attempt
failures that are NOT that signature, a true 0.00% baseline on both hosts -- and only alerts on
the known signature. Accepted blind spot: a real 2.59 regression emitting the same string alerts
rather than trips. The doomed-send trip stays armed and is the more direct burn signal.


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

## Port-interaction defects

Defects that exist in neither side alone, only in the combination. They belong in none of the
lists above: nothing was deviated from and no fork behaviour was dropped — two correct designs
met and left a hole. Expect more of these than of either plain regression.

1. **Non-streaming canonical Anthropic replay logged no usage under key auth.**
   Upstream added `keyUsageOwners` (`src/server/request-log.ts`): a key-auth attempt owns its own
   wire usage snapshot, so settle deliberately refuses to attribute request-level `logCtx.usage`
   to it. The fork's replay tap never needed to take that snapshot, because the fork's settle
   guard was only `usageIsRequestAggregate` — writing `logCtx.usage` was sufficient there. Ported
   together, a key-auth non-streaming replay finished a fully populated attempt with no usage, and
   since the persisted row totals from its attempts (`aggregateAttemptUsage`), the whole row
   reported zero. Real traffic on that lane spent quota while logging none, which is a blind spot
   for any burn guard reading these rows — not just a red test.
   Isolated by driving the live endpoint three ways with the same replayed body: combo + key
   (no usage), direct + key (no usage — so the variable is auth mode, not combo), combo + forward
   (usage present). Streaming replay was never affected: its inspector attributes frame by frame
   as it parses. Fixed at the non-streaming tap in `src/server/claude-messages.ts` by routing
   through `recordKeyWireAttemptUsage` with a direct attempt write as the non-key fallback.

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

- `tests/responses`: 2664 pass / **16 fail**, confirmed pre-existing by neutralising the port's
  two runtime-live deltas in that lane and re-running to an identical result.

## Gate 2 result: 31 of 32 fork exports present, 1 deliberately superseded

Derived mechanically from `f47552136`'s own added exports rather than from memory. The single
absent symbol is `transitionRequestAttempt`, and it is superseded rather than lost:

- Its chat-native call site fired only on `recovery === "key-429"`. The split now lives in
  `noteProviderAttemptSend`, which fires whenever the account label actually changed. That is
  broader (it covers OAuth pool labels, which the key-only pattern never matched) and safer: a
  rotation that lands on the SAME account must not split, or one account reports as two attempts.
- Its two `core.ts` sites went with that file. All four account-changing send paths in 2.59 route
  through `noteProviderAttemptSend`.
- The one remaining direct `noteAttemptSend` (`request-send-budget.ts:114`) is upstream's
  adapter-inner-retry accounting, which is same-account by construction and must not split.

**CORRECTION -- the supersession claim above is wrong for the Anthropic OAuth pool lane.** It
holds for chat-native's key-429 case and was verified there. It does not hold here, and the
difference is that a label-change comparison needs a label on BOTH sides:

- The pool's initial resolution never stamps an account label. The fork did not either -- it
  carried the account in `logCtx.provider` via `formatAnthropicProviderForLog`. So a pool turn's
  first send is legitimately unlabelled, and that is fork behaviour, not a port omission.
- `sealRequestAttemptIdentity` exempts a sent attempt whose label is `undefined`, so a
  late-arriving label still lands. For this lane "unlabelled after the first send" is the NORMAL
  state, so a genuine rotation is misread as a late arrival: the new account's label is stamped
  onto the already-sent first attempt, `noteProviderAttemptSend` then sees
  `previous === next`, and no second row opens.
- The fork did not rely on labels at all here. `core.ts:3899` called
  `transitionRequestAttempt(..., forceNew: true)` from a helper fired on ANY credential recovery
  with a defined `credentialRecoveryStatus`.

An explicit split on credential recovery is therefore load-bearing and must be restored in some
form. Measured consequence while it is absent: the three OAuth rotation tests rotate correctly --
`needsReauth` flips on the denied account -- and then fail on
`expect(entry?.attempts).toHaveLength(2)`, receiving 1. Two accounts' sends fold into one row, so
per-account attribution for pool turns is wrong in exactly the way the quota work cares about.

- `claude-messages-endpoint`: `custom forward openai-responses route never receives the main
  ChatGPT credential` is a **deviation #6 baseline entry, not a defect**. Probed rather than
  inferred: the captured upstream body arrives as `["model","input","store","stream","temperature",
  "top_p","stop","user","prompt_cache_key"]`, so upstream's canonical-only sampling gate works
  correctly on a non-canonical forward route. The single failing expectation is
  `max_output_tokens: 16`, stripped by `stripUnsupportedForwardParams` for any `forward` provider
  under the Codex REST allowlist -- deviation #6 verbatim. The credential assertion the test is
  named for passes. Do not restore fork behaviour to make this green.

**Gate 3 therefore reads: no failure outside this inherited set.** A new failure is port damage;
one of these is upstream's own. Re-measure the baseline if upstream is bumped again.
