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
6. Burn checks clean on the staged host: zero doomed sends (real send -> **429 ONLY**), zero
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
  0. BUILD the deploy artifact from THIS BRANCH: `npm pack`. There is no pre-built 2.59 artifact
     to deploy. The repo root holds `UPSTREAM-REFERENCE-ONLY-DO-NOT-DEPLOY-2.59.0.tgz`, which is
     STOCK UPSTREAM -- verified to contain zero `QUOTA_CAP_COOLDOWN_MS`/`ceilingMs` code and no
     `anthropic-source-replay` module. Installing it would erase every fix in this port. It was
     previously named `bitkyc08-opencodex-2.59.0.tgz`, sitting beside the rollback tarball and
     looking exactly like the thing to deploy. Confirm any candidate artifact before installing:
         tar -xzOf <tgz> package/src/combos/failover.ts | grep -c ceilingMs      # must be > 0
         tar -tzf <tgz> | grep -c anthropic-source-replay                        # must be > 0
  1. Resolve the install prefix from the LIVE process, never PATH (see the two-tree hazard above).
  2. Install and restart in one command; prove it by readback -- `ocx --version` AND `ocx health`.
     The install exit code is not evidence: an install into a tree nothing runs also exits 0.
  3. THEN load the burn guard on that host. Re-run `ocx-burn-guard.sh baseline` IMMEDIATELY
     before loading: the staged state file's `last_ts` dates from when the baseline was taken, so
     an old one makes the first check emit a ring-gap alert. Self-correcting on the next poll,
     but it looks like a fault if nobody expects it. Then:
         launchctl load -w ~/Library/LaunchAgents/com.gigradar.ocx-burn-guard.plist
         launchctl list | grep burn-guard    # confirm it took
     `StartInterval` 600, `RunAtLoad` true, so arming checks immediately rather than in ten
     minutes. Staged unloaded on both hosts; `plutil -lint` passes. Loading before a deploy is
     pointless and not harmless -- a trip would reinstall 2.43 over 2.43 and restart a healthy
     gateway for nothing.
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
4. **PARTLY REVERSED after it was caught regressing.** 2.59 treats a server `Retry-After` as
   authoritative and uncapped. Taking that wholesale left `QUOTA_CAP_COOLDOWN_MS` present but
   INERT, because the write clamped every cooldown with `Math.min(..., MAX_COOLDOWN_MS)` and
   `MAX_COOLDOWN_MS` is 60s: a proven-empty account window was re-offered every minute, which is
   the 942-doomed-sends bug the fork exists to fix. It was invisible because the test that guards
   it (`tests/routing/router-combo-failover-classification.test.ts`) did not parse -- see the
   port-interaction section.
   Settled rule, matching the live 2.43 build and passing BOTH sides' tests:
   - the ceiling is reason-dependent: `QUOTA_CAP_COOLDOWN_MS` (10 min) for an exhausted window or
     a reset-derived delay, `MAX_COOLDOWN_MS` (60s) for a transient 429;
   - a server `Retry-After` is clamped by the SAME reason-dependent ceiling as everything else.
     An earlier version of this gave ANY server delay the ten-minute ceiling, which blacked a
     target out for ten minutes on a transient 429 carrying `Retry-After: 600` -- ten times the
     deployed build, from a failure class that is explicitly not exhaustion. Deployed clamps
     every `Retry-After` at 60s (`origin/main:src/combos/failover.ts`, both parse branches).
     **The earlier note justified that behaviour as "matching the live 2.43 build". It did not.**

     Consequence, recorded as a deviation: upstream's
     `429 Retry-After 120 keeps A cooling at 60 seconds and restores it at 120` now fails, because
     it wants a 120s transient delay honoured. The fork's quota behaviour outranks upstream's, and
     on a two-target combo 60s of blackout already produces zero-attempt 529s. So
     `server-combo-failover-e2e` is 175/1 rather than 176/0, and that one failure is deliberate.
5. **Bare `error` events are no longer classified in `captureTerminalHttpStatus`**
   (`src/server/request-log.ts`). The fork classified them from `json.error`, pre-setting
   `terminalHttpStatus = 502`; 2.59 decides a bare error's status downstream via
   `bareErrorStatus`, and the fork branch shadowed it, making every bare error look like a
   transport failure. Upstream taken. Two fork tests that pinned the old policy are dropped.
6. **REVERSED under the standing rule that quota-burn fixes outrank upstream.** Upstream applies
   `stripUnsupportedForwardParams` to EVERY forward provider; the fork stripped only on the
   canonical ChatGPT backend. `max_output_tokens` is a COST control -- a caller sets it to bound
   what a turn may spend. The canonical backend rejects the field outright (`{"detail":
   "Unsupported parameter: ..."}`), so stripping there is required to make the request work at
   all, but stripping it on a gateway that HONOURS it silently removes the caller's cap and lets
   the turn generate far more than it asked to pay for. The call now sits inside the
   `isCanonicalOpenAiForwardProvider` guard, next to the comment that already stated the
   principle ("a self-hosted or third-party forward gateway may still accept it, so this must not
   be widened") while the call sat outside it.
   This was previously recorded as an accepted deviation with its failing test treated as a
   baseline entry. Judged as a cost path rather than a compatibility one, it was a defect: fixing
   it took the replay lane to 115 pass / 0 fail.

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

2. **Anthropic pool rotations folded two accounts into one attempt row.**
   The attempt split in `noteProviderAttemptSend` (`src/server/request-log.ts`) compares the
   account LABEL across a send. Anthropic pool turns never carry one: `stampOAuthAccountLabel`
   (`src/providers/label.ts:68`) returns early for base `openai` and `anthropic`, so an Anthropic
   attempt has no `accountLogLabel` on ANY send, by design. That provider's account identity
   lives in the account-qualified log provider string instead — `anthropic-pfc164f` (hyphen form: `${providerName}-${ordinal}`), written by
   `formatAnthropicProviderForLog` — which is the axis the fork's own `transitionRequestAttempt`
   compared. Label-only comparison is therefore not merely unfired on that lane, it is
   structurally incapable of firing, and a 403 credential-denial rotation reported one attempt on
   the wrong account with both accounts' tokens on it.
   Two stacked defects, both needed fixing: the split gate now compares the provider string as
   well as the label, AND `sealRequestAttemptIdentity` no longer re-stamps a sent attempt's
   provider — its pre-send re-stamp was laundering the rotation on the very axis the gate reads.
   Rotation itself was separately dead until the Anthropic pool branch started setting
   `replayOAuthCredentialSnapshot` (`src/server/responses/request-transport.ts`), which
   `rotateAnthropicProviderOnCredentialDenial` gates on; without it the 403 simply stood.

   **Consequence to carry forward:** per-account attribution for the Anthropic pool rests
   entirely on `attempt.provider`. Anything grouping per-account usage by `accountLogLabel` —
   including the burn guard staged for the deploy, which groups by attempt fields — shows
   Anthropic pool traffic as unlabelled. Group by the provider string for that provider, or
   treat an absent label on an `anthropic` attempt as "pool account, see provider".

2. **A bare upstream `error` frame lost its classification, so client refusals arrived as 502.**
   `upstreamErrorTailFrame` (`src/server/relay.ts`) is upstream-only: when a 200 SSE ends with no
   protocol terminal it synthesizes a `response.failed`, stamping a fixed
   `{type: "upstream_error", code: "upstream_server_error"}` and keeping only the message. The
   fork had no tail synthesis at all, so a bare error's own `invalid_request_error` survived to
   the surface. Ported together, every classifier downstream was told the turn was a 5xx: Claude
   Code saw `502 overloaded_error` — retryable — for an upstream that had refused the request
   outright, and the same relabelling hid Anthropic OAuth subscription denials that credential
   rotation keys off. Fixed by carrying the frame's declared `type`/`code` into the synthesized
   terminal, and by deriving the non-streaming Claude error status from the collected error
   instead of a flat 502 (`api_error` still maps to 502, so unclassified errors are unchanged).
   Not a deviation and not a dropped fork behaviour: neither side has this bug alone.

3. **`text = text` in `core-combo-failure.ts` blinded every combo failure decision.**
   Porting the canonical-replay branch into `consumeComboFailure` split the body read in two, and
   the non-replay arm assigned `text = text` instead of `text = body.text`. Every combo failure
   therefore classified against an empty body: `classificationText` fell back to
   `Provider error <status>`, `upstreamCode` stayed undefined, and `codexQuotaFailureMessage`
   never matched. On the burn path that is the expensive half — body-confirmed quota evidence is
   how a depleted window earns the ten-minute cooldown, so without it a combo child kept
   re-offering an exhausted account, and a hopping 413 stopped on its first target instead of
   advancing. One character; it closed 12 failures in `server-combo-failover-e2e` and 10 in
   `tests/responses`, and it had been mistaken for an inherited upstream baseline for a day.

## Inherited upstream failures (baseline, corrected 2026-09-20)

**The 2026-09-19 numbers in this section were wrong and gate 3 must not use them.** They were
produced by copying upstream's unmodified *test files* into /tmp and running them against **this
tree's source**. That measures our source, not upstream's, so every port defect those files touch
was recorded as inherited. The honest method is the stock worktree at `134c92a01`
(`/tmp/upstream259`, node_modules symlinked), running each suite in both trees and diffing the
sorted failure lists.

Re-measured that way:

- `tests/server/server-combo-failover-e2e.test.ts`: upstream 174/0, ours **176 pass / 0 fail**.
  The twelve "inherited" failures previously listed here were one port defect —
  `text = text` in `core-combo-failure.ts` (below). None were upstream's.
- `tests/responses`: upstream 2670 pass / **1 fail**, ours 2679 / **1 fail** — the same single
  failure in both (`Windows spill queue advances past never-settling principal and icacls
  runners`). The previously recorded "16 inherited" were ours: ten were the same `text = text`
  defect, the rest were fixed alongside it.
- `tests/codex-integration`: upstream 5612 tests / **29 fail**, ours 5614 / **36 fail**. The 29
  are identical in both trees and genuinely inherited. The 7 extra are ours and all sit in one
  describe — see deviation 4: our clamp of a server `Retry-After` to the ten-minute ceiling is
  what they measure, against an upstream test file that is byte-identical in both trees.
- `tests/web-search`: upstream 288/0, ours **290 / 2**. The 2 are fork-only tests (absent from
  the stock tree) that contradict upstream behaviour we took on purpose: one pins `ctx.onRetry`,
  removed in favour of `onPhysicalSend` (deviation 2) — its identical twin in `tests/images` was
  dropped for that reason — and one expects a single `onIterationUsage` call where upstream's
  forced-answer pass now retries once and reports both iterations. The previously recorded
  "1 inherited failure" in this lane was ours: the pacing wiring cleared and re-armed the
  cumulative header deadline on the first dispatch even when no pacing hook was wired, which
  renewed a budget that had measured no wait. Fixed in `src/web-search/loop.ts`.
- `tests/images/*`: recorded on 2026-09-19 by the old method and NOT yet re-measured against the
  stock tree. Treat as unverified until it is.

## Gate 2 result: 31 of 32 fork exports present, 1 deliberately superseded

Derived mechanically from added exports rather than from memory -- but ORIGINALLY FROM THE WRONG
COMMIT. `f47552136` is not the lane that ran in production: `origin/main` is that commit plus five
more, three touching `src` (`1150a5da2`, `2687eb831`, `9ff58e185`). Nothing those added was
covered by the first audit, and the dropped Anthropic 60s cooldown cap was one of four things
`1150a5da2` changed -- the only one that did not land.

RE-DERIVED from `git diff 06ec55363 origin/main -- src`: 32 added exports, ALL present. The two
sibling cooldown caps from the same commit (`generic-account-failover.ts:28`,
`key-failover.ts:25`) both survived; the Anthropic one was the sole casualty and is fixed at
`ddb2bc9ce`/`3344f3d41`. Note the cap was a plain constant, so an EXPORT scan could never have
found it -- use the file-level diff, not just exports. The single
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

- ~~`claude-messages-endpoint`: `custom forward openai-responses route never receives the main
  ChatGPT credential`~~ **-- NO LONGER A BASELINE ENTRY.** Reclassified as a real defect and fixed
  under the quota-burn-priority rule; see deviation 6. The original (now superseded) reasoning: Probed rather than
  inferred: the captured upstream body arrives as `["model","input","store","stream","temperature",
  "top_p","stop","user","prompt_cache_key"]`, so upstream's canonical-only sampling gate works
  correctly on a non-canonical forward route. The single failing expectation is
  `max_output_tokens: 16`, stripped by `stripUnsupportedForwardParams` for any `forward` provider
  under the Codex REST allowlist -- deviation #6 verbatim. The credential assertion the test is
  named for passes. Do not restore fork behaviour to make this green.

## Known nits, recorded and deliberately NOT fixed in this port

- **A mixed row drops `estimated: true`.** `aggregateAttemptUsage` attaches the marker only when
  the aggregate's own status is `estimated` (`request-log.ts:1967`). Two failed legs plus one
  estimated leg make the row `unreported`, so the marker is suppressed even though the only
  usage in the total came from an estimate. The line is identical on both sides (fork
  `request-log.ts:1442`, upstream `:1800`), so it is pre-existing, not a port regression.
  Blast radius is presentation only: `usage/summary.ts:537` rebuilds the row from attempts and
  ORs the marker across them, and quota capacity (`codex/quota-capacity.ts:28`) reads per-attempt
  usage, which always carries it. The single exposure is `/api/logs` via
  `management/shared.ts:114`, where the GUI shows estimated tokens without the qualifier.
  Fix is one line aligning `aggregateAttemptUsage` with the rule `summary.ts` already uses --
  deliberately deferred so this port does not carry opportunistic changes.

## The inherited baseline was re-measured properly (the earlier one was wrong)

The original "63 failures" was derived by running upstream's own test FILES against THIS tree,
which is not the same thing and gave badly wrong numbers -- `tests/responses` was recorded as 16
inherited when upstream's own tree has **1**. The real baseline is measured in a worktree at
`134c92a01` (stock upstream 2.59.0, this branch's base):

| dir | upstream 2.59 | notes |
|---|---|---|
| responses | 2670 / **1** | the 1 is `Windows spill queue advances past never-settling principal` |
| providers | 4466 / **31** | every one of these also fails here |
| codex-integration | 5546 / **29** | |
| ci-workflows | 839 / 0 | |
| claude-integration | 679 / 0 | |
| cli | 1938 / 1 | |

Re-measure the same way before trusting any baseline claim in this document.

### The doomed-send rejection set is 429 only -- 502/529 would have rolled back on an outage

This document originally said `429/502/529`, and the guard inherited that wording. It is wrong:
502 and 529 are upstream CAPACITY errors, not quota exhaustion, and re-sending after one is
ordinary retry behaviour. Measured consequence: re-baselining the Mini jumped doomed from 0.10%
to 1.55% -- all 31 events provider `openai`, in a single 7-minute burst (18:51:45-18:58:40), every
rejection a 502 and not one 429. An upstream provider outage was feeding the one signal wired to
auto-rollback. **Had the guard been armed, the Mini would have rolled back because OpenAI had a
bad seven minutes.**

Narrowed to 429 only, at no cost in real signal: laptop live ring 4 doomed before and after,
recorded evidence 3 before and after -- genuine Anthropic quota events across four accounts. Mini
goes 31 to 0. A 400-row 502 blip is now silent; a 400-row 429 burn still trips.

### The waterfall fault is on BOTH hosts, and is currently active

It was zero on the Mini in every earlier capture, which is why the Mini was treated as the clean
host. The Mini's current ring carries 75 events, first 13:52:41 and last 19:52:30 -- the most
recent row in the ring, so it is still going. Hourly 12:00-19:00: 0,1,7,7,5,5,40,10. Both hosts
are on 2.43, so this is not a 2.59 symptom. Residual zero-attempt is still 0.00% on both, so the
signature split holds and the trip stays armed.

### Root cause of "No available targets for combo: waterfall" (BOTH hosts, 2.43 and 2.59)

`combos/waterfall` has two targets: `anthropic/claude-opus-5` and `openai/gpt-6-astra`. The split
is by request shape and it is near-absolute: 1753 streaming requests all served by anthropic with
zero faults; 94 non-streaming, of which anthropic served 2 and 75 faulted.

The chain:
1. `comboTargetAcceptsAnthropicSource` drops the anthropic target for a body carrying
   `output_config.effort` WITHOUT `thinking: adaptive|disabled`. Probed directly against the live
   config: that shape returns false, while plain bodies, typed tools, function tools and
   `thinking: adaptive` all return true. `max_tokens > 64000` also drops it
   (anthropic `defaultMaxOutputTokens` is 64000).
2. That leaves openai as the ONLY target for those requests.
3. openai reads `weeklyPercent: 100` and returned 502 on ~50 of ~67 attempts.
4. A 502 is neither a quota cap nor a transient rate limit, so `coolComboTarget` takes
   `DEFAULT_COOLDOWN_MS` (60s). Each 502 blacks out the last remaining target for a minute.
5. A request arriving in that window finds zero eligible targets: 529, `durationMs: 0`, zero
   attempts. No upstream was ever contacted.

Anthropic was never the problem -- zero 429s in the entire ring, and it served 3-8 successful
turns in the same minutes as the faults.

**Why this is a defect, not strictness.** A NON-canonical target is eligible whenever the source
does not REQUIRE exact replay, because it can be translated. The canonical Anthropic target is
dropped outright when it cannot replay exactly, even when translation would serve. The one target
guaranteed able to answer an Anthropic request is held to the strictest standard.

**Counterfactual on availability (not an execution of the patch):** measuring each fault's
distance to the nearest `combo/waterfall` turn that resolved to `anthropic/claude-opus-5` with a
200 -- a cooled or quota-filtered target cannot produce a 200, so a nearby success proves the
target was pickable: 73 within 30s, 2 within 60s, **75/75**. Re-verify against the ring once the
fix lands.

### EITHER provider's 502 triggers the fault -- single-homing is the root cause

On the Mini the 502s came from openai; on the laptop two isolated events are each preceded by
7 seconds by an ANTHROPIC 502 that carried attempts:

    23:11:19  502  anthropic-p126351  ->  23:11:26  503 zero-attempt
    01:11:19  502  anthropic-p126351  ->  01:11:25  503 zero-attempt

An anthropic 502 cools the anthropic target for 60s, and a non-stream waterfall request arriving
inside that window finds anthropic cooled AND openai unavailable, so both targets are gone. Same
failure with the roles reversed. Neither provider's flakiness is the cause -- the cause is that
these requests have only one usable target to begin with.

### "zero-attempt 503" names TWO different quantities -- do not confuse them

This ambiguity has now cost two separate investigations, so it is recorded in words:

- **total zero-attempt 5xx** -- every zero-attempt 5xx row. On the laptop's current ring: 20.
- **residual zero-attempt** -- the same set EXCLUDING the known waterfall signature. On that same
  ring: 0.

**Only the RESIDUAL is wired to auto-rollback.** The total is tracked and alerts separately as
`known_fault`. A raw zero-attempt count compared against the trip threshold will look alarming
and mean nothing. The guard's own design note originally claimed "the trip signal has a true-zero
baseline", which is true of the residual and false of the total.

The weakest joint in the design, and its mitigation: the signature was matched on
`upstreamError` STARTING WITH "No available targets" -- a string-match on an upstream message. If
that wording ever changes, or the same fault appears under a different combo id, those events
stop matching and land in the RESIDUAL, where they would trip a rollback on a pre-existing fault.
Matching the structural fields instead (`errorCode == "server_is_overloaded"` with zero attempts)
removes that dependency.

### Tightening a system silently invalidates fixtures that relied on the looser behaviour

Twice today, coverage looked present and was not, and neither failure announced itself:

- `tests/routing/router-combo-failover-classification.test.ts` did not PARSE, so the tests
  guarding the ten-minute cooldown ceiling never ran for the entire port. The ceiling regression
  sat undetected behind a file reporting `0 pass / 1 fail`.
- Hardening the burn guard's signature predicate flipped three of its own fixtures from TRIP to
  ALERT -- including the sub-floor burn repro -- because those fixtures were built with the
  errorCode the tighter predicate now masks. The regression silently lost coverage of the two
  behaviours the guard exists to protect, AT THE MOMENT the guard became more correct.

The general form: a fixture encodes an assumption about the system, and tightening the system
invalidates fixtures that relied on the looser behaviour WITHOUT failing. TRIP becoming ALERT is a
PASS-shaped outcome in any suite that only checks for crashes.

The defence is not vigilance, it is asserting the expected VERDICT per case so a silent downgrade
fails loudly. Any regression harness here should assert its expected outcome, not print a table
for a human to read.

### Cost signals count ROWS; availability signals count OUTAGES

Retry ladders inflate row counts 2-4x over genuine outages (laptop 20 rows -> 5 outages, Mini 75
-> 38, laptop evidence 128 -> 63), because one caller backing off at 2s/2s/2s/5s/8s writes six
rows for one failure. The obvious fix -- put the trip floor on collapsed incidents instead of
rows -- was implemented, regression-tested, and REVERTED: it silenced both the "new failure mode"
case and the sub-floor burn repro, because a sustained burn inside one conversation collapses to
a single incident and never produces a second one. It would have traded 2-4x over-sensitivity for
a PERMANENT blind spot on exactly the shape the guard exists to catch.

The incident count is still computed and printed as context:
`TRIP zero-attempt-503 30 rows [5 incidents] over 430 rows = 6.98%`.

The rule that came out of it, and the reason it must not be "unified" later:
- **doomed-sends counts ROWS.** Every doomed send is a physical request that costs money; six
  retries are six wasted sends, not one.
- **availability signals count OUTAGES**, which is why their row floors read as more sensitive
  than documented: 5 genuine outages producing 30 rows will trip where the note said 25 rows.
  Accepted deliberately -- the residual baseline is a true zero on every ring measured, so any
  residual event at all is a failure mode never seen on 2.43. The floor filters flukes, it does
  not require severity, and on a freshly-deployed build erring toward acting is the right error:
  a false rollback costs a restart onto a known-good build, a missed burn costs money.

### Account-layer cooldown ceilings (checked, and one cliff recorded)

- **Anthropic accounts are capped at 60 seconds** (`oauth/anthropic-routing.ts`, DEFAULT and MAX
  both `60_000`, clamped at both parse sites). No anthropic account can be blacked out longer
  than a minute by the routing layer. This is what ruled out an anthropic-side blackout and left
  the eligibility filter as the only explanation.
- **`exhaustedCooldownMs` in `account-quota-rank.ts` carries a [5 minute, 24 HOUR] range** and its
  exhausted branch BYPASSES the generic 60s clamp at `generic-account-failover.ts:231`. It is
  hard-gated `if (provider !== "kiro") return null;` on its first line, and neither host has a
  kiro provider (both configs list exactly `anthropic`, `openai`; no kiro credentials). So it is
  NOT reachable today. Recorded because it is a real cliff the day a kiro provider is added: one
  429 with no `Retry-After` could park an account for up to a day with nothing at the generic
  layer limiting it. The trigger to revisit is a CONFIG CHANGE, not a runtime signal -- no guard
  detector for it, since a permanently-zero baseline detects nothing.

### Code-review findings: 8 of 10 fixed, 2 recorded

Fixed, each with a planted-defect-checked regression:

| # | defect | commit |
|---|---|---|
| 1 | `.map()` on a string `system` 500'd every documented-shape replay | `082073946` |
| 3 | streaming replay logged `output_tokens` ≈ 1 for a 4,000-token turn | `834d79cfc` |
| 5 | generic OAuth clamped a stated `Retry-After` to 60s | `8cf64f54a` |
| 6 | a row whose attempts name another provider lost its health sample | `36db0193b` |
| 8 | a refused attempt zeroed the whole request's cost | `ccca015d8` |
| 10 | a reset header alone granted the ten-minute ceiling | `581304ff2` |

Plus two of my own regressions the review caught: the Anthropic account cooldown
ceiling (`4d9becd45`) and the transient-429 clamp (`a53df96bb`).

**NOT fixed, deliberately:**

- **Finding 9 -- `response.incomplete` with no reason is retryable.** Reported as a widening
  versus upstream, and it is: upstream requires a known reason. But this matches `origin/main`
  EXACTLY (fork `combo-stream-preflight.ts:58-60`), so it is deployed fork behaviour rather than
  port damage. The concern is real -- a content-filter stop that omits the reason would be
  retried on another target, costing a second billed send -- but changing behaviour that has run
  in production on a hypothetical, with no instance in any ring, is the speculative change this
  port has refused throughout. Needs one observed instance before it moves.
- **Finding 4 -- `count_tokens` makes an unlogged upstream call.** Confirmed: the handler has no
  `addFinalRequestLog` on any path, so the call is invisible in `usage.jsonl` and `/api/logs`,
  commits a pool account, advances the combo cursor, and does not cool the account on a 429. It
  arrived with an upstream feature commit, not the port. Deferred because the endpoint consumes
  RATE LIMIT rather than tokens, so it is not the dollar spend the other findings are, and
  threading a log context through ~400 lines of handler with a dozen return paths is a larger
  change than anything else here. Worth doing; not worth doing at the end of this batch.
- **Findings 2 and 7** (sidecar rounds overwriting each other's usage; `cachedInputTokens`
  read/write ambiguity) are recorded unfixed for the same budget reason. Both under-report rather
  than over-report.

### Outstanding, classified, NOT defects

- **`opencode-go-session-header.test.ts` (28 failures)**: an EXPECTATION CHANGE, not a duplicate
  send. The file is byte-identical to upstream's and passes 88/0 there. It rigs entropy
  (`mockReturnValue(0.9)` with `mockReturnValueOnce(0)`) to encode two INDEPENDENT draws --
  preliminary route checks target[0], dispatch independently picks target[1]. The fork passes
  `comboRandomSeed` (`anthropicRequestCorrelationKey`, a sha256 over count-relevant body fields)
  to both, so `combos/resolve.ts:275` uses `correlatedRandom(seed, comboId)` instead of
  `Math.random()`, the mocked sequence is never consumed, and both picks agree. That agreement is
  the feature: `count_tokens and following generation share a random combo target` depends on it.
  Setting `comboRandomSeed = undefined` turns the file 88/0 and is the sole cause of all 28.
  Do NOT "fix" these by dropping the seed.
  The second request is not a duplicate: target[0] returns `404 model_not_found` and advancing on
  failure is strategy-independent. Attempt rows, seeded vs not:

      seeded:   [{o:1, provider:"other", sends:1, status:404}, {o:2, provider:"renamed-go", sends:1, status:0}]
      unseeded: [{o:1, provider:"renamed-go", sends:1, status:0}]

  Burn is bounded: the correlation key is per-request-body so it varies across turns rather than
  pinning a conversation to one target, and `isComboTargetInCooldown` sits inside
  `pickComboTarget`'s eligibility filter, so a repeatedly-failing target drops out of the pick.

- **kiro (3 failures)**: `kiro-retry.test.ts` (2) and `kiro-stream.test.ts` (1) carry fork
  assertions upstream does not have -- kiro retries flowing through the request executor with a
  next-dispatch recovery label. Upstream's own versions pass upstream. This is fork behaviour not
  yet ported, not a regression.
- **`a combo stops on a zero-output failure from the stored-account replay stream`**: passes in
  isolation (28/28 repeated) both before and after the pool-401 fix; fails only in some
  full-suite orderings. Flaky-under-ordering, not a defect.
- **`tests/server` whole-directory runs**: 243 failures when the directory shares one `bun test`
  process, while the same suites pass individually (`server-combo-failover-e2e` 164/12,
  `server-key-failover-e2e` 42/42, `server-auth` 112/112). `mock.module` leakage across files,
  pre-existing. Run per file.
- **`tests/routing` and `tests/adapters` hang** when run as a whole directory. `--timeout` is
  per-test and does not catch a hang between tests. Run per file with a wall-clock kill.

**Gate 3 therefore reads: no failure outside this inherited set.** A new failure is port damage;
one of these is upstream's own. Re-measure the baseline if upstream is bumped again.
