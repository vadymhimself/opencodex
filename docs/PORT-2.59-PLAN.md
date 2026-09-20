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
   - a server `Retry-After` is authoritative UP TO ten minutes and clamped beyond it. Upstream's
     own test wants a 120s `Retry-After` honoured; the fork's invariant wants a 2h one clamped.
     Both are right about different magnitudes.
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
   lives in the account-qualified log provider string instead — `anthropic (pfc164f)`, written by
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
