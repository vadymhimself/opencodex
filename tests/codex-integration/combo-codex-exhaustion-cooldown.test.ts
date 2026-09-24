import { afterEach, describe, expect, test } from "bun:test";
import {
  clearComboTargetCooldowns,
  comboFailureCooldownScope,
  comboFailureDecision,
  coolComboTarget,
  isComboTargetInCooldown,
} from "../../src/combos/failover";

/**
 * The ChatGPT Codex backend reports a depleted plan window as HTTP 502 `upstream_server_error`
 * carrying the prose `The usage limit has been reached`, never the documented 429. Upstream's
 * quota-cap predicate is gated on 429, so a dead account took the generic 60-second cooldown and
 * was re-offered every minute: 942 doomed sends in 72h of production ledger, each surfacing as
 * `adapter_eof`, plus 94 downstream `503 No available targets`.
 *
 * This file is the guard against re-importing that bug on the next upstream rebase. Delete the
 * exhaustion branch in `coolComboTarget` and the ten-minute assertions below go back to 60s.
 */
const combo = "codex-exhaustion-burn-test";
const target = { provider: "openai", model: "gpt-6-astra" };
const now = Date.UTC(2026, 8, 24, 12, 0, 0);
const EXHAUSTED = "The usage limit has been reached";

afterEach(() => clearComboTargetCooldowns(combo));

test("permanent credential and billing failures use ten minutes unless a deadline is stated", () => {
  for (const code of ["invalid_api_key", "insufficient_quota", "subscription_required", "payment_required", "billing_error", "insufficient_balance"]) {
    coolComboTarget(combo, target, { now, status: 401, code });
    expect(isComboTargetInCooldown(combo, target, now + 60_000)).toBe(true);
    expect(isComboTargetInCooldown(combo, target, now + 599_999)).toBe(true);
    expect(isComboTargetInCooldown(combo, target, now + 600_000)).toBe(false);
    for (const deadline of [{ retryAfter: "30" }, { resetAt: now + 30_000 }, { cooldownMs: 30_000 }]) {
      coolComboTarget(combo, target, { now, status: 401, code, ...deadline });
      expect(isComboTargetInCooldown(combo, target, now + 29_999)).toBe(true);
      expect(isComboTargetInCooldown(combo, target, now + 30_000)).toBe(false);
    }
  }
});

describe("a depleted Codex plan window", () => {
  test.each([
    ["502 prose", 502, "upstream_server_error", EXHAUSTED],
    ["429 structured code", 429, "usage_limit_exceeded", "quota"],
    ["vendor 5-hour window", 429, "1308", "Usage limit reached for 5 hour"],
  ])("cools the target for ten minutes, not 60s (%s)", (_label, status, code, message) => {
    coolComboTarget(combo, target, { now, status, code, message });
    // The whole point: still cooling a minute later, when the 60s default would have expired.
    expect(isComboTargetInCooldown(combo, target, now + 60_000)).toBe(true);
    expect(isComboTargetInCooldown(combo, target, now + 10 * 60_000 - 1)).toBe(true);
    expect(isComboTargetInCooldown(combo, target, now + 10 * 60_000)).toBe(false);
  });

  test("hops without blacking out the whole provider", () => {
    // Model-family scope: a sibling model on the same provider row stays selectable, which is
    // why the exhaustion match is kept out of `isProviderScopedQuotaCap`.
    expect(comboFailureCooldownScope(502, EXHAUSTED, { code: "upstream_server_error" })).toBe("target");
    expect(comboFailureDecision(502, EXHAUSTED, { code: "upstream_server_error" })).toBe("hop");
  });

  test("an unrelated 502 keeps the 60-second default", () => {
    coolComboTarget(combo, target, { now, status: 502, code: "upstream_server_error", message: "bad gateway" });
    expect(isComboTargetInCooldown(combo, target, now + 60_000 - 1)).toBe(true);
    expect(isComboTargetInCooldown(combo, target, now + 60_000)).toBe(false);
  });

  test("an explicit server Retry-After still outranks the exhaustion default", () => {
    coolComboTarget(combo, target, { now, status: 502, code: "upstream_server_error", message: EXHAUSTED, retryAfter: "30" });
    expect(isComboTargetInCooldown(combo, target, now + 30_000)).toBe(false);
  });
});
