import { afterEach, describe, expect, test } from "bun:test";
import { clearComboTargetCooldowns, coolComboTarget, isComboTargetInCooldown } from "../../src/combos/failover";

/**
 * Regression for the 2026-09-28 pool-cooldown outage.
 *
 * A combo target key is `comboId` + provider/model; every account in that provider's pool shares
 * it. One account answered `Retry-After: 318747` (88.5h, weekly limit spent) and the gateway wrote
 * that straight onto the shared key, clamped only by the 24h server-delay ceiling. Four healthy
 * accounts became unreachable and the gateway returned 503 with zero upstream sends for four hours.
 *
 * A `Retry-After` is authoritative about the account that answered it, not about the target. Until
 * the key carries the account, the ceiling is what bounds the blast radius.
 */
const combo = "pool";
const target = { provider: "anthropic", model: "claude-opus-5-5" };
const TEN_MIN = 10 * 60_000;

afterEach(() => { clearComboTargetCooldowns(); });

describe("a pooled target's cooldown is bounded", () => {
  test("an 88-hour Retry-After cannot hold the target for more than ten minutes", () => {
    const now = 1_000;
    coolComboTarget(combo, target, { now, status: 429, message: "rate limited", retryAfter: "318747" });
    expect(isComboTargetInCooldown(combo, target, now + TEN_MIN - 1)).toBe(true);
    expect(isComboTargetInCooldown(combo, target, now + TEN_MIN)).toBe(false);
  });

  test("a 24-hour Retry-After is bounded the same way", () => {
    const now = 1_000;
    coolComboTarget(combo, target, { now, status: 429, message: "rate limited", retryAfter: "86400" });
    expect(isComboTargetInCooldown(combo, target, now + TEN_MIN)).toBe(false);
  });

  test("a short server delay is still honoured exactly, not rounded up", () => {
    const now = 1_000;
    coolComboTarget(combo, target, { now, status: 429, message: "slow down", retryAfter: "30" });
    expect(isComboTargetInCooldown(combo, target, now + 30_000 - 1)).toBe(true);
    expect(isComboTargetInCooldown(combo, target, now + 30_000)).toBe(false);
  });

  test("an immediate Retry-After: 0 stays immediate", () => {
    const now = 1_000;
    coolComboTarget(combo, target, { now, status: 429, message: "retry now", retryAfter: "0" });
    expect(isComboTargetInCooldown(combo, target, now + 1)).toBe(false);
  });
});
