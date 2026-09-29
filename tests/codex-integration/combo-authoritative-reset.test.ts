import { afterEach, describe, expect, test } from "bun:test";
import {
  clearComboTargetCooldowns,
  coolComboTarget,
  earliestComboCooldownExpiry,
  isComboTargetInCooldown,
  parseRetryAfterMs,
} from "../../src/combos/failover";

const target = { provider: "devin", model: "swe-2" };
const combo = "stated-reset-hardening-test";
const now = Date.UTC(2026, 8, 18, 8, 0, 0);
afterEach(() => clearComboTargetCooldowns(combo));

describe("explicit server cooldown versus local wait allowance", () => {
  test("keeps legacy bounded parsing unless preserving a server lower bound", () => {
    expect(parseRetryAfterMs("3600", now)).toBe(600_000);
    expect(parseRetryAfterMs("3600", now, { preserveServerDelay: true })).toBe(3_600_000);
  });

  // FORK DEVIATION, 2026-09-28 pool-cooldown outage: upstream keeps a server delay at full
  // length. A target key is shared by every account in the provider's pool, so one spent account
  // answering `Retry-After: 318747` blacked out `anthropic/claude-opus-5-5` for four hours while
  // four accounts were healthy. Every cooldown is capped at ten minutes until the key carries the
  // account. A delay at or under the cap is still kept exactly.
  test.each([30, 120, 599])("keeps a %i-second server delay exactly", seconds => {
    coolComboTarget(combo, target, { now, retryAfter: String(seconds) });
    expect(earliestComboCooldownExpiry(combo, [target], now)).toBe(now + seconds * 1000);
    expect(isComboTargetInCooldown(combo, target, now + seconds * 1000 - 1)).toBe(true);
    expect(isComboTargetInCooldown(combo, target, now + seconds * 1000)).toBe(false);
  });

  test.each([780, 3600, 7200, 318747])("caps a %i-second server delay at ten minutes", seconds => {
    coolComboTarget(combo, target, { now, retryAfter: String(seconds) });
    expect(earliestComboCooldownExpiry(combo, [target], now)).toBe(now + 600_000);
    expect(isComboTargetInCooldown(combo, target, now + 600_000)).toBe(false);
  });

  test("an HTTP-date reset an hour out is capped the same way", () => {
    coolComboTarget(combo, target, { now, retryAfter: new Date(now + 3_600_000).toUTCString() });
    expect(earliestComboCooldownExpiry(combo, [target], now)).toBe(now + 600_000);
  });

  test("a configured fallback is still bounded to ten minutes", () => {
    coolComboTarget(combo, target, { now, cooldownMs: 99_000_000 });
    expect(earliestComboCooldownExpiry(combo, [target], now)).toBe(now + 600_000);
  });

  test("explicit immediate retry stays immediate", () => {
    coolComboTarget(combo, target, { now, retryAfter: "0" });
    expect(earliestComboCooldownExpiry(combo, [target], now)).toBe(now + 1);
  });
});
