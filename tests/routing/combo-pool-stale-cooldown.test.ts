import { afterEach, expect, test } from "bun:test";
import { canReplayAnthropicSource } from "../../src/adapters/anthropic";
import {
  clearComboTargetCooldowns,
  coolComboTarget,
  isComboTargetInCooldown,
  setComboPoolEligibilityProbe,
} from "../../src/combos/failover";
import type { OcxProviderConfig } from "../../src/types";

afterEach(() => {
  setComboPoolEligibilityProbe(undefined);
  clearComboTargetCooldowns();
});

const target = { provider: "anthropic", model: "claude-opus-5-5" };

test("a quota cooldown on a pooled target lifts once the pool has an eligible account", () => {
  coolComboTarget("w", target, { retryAfter: "300", status: 429 });
  setComboPoolEligibilityProbe(() => false);
  expect(isComboTargetInCooldown("w", target)).toBe(true);
  setComboPoolEligibilityProbe(provider => provider === "anthropic");
  expect(isComboTargetInCooldown("w", target)).toBe(false);
});

test("a non-quota cooldown is not lifted by pool eligibility", () => {
  coolComboTarget("w", target, { retryAfter: "300", status: 502 });
  setComboPoolEligibilityProbe(() => true);
  expect(isComboTargetInCooldown("w", target)).toBe(true);
});

test("effort without a thinking block replays to an adaptive target", () => {
  const provider = { adapter: "anthropic", baseUrl: "https://api.anthropic.com", authMode: "oauth" } as OcxProviderConfig;
  const body = {
    model: "combo/waterfall",
    max_tokens: 32000,
    output_config: { effort: "high" },
    messages: [{ role: "user", content: "hi" }],
  };
  expect(canReplayAnthropicSource(body, "claude-opus-5-5", provider)).toBe(true);
  expect(canReplayAnthropicSource({ ...body, output_config: { effort: "minimal" } }, "claude-opus-5-5", provider)).toBe(false);
});
