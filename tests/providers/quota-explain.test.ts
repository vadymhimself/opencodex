import { describe, expect, test } from "bun:test";
import { explainQuotaRefusal, explainPoolQuotaRefusal } from "../../src/providers/quota-explain";
import type { ProviderQuota } from "../../src/providers/quota-types";

const now = 1_800_000_000_000;
const q = (extra: Record<string, unknown>): ProviderQuota =>
  ({ updatedAt: now, ...extra }) as unknown as ProviderQuota;

describe("quota refusal explanation", () => {
  test("names the spent model-family window instead of leaving a generic rate limit", () => {
    const note = explainQuotaRefusal(
      q({ customWindows: [{ label: "Fable", scope: "model", percent: 100, resetAt: now + 6 * 3_600_000 }] }),
      "claude-fable-5-1", now,
    );
    expect(note).toContain("Fable window at 100%");
    expect(note).toContain("resets in 6h");
  });

  test("a model-scoped window says nothing about a different family", () => {
    // The whole point of the producer-set scope flag: Fable being spent is not evidence about
    // Opus, and saying so would send the operator after a quota problem they do not have.
    expect(explainQuotaRefusal(
      q({ customWindows: [{ label: "Fable", scope: "model", percent: 100, resetAt: now + 3_600_000 }] }),
      "claude-opus-5", now,
    )).toBeUndefined();
  });

  test("a provider-wide window applies to every model", () => {
    expect(explainQuotaRefusal(
      q({ customWindows: [{ label: "Prepaid credits", percent: 100, resetAt: now + 3_600_000 }] }),
      "claude-opus-5", now,
    )).toContain("Prepaid credits at 100%");
  });

  test("stays silent when nothing is exhausted, so throttling still reads as throttling", () => {
    expect(explainQuotaRefusal(q({ weeklyPercent: 40 }), "claude-fable-5-1", now)).toBeUndefined();
  });

  test("a window whose reset already passed is not reported as spent", () => {
    expect(explainQuotaRefusal(q({ weeklyPercent: 100, weeklyResetAt: now - 1 }), "claude-opus-5", now))
      .toBeUndefined();
  });

  test("a pool with no cached reading explains nothing rather than guessing", () => {
    // Partial evidence is the dangerous case: an account the cache cannot speak for might have
    // headroom, so claiming a pool-wide outage from it would be a confident wrong answer.
    expect(explainPoolQuotaRefusal("anthropic", ["never-cached"], "claude-fable-5-1", now))
      .toBeUndefined();
    expect(explainPoolQuotaRefusal("anthropic", [], "claude-fable-5-1", now)).toBeUndefined();
  });
});
