import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { appendFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { handleManagementAPI } from "../src/server/management-api";
import { ManagementRequest } from "./helpers/management-auth";
import {
  appendUsageEntry,
  resetUsageReadCacheForTests,
  usageLogPath,
  type PersistedUsageAttempt,
  type PersistedUsageEntry,
} from "../src/usage/log";
import {
  closeRequestHistoryIndex,
  openRequestHistoryIndex,
  requestHistoryDb,
} from "../src/routing/history/indexer";
import { computeRoutingAnalytics } from "../src/routing/analytics";
import { estimateComboCost } from "../src/usage/cost";
import type { OcxConfig } from "../src/types";

let testDir = "";
let previousHome: string | undefined;

function entry(
  requestId: string,
  overrides: Partial<PersistedUsageEntry> & { timestamp: number; status: number; durationMs: number },
): PersistedUsageEntry {
  return {
    requestId,
    provider: "a",
    model: "m1",
    usageStatus: "reported",
    ...overrides,
  };
}

function attempt(overrides: Partial<PersistedUsageAttempt> = {}): PersistedUsageAttempt {
  return {
    ordinal: 1,
    provider: "a",
    model: "m1",
    adapter: "openai-chat",
    status: 200,
    durationMs: 10,
    sendCount: 1,
    recoveryKinds: [],
    usageStatus: "reported",
    ...overrides,
  };
}

beforeEach(() => {
  previousHome = process.env.OPENCODEX_HOME;
  testDir = mkdtempSync(join(tmpdir(), "ocx-analytics-"));
  process.env.OPENCODEX_HOME = testDir;
  resetUsageReadCacheForTests();
  closeRequestHistoryIndex();
});

afterEach(() => {
  closeRequestHistoryIndex();
  if (previousHome === undefined) delete process.env.OPENCODEX_HOME;
  else process.env.OPENCODEX_HOME = previousHome;
  if (testDir) rmSync(testDir, { recursive: true, force: true });
});

function config(): OcxConfig {
  return {
    port: 10100,
    defaultProvider: "a",
    providers: { a: { adapter: "openai-chat", baseUrl: "https://a.example/v1", apiKey: "ka", models: ["m1"] } },
  };
}

describe("routing analytics (RI-03)", () => {
  test("classifies success, failure, cancellation and incomplete streams", async () => {
    appendUsageEntry(entry("r1", { timestamp: 1000, status: 200, durationMs: 100, firstOutputMs: 10 }));
    appendUsageEntry(entry("r2", { timestamp: 2000, status: 200, durationMs: 200, firstOutputMs: 30 }));
    appendUsageEntry(entry("r3", { timestamp: 3000, status: 429, durationMs: 300 }));
    appendUsageEntry(entry("r4", { timestamp: 4000, status: 499, durationMs: 50, closeReason: "client_cancel" }));
    appendUsageEntry(entry("r5", { timestamp: 5000, status: 200, durationMs: 400, terminalStatus: "incomplete" }));

    const result = await computeRoutingAnalytics({});
    expect(result.totalRequests).toBe(5);
    expect(result.successRate).toBe(0.4);
    expect(result.failureRate).toBe(0.4);
    expect(result.cancelledRate).toBe(0.2);
    expect(result.incompleteStreamRate).toBe(0.2);
    expect(result.cooldownTriggeringFailures).toBe(1);
    expect(result.confidence).toBe("low");
    expect(result.historyTruncated).toBe(false);
  });

  test("computes duration and TTFT percentiles with coverage", async () => {
    appendUsageEntry(entry("r1", { timestamp: 1, status: 200, durationMs: 100, firstOutputMs: 10 }));
    appendUsageEntry(entry("r2", { timestamp: 2, status: 200, durationMs: 200, firstOutputMs: 20 }));
    appendUsageEntry(entry("r3", { timestamp: 3, status: 200, durationMs: 300, firstOutputMs: 30 }));
    appendUsageEntry(entry("r4", { timestamp: 4, status: 200, durationMs: 400 }));

    const result = await computeRoutingAnalytics({});
    // Nearest-rank percentiles over [100,200,300,400]:
    expect(result.durationMs.p50).toBe(200);
    expect(result.durationMs.p95).toBe(400);
    expect(result.durationMs.p99).toBe(400);
    expect(result.durationMs.sampleCount).toBe(4);
    expect(result.firstOutputMs.p50).toBe(20);
    expect(result.firstOutputMs.sampleCount).toBe(3);
    expect(result.firstOutputMs.coverage).toBe(0.75);
  });

  test("fallback rate counts multi-attempt requests", async () => {
    appendUsageEntry(entry("r1", {
      timestamp: 1,
      status: 200,
      durationMs: 100,
      attempts: [
        { ordinal: 1, provider: "a", model: "m1", adapter: "openai-chat", status: 503, durationMs: 50, sendCount: 1, recoveryKinds: ["transient-5xx"], usageStatus: "unreported" },
        { ordinal: 2, provider: "a", model: "m1", adapter: "openai-chat", status: 200, durationMs: 50, sendCount: 1, recoveryKinds: [], usageStatus: "reported" },
      ],
    }));
    appendUsageEntry(entry("r2", { timestamp: 2, status: 200, durationMs: 100 }));

    const result = await computeRoutingAnalytics({});
    expect(result.fallbackRate).toBe(0.5);
    expect(result.totalAttempts).toBe(3);
    expect(result.averageAttemptsPerRequest).toBe(1.5);
  });

  test("preserves indexed legacy fallback metadata when row JSON has no attempts", async () => {
    appendUsageEntry(entry("legacy", { timestamp: 1, status: 200, durationMs: 100 }));
    await openRequestHistoryIndex();
    requestHistoryDb().query(
      "UPDATE requests SET attempt_count = 3, fallback = 1 WHERE request_id = ?",
    ).run("legacy");

    const result = await computeRoutingAnalytics({});
    expect(result.fallbackRate).toBe(1);
    expect(result.totalAttempts).toBe(3);
    expect(result.averageAttemptsPerRequest).toBe(3);
    expect(result.physicalAttempts).toBe(1);
  });

  test("breakdown groups by provider/model/account and profile", async () => {
    appendUsageEntry(entry("r1", {
      timestamp: 1,
      status: 200,
      durationMs: 100,
      apiKeyId: "key-a",
      routeDecision: {
        version: 1,
        decisionId: "a00000000001",
        createdAt: 1,
        requestedModel: "policy/fast",
        routeKind: "policy",
        profile: { id: "fast", revision: "abc123" },
        requirements: [],
        candidates: [{ provider: "a", model: "m1", eligible: true, exclusions: [] }],
        selected: { candidateIndex: 0, provider: "a", model: "m1", reason: "policy" },
      },
    }));
    appendUsageEntry(entry("r2", {
      timestamp: 2,
      status: 500,
      durationMs: 200,
      apiKeyId: "key-a",
      routeDecision: {
        version: 1,
        decisionId: "a00000000002",
        createdAt: 2,
        requestedModel: "policy/fast",
        routeKind: "policy",
        profile: { id: "fast", revision: "abc123" },
        requirements: [],
        candidates: [{ provider: "a", model: "m1", eligible: true, exclusions: [] }],
        selected: { candidateIndex: 0, provider: "a", model: "m1", reason: "policy" },
      },
    }));

    const result = await computeRoutingAnalytics({});
    expect(result.breakdown.length).toBe(1);
    expect(result.breakdown[0]).toMatchObject({
      provider: "a",
      model: "m1",
      accountRef: "key-a",
      profileId: "fast",
      requests: 2,
      successes: 1,
      failures: 1,
      successRate: 0.5,
    });
    expect(result.profileBreakdown).toEqual([
      { profileId: "fast", profileRevision: "abc123", requests: 2, successes: 1, failures: 1, fallbacks: 0, successRate: 0.5 },
    ]);
  });

  test("usage and price coverage are honest about unknown data", async () => {
    appendUsageEntry(entry("r1", { timestamp: 1, status: 200, durationMs: 100, usageStatus: "reported", usage: { inputTokens: 1000, outputTokens: 100 } }));
    appendUsageEntry(entry("r2", { timestamp: 2, status: 200, durationMs: 100, usageStatus: "unreported" }));

    const result = await computeRoutingAnalytics({});
    expect(result.usageCoverage).toBe(0.5);
    // Unknown price for provider "a": the estimate stays null, never zero.
    expect(result.estimatedCostUsdPerSuccessfulRequest).toBeNull();
    expect(result.priceCoverage).toBe(0);
  });

  test("prices combo requests from their physical attempts", async () => {
    const attempts = [
      attempt({
        provider: "openai-apikey",
        model: "gpt-5.6-sol",
        usage: { inputTokens: 1_000, outputTokens: 100 },
      }),
      attempt({
        ordinal: 2,
        provider: "openai-apikey",
        model: "gpt-5.6-sol",
        usage: { inputTokens: 2_000, outputTokens: 200 },
      }),
    ];
    const expected = estimateComboCost(attempts);
    expect(expected).not.toBeNull();
    appendUsageEntry(entry("combo-cost", {
      timestamp: 1,
      provider: "combo",
      model: "policy/fast",
      status: 200,
      durationMs: 100,
      attempts,
    }));

    const result = await computeRoutingAnalytics({});
    expect(result.estimatedCostUsdTotalSuccessful).toBeCloseTo(expected!.cost.total);
    expect(result.estimatedCostUsdPerSuccessfulRequest).toBeCloseTo(expected!.cost.total);
    expect(result.priceCoverage).toBe(1);
  });

  test("filters scope the analysis", async () => {
    appendUsageEntry(entry("r1", { timestamp: 1, status: 200, durationMs: 100, provider: "a" }));
    appendUsageEntry(entry("r2", { timestamp: 2, status: 200, durationMs: 100, provider: "b", model: "m2" }));

    const result = await computeRoutingAnalytics({ provider: "b" });
    expect(result.totalRequests).toBe(1);
    expect(result.breakdown[0]).toMatchObject({ provider: "b", model: "m2" });
  });

  test("expands valid physical attempts and reports quota telemetry without root double-counting", async () => {
    appendUsageEntry(entry("r1", {
      timestamp: 0,
      provider: "root-a",
      model: "root-model",
      status: 200,
      durationMs: 100,
      attempts: [
        attempt({
          provider: "p1",
          model: "m1",
          usage: { inputTokens: 100, outputTokens: 1, cacheReadInputTokens: 60, cacheCreationInputTokens: 10 },
        }),
        attempt({
          ordinal: 2,
          provider: "p2",
          model: "m2",
          sendCount: 2,
          recoveryKinds: ["connection-reset", "transient-5xx"],
          usageStatus: "estimated",
          usage: { inputTokens: 200, outputTokens: 1, cachedInputTokens: 150, cacheCreationInputTokens: 50, estimated: true },
        }),
      ],
    }));
    const rawRows = [
      {
        ...entry("r2", { timestamp: 3_600_000, provider: "root-b", model: "root-model", status: 200, durationMs: 100 }),
        attempts: [
          attempt({ provider: "p3", model: "m3", usageStatus: "unsupported" }),
          { ordinal: 2, provider: "bad", model: "bad" },
        ],
      },
      {
        ...entry("r3", {
          timestamp: 7_200_000,
          provider: "root-c",
          model: "root-model",
          status: 200,
          durationMs: 100,
          usage: { inputTokens: 50, outputTokens: 1, cachedInputTokens: 20 },
        }),
        attempts: [{ ordinal: 1, provider: "bad", model: "bad" }],
      },
      entry("r4", {
        timestamp: 10_800_000,
        provider: "root-d",
        model: "root-model",
        status: 200,
        durationMs: 100,
        usageStatus: "unreported",
      }),
    ];
    appendFileSync(usageLogPath(), `${rawRows.map(row => JSON.stringify(row)).join("\n")}\n`);

    const result = await computeRoutingAnalytics({});
    expect(result.totalRequests).toBe(4);
    expect(result.totalAttempts).toBe(5);
    expect(result.physicalAttempts).toBe(5);
    expect(result.physicalSends).toBe(6);
    expect(result.repeatedSendAttempts).toBe(1);
    expect(result.recoveryAttempts).toBe(1);
    expect(result.recoveryEvents).toBe(2);
    expect(result.recoveryRate).toBe(0.2);
    expect(result.fallbackRate).toBe(0.25);
    expect(result.comboFailoverRequests).toBe(0);
    expect(result.comboFailoverRate).toBe(0);
    expect(result.requestRatePerHour).toBeCloseTo(4 / 3);
    expect(result.attemptUsage).toEqual({
      inclusiveInputTokens: 350,
      rawInputTokens: 60,
      cacheReadInputTokens: 230,
      cacheWriteInputTokens: 60,
      rawInputShare: 60 / 350,
      cacheReadShare: 230 / 350,
      cacheWriteShare: 60 / 350,
    });
    expect(result.physicalUsageCoverage).toEqual({
      totalAttempts: 5,
      measuredAttempts: 3,
      reportedAttempts: 2,
      estimatedAttempts: 1,
      unreportedAttempts: 1,
      unsupportedAttempts: 1,
      ratio: 0.6,
      supportedRatio: 0.75,
    });
    expect(result.usageCoverage).toBe(0.6);
    expect(result.physicalBreakdown.map(row => `${row.provider}/${row.model}`).sort()).toEqual([
      "p1/m1", "p2/m2", "p3/m3", "root-c/root-model", "root-d/root-model",
    ]);
    expect(result.physicalBreakdown.find(row => row.provider === "root-b")).toBeUndefined();
    expect(result.physicalBreakdown.find(row => row.provider === "root-c")?.attemptUsage)
      .toMatchObject({ inclusiveInputTokens: 50, cacheReadInputTokens: 20 });
    expect(result.physicalBreakdown.find(row => row.provider === "p1")).toMatchObject({
      comboFailoverRequests: 0,
      comboFailoverRate: 0,
    });

    const rootFiltered = await computeRoutingAnalytics({ provider: "root-a" });
    expect(rootFiltered.totalRequests).toBe(1);
    expect(rootFiltered.physicalBreakdown.map(row => row.provider).sort()).toEqual(["p1", "p2"]);
  });

  test("uses one requested window for comparable overall and route request rates", async () => {
    appendUsageEntry(entry("r1", { timestamp: 1, status: 200, durationMs: 10 }));
    appendUsageEntry(entry("r2", { timestamp: 1_001, status: 200, durationMs: 10 }));

    const result = await computeRoutingAnalytics({ from: 0, to: 30 * 24 * 3_600_000 });
    expect(result.requestRatePerHour).toBeCloseTo(2 / (30 * 24));
    expect(result.physicalBreakdown[0]?.requestRatePerHour).toBeCloseTo(2 / (30 * 24));
  });

  test("does not add aggregate root usage when any attempt reports valid usage", async () => {
    appendUsageEntry(entry("root-fallback", {
      timestamp: 1,
      status: 200,
      durationMs: 10,
      usage: {
        inputTokens: 250,
        outputTokens: 1,
        cachedInputTokens: 10,
        cacheReadInputTokens: 200,
        cacheCreationInputTokens: 20,
      },
      attempts: [
        attempt({
          accountLogLabel: "oaaaaaa",
          usage: { inputTokens: 100, outputTokens: 1, cachedInputTokens: 60 },
        }),
        attempt({
          ordinal: 2,
          accountLogLabel: "obbbbbb",
          usageStatus: "unreported",
        }),
      ],
    }));

    const result = await computeRoutingAnalytics({});
    expect(result.attemptUsage).toEqual({
      inclusiveInputTokens: 100,
      rawInputTokens: 40,
      cacheReadInputTokens: 60,
      cacheWriteInputTokens: 0,
      rawInputShare: 0.4,
      cacheReadShare: 0.6,
      cacheWriteShare: 0,
    });
    expect(result.physicalUsageCoverage.measuredAttempts).toBe(1);
    expect(result.physicalBreakdown.map(row => row.accountRef).sort()).toEqual([
      "oaaaaaa",
      "obbbbbb",
    ]);
    expect(result.physicalBreakdown.find(row => row.accountRef === "obbbbbb")?.attemptUsage)
      .toMatchObject({ inclusiveInputTokens: 0, cacheReadInputTokens: 0 });
  });

  test("attributes a legacy root account to the final unlabeled physical attempt", async () => {
    appendUsageEntry(entry("combo-final", {
      timestamp: 1,
      provider: "combo",
      model: "requested-model",
      accountLogLabel: "oaaaaaa",
      status: 200,
      durationMs: 10,
      attempts: [
        attempt({ provider: "other", model: "m-other" }),
        attempt({ ordinal: 2, provider: "final", model: "m-final" }),
      ],
    }));
    appendUsageEntry(entry("same-route", {
      timestamp: 2,
      provider: "same",
      model: "m-same",
      accountLogLabel: "obbbbbb",
      status: 200,
      durationMs: 10,
      attempts: [
        attempt({ provider: "same", model: "m-same" }),
        attempt({ ordinal: 2, provider: "same", model: "m-same" }),
      ],
    }));

    const result = await computeRoutingAnalytics({});
    expect(result.physicalBreakdown.find(row => row.provider === "final")?.accountRef).toBe("oaaaaaa");
    expect(result.physicalBreakdown.find(row => row.provider === "other")?.accountRef).toBeUndefined();
    expect(result.physicalBreakdown.filter(row => row.provider === "same")
      .map(row => row.accountRef ?? "none").sort()).toEqual(["none", "obbbbbb"]);
  });

  test("counts recovery occurrences and excludes duplicate and locally answered attempts", async () => {
    const physical = attempt({
      sendCount: 4,
      recoveryKinds: ["connection-reset"],
      recoveryCount: 3,
    });
    const row = {
      ...entry("attempt-normalization", { timestamp: 1, status: 200, durationMs: 10 }),
      attempts: [
        physical,
        { ...physical, provider: "duplicate" },
        attempt({ ordinal: 2, provider: "local", sendCount: 0, locallyAnswered: true }),
      ],
    };
    appendFileSync(usageLogPath(), `${JSON.stringify(row)}\n`);

    const result = await computeRoutingAnalytics({});
    expect(result.physicalAttempts).toBe(1);
    expect(result.physicalSends).toBe(4);
    expect(result.recoveryAttempts).toBe(1);
    expect(result.recoveryEvents).toBe(3);
    expect(result.physicalBreakdown.map(route => route.provider)).toEqual(["a"]);
    expect(result.redAlerts.find(alert => alert.kind === "recovery")?.value).toBe(3);
  });

  test("keeps unsupported usage out of measured coverage", async () => {
    appendUsageEntry(entry("unsupported", {
      timestamp: 1,
      status: 200,
      durationMs: 10,
      attempts: [attempt({
        usageStatus: "unsupported",
        usage: { inputTokens: 100, outputTokens: 1 },
      })],
    }));

    const result = await computeRoutingAnalytics({});
    expect(result.attemptUsage).toMatchObject({
      inclusiveInputTokens: 0,
      rawInputTokens: 0,
      cacheReadInputTokens: 0,
      cacheWriteInputTokens: 0,
    });
    expect(result.physicalBreakdown[0]?.attemptUsage.inclusiveInputTokens).toBe(0);
    expect(result.physicalUsageCoverage).toMatchObject({
      totalAttempts: 1,
      measuredAttempts: 0,
      reportedAttempts: 0,
      unsupportedAttempts: 1,
      ratio: 0,
      supportedRatio: null,
    });
  });

  test("keeps unreported usage out of measured coverage even when counters are present", async () => {
    appendUsageEntry(entry("unreported", {
      timestamp: 1,
      status: 200,
      durationMs: 10,
      attempts: [attempt({
        usageStatus: "unreported",
        usage: { inputTokens: 100, outputTokens: 1 },
      })],
    }));

    const result = await computeRoutingAnalytics({});
    expect(result.attemptUsage.inclusiveInputTokens).toBe(0);
    expect(result.physicalUsageCoverage).toMatchObject({
      totalAttempts: 1,
      measuredAttempts: 0,
      reportedAttempts: 0,
      unreportedAttempts: 1,
      ratio: 0,
    });
  });

  test("rejects impossible cache decomposition from measured coverage", async () => {
    appendUsageEntry(entry("invalid-cache", {
      timestamp: 1,
      status: 200,
      durationMs: 10,
      attempts: [attempt({
        usageStatus: "reported",
        usage: {
          inputTokens: 100,
          outputTokens: 1,
          cacheReadInputTokens: 80,
          cacheCreationInputTokens: 30,
        },
      })],
    }));

    const result = await computeRoutingAnalytics({});
    expect(result.attemptUsage.inclusiveInputTokens).toBe(0);
    expect(result.physicalUsageCoverage).toMatchObject({
      totalAttempts: 1,
      measuredAttempts: 0,
      reportedAttempts: 0,
      unreportedAttempts: 1,
      ratio: 0,
    });
  });

  test("classifies only marked structural combo rows as combo failover", async () => {
    appendUsageEntry(entry("policy", {
      timestamp: 1,
      status: 200,
      durationMs: 10,
      provider: "policy",
      attempts: [attempt(), attempt({ ordinal: 2, provider: "b" })],
    }));
    appendUsageEntry(entry("combo-account-rotation", {
      timestamp: 2,
      status: 200,
      durationMs: 10,
      provider: "combo",
      attempts: [attempt(), attempt({ ordinal: 2, provider: "b" })],
    }));
    appendUsageEntry(entry("combo", {
      timestamp: 3,
      status: 200,
      durationMs: 10,
      provider: "combo",
      comboTargetAdvanced: true,
      attempts: [attempt(), attempt({ ordinal: 2, provider: "b" })],
    }));

    const result = await computeRoutingAnalytics({});
    expect(result.fallbackRate).toBe(1);
    expect(result.comboFailoverRequests).toBe(1);
    expect(result.redAlerts.filter(alert => alert.kind === "combo-failover").map(alert => alert.requestId))
      .toEqual(["combo"]);
  });

  test("uses canonical Codex and Claude surface groups", async () => {
    appendUsageEntry(entry("codex", { timestamp: 1, status: 200, durationMs: 10 }));
    appendUsageEntry(entry("claude", { timestamp: 2, status: 200, durationMs: 10, surface: "claude" }));
    appendUsageEntry(entry("desktop", { timestamp: 3, status: 200, durationMs: 10, surface: "claude-desktop" }));
    appendUsageEntry(entry("grok", { timestamp: 4, status: 200, durationMs: 10, surface: "grok" }));

    expect((await computeRoutingAnalytics({ surface: "all" })).totalRequests).toBe(4);
    expect((await computeRoutingAnalytics({ surface: "codex" })).totalRequests).toBe(1);
    expect((await computeRoutingAnalytics({ surface: "claude" })).totalRequests).toBe(2);
    expect((await computeRoutingAnalytics({ surface: "grok" })).totalRequests).toBe(1);
  });

  test("routing analytics API uses canonical surface groups", async () => {
    appendUsageEntry(entry("codex", { timestamp: 1, status: 200, durationMs: 10 }));
    appendUsageEntry(entry("claude", { timestamp: 2, status: 200, durationMs: 10, surface: "claude" }));
    appendUsageEntry(entry("desktop", { timestamp: 3, status: 200, durationMs: 10, surface: "claude-desktop" }));
    appendUsageEntry(entry("grok", { timestamp: 4, status: 200, durationMs: 10, surface: "grok" }));

    for (const [surface, expected] of [["codex", 1], ["claude", 2], ["grok", 1], ["unknown", 4]] as const) {
      const req = new ManagementRequest(`http://localhost/api/routing-analytics?surface=${surface}`, { method: "GET" });
      const response = await handleManagementAPI(req, new URL(req.url), config(), { refreshCodexCatalog: async () => {} });
      expect(response).not.toBeNull();
      expect(response!.status).toBe(200);
      const body = await response!.json() as { totalRequests?: number };
      expect(body.totalRequests).toBe(expected);
    }
  });

  test("isolates alert sequences by physical account and request fallback", async () => {
    const add = (
      requestId: string,
      timestamp: number,
      accountLogLabel: "oaaaaaa" | "obbbbbb",
      conversationId?: string,
    ) => appendUsageEntry(entry(requestId, {
      timestamp,
      status: 200,
      durationMs: 10,
      ...(conversationId ? { conversationId } : {}),
      attempts: [attempt({
        accountLogLabel,
        usage: { inputTokens: 120_000 + timestamp, outputTokens: 1 },
      })],
    }));
    add("a-first", 1, "oaaaaaa", "conversation");
    add("b-first", 2, "obbbbbb", "conversation");
    add("a-second", 3, "oaaaaaa", "conversation");
    add("isolated-1", 4, "oaaaaaa");
    add("isolated-2", 5, "oaaaaaa");

    const result = await computeRoutingAnalytics({});
    expect(result.redAlerts.filter(alert => alert.kind === "consecutive-high-raw-input"))
      .toEqual([expect.objectContaining({ requestId: "a-second", accountRef: "oaaaaaa" })]);
    expect(result.physicalBreakdown.map(route => route.accountRef).sort()).toEqual([
      "oaaaaaa",
      "obbbbbb",
    ]);
    expect(result.sequentialRoutes).toBe(4);
    expect(result.warmedRoutes).toBe(0);
  });

  test("emits route-sequential red alerts in timestamp and request-id order", async () => {
    const addAttemptRow = (
      requestId: string,
      timestamp: number,
      usage: PersistedUsageAttempt["usage"],
      overrides: Partial<PersistedUsageEntry> = {},
    ) => appendUsageEntry(entry(requestId, {
      timestamp,
      status: 200,
      durationMs: 10,
      conversationId: "conversation",
      attempts: [attempt({ usage })],
      ...overrides,
    }));

    addAttemptRow("later", 2, { inputTokens: 120_000, outputTokens: 1 });
    addAttemptRow("earlier", 1, { inputTokens: 110_000, outputTokens: 1 });
    addAttemptRow("warm", 3, { inputTokens: 1_000, outputTokens: 1, cacheReadInputTokens: 950 });
    addAttemptRow("degrade", 4, { inputTokens: 12_000, outputTokens: 1, cacheReadInputTokens: 9_000, cacheCreationInputTokens: 2_000 });
    addAttemptRow("bad-cache", 5, { inputTokens: 20_000, outputTokens: 1, cacheReadInputTokens: 8_000, cacheCreationInputTokens: 11_000 });
    addAttemptRow("missing-1", 6, { inputTokens: 130_000, outputTokens: 1 }, { conversationId: undefined });
    addAttemptRow("missing-2", 7, { inputTokens: 140_000, outputTokens: 1 }, { conversationId: undefined });
    appendUsageEntry(entry("operational", {
      timestamp: 8,
      status: 200,
      durationMs: 10,
      attempts: [attempt({ sendCount: 2, recoveryKinds: ["connection-reset"] })],
    }));

    const result = await computeRoutingAnalytics({});
    const alertsFor = (requestId: string) => result.redAlerts
      .filter(alert => alert.requestId === requestId)
      .map(alert => alert.kind);
    expect(alertsFor("later")).toEqual(["consecutive-high-raw-input"]);
    expect(alertsFor("bad-cache").sort()).toEqual([
      "falling-cache-read",
      "high-cache-write-after-warmup",
      "low-cache-read-share-after-warmup",
    ]);
    expect(alertsFor("missing-2")).toEqual([]);
    expect(alertsFor("operational").sort()).toEqual(["recovery", "repeated-send"]);
    expect(result.sequentialRoutes).toBe(4);
    expect(result.warmedRoutes).toBe(1);
    expect(result.redAlertsPartial).toBe(false);
  });

  test("bounds red alerts and marks capped history partial", async () => {
    for (let index = 0; index < 105; index++) {
      appendUsageEntry(entry(`r${String(index).padStart(3, "0")}`, {
        timestamp: index,
        status: 200,
        durationMs: 10,
        attempts: [attempt({ recoveryKinds: ["connection-reset"] })],
      }));
    }

    const bounded = await computeRoutingAnalytics({});
    expect(bounded.redAlerts).toHaveLength(100);
    expect(bounded.redAlertsPartial).toBe(true);
    const capped = await computeRoutingAnalytics({}, { maxRows: 10 });
    expect(capped.historyTruncated).toBe(true);
    expect(capped.redAlertsPartial).toBe(true);
  });

  test("explicit truncated-history indicator when the cap is hit", async () => {
    for (let index = 0; index < 12; index++) {
      appendUsageEntry(entry(`r${index}`, { timestamp: index, status: 200, durationMs: 10 }));
    }
    const result = await computeRoutingAnalytics({}, { maxRows: 10 });
    expect(result.scannedRows).toBe(10);
    expect(result.historyTruncated).toBe(true);
  });

  test("API endpoint returns the analytics payload", async () => {
    appendUsageEntry(entry("r1", { timestamp: 1, status: 200, durationMs: 100 }));
    const req = new ManagementRequest("http://localhost/api/routing-analytics", { method: "GET" });
    const response = await handleManagementAPI(req, new URL(req.url), config(), { refreshCodexCatalog: async () => {} });
    expect(response).not.toBeNull();
    expect(response!.status).toBe(200);
    const body = await response!.json() as { totalRequests?: number; successRate?: number | null };
    expect(body.totalRequests).toBe(1);
    expect(body.successRate).toBe(1);
  });

  test("counts cooldownTriggeringFailures for non-4xx failures with recovery attempts", async () => {
    appendUsageEntry(
      entry("r1", {
        timestamp: 1,
        status: 503,
        durationMs: 100,
        attempts: [
          {
            ordinal: 1,
            provider: "a",
            model: "m1",
            adapter: "openai-chat",
            status: 503,
            durationMs: 100,
            sendCount: 1,
            recoveryKinds: ["rate-limit-429"],
            usageStatus: "unreported",
          },
        ],
      }),
    );
    const result = await computeRoutingAnalytics({});
    expect(result.cooldownTriggeringFailures).toBe(1);
  });

  test("ignores malformed attempts while preserving explicit 429 classification", async () => {
    appendUsageEntry(entry("baseline", { timestamp: 1, status: 200, durationMs: 10 }));
    const historicalRows = [
      {
        ...entry("missing-recovery-kinds", { timestamp: 2, status: 503, durationMs: 20 }),
        attempts: [{ ordinal: 1 }],
      },
      {
        ...entry("malformed-attempts", { timestamp: 3, status: 503, durationMs: 30 }),
        attempts: { recoveryKinds: ["rate-limit-429"] },
      },
      {
        ...entry("malformed-recovery-kinds", { timestamp: 4, status: 503, durationMs: 40 }),
        attempts: [null, { recoveryKinds: "rate-limit-429" }, { recoveryKinds: [null, 42, "unknown"] }],
      },
      {
        ...entry("malformed-429", { timestamp: 5, status: 429, durationMs: 50 }),
        attempts: { recoveryKinds: ["unknown"] },
      },
    ];
    appendFileSync(usageLogPath(), `${historicalRows.map(row => JSON.stringify(row)).join("\n")}\n`);

    const result = await computeRoutingAnalytics({});
    expect(result.totalRequests).toBe(5);
    expect(result.cooldownTriggeringFailures).toBe(1);
  });

  test("routing analytics API applies usage calendar ranges in the server timezone", async () => {
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    start.setDate(start.getDate() - 6);
    appendUsageEntry(entry("outside", { timestamp: start.getTime() - 1, status: 200, durationMs: 10 }));
    appendUsageEntry(entry("inside", { timestamp: start.getTime(), status: 200, durationMs: 10 }));

    const req = new ManagementRequest("http://localhost/api/routing-analytics?range=7d", { method: "GET" });
    const response = await handleManagementAPI(req, new URL(req.url), config(), { refreshCodexCatalog: async () => {} });
    expect(response?.status).toBe(200);
    const body = await response!.json() as { totalRequests?: number };
    expect(body.totalRequests).toBe(1);
  });

  test("routing analytics API returns 400 for invalid from/to/range/limit", async () => {
    const cases = [
      { query: "from=abc", code: "invalid_from" },
      { query: "to=xyz", code: "invalid_to" },
      { query: "from=10&to=5", code: "invalid_range" },
      { query: "range=quarter", code: "invalid_range" },
      { query: "limit=0", code: "invalid_limit" },
    ] as const;
    for (const { query, code } of cases) {
      const req = new ManagementRequest(`http://localhost/api/routing-analytics?${query}`, { method: "GET" });
      const response = await handleManagementAPI(req, new URL(req.url), config(), { refreshCodexCatalog: async () => {} });
      expect(response).not.toBeNull();
      expect(response!.status).toBe(400);
      const body = await response!.json() as { error?: { code?: string } };
      expect(body.error?.code).toBe(code);
    }
  });
});
