import { expect, test } from "bun:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { en } from "../src/i18n/en";
import { interpolate, type TFn } from "../src/i18n/shared";
import { fetchRoutingAnalytics, normalizeInjectionSelection } from "../src/pages/dashboard-core-poll";
import { DashboardQuotaObservability, UsageQuotaObservability } from "../src/pages/dashboard-quota-observability";
import type { RoutingAnalyticsResult } from "../src/pages/dashboard-shared";
import { PROJECT_CONFIG_DIAGNOSTICS_POLL_MS, beginPollEpoch, beginPollEpochs } from "../src/startup-health-ui";

test("project-config diagnostics poll cadence is owned by the shared constant", () => {
  expect(PROJECT_CONFIG_DIAGNOSTICS_POLL_MS).toBe(30_000);
});

test("dashboard poll epochs share beginPollEpoch", () => {
  const refs = {
    settingsRequest: { current: 0 },
    settingsMutation: { current: 2 },
    shadowRequest: { current: 0 },
    shadowMutation: { current: 4 },
  };
  const paired = beginPollEpochs(refs);
  expect(paired.settings).toEqual({ request: 1, mutation: 2 });
  expect(paired.shadow).toEqual({ request: 1, mutation: 4 });
  expect(beginPollEpoch(refs.settingsRequest, refs.settingsMutation)).toEqual({ request: 2, mutation: 2 });
});

test("Dashboard wires a single project-config diagnostics owner outside the settings poll", async () => {
  const core = await Bun.file(new URL("../src/pages/dashboard-core-poll.ts", import.meta.url)).text();
  const hook = await Bun.file(new URL("../src/pages/use-dashboard-data.ts", import.meta.url)).text();
  // Diagnostics live in their own fetcher + client-resource poll, not inside core health.
  expect(core.match(/diagnostics\/project-config/g)?.length ?? 0).toBe(1);
  expect(hook).toContain("fetchProjectConfigDiagnostics");
  expect(hook).toContain("PROJECT_CONFIG_DIAGNOSTICS_POLL_MS");
  // Overview poll must not own the diagnostics endpoint.
  const overviewFnStart = core.indexOf("export async function fetchDashboardOverview");
  expect(overviewFnStart).toBeGreaterThan(-1);
  const overviewBody = core.slice(overviewFnStart, core.indexOf("export async function fetchDashboardMultiAgent"));
  expect(overviewBody).not.toContain("diagnostics/project-config");
});

test("Dashboard usage polling cannot delay core health and settings", async () => {
  const core = await Bun.file(new URL("../src/pages/dashboard-core-poll.ts", import.meta.url)).text();
  const hook = await Bun.file(new URL("../src/pages/use-dashboard-data.ts", import.meta.url)).text();
  const overviewFnStart = core.indexOf("export async function fetchDashboardOverview");
  const usageFnStart = core.indexOf("export async function fetchDashboardUsage");
  const sidecarsFnStart = core.indexOf("export async function fetchDashboardSidecars");
  expect(overviewFnStart).toBeGreaterThan(-1);
  expect(usageFnStart).toBeGreaterThan(-1);
  expect(sidecarsFnStart).toBeGreaterThan(-1);
  expect(core.slice(overviewFnStart, core.indexOf("export async function fetchDashboardMultiAgent"))).not.toContain("/api/usage?range=30d");
  expect(core.slice(overviewFnStart, core.indexOf("export async function fetchDashboardMultiAgent"))).not.toContain("/api/sidecar-settings");
  expect(core.slice(overviewFnStart, core.indexOf("export async function fetchDashboardMultiAgent"))).not.toContain("/api/shadow-call-settings");
  expect(core.slice(sidecarsFnStart)).toContain("/api/sidecar-settings");
  expect(hook).toContain("usageSummary30dResourceKey(apiBase)");
  expect(hook).toContain("dashboard-sidecars:${apiBase}");
  expect(hook).toContain("dashboard-overview:${apiBase}");
  expect(hook).toContain("fetchDashboardUsage(apiBase, signal)");
  expect(hook).toContain("fetchDashboardSidecars");
  expect(hook).toContain("fetchDashboardOverview");
  const usageResourceStart = hook.indexOf("const usagePoll = useKeyedClientResource");
  const quotaResourceStart = hook.indexOf("const quotaAnalyticsPoll = useKeyedClientResource");
  expect(usageResourceStart).toBeGreaterThan(-1);
  expect(quotaResourceStart).toBeGreaterThan(usageResourceStart);
  expect(hook.slice(usageResourceStart, quotaResourceStart)).not.toContain("pollMs:");
});

test("Dashboard quota analytics poll is independent of health and settings", async () => {
  const core = await Bun.file(new URL("../src/pages/dashboard-core-poll.ts", import.meta.url)).text();
  const hook = await Bun.file(new URL("../src/pages/use-dashboard-data.ts", import.meta.url)).text();
  const panels = await Bun.file(new URL("../src/pages/dashboard-overview-panels.tsx", import.meta.url)).text();
  const quota = await Bun.file(new URL("../src/pages/dashboard-quota-observability.tsx", import.meta.url)).text();
  const overviewStart = core.indexOf("export async function fetchDashboardOverview");
  const multiStart = core.indexOf("export async function fetchDashboardMultiAgent");

  expect(core).toContain("export async function fetchRoutingAnalytics");
  expect(core).toContain("data.repeatedSendAttempts");
  expect(core).toContain("data.sequentialRoutes");
  expect(core).toContain("data.warmedRoutes");
  expect(core).toContain("value.repeatedSendAttempts");
  expect(core).toContain("/api/routing-analytics");
  expect(core.slice(overviewStart, multiStart)).not.toContain("/api/routing-analytics");
  expect(hook).toContain("dashboard-routing-analytics:${apiBase}");
  expect(hook).toContain("quotaAnalyticsPoll");
  expect(hook).toContain('enabled: selectedSection === "overview"');
  expect(hook).toContain('range: "30d"');
  expect(hook).toContain("!quotaAnalyticsPoll.lastAttemptOk");
  expect(panels).toContain("<DashboardQuotaObservability");
  expect(quota).toContain("physicalUsageCoverage.ratio");
  expect(quota).toContain("data.recoveryEvents");
  expect(quota).toContain("data.repeatedSendAttempts");
  expect(quota).toContain("data.warmedRoutes < data.sequentialRoutes");
  expect(quota).toContain("data.redAlerts.slice(0, 3)");
});

function routingAnalyticsPayload(): RoutingAnalyticsResult {
  const attemptUsage = {
    inclusiveInputTokens: 10,
    rawInputTokens: 4,
    cacheReadInputTokens: 6,
    cacheWriteInputTokens: 0,
    rawInputShare: 0.4,
    cacheReadShare: 0.6,
    cacheWriteShare: 0,
  };
  const coverage = {
    totalAttempts: 1,
    measuredAttempts: 1,
    reportedAttempts: 1,
    estimatedAttempts: 0,
    unreportedAttempts: 0,
    unsupportedAttempts: 0,
    ratio: 1,
    supportedRatio: 1,
  };
  return {
    generatedAt: 1,
    totalRequests: 1,
    physicalSends: 2,
    repeatedSendAttempts: 1,
    recoveryEvents: 1,
    requestRatePerHour: 1.5,
    attemptUsage,
    physicalUsageCoverage: coverage,
    physicalBreakdown: [{
      provider: "anthropic",
      model: "claude-test",
      accountRef: "oa-test",
      requests: 1,
      physicalAttempts: 1,
      physicalSends: 2,
      repeatedSendAttempts: 1,
      recoveryEvents: 1,
      comboFailoverRequests: 0,
      requestRatePerHour: 1.5,
      attemptUsage: { ...attemptUsage },
      usageCoverage: { ...coverage },
    }],
    redAlerts: [{
      kind: "recovery",
      requestId: "request-1",
      timestamp: 1,
      conversationId: "conversation-1",
      provider: "anthropic",
      model: "claude-test",
      accountRef: "oa-test",
      attemptOrdinal: 1,
      value: 1,
      previousValue: 0,
      recoveryKinds: ["network-error"],
    }],
    redAlertsPartial: false,
    sequentialRoutes: 1,
    warmedRoutes: 1,
  };
}

async function fetchRoutingAnalyticsPayload(payload: unknown): Promise<RoutingAnalyticsResult> {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async () => ({
    ok: true,
    status: 200,
    json: async () => payload,
  })) as unknown as typeof fetch;
  try {
    return await fetchRoutingAnalytics("http://test", new AbortController().signal);
  } finally {
    globalThis.fetch = originalFetch;
  }
}

test("fetchRoutingAnalytics accepts recursively valid HTTP-200 payloads and preserves null shares", async () => {
  const valid = routingAnalyticsPayload();
  expect(await fetchRoutingAnalyticsPayload(valid)).toBe(valid);

  const empty: RoutingAnalyticsResult = {
    ...valid,
    totalRequests: 0,
    physicalSends: 0,
    repeatedSendAttempts: 0,
    recoveryEvents: 0,
    requestRatePerHour: null,
    attemptUsage: {
      inclusiveInputTokens: 0,
      rawInputTokens: 0,
      cacheReadInputTokens: 0,
      cacheWriteInputTokens: 0,
      rawInputShare: null,
      cacheReadShare: null,
      cacheWriteShare: null,
    },
    physicalUsageCoverage: {
      totalAttempts: 0,
      measuredAttempts: 0,
      reportedAttempts: 0,
      estimatedAttempts: 0,
      unreportedAttempts: 0,
      unsupportedAttempts: 0,
      ratio: null,
      supportedRatio: null,
    },
    physicalBreakdown: [],
    redAlerts: [],
    sequentialRoutes: 0,
    warmedRoutes: 0,
  };
  const accepted = await fetchRoutingAnalyticsPayload(empty);
  expect(accepted).toBe(empty);
  expect(accepted.attemptUsage.rawInputShare).toBeNull();
  expect(accepted.physicalUsageCoverage.ratio).toBeNull();
});

test("fetchRoutingAnalytics rejects malformed HTTP-200 payloads recursively", async () => {
  const valid = routingAnalyticsPayload();
  const row = valid.physicalBreakdown[0]!;
  const alert = valid.redAlerts[0]!;
  const malformed: Array<[string, unknown]> = [
    ["NaN scalar", { ...valid, requestRatePerHour: Number.NaN }],
    ["infinite usage", { ...valid, attemptUsage: { ...valid.attemptUsage, inclusiveInputTokens: Number.POSITIVE_INFINITY } }],
    ["negative counter", { ...valid, physicalSends: -1 }],
    ["invalid share", { ...valid, attemptUsage: { ...valid.attemptUsage, rawInputShare: 1.1 } }],
    ["invalid null share", { ...valid, attemptUsage: { ...valid.attemptUsage, cacheReadShare: null } }],
    ["coverage counter invariant", {
      ...valid,
      physicalUsageCoverage: { ...valid.physicalUsageCoverage, measuredAttempts: 0 },
    }],
    ["coverage ratio invariant", {
      ...valid,
      physicalUsageCoverage: { ...valid.physicalUsageCoverage, ratio: null },
    }],
    ["malformed physical row", {
      ...valid,
      physicalBreakdown: [{ ...row, recoveryEvents: -1 }],
    }],
    ["malformed row optional", {
      ...valid,
      physicalBreakdown: [{ ...row, accountRef: 42 }],
    }],
    ["unknown alert kind", {
      ...valid,
      redAlerts: [{ ...alert, kind: "unknown-alert" }],
    }],
    ["malformed alert optional", {
      ...valid,
      redAlerts: [{ ...alert, conversationId: null }],
    }],
    ["infinite alert value", {
      ...valid,
      redAlerts: [{ ...alert, value: Number.NEGATIVE_INFINITY }],
    }],
  ];

  for (const [name, payload] of malformed) {
    try {
      await fetchRoutingAnalyticsPayload(payload);
      throw new Error(`accepted malformed case: ${name}`);
    } catch (error) {
      expect(error).toEqual(new Error("invalid routing analytics response"));
    }
  }
});

test("quota state distinguishes physical-attempt telemetry gaps and historical warnings", () => {
  const t: TFn = (key, vars) => interpolate(en[key], vars);
  const coverage = {
    totalAttempts: 1,
    measuredAttempts: 1,
    reportedAttempts: 1,
    estimatedAttempts: 0,
    unreportedAttempts: 0,
    unsupportedAttempts: 0,
    ratio: 1,
    supportedRatio: 1,
  };
  const data: RoutingAnalyticsResult = {
    generatedAt: Date.UTC(2026, 7, 31, 0, 0),
    totalRequests: 1,
    physicalSends: 1,
    repeatedSendAttempts: 0,
    recoveryEvents: 0,
    requestRatePerHour: 1,
    attemptUsage: {
      inclusiveInputTokens: 1,
      rawInputTokens: 1,
      cacheReadInputTokens: 0,
      cacheWriteInputTokens: 0,
      rawInputShare: 1,
      cacheReadShare: 0,
      cacheWriteShare: 0,
    },
    physicalUsageCoverage: coverage,
    physicalBreakdown: [],
    redAlerts: [],
    redAlertsPartial: false,
    sequentialRoutes: 1,
    warmedRoutes: 1,
  };
  const render = (next: RoutingAnalyticsResult, error = false) => renderToStaticMarkup(createElement(
    UsageQuotaObservability,
    { data: next, loading: false, error, locale: "en", t },
  ));

  expect(render({
    ...data,
    physicalUsageCoverage: { ...coverage, totalAttempts: 0, measuredAttempts: 0, reportedAttempts: 0, ratio: null, supportedRatio: null },
  })).toContain("none sent an upstream inference");
  expect(render({
    ...data,
    physicalUsageCoverage: { ...coverage, measuredAttempts: 0, reportedAttempts: 0, unsupportedAttempts: 1, ratio: 0, supportedRatio: null },
  })).toContain("unsupported for 1 physical attempt");
  expect(render({
    ...data,
    physicalUsageCoverage: { ...coverage, measuredAttempts: 0, reportedAttempts: 0, unreportedAttempts: 1, ratio: 0, supportedRatio: 0 },
  })).toContain("missing for 1 physical attempt");
  expect(render({
    ...data,
    physicalUsageCoverage: {
      ...coverage,
      totalAttempts: 3,
      measuredAttempts: 1,
      unreportedAttempts: 1,
      unsupportedAttempts: 1,
      ratio: 1 / 3,
      supportedRatio: 1 / 2,
    },
  })).toContain("covers 1 of 3 physical attempts (1 unsupported, 1 unreported)");

  const alert = {
    kind: "repeated-send" as const,
    requestId: "request-1",
    timestamp: 1,
    provider: "anthropic",
    model: "claude-test",
    attemptOrdinal: 1,
    value: 2,
  };
  const historical = render({ ...data, redAlerts: [alert] });
  expect(historical).toContain("1 quota warning occurrence(s) in scanned history");
  expect(historical).not.toContain("active quota warning");

  const invalidAlertTime = render({
    ...data,
    redAlerts: [{ ...alert, timestamp: Number.MAX_SAFE_INTEGER }],
  });
  expect(invalidAlertTime).toContain("<td>—</td>");
  expect(invalidAlertTime).not.toContain("Invalid Date");
  const invalidStaleTime = render({ ...data, generatedAt: Number.MAX_SAFE_INTEGER }, true);
  expect(invalidStaleTime).toContain("The latest refresh failed. The values below may be stale.");
  expect(invalidStaleTime).not.toContain("Invalid Date");

  const partialAlert = render({ ...data, redAlerts: [alert], redAlertsPartial: true });
  expect(partialAlert).toContain("notice notice-err");
  expect(partialAlert).toContain("Results are partial because request history or alert output reached its limit.");
  const alertWithPartialTelemetry = render({
    ...data,
    redAlerts: [alert],
    physicalUsageCoverage: {
      ...coverage,
      totalAttempts: 2,
      unreportedAttempts: 1,
      ratio: 0.5,
      supportedRatio: 0.5,
    },
  });
  expect(alertWithPartialTelemetry).toContain("notice notice-err");
  expect(alertWithPartialTelemetry).toContain("1 quota warning occurrence(s) in scanned history");

  const stale = render(data, true);
  expect(stale).toContain("Latest refresh failed. Values below are stale from");
  expect(stale).toContain("2026");
  expect(stale).toContain("Inclusive input");
  expect(stale).not.toContain("No quota warning occurrences in scanned history.");

  const warming = render({ ...data, sequentialRoutes: 2, warmedRoutes: 1 });
  expect(warming).toContain("notice notice-warn");
  expect(warming).toContain("Route is warming up");
  expect(render({ ...data, sequentialRoutes: 2, warmedRoutes: 2 })).toContain("notice notice-ok");

  const dashboard = renderToStaticMarkup(createElement(DashboardQuotaObservability, {
    data: {
      ...data,
      redAlerts: Array.from({ length: 4 }, (_, index) => ({ ...alert, requestId: `request-${index + 1}` })),
    },
    loading: false,
    error: false,
    locale: "en",
    t,
  }));
  expect(dashboard).toContain("Showing 3 of 4 returned warning occurrences.");
  expect(dashboard).toContain("request-3");
  expect(dashboard).not.toContain("request-4");
});

test("Dashboard interactive controls load independently of health/providers", async () => {
  const core = await Bun.file(new URL("../src/pages/dashboard-core-poll.ts", import.meta.url)).text();
  const sidecarsFnStart = core.indexOf("export async function fetchDashboardSidecars");
  const settingsFnStart = core.indexOf("export async function fetchDashboardSettings");
  expect(sidecarsFnStart).toBeGreaterThan(-1);
  expect(settingsFnStart).toBeGreaterThan(-1);
  const sidecarsBody = core.slice(sidecarsFnStart, settingsFnStart > sidecarsFnStart ? settingsFnStart : undefined);
  expect(sidecarsBody).toContain("/api/sidecar-settings");
  expect(sidecarsBody).toContain("/api/shadow-call-settings");
  expect(sidecarsBody).not.toContain("/api/settings");
  expect(sidecarsBody).not.toContain("/healthz");
  expect(sidecarsBody).not.toContain("/api/providers");
  expect(sidecarsBody).not.toContain("/api/usage");
});

test("Dashboard overview status widgets do not wait on injection-model", async () => {
  const core = await Bun.file(new URL("../src/pages/dashboard-core-poll.ts", import.meta.url)).text();
  const hook = await Bun.file(new URL("../src/pages/use-dashboard-data.ts", import.meta.url)).text();
  const overviewStart = core.indexOf("export async function fetchDashboardOverview");
  const multiStart = core.indexOf("export async function fetchDashboardMultiAgent");
  expect(overviewStart).toBeGreaterThan(-1);
  expect(multiStart).toBeGreaterThan(overviewStart);
  const overviewBody = core.slice(overviewStart, multiStart);
  expect(overviewBody).toContain("/healthz");
  expect(overviewBody).toContain("/api/providers");
  expect(overviewBody).not.toContain("/api/injection-model");
  expect(overviewBody).not.toContain("/api/v2");
  expect(overviewBody).not.toContain("/api/effort-caps");
  expect(core.slice(multiStart)).toContain("/api/injection-model");
  expect(hook).toContain("dashboard-overview:${apiBase}");
  expect(hook).toContain("dashboard-multi-agent:${apiBase}");
  expect(hook).toContain("enabled: overviewReady");
});

test("Dashboard MA mode and sidecars do not wait on settings or injection", async () => {
  const core = await Bun.file(new URL("../src/pages/dashboard-core-poll.ts", import.meta.url)).text();
  const hook = await Bun.file(new URL("../src/pages/use-dashboard-data.ts", import.meta.url)).text();
  const maStart = core.indexOf("export async function fetchDashboardMaMode");
  const sidecarsStart = core.indexOf("export async function fetchDashboardSidecars");
  const settingsStart = core.indexOf("export async function fetchDashboardSettings");
  const overviewStart = core.indexOf("export async function fetchDashboardOverview");
  expect(maStart).toBeGreaterThan(-1);
  expect(sidecarsStart).toBeGreaterThan(-1);
  expect(settingsStart).toBeGreaterThan(-1);
  const maBody = core.slice(maStart, overviewStart > maStart ? overviewStart : undefined);
  const sidecarsBody = core.slice(sidecarsStart, settingsStart > sidecarsStart ? settingsStart : undefined);
  expect(maBody).toContain("/api/v2");
  expect(maBody).not.toContain("/api/injection-model");
  expect(maBody).not.toContain("/api/settings");
  expect(sidecarsBody).toContain("/api/sidecar-settings");
  expect(sidecarsBody).not.toContain("/api/settings");
  expect(hook).toContain("dashboard-ma-mode:${apiBase}");
  expect(hook).toContain("dashboard-sidecars:${apiBase}");
  expect(hook).toContain("dashboard-settings:${apiBase}");
  expect(hook).not.toContain("dashboard-controls:${apiBase}");
});

test("Dashboard workspace pane is a labelled section, not a nested main landmark", async () => {
  const src = await Bun.file(new URL("../src/pages/Dashboard.tsx", import.meta.url)).text();
  expect(src).toContain("dashboard-workspace-main");
  expect(src).toContain("dash.workspace.sections");
  expect(src).not.toMatch(/<main\b[^>]*dashboard-workspace-main/);
  expect(src).toMatch(/<(section)\b[^>]*dashboard-workspace-main/);
});

test("native Codex subagent defaults stay separate from OpenCodex guidance", async () => {
  const core = await Bun.file(new URL("../src/pages/dashboard-core-poll.ts", import.meta.url)).text();
  // The controls live on the Subagents tab now; the Dashboard keeps only a link to them.
  const sections = await Bun.file(new URL("../src/components/subagents-workspace/SubagentDelegationSection.tsx", import.meta.url)).text();
  const head = await Bun.file(new URL("../src/pages/dashboard-overview-head.tsx", import.meta.url)).text();
  expect(core).toContain("syncCodexSubagentDefaults: data.syncCodexSubagentDefaults === true");
  expect(sections).toContain("onSave({ syncCodexSubagentDefaults: !syncCodexDefaults })");
  expect(sections).toContain("disabled={saving || !model}");
  expect(sections).not.toContain("saving || !guidanceEnabled");
  expect(sections).not.toContain("dash.injectionActive");
  // Two promises this copy must keep, asserted by meaning rather than by an exact
  // sentence so the wording can be made plainer without breaking the contract:
  // the off state is explained, and it does not clobber hand-written [agents] settings.
  expect(en["dash.syncCodexSubagentDefaultsHint"]).toMatch(/\boff\b/i);
  expect(en["dash.syncCodexSubagentDefaultsHint"]).toMatch(/\[agents\][^.]*\b(left alone|preserved|not overwritten|untouched)\b/i);
  expect(en["dash.multiAgentGuidanceHint"]).not.toContain("proactive");
  expect(head).toContain("models.v2Mode_");
});

test("injection writes consume the server's model-clear normalization", () => {
  expect(normalizeInjectionSelection({
    multiAgentGuidanceEnabled: true,
    syncCodexSubagentDefaults: false,
    model: null,
    effort: null,
  })).toEqual({
    multiAgentGuidanceEnabled: true,
    syncCodexSubagentDefaults: false,
    injectionModel: "",
    injectionEffort: "",
  });
});

test("Dashboard sync surfaces native subagent default warnings", async () => {
  const sections = await Bun.file(new URL("../src/pages/dashboard-overview-sections.tsx", import.meta.url)).text();
  expect(sections).toContain("syncResult.nativeSubagentDefaultsWarning");
  expect(sections).toContain('"notice-warn"');
  expect(sections).toContain("<IconAlert />");
});

test("fetchStartupHealth does not map abort into a sticky error status", async () => {
  const { fetchStartupHealth } = await import("../src/pages/dashboard-core-poll");
  const controller = new AbortController();
  controller.abort();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (_input: RequestInfo | URL, init?: RequestInit) => {
    if (init?.signal?.aborted) {
      throw new DOMException("The operation was aborted.", "AbortError");
    }
    return Response.json({ status: "protected", diagnosticStale: false });
  }) as typeof fetch;
  try {
    await expect(fetchStartupHealth("http://test", controller.signal)).rejects.toMatchObject({ name: "AbortError" });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

// The chip used to sit on the server's conservative placeholder until the next 30s tick, which is
// why an unrelated action (refresh quota, tab hop) looked like the thing that fixed it. The probe
// has to carry `stale` through so the caller can re-ask in seconds.
test("fetchStartupHealth reports whether the server answer is still being resolved", async () => {
  const { fetchStartupHealth } = await import("../src/pages/dashboard-core-poll");
  const { probeNeedsFastRetry } = await import("../src/startup-health-ui");
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = (async () => Response.json({ status: "at-risk", diagnosticStale: true })) as typeof fetch;
    const stale = await fetchStartupHealth("http://test", new AbortController().signal);
    expect(stale).toEqual({ status: "at-risk", stale: true });
    expect(probeNeedsFastRetry(stale)).toBe(true);

    globalThis.fetch = (async () => Response.json({ status: "protected", diagnosticStale: false })) as typeof fetch;
    const settled = await fetchStartupHealth("http://test", new AbortController().signal);
    expect(settled).toEqual({ status: "protected", stale: false });
    expect(probeNeedsFastRetry(settled)).toBe(false);

    // A hard failure is the normal poll's job; re-asking every 2s would just hammer it.
    globalThis.fetch = (async () => new Response("nope", { status: 503 })) as typeof fetch;
    const failed = await fetchStartupHealth("http://test", new AbortController().signal);
    expect(failed).toEqual({ status: "error", stale: false });
    expect(probeNeedsFastRetry(failed)).toBe(false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
