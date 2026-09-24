#!/usr/bin/env bun

import { randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { createServer, createConnection } from "node:net";
import { hostname, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { removeTreeWithRetry } from "../tests/helpers/remove-tree";

const SCHEMA_VERSION = 1;
const HARD_SEND_CAP = 24;
const PLANNED_SENDS = 11;
const REQUEST_TIMEOUT_MS = 90_000;
const WARM_READ_SHARE_MIN = 0.9;
const READ_SHARE_DELTA_MAX = 0.02;
const EXCESS_WRITE_MAX = 10_000;
const LIVE_OPT_IN = "I_ACCEPT_ANTHROPIC_QUOTA_USAGE";
const ANTHROPIC_ORIGIN = "https://api.anthropic.com";
const API_VERSION = "2023-06-01";

type UsageMetrics = {
  totalInput: number;
  rawInput: number;
  cacheRead: number;
  cacheWrite: number;
  cacheReadShare: number;
};

type Transition = {
  classification: "natural-cold-cache-write" | "stable-warmed" | "quota-burn-prefix-rewrite";
  newInputGrowth: number;
  excessCacheWrite: number;
  retainedPrefixReuse: number | null;
};

type PhysicalTarget = {
  provider: "anthropic";
  endpoint: "count_tokens" | "messages";
  model: string;
};

type PhysicalCall = {
  id: string;
  arm: "native" | "gateway";
  endpoint: PhysicalTarget["endpoint"];
};

type PhysicalEvent = PhysicalTarget & { callId: string };

type GenerationEvidence = {
  usage: UsageMetrics;
  attempts: number;
  physicalSends: number;
  recoveries: number;
  failovers: number;
  target: PhysicalTarget;
};

type CountEvidence = {
  inputTokens: number;
  physicalSends: number;
  target: PhysicalTarget;
};

type ArmEvidence = {
  count: CountEvidence;
  generation: GenerationEvidence;
  transition: Transition;
};

type Check = { name: string; passed: boolean };

type ArmComparison = {
  totalInputDelta: number;
  rawInputDelta: number;
  cacheReadDelta: number;
  cacheWriteDelta: number;
  cacheReadShareDelta: number;
  attemptsDelta: number;
  generationPhysicalSendsDelta: number;
  recoveriesDelta: number;
  failoversDelta: number;
  countPhysicalSendsDelta: number;
  countTokensDelta: number;
  countTargetMatches: boolean;
  generationTargetMatches: boolean;
  gatewayCountGenerationTargetMatches: boolean;
  gatewayExcessCacheWrite: number;
};

class EvalFailure extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

function invariant(value: unknown, code: string): asserts value {
  if (!value) throw new EvalFailure(code);
}

function nonnegativeInteger(value: unknown, code: string): number {
  invariant(typeof value === "number" && Number.isSafeInteger(value) && value >= 0, code);
  return value;
}

function roundedRatio(numerator: number, denominator: number): number {
  return denominator === 0 ? 0 : Math.round((numerator / denominator) * 1_000_000) / 1_000_000;
}

function usageMetrics(totalInput: number, cacheRead: number, cacheWrite: number): UsageMetrics {
  for (const [name, value] of Object.entries({ totalInput, cacheRead, cacheWrite })) {
    nonnegativeInteger(value, `invalid_${name}`);
  }
  const rawInput = totalInput - cacheRead - cacheWrite;
  invariant(rawInput >= 0, "cache_detail_exceeds_total_input");
  return { totalInput, rawInput, cacheRead, cacheWrite, cacheReadShare: totalInput === 0 ? 0 : cacheRead / totalInput };
}

function anthropicUsage(value: unknown): UsageMetrics {
  invariant(value !== null && typeof value === "object" && !Array.isArray(value), "invalid_anthropic_usage");
  const usage = value as Record<string, unknown>;
  const rawInput = nonnegativeInteger(usage.input_tokens, "missing_anthropic_input_tokens");
  const cacheRead = usage.cache_read_input_tokens === undefined
    ? 0
    : nonnegativeInteger(usage.cache_read_input_tokens, "invalid_anthropic_cache_read");
  const cacheWrite = usage.cache_creation_input_tokens === undefined
    ? 0
    : nonnegativeInteger(usage.cache_creation_input_tokens, "invalid_anthropic_cache_write");
  return usageMetrics(rawInput + cacheRead + cacheWrite, cacheRead, cacheWrite);
}

function persistedUsage(value: unknown): UsageMetrics {
  invariant(value !== null && typeof value === "object" && !Array.isArray(value), "missing_persisted_usage");
  const usage = value as Record<string, unknown>;
  return usageMetrics(
    nonnegativeInteger(usage.inputTokens, "invalid_persisted_input_tokens"),
    usage.cacheReadInputTokens === undefined
      ? nonnegativeInteger(usage.cachedInputTokens ?? 0, "invalid_persisted_cache_read")
      : nonnegativeInteger(usage.cacheReadInputTokens, "invalid_persisted_cache_read"),
    nonnegativeInteger(usage.cacheCreationInputTokens ?? 0, "invalid_persisted_cache_write"),
  );
}

function classifyTransition(previous: UsageMetrics | undefined, current: UsageMetrics): Transition {
  if (!previous) {
    const cold = current.cacheWrite > 0 && current.cacheRead === 0;
    return {
      classification: cold ? "natural-cold-cache-write" : "stable-warmed",
      newInputGrowth: current.totalInput,
      excessCacheWrite: 0,
      retainedPrefixReuse: null,
    };
  }
  const newInputGrowth = Math.max(0, current.totalInput - previous.totalInput);
  const excessCacheWrite = Math.max(0, current.cacheWrite - newInputGrowth);
  const previousCacheable = previous.cacheRead + previous.cacheWrite;
  const retainedPrefixReuse = previousCacheable === 0 ? null : roundedRatio(current.cacheRead, previousCacheable);
  const rewrite = excessCacheWrite > EXCESS_WRITE_MAX
    && retainedPrefixReuse !== null
    && retainedPrefixReuse < WARM_READ_SHARE_MIN;
  return {
    classification: rewrite ? "quota-burn-prefix-rewrite" : "stable-warmed",
    newInputGrowth,
    excessCacheWrite,
    retainedPrefixReuse,
  };
}

class SendBudget {
  observed = 0;

  constructor(readonly cap: number) {
    invariant(Number.isSafeInteger(cap) && cap > 0 && cap <= HARD_SEND_CAP, "invalid_physical_send_cap");
  }

  claim(): void {
    if (this.observed >= this.cap) throw new EvalFailure("physical_send_cap_exceeded");
    this.observed += 1;
  }
}

function targetsMatch(left: PhysicalTarget, right: PhysicalTarget, includeEndpoint = true): boolean {
  return left.provider === right.provider
    && left.model === right.model
    && (!includeEndpoint || left.endpoint === right.endpoint);
}

function compareArms(native: ArmEvidence, gateway: ArmEvidence): ArmComparison {
  const nativeGeneration = native.generation;
  const gatewayGeneration = gateway.generation;
  const delta = (gatewayValue: number, nativeValue: number): number =>
    Math.round((gatewayValue - nativeValue) * 1_000_000) / 1_000_000;
  return {
    totalInputDelta: gatewayGeneration.usage.totalInput - nativeGeneration.usage.totalInput,
    rawInputDelta: gatewayGeneration.usage.rawInput - nativeGeneration.usage.rawInput,
    cacheReadDelta: gatewayGeneration.usage.cacheRead - nativeGeneration.usage.cacheRead,
    cacheWriteDelta: gatewayGeneration.usage.cacheWrite - nativeGeneration.usage.cacheWrite,
    cacheReadShareDelta: delta(gatewayGeneration.usage.cacheReadShare, nativeGeneration.usage.cacheReadShare),
    attemptsDelta: gatewayGeneration.attempts - nativeGeneration.attempts,
    generationPhysicalSendsDelta: gatewayGeneration.physicalSends - nativeGeneration.physicalSends,
    recoveriesDelta: gatewayGeneration.recoveries - nativeGeneration.recoveries,
    failoversDelta: gatewayGeneration.failovers - nativeGeneration.failovers,
    countPhysicalSendsDelta: gateway.count.physicalSends - native.count.physicalSends,
    countTokensDelta: gateway.count.inputTokens - native.count.inputTokens,
    countTargetMatches: targetsMatch(native.count.target, gateway.count.target),
    generationTargetMatches: targetsMatch(nativeGeneration.target, gatewayGeneration.target),
    gatewayCountGenerationTargetMatches: targetsMatch(gateway.count.target, gatewayGeneration.target, false),
    gatewayExcessCacheWrite: Math.max(0, gatewayGeneration.usage.cacheWrite - nativeGeneration.usage.cacheWrite),
  };
}

function stableWarmChecks(prefix: string, native: ArmEvidence, gateway: ArmEvidence): Check[] {
  const nativeGeneration = native.generation;
  const gatewayGeneration = gateway.generation;
  const comparison = compareArms(native, gateway);
  return [
    { name: `${prefix}_count_token_parity`, passed: comparison.countTokensDelta === 0 },
    { name: `${prefix}_native_count_generation_token_parity`, passed: native.count.inputTokens === nativeGeneration.usage.totalInput },
    { name: `${prefix}_gateway_count_generation_token_parity`, passed: gateway.count.inputTokens === gatewayGeneration.usage.totalInput },
    { name: `${prefix}_count_target_parity`, passed: comparison.countTargetMatches },
    { name: `${prefix}_generation_target_parity`, passed: comparison.generationTargetMatches },
    { name: `${prefix}_count_generation_target_parity`, passed: comparison.gatewayCountGenerationTargetMatches },
    { name: `${prefix}_total_input_parity`, passed: comparison.totalInputDelta === 0 },
    { name: `${prefix}_raw_input_parity`, passed: comparison.rawInputDelta === 0 },
    { name: `${prefix}_cache_read_tokens_near_native`, passed: Math.abs(comparison.cacheReadDelta) <= EXCESS_WRITE_MAX },
    { name: `${prefix}_cache_write_tokens_near_native`, passed: Math.abs(comparison.cacheWriteDelta) <= EXCESS_WRITE_MAX },
    { name: `${prefix}_attempt_parity`, passed: comparison.attemptsDelta === 0 },
    { name: `${prefix}_generation_send_parity`, passed: comparison.generationPhysicalSendsDelta === 0 },
    { name: `${prefix}_recovery_parity`, passed: comparison.recoveriesDelta === 0 },
    { name: `${prefix}_failover_parity`, passed: comparison.failoversDelta === 0 },
    {
      name: `${prefix}_cache_read_share_near_native`,
      passed: Math.abs(nativeGeneration.usage.cacheReadShare - gatewayGeneration.usage.cacheReadShare) <= READ_SHARE_DELTA_MAX,
    },
    { name: `${prefix}_gateway_cache_read_share`, passed: gatewayGeneration.usage.cacheReadShare >= WARM_READ_SHARE_MIN },
    { name: `${prefix}_gateway_one_attempt`, passed: gatewayGeneration.attempts === 1 },
    { name: `${prefix}_gateway_one_generation_send`, passed: gatewayGeneration.physicalSends === 1 },
    { name: `${prefix}_gateway_no_recovery`, passed: gatewayGeneration.recoveries === 0 },
    { name: `${prefix}_gateway_no_failover`, passed: gatewayGeneration.failovers === 0 },
    { name: `${prefix}_gateway_stable_warm`, passed: gateway.transition.classification === "stable-warmed" },
    {
      name: `${prefix}_gateway_excess_write_bound`,
      passed: gateway.transition.excessCacheWrite <= EXCESS_WRITE_MAX
        && comparison.gatewayExcessCacheWrite <= EXCESS_WRITE_MAX,
    },
  ];
}

function selfCheckReport(): Record<string, unknown> {
  const cold = usageMetrics(100_002, 0, 100_000);
  const healthyGrowth = usageMetrics(125_002, 100_000, 25_000);
  const warmed = usageMetrics(125_002, 125_000, 0);
  const burned = usageMetrics(125_002, 10_000, 115_000);
  const coldTransition = classifyTransition(undefined, cold);
  const initiallyWarmedTransition = classifyTransition(undefined, warmed);
  const growthTransition = classifyTransition(cold, healthyGrowth);
  const warmTransition = classifyTransition(healthyGrowth, warmed);
  const burnTransition = classifyTransition(cold, burned);
  const target: PhysicalTarget = { provider: "anthropic", endpoint: "messages", model: "claude-opus-5" };
  const countTarget: PhysicalTarget = { ...target, endpoint: "count_tokens" };
  const arm = (usage: UsageMetrics, transition: Transition): ArmEvidence => ({
    count: { inputTokens: usage.totalInput, physicalSends: 1, target: countTarget },
    generation: { usage, attempts: 1, physicalSends: 1, recoveries: 0, failovers: 0, target },
    transition,
  });
  const mismatchNative = arm(usageMetrics(1_000_002, 950_000, 50_000), warmTransition);
  const mismatchGateway = arm(usageMetrics(1_000_002, 935_000, 65_000), warmTransition);
  const mismatchChecks = stableWarmChecks("mismatch", mismatchNative, mismatchGateway);
  const checks: Check[] = [
    { name: "inclusive_accounting", passed: burned.rawInput === 2 && burned.totalInput === burned.rawInput + burned.cacheRead + burned.cacheWrite },
    { name: "initial_write_is_natural", passed: coldTransition.classification === "natural-cold-cache-write" },
    { name: "initial_cache_hit_is_not_cold", passed: initiallyWarmedTransition.classification === "stable-warmed" },
    { name: "prompt_growth_not_rewrite", passed: growthTransition.classification === "stable-warmed" && growthTransition.excessCacheWrite === 0 },
    { name: "stable_warm_reuses_prefix", passed: warmTransition.classification === "stable-warmed" && warmTransition.retainedPrefixReuse === 1 },
    {
      name: "tool_result_quota_burn_is_prefix_rewrite",
      passed: burnTransition.classification === "quota-burn-prefix-rewrite"
        && burnTransition.newInputGrowth === 25_000
        && burnTransition.excessCacheWrite === 90_000
        && burnTransition.retainedPrefixReuse === 0.1,
    },
    { name: "root_cause_not_raw_input", passed: burned.rawInput === 2 },
    { name: "root_cause_not_duplicate_dispatch", passed: arm(burned, burnTransition).generation.physicalSends === 1 },
    { name: "root_cause_not_recovery", passed: arm(burned, burnTransition).generation.recoveries === 0 },
    { name: "root_cause_not_failover", passed: arm(burned, burnTransition).generation.failovers === 0 },
    {
      name: "absolute_cache_deltas_are_enforced",
      passed: mismatchChecks.some(check => check.name === "mismatch_cache_read_tokens_near_native" && !check.passed)
        && mismatchChecks.some(check => check.name === "mismatch_cache_write_tokens_near_native" && !check.passed)
        && mismatchChecks.some(check => check.name === "mismatch_cache_read_share_near_native" && check.passed),
    },
    ...stableWarmChecks("fixture", arm(warmed, warmTransition), arm(warmed, warmTransition)),
  ];
  const budget = new SendBudget(3);
  budget.claim();
  budget.claim();
  budget.claim();
  let capRejected = false;
  try {
    budget.claim();
  } catch (error) {
    capRejected = error instanceof EvalFailure && error.code === "physical_send_cap_exceeded";
  }
  checks.push({ name: "physical_send_cap_fails_closed", passed: capRejected && budget.observed === 3 });
  return {
    schemaVersion: SCHEMA_VERSION,
    mode: "self-check",
    passed: checks.every(check => check.passed),
    limits: {
      hardPhysicalSendCap: HARD_SEND_CAP,
      excessCacheWriteTokens: EXCESS_WRITE_MAX,
      nearNativeCacheTokenDeltaMax: EXCESS_WRITE_MAX,
      warmedCacheReadShareMin: WARM_READ_SHARE_MIN,
      nearNativeReadShareDeltaMax: READ_SHARE_DELTA_MAX,
    },
    evidence: {
      accounting: burned,
      initialCold: { usage: cold, transition: coldTransition },
      promptGrowth: { usage: healthyGrowth, transition: growthTransition },
      stableWarm: { usage: warmed, transition: warmTransition },
      initiallyWarmed: { usage: warmed, transition: initiallyWarmedTransition },
      toolResultBurn: { usage: burned, transition: burnTransition },
      rootCause: {
        primary: "cache_creation_after_warm_prefix_loss",
        boundary: "tool_result_continuation",
        excluded: ["raw_input_growth", "duplicate_dispatch", "transport_recovery", "provider_failover"],
      },
      physicalSendCap: { configured: budget.cap, observed: budget.observed, overflowRejected: capRejected },
      targets: { count: countTarget, generation: target },
      comparison: compareArms(arm(warmed, warmTransition), arm(warmed, warmTransition)),
    },
    checks,
  };
}

function parseArgs(argv: string[]): { live: boolean; reportPath?: string; sendCap: number } {
  let live = false;
  let reportPath: string | undefined;
  let capRaw: string | undefined;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]!;
    if (arg === "--self-check") continue;
    if (arg === "--live") {
      live = true;
      continue;
    }
    if (arg === "--report") {
      reportPath = argv[++index];
      invariant(reportPath, "missing_report_path");
      continue;
    }
    if (arg.startsWith("--report=")) {
      reportPath = arg.slice("--report=".length);
      invariant(reportPath, "missing_report_path");
      continue;
    }
    if (arg.startsWith("--max-sends=")) {
      capRaw = arg.slice("--max-sends=".length);
      continue;
    }
    throw new EvalFailure("unknown_argument");
  }
  const sendCap = Number(capRaw ?? (live ? process.env.OPENCODEX_PARITY_MAX_SENDS : undefined) ?? HARD_SEND_CAP);
  invariant(Number.isSafeInteger(sendCap) && sendCap > 0 && sendCap <= HARD_SEND_CAP, "invalid_physical_send_cap");
  if (live) invariant(sendCap >= PLANNED_SENDS, "physical_send_cap_below_live_plan");
  return { live, reportPath: reportPath ? resolve(reportPath) : undefined, sendCap };
}

async function freeLoopbackPort(): Promise<number> {
  return await new Promise((resolvePort, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen({ host: "127.0.0.1", port: 0 }, () => {
      const address = probe.address();
      invariant(address !== null && typeof address === "object", "ephemeral_port_unavailable");
      probe.close(error => error ? reject(error) : resolvePort(address.port));
    });
  });
}

async function portAcceptsConnections(port: number): Promise<boolean> {
  return await new Promise(resolveConnection => {
    const socket = createConnection({ host: "127.0.0.1", port });
    const done = (open: boolean) => {
      socket.destroy();
      resolveConnection(open);
    };
    socket.setTimeout(250, () => done(false));
    socket.once("connect", () => done(true));
    socket.once("error", () => done(false));
  });
}

async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs: number, code: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await Bun.sleep(50);
  }
  throw new EvalFailure(code);
}

function parseUsageRows(path: string): Array<Record<string, unknown>> {
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8")
    .split("\n")
    .filter(Boolean)
    .map(line => JSON.parse(line) as Record<string, unknown>);
}

function persistedAttempts(row: Record<string, unknown>): Array<Record<string, unknown>> {
  const attempts = Array.isArray(row.attempts) ? row.attempts : [];
  return attempts.map(value => {
    invariant(value !== null && typeof value === "object" && !Array.isArray(value), "invalid_persisted_attempt");
    return value as Record<string, unknown>;
  });
}

function gatewayGenerationEvidence(
  row: Record<string, unknown>,
  responseUsage: UsageMetrics,
  events: PhysicalEvent[],
): GenerationEvidence {
  invariant(row.inboundProtocol === "messages", "unexpected_persisted_protocol");
  const usage = persistedUsage(row.usage);
  invariant(JSON.stringify(usage) === JSON.stringify(responseUsage), "response_ledger_usage_mismatch");
  const attempts = persistedAttempts(row);
  const physicalAttempts = attempts.filter(
    attempt => nonnegativeInteger(attempt.sendCount, "invalid_attempt_send_count") > 0,
  );
  const physicalSends = physicalAttempts.reduce(
    (sum, attempt) => sum + nonnegativeInteger(attempt.sendCount, "invalid_attempt_send_count"),
    0,
  );
  const recoveries = physicalAttempts.reduce((sum, attempt) => {
    if (attempt.recoveryCount !== undefined) {
      return sum + nonnegativeInteger(attempt.recoveryCount, "invalid_attempt_recovery_count");
    }
    return sum + (Array.isArray(attempt.recoveryKinds) ? attempt.recoveryKinds.length : 0);
  }, 0);
  invariant(events.length === physicalSends, "wire_ledger_send_count_mismatch");
  invariant(events.length > 0, "missing_generation_physical_target");
  invariant(events.every(event => event.endpoint === "messages" && event.model === events[0]!.model), "generation_target_changed");
  const lastAttempt = physicalAttempts.at(-1);
  invariant(lastAttempt?.model === events[0]!.model, "wire_ledger_target_mismatch");
  return {
    usage,
    attempts: physicalAttempts.length,
    physicalSends,
    recoveries,
    failovers: row.comboTargetAdvanced === true ? 1 : 0,
    target: { provider: "anthropic", endpoint: "messages", model: events[0]!.model },
  };
}

function directGenerationEvidence(responseUsage: UsageMetrics, events: PhysicalEvent[]): GenerationEvidence {
  invariant(events.length === 1 && events[0]!.endpoint === "messages", "native_generation_send_count_mismatch");
  return {
    usage: responseUsage,
    attempts: 1,
    physicalSends: 1,
    recoveries: 0,
    failovers: 0,
    target: { provider: "anthropic", endpoint: "messages", model: events[0]!.model },
  };
}

function countEvidence(inputTokens: number, events: PhysicalEvent[]): CountEvidence {
  invariant(events.length === 1 && events[0]!.endpoint === "count_tokens", "count_send_count_mismatch");
  return {
    inputTokens,
    physicalSends: 1,
    target: { provider: "anthropic", endpoint: "count_tokens", model: events[0]!.model },
  };
}

function isMacLaptop(): boolean {
  try {
    return execFileSync("/usr/bin/pmset", ["-g", "batt"], { encoding: "utf8" }).includes("InternalBattery");
  } catch {
    return false;
  }
}

async function liveReport(sendCap: number): Promise<Record<string, unknown>> {
  invariant(process.platform === "darwin" && isMacLaptop(), "live_eval_requires_macos_laptop");
  invariant(process.env.OPENCODEX_PARITY_LIVE === LIVE_OPT_IN, "live_eval_opt_in_missing");
  invariant(process.env.OPENCODEX_PARITY_LAPTOP_HOST === hostname(), "laptop_hostname_confirmation_missing");
  const apiKey = process.env.ANTHROPIC_API_KEY?.trim();
  invariant(apiKey, "anthropic_api_key_missing");
  const model = "claude-opus-5";

  const port = await freeLoopbackPort();
  const root = mkdtempSync(join(tmpdir(), "ocx-claude-parity-"));
  const opencodexHome = join(root, "opencodex");
  const usagePath = join(opencodexHome, "usage.jsonl");
  const originalEnv = new Map<string, string | undefined>();
  const isolatedEnv = {
    OPENCODEX_HOME: opencodexHome,
    CODEX_HOME: join(root, "codex"),
    CLAUDE_CONFIG_DIR: join(root, "claude"),
    OPENCODEX_CLAUDE_DESKTOP_CONFIG_DIR: join(root, "claude-desktop"),
  };
  for (const [name, value] of Object.entries(isolatedEnv)) {
    originalEnv.set(name, process.env[name]);
    process.env[name] = value;
  }

  const originalFetch = globalThis.fetch;
  const originalConsole = {
    log: console.log,
    info: console.info,
    warn: console.warn,
    error: console.error,
  };
  console.log = console.info = console.warn = console.error = () => {};
  const budget = new SendBudget(sendCap);
  const wireEvents: PhysicalEvent[] = [];
  let activeCall: PhysicalCall | undefined;
  let server: { stop(closeActiveConnections?: boolean): Promise<void> } | undefined;
  const cleanup = {
    gatewayStopped: false,
    portClosed: false,
    isolatedStateRemoved: false,
    environmentRestored: false,
    existingConfigAccessed: true,
  };
  let result: Record<string, unknown> | undefined;
  let failureCode: string | undefined;

  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    const loopback = url.hostname === "127.0.0.1" || url.hostname === "localhost" || url.hostname === "::1";
    if (url.origin !== ANTHROPIC_ORIGIN) {
      if (!loopback) throw new EvalFailure("unexpected_network_egress_blocked");
      return originalFetch(input, init);
    }
    invariant(activeCall, "unattributed_anthropic_send_blocked");
    const expectedPath = activeCall.endpoint === "count_tokens" ? "/v1/messages/count_tokens" : "/v1/messages";
    invariant(url.pathname === expectedPath, "unexpected_anthropic_endpoint_blocked");
    const body = await request.clone().json() as Record<string, unknown>;
    invariant(body.model === model, "unexpected_physical_model_blocked");
    invariant(!wireEvents.some(event => event.callId === activeCall!.id), "logical_call_send_cap_exceeded");
    budget.claim();
    wireEvents.push({
      callId: activeCall.id,
      provider: "anthropic",
      endpoint: activeCall.endpoint,
      model: body.model,
    });
    return originalFetch(input, init);
  };

  const eventsFor = (id: string): PhysicalEvent[] => wireEvents.filter(event => event.callId === id);
  const withCall = async <T>(call: PhysicalCall, run: () => Promise<T>): Promise<T> => {
    invariant(activeCall === undefined, "overlapping_physical_calls_blocked");
    activeCall = call;
    try {
      return await run();
    } finally {
      activeCall = undefined;
    }
  };
  const fetchJson = async (url: string, init: RequestInit, code: string): Promise<Record<string, unknown>> => {
    const response = await fetch(url, {
      ...init,
      redirect: "error",
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    const text = await response.text();
    if (!response.ok) throw new EvalFailure(`${code}_http_${response.status}`);
    try {
      const parsed = JSON.parse(text);
      invariant(parsed !== null && typeof parsed === "object" && !Array.isArray(parsed), `${code}_invalid_json`);
      return parsed as Record<string, unknown>;
    } catch (error) {
      if (error instanceof EvalFailure) throw error;
      throw new EvalFailure(`${code}_invalid_json`);
    }
  };

  try {
    const { getConfigDir, saveConfig } = await import("../src/config");
    cleanup.existingConfigAccessed = resolve(getConfigDir()) !== resolve(opencodexHome);
    invariant(!cleanup.existingConfigAccessed, "config_isolation_failed");
    saveConfig({
      port,
      hostname: "127.0.0.1",
      defaultProvider: "anthropic-parity-eval",
      providers: {
        "anthropic-parity-eval": {
          adapter: "anthropic",
          baseUrl: ANTHROPIC_ORIGIN,
          authMode: "key",
          apiKey: "${ANTHROPIC_API_KEY}",
          liveModels: false,
          models: [model],
        },
      },
      combos: {
        "parity-eval": {
          strategy: "failover",
          targets: [{ provider: "anthropic-parity-eval", model }],
        },
      },
      claudeCode: { modelMap: { [model]: "combo/parity-eval" } },
    });
    const { startServer } = await import("../src/server");
    server = startServer(port);
    await waitFor(async () => {
      try {
        const response = await originalFetch(`http://127.0.0.1:${port}/healthz`, {
          redirect: "error",
          signal: AbortSignal.timeout(500),
        });
        await response.body?.cancel();
        return response.ok;
      } catch {
        return false;
      }
    }, 10_000, "gateway_readiness_timeout");

    const directHeaders = {
      "content-type": "application/json",
      "anthropic-version": API_VERSION,
      "x-api-key": apiKey,
    };
    const gatewayHeaders = {
      "content-type": "application/json",
      "anthropic-version": API_VERSION,
    };
    const gatewayUrl = `http://127.0.0.1:${port}`;
    let callOrdinal = 0;
    const nextId = (arm: PhysicalCall["arm"], endpoint: PhysicalCall["endpoint"]): string => `${++callOrdinal}-${arm}-${endpoint}`;

    const runCount = async (arm: PhysicalCall["arm"], body: Record<string, unknown>): Promise<CountEvidence> => {
      const before = arm === "gateway" ? parseUsageRows(usagePath).length : undefined;
      const countBody = { ...body };
      delete countBody.max_tokens;
      delete countBody.stream;
      const id = nextId(arm, "count_tokens");
      const json = await withCall({ id, arm, endpoint: "count_tokens" }, () => fetchJson(
        arm === "native" ? `${ANTHROPIC_ORIGIN}/v1/messages/count_tokens` : `${gatewayUrl}/v1/messages/count_tokens`,
        { method: "POST", headers: arm === "native" ? directHeaders : gatewayHeaders, body: JSON.stringify(countBody) },
        `${arm}_count`,
      ));
      if (before !== undefined) {
        invariant(parseUsageRows(usagePath).length === before, "count_generated_telemetry");
      }
      return countEvidence(nonnegativeInteger(json.input_tokens, "missing_count_input_tokens"), eventsFor(id));
    };

    const runGeneration = async (
      arm: PhysicalCall["arm"],
      body: Record<string, unknown>,
    ): Promise<GenerationEvidence> => {
      const before = parseUsageRows(usagePath).length;
      const id = nextId(arm, "messages");
      const json = await withCall({ id, arm, endpoint: "messages" }, () => fetchJson(
        arm === "native" ? `${ANTHROPIC_ORIGIN}/v1/messages` : `${gatewayUrl}/v1/messages`,
        { method: "POST", headers: arm === "native" ? directHeaders : gatewayHeaders, body: JSON.stringify(body) },
        `${arm}_generation`,
      ));
      const responseUsage = anthropicUsage(json.usage);
      if (arm === "native") return directGenerationEvidence(responseUsage, eventsFor(id));
      await waitFor(() => parseUsageRows(usagePath).length === before + 1, 10_000, "usage_log_flush_timeout");
      const rows = parseUsageRows(usagePath);
      return gatewayGenerationEvidence(rows.at(-1)!, responseUsage, eventsFor(id));
    };

    const runNonce = randomUUID();
    const toolPrefix = `tool-result parity ${runNonce} ${"stable cache parity fixture text ".repeat(3_000)}`;
    const growthPrefix = `prompt-growth parity ${runNonce} ${"stable cache parity fixture text ".repeat(3_000)}`;
    const toolResult = "bounded synthetic tool result line ".repeat(1_500);
    const growth = "bounded appended prompt growth ".repeat(1_000);
    const cached = (text: string) => ({ type: "text", text, cache_control: { type: "ephemeral" } });
    const common = { model, max_tokens: 1, stream: false };
    const toolBody = {
      ...common,
      system: [cached(toolPrefix)],
      tools: [{ name: "read_fixture", description: "Return fixed evaluation content.", input_schema: { type: "object", properties: {} } }],
      messages: [
        { role: "user", content: [{ type: "text", text: "Read the fixed evaluation fixture." }] },
        { role: "assistant", content: [{ type: "tool_use", id: "toolu_parity_eval", name: "read_fixture", input: {} }] },
        { role: "user", content: [{ type: "tool_result", tool_use_id: "toolu_parity_eval", content: toolResult, cache_control: { type: "ephemeral" } }] },
      ],
    };
    const growthBaseBody = {
      ...common,
      system: [cached(growthPrefix)],
      messages: [{ role: "user", content: [cached("stable baseline message ".repeat(1_000))] }],
    };
    const growthBody = {
      ...common,
      system: [cached(growthPrefix)],
      messages: [
        { role: "user", content: [cached("stable baseline message ".repeat(1_000))] },
        { role: "assistant", content: [{ type: "text", text: "Acknowledged." }] },
        { role: "user", content: [cached(growth)] },
      ],
    };

    const checks: Check[] = [];
    const toolColdGeneration = await runGeneration("native", toolBody);
    const toolColdTransition = classifyTransition(undefined, toolColdGeneration.usage);
    checks.push({ name: "tool_result_initial_write_is_natural", passed: toolColdTransition.classification === "natural-cold-cache-write" });
    const toolNative: ArmEvidence = {
      count: await runCount("native", toolBody),
      generation: await runGeneration("native", toolBody),
      transition: { classification: "stable-warmed", newInputGrowth: 0, excessCacheWrite: 0, retainedPrefixReuse: null },
    };
    toolNative.transition = classifyTransition(toolColdGeneration.usage, toolNative.generation.usage);
    const toolGateway: ArmEvidence = {
      count: await runCount("gateway", toolBody),
      generation: await runGeneration("gateway", toolBody),
      transition: { classification: "stable-warmed", newInputGrowth: 0, excessCacheWrite: 0, retainedPrefixReuse: null },
    };
    toolGateway.transition = classifyTransition(toolNative.generation.usage, toolGateway.generation.usage);
    checks.push(...stableWarmChecks("tool_result", toolNative, toolGateway));

    const growthBase = await runGeneration("native", growthBaseBody);
    const growthBaseTransition = classifyTransition(undefined, growthBase.usage);
    checks.push({ name: "prompt_growth_initial_write_is_natural", passed: growthBaseTransition.classification === "natural-cold-cache-write" });
    const growthFirst = await runGeneration("native", growthBody);
    const growthFirstTransition = classifyTransition(growthBase.usage, growthFirst.usage);
    checks.push({
      name: "appended_prompt_growth_is_not_prefix_rewrite",
      passed: growthFirstTransition.classification === "stable-warmed" && growthFirstTransition.excessCacheWrite <= EXCESS_WRITE_MAX,
    });
    const growthNative: ArmEvidence = {
      count: await runCount("native", growthBody),
      generation: await runGeneration("native", growthBody),
      transition: { classification: "stable-warmed", newInputGrowth: 0, excessCacheWrite: 0, retainedPrefixReuse: null },
    };
    growthNative.transition = classifyTransition(growthFirst.usage, growthNative.generation.usage);
    const growthGateway: ArmEvidence = {
      count: await runCount("gateway", growthBody),
      generation: await runGeneration("gateway", growthBody),
      transition: { classification: "stable-warmed", newInputGrowth: 0, excessCacheWrite: 0, retainedPrefixReuse: null },
    };
    growthGateway.transition = classifyTransition(growthNative.generation.usage, growthGateway.generation.usage);
    checks.push(...stableWarmChecks("prompt_growth", growthNative, growthGateway));
    checks.push({ name: "planned_physical_send_count", passed: budget.observed === PLANNED_SENDS });

    result = {
      schemaVersion: SCHEMA_VERSION,
      mode: "live",
      passed: checks.every(check => check.passed),
      model,
      limits: {
        hardPhysicalSendCap: HARD_SEND_CAP,
        configuredPhysicalSendCap: sendCap,
        plannedPhysicalSends: PLANNED_SENDS,
        observedPhysicalSends: budget.observed,
        excessCacheWriteTokens: EXCESS_WRITE_MAX,
        warmedCacheReadShareMin: WARM_READ_SHARE_MIN,
        nearNativeReadShareDeltaMax: READ_SHARE_DELTA_MAX,
      },
      scenarios: {
        warmToolResult: {
          initialCold: { generation: toolColdGeneration, transition: toolColdTransition },
          nativeWarm: toolNative,
          gatewayWarm: toolGateway,
          comparison: compareArms(toolNative, toolGateway),
        },
        promptGrowth: {
          initialCold: { generation: growthBase, transition: growthBaseTransition },
          firstGrowth: { generation: growthFirst, transition: growthFirstTransition },
          nativeWarm: growthNative,
          gatewayWarm: growthGateway,
          comparison: compareArms(growthNative, growthGateway),
        },
      },
      checks,
    };
  } catch (error) {
    failureCode = error instanceof EvalFailure ? error.code : "unexpected_failure";
  } finally {
    try {
      if (server) await server.stop(true);
      cleanup.gatewayStopped = true;
    } catch {
      failureCode ??= "gateway_stop_failed";
    }
    try {
      await waitFor(async () => !(await portAcceptsConnections(port)), 5_000, "gateway_port_still_open");
      cleanup.portClosed = true;
    } catch {
      failureCode ??= "gateway_port_still_open";
    }
    globalThis.fetch = originalFetch;
    console.log = originalConsole.log;
    console.info = originalConsole.info;
    console.warn = originalConsole.warn;
    console.error = originalConsole.error;
    try {
      removeTreeWithRetry(root);
      cleanup.isolatedStateRemoved = !existsSync(root);
      if (!cleanup.isolatedStateRemoved) failureCode ??= "isolated_state_cleanup_failed";
    } catch {
      failureCode ??= "isolated_state_cleanup_failed";
    }
    for (const [name, value] of originalEnv) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    cleanup.environmentRestored = Object.entries(isolatedEnv).every(([name]) => process.env[name] === originalEnv.get(name));
    if (!cleanup.environmentRestored) failureCode ??= "environment_restore_failed";
  }

  const cleanupPassed = cleanup.gatewayStopped
    && cleanup.portClosed
    && cleanup.isolatedStateRemoved
    && cleanup.environmentRestored
    && !cleanup.existingConfigAccessed;
  if (failureCode || !result) {
    return {
      schemaVersion: SCHEMA_VERSION,
      mode: "live",
      passed: false,
      limits: { hardPhysicalSendCap: HARD_SEND_CAP, configuredPhysicalSendCap: sendCap, observedPhysicalSends: budget.observed },
      error: { code: failureCode ?? "unexpected_failure" },
      cleanup,
    };
  }
  result.cleanup = cleanup;
  result.passed = result.passed === true && cleanupPassed;
  return result;
}

function writeReport(report: Record<string, unknown>, reportPath?: string): void {
  const json = `${JSON.stringify(report, null, 2)}\n`;
  if (reportPath) {
    invariant(existsSync(dirname(reportPath)), "report_parent_missing");
    writeFileSync(reportPath, json, { encoding: "utf8", flag: "wx", mode: 0o600 });
  }
  process.stdout.write(json);
}

async function main(): Promise<void> {
  let parsed: ReturnType<typeof parseArgs>;
  try {
    parsed = parseArgs(process.argv.slice(2));
  } catch (error) {
    writeReport({
      schemaVersion: SCHEMA_VERSION,
      mode: "self-check",
      passed: false,
      error: { code: error instanceof EvalFailure ? error.code : "unexpected_failure" },
    });
    process.exitCode = 1;
    return;
  }
  let report: Record<string, unknown>;
  try {
    report = parsed.live ? await liveReport(parsed.sendCap) : selfCheckReport();
  } catch (error) {
    report = {
      schemaVersion: SCHEMA_VERSION,
      mode: parsed.live ? "live" : "self-check",
      passed: false,
      error: { code: error instanceof EvalFailure ? error.code : "unexpected_failure" },
    };
  }
  try {
    writeReport(report, parsed.reportPath);
  } catch (error) {
    writeReport({
      schemaVersion: SCHEMA_VERSION,
      mode: parsed.live ? "live" : "self-check",
      passed: false,
      error: { code: error instanceof EvalFailure ? error.code : "report_write_failed" },
    });
    process.exitCode = 1;
    return;
  }
  if (report.passed !== true) process.exitCode = 1;
}

await main();
