/**
 * Source-backed routing analytics (RI-03).
 *
 * All metrics derive from the rebuildable request-history index
 * (`routing-history.sqlite`), never from repeated full JSONL scans. The
 * analysis is read-only: no routing decision, profile, or weight changes here
 * (ADR-10 - no automatic self-tuning).
 *
 * Bounds: at most ANALYTICS_MAX_ROWS matching rows are analyzed per call; a
 * larger population sets `historyTruncated: true` so readers never mistake a
 * sample for the full history.
 */

import {
  normalizeUsageEntryForTest,
  type AttemptRecoveryKind,
  type PersistedUsageAttempt,
  type PersistedUsageEntry,
  type UsageStatus,
} from "../usage/log";
import { estimateComboCost, estimateRequestCost, serviceTierContext } from "../usage/cost";
import type { UsageSurface } from "../usage/summary";
import { openRequestHistoryIndex, requestHistoryDb } from "./history/indexer";

export const ANALYTICS_MAX_ROWS = 50_000;
/** Default row cap for the management API (full cap remains available via `limit`). */
export const ANALYTICS_API_DEFAULT_ROWS = 5_000;
export const ANALYTICS_MAX_RED_ALERTS = 100;

export interface RoutingAnalyticsFilters {
  provider?: string;
  model?: string;
  profileId?: string;
  surface?: UsageSurface;
  from?: number;
  to?: number;
}

export type AnalyticsConfidence = "high" | "medium" | "low";

export interface AnalyticsBreakdownRow {
  provider: string;
  model: string;
  accountRef?: string;
  profileId?: string;
  requests: number;
  successes: number;
  failures: number;
  cancelled: number;
  successRate: number | null;
  p50DurationMs?: number;
  estimatedCostUsdPerSuccessfulRequest?: number | null;
}

export interface AnalyticsProfileRow {
  profileId: string;
  profileRevision?: string;
  requests: number;
  successes: number;
  failures: number;
  fallbacks: number;
  successRate: number | null;
}

/** Attempt-reported input telemetry. It is not a provider invoice or raw wire capture. */
export interface AnalyticsAttemptUsage {
  inclusiveInputTokens: number;
  rawInputTokens: number;
  cacheReadInputTokens: number;
  cacheWriteInputTokens: number;
  rawInputShare: number | null;
  cacheReadShare: number | null;
  cacheWriteShare: number | null;
}

export interface AnalyticsPhysicalUsageCoverage {
  totalAttempts: number;
  measuredAttempts: number;
  reportedAttempts: number;
  estimatedAttempts: number;
  unreportedAttempts: number;
  unsupportedAttempts: number;
  ratio: number | null;
  supportedRatio: number | null;
}

export interface AnalyticsPhysicalBreakdownRow {
  provider: string;
  model: string;
  accountRef?: string;
  requests: number;
  physicalAttempts: number;
  physicalSends: number;
  repeatedSendAttempts: number;
  recoveryAttempts: number;
  recoveryEvents: number;
  recoveryRate: number | null;
  comboFailoverRequests: number;
  comboFailoverRate: number | null;
  requestRatePerHour: number | null;
  attemptUsage: AnalyticsAttemptUsage;
  usageCoverage: AnalyticsPhysicalUsageCoverage;
}

export type AnalyticsRedAlertKind =
  | "consecutive-high-raw-input"
  | "high-cache-write-after-warmup"
  | "low-cache-read-share-after-warmup"
  | "falling-cache-read"
  | "recovery"
  | "repeated-send"
  | "combo-failover";

export interface AnalyticsRedAlert {
  kind: AnalyticsRedAlertKind;
  requestId: string;
  timestamp: number;
  conversationId?: string;
  provider: string;
  model: string;
  accountRef?: string;
  attemptOrdinal: number;
  value?: number;
  previousValue?: number;
  recoveryKinds?: AttemptRecoveryKind[];
}

export interface RoutingAnalyticsResult {
  generatedAt: number;
  totalRequests: number;
  scannedRows: number;
  historyTruncated: boolean;
  confidence: AnalyticsConfidence | null;
  successRate: number | null;
  failureRate: number | null;
  cancelledRate: number | null;
  fallbackRate: number | null;
  totalAttempts: number;
  averageAttemptsPerRequest: number | null;
  physicalAttempts: number;
  physicalSends: number;
  repeatedSendAttempts: number;
  recoveryAttempts: number;
  recoveryEvents: number;
  recoveryRate: number | null;
  comboFailoverRequests: number;
  comboFailoverRate: number | null;
  requestRatePerHour: number | null;
  attemptUsage: AnalyticsAttemptUsage;
  physicalUsageCoverage: AnalyticsPhysicalUsageCoverage;
  physicalBreakdown: AnalyticsPhysicalBreakdownRow[];
  redAlerts: AnalyticsRedAlert[];
  redAlertsPartial: boolean;
  sequentialRoutes: number;
  warmedRoutes: number;
  incompleteStreamRate: number | null;
  cooldownTriggeringFailures: number;
  durationMs: {
    p50?: number;
    p95?: number;
    p99?: number;
    sampleCount: number;
  };
  firstOutputMs: {
    p50?: number;
    p95?: number;
    p99?: number;
    sampleCount: number;
    /** Share of scanned requests with a TTFT measurement (0..1). */
    coverage: number | null;
  };
  estimatedCostUsdPerSuccessfulRequest: number | null;
  estimatedCostUsdTotalSuccessful: number | null;
  /** Measured physical attempts divided by all physical attempts. */
  usageCoverage: number | null;
  priceCoverage: number | null;
  breakdown: AnalyticsBreakdownRow[];
  profileBreakdown: AnalyticsProfileRow[];
}

interface ScannedRow {
  requestId: string;
  timestamp: number;
  provider: string;
  model: string;
  apiKeyId?: string | null;
  profileId?: string | null;
  profileRevision?: string | null;
  status: number;
  durationMs: number;
  firstOutputMs?: number | null;
  closeReason?: string | null;
  terminalStatus?: string | null;
  usageStatus: string;
  attemptCount: number;
  fallback: number;
  rowJson: string;
}

interface Bucket extends AnalyticsBreakdownRow {
  durations: number[];
  costUsdSum: number;
  costRows: number;
}

interface UsageAccumulator {
  inclusiveInputTokens: number;
  rawInputTokens: number;
  cacheReadInputTokens: number;
  cacheWriteInputTokens: number;
}

interface CacheDecomposition {
  inclusive: number;
  raw: number;
  read: number;
  write: number;
}

interface CoverageAccumulator {
  totalAttempts: number;
  reportedAttempts: number;
  estimatedAttempts: number;
  unreportedAttempts: number;
  unsupportedAttempts: number;
}

interface PhysicalAccumulator {
  provider: string;
  model: string;
  accountRef?: string;
  requests: number;
  physicalAttempts: number;
  physicalSends: number;
  repeatedSendAttempts: number;
  recoveryAttempts: number;
  recoveryEvents: number;
  comboFailoverRequests: number;
  usage: UsageAccumulator;
  coverage: CoverageAccumulator;
}

interface PhysicalAttempt {
  ordinal: number;
  provider: string;
  model: string;
  accountRef?: string;
  sendCount: number;
  recoveryKinds: AttemptRecoveryKind[];
  recoveryCount: number;
  usageStatus: UsageStatus;
  usage?: PersistedUsageEntry["usage"];
  cache?: CacheDecomposition;
}

interface ExpandedRow {
  row: ScannedRow;
  entry: PersistedUsageEntry | null;
  attempts: PhysicalAttempt[];
  fallback: boolean;
  attemptCount: number;
  comboFailover: boolean;
}

const COOLDOWN_RECOVERY_KINDS = new Set([
  "rate-limit-429",
  "key-429",
  "oauth-401",
  "anthropic-oauth-429",
  "oauth-account-429",
]);
const USAGE_STATUSES = new Set<UsageStatus>([
  "reported", "unreported", "unsupported", "estimated",
]);
const HOUR_MS = 3_600_000;

function percentile(sorted: number[], p: number): number | undefined {
  if (sorted.length === 0) return undefined;
  const index = Math.max(0, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.min(index, sorted.length - 1)];
}

function classifyRow(row: ScannedRow): "success" | "failure" | "cancelled" {
  if (row.closeReason === "client_cancel" || row.status === 499) return "cancelled";
  if (row.terminalStatus === "incomplete") return "failure";
  if (row.terminalStatus && row.terminalStatus !== "completed") return "failure";
  if (row.status >= 400) return "failure";
  return "success";
}

interface ParsedEntry {
  entry: PersistedUsageEntry | null;
  malformedAttempts: boolean;
}

function parseEntry(rowJson: string): ParsedEntry {
  try {
    const parsed = JSON.parse(rowJson) as PersistedUsageEntry;
    if (!parsed || typeof parsed !== "object" || typeof parsed.requestId !== "string") {
      return { entry: null, malformedAttempts: false };
    }
    const entry = normalizeUsageEntryForTest(parsed);
    return {
      entry,
      malformedAttempts: Array.isArray(parsed.attempts)
        && parsed.attempts.length > 0
        && entry.attempts?.length === 0,
    };
  } catch {
    return { entry: null, malformedAttempts: false };
  }
}

function usageStatus(value: unknown): UsageStatus {
  return typeof value === "string" && USAGE_STATUSES.has(value as UsageStatus)
    ? value as UsageStatus
    : "unreported";
}

function cacheDecomposition(value: PersistedUsageEntry["usage"] | undefined): CacheDecomposition | null {
  if (!value || !Number.isFinite(value.inputTokens) || value.inputTokens < 0) return null;
  const read = value.cacheReadInputTokens ?? value.cachedInputTokens ?? 0;
  const write = value.cacheCreationInputTokens ?? 0;
  if (!Number.isFinite(read) || read < 0 || !Number.isFinite(write) || write < 0
    || read + write > value.inputTokens) return null;
  return {
    inclusive: value.inputTokens,
    raw: value.inputTokens - read - write,
    read,
    write,
  };
}

function physicalAttemptsFor(
  row: ScannedRow,
  entry: PersistedUsageEntry | null,
  malformedAttempts = false,
): PhysicalAttempt[] {
  const attempts = entry?.attempts;
  if (entry && attempts !== undefined) {
    const seen = new Set<number>();
    const durable = attempts.filter(attempt => {
      if (attempt.locallyAnswered || seen.has(attempt.ordinal)) return false;
      seen.add(attempt.ordinal);
      return true;
    });
    const legacyAccountAttempt = entry.accountLogLabel
      ? durable.reduce<PersistedUsageAttempt | undefined>((final, attempt) =>
          !final || attempt.ordinal > final.ordinal ? attempt : final, undefined)
      : undefined;
    const physical = durable
      .map((attempt): PhysicalAttempt => {
        const cache = cacheDecomposition(attempt.usage);
        const accountRef = attempt.accountLogLabel
          ?? (attempt === legacyAccountAttempt ? entry.accountLogLabel : undefined);
        return {
          ordinal: attempt.ordinal,
          provider: attempt.provider,
          model: attempt.model,
          ...(accountRef ? { accountRef } : {}),
          sendCount: attempt.sendCount,
          recoveryKinds: attempt.recoveryKinds,
          recoveryCount: attempt.recoveryCount ?? attempt.recoveryKinds.length,
          usageStatus: attempt.usageStatus,
          ...(attempt.usage ? { usage: attempt.usage } : {}),
          ...(cache ? { cache } : {}),
        };
      })
      .sort((a, b) => a.ordinal - b.ordinal);
    const rootCache = cacheDecomposition(entry.usage);
    const finalAttempt = physical.at(-1);
    if (!physical.some(attempt => attempt.cache) && finalAttempt && rootCache) {
      finalAttempt.usage = entry.usage;
      finalAttempt.cache = rootCache;
      finalAttempt.usageStatus = usageStatus(entry.usageStatus);
    }
    if (physical.length > 0 || !malformedAttempts) return physical;
  }
  const cache = cacheDecomposition(entry?.usage);
  return [{
    ordinal: 1,
    provider: row.provider,
    model: row.model,
    ...(entry?.accountLogLabel ? { accountRef: entry.accountLogLabel } : {}),
    sendCount: 1,
    recoveryKinds: [],
    recoveryCount: 0,
    usageStatus: usageStatus(entry?.usageStatus ?? row.usageStatus),
    ...(entry?.usage ? { usage: entry.usage } : {}),
    ...(cache ? { cache } : {}),
  }];
}

function cooldownTriggering(entry: PersistedUsageEntry | null, status: number): boolean {
  if (status === 429) return true;
  return entry?.attempts?.some(attempt =>
    attempt.recoveryKinds.some(kind => COOLDOWN_RECOVERY_KINDS.has(kind))) ?? false;
}

function successCostUsd(
  row: Pick<ScannedRow, "provider" | "model">,
  entry: PersistedUsageEntry,
): number | null {
  if (!entry.usage && !entry.attempts?.length) return null;
  const tier = serviceTierContext(entry);
  const estimate = entry.attempts?.length
    ? estimateComboCost(entry.attempts, undefined, tier)
    : estimateRequestCost({
      provider: row.provider,
      model: row.model,
      usage: entry.usage,
      usageStatus: entry.usageStatus,
      serviceTier: tier,
    });
  return estimate ? estimate.cost.total : null;
}

function blankUsage(): UsageAccumulator {
  return { inclusiveInputTokens: 0, rawInputTokens: 0, cacheReadInputTokens: 0, cacheWriteInputTokens: 0 };
}

function blankCoverage(): CoverageAccumulator {
  return { totalAttempts: 0, reportedAttempts: 0, estimatedAttempts: 0, unreportedAttempts: 0, unsupportedAttempts: 0 };
}

function addAttemptUsage(usage: UsageAccumulator, cache: CacheDecomposition | undefined): void {
  if (!cache) return;
  usage.inclusiveInputTokens += cache.inclusive;
  usage.cacheReadInputTokens += cache.read;
  usage.cacheWriteInputTokens += cache.write;
  usage.rawInputTokens += cache.raw;
}

function addCoverage(coverage: CoverageAccumulator, attempt: PhysicalAttempt): void {
  coverage.totalAttempts += 1;
  if (attempt.usageStatus === "unsupported") {
    coverage.unsupportedAttempts += 1;
  } else if (
    attempt.cache
    && (attempt.usageStatus === "reported" || attempt.usageStatus === "estimated")
  ) {
    if (attempt.usage?.estimated || attempt.usageStatus === "estimated") coverage.estimatedAttempts += 1;
    else coverage.reportedAttempts += 1;
  } else {
    coverage.unreportedAttempts += 1;
  }
}

function finalUsage(usage: UsageAccumulator): AnalyticsAttemptUsage {
  const total = usage.inclusiveInputTokens;
  return {
    ...usage,
    rawInputShare: total > 0 ? usage.rawInputTokens / total : null,
    cacheReadShare: total > 0 ? usage.cacheReadInputTokens / total : null,
    cacheWriteShare: total > 0 ? usage.cacheWriteInputTokens / total : null,
  };
}

function finalCoverage(coverage: CoverageAccumulator): AnalyticsPhysicalUsageCoverage {
  const measuredAttempts = coverage.reportedAttempts + coverage.estimatedAttempts;
  const supportedAttempts = coverage.totalAttempts - coverage.unsupportedAttempts;
  return {
    ...coverage,
    measuredAttempts,
    ratio: coverage.totalAttempts > 0 ? measuredAttempts / coverage.totalAttempts : null,
    supportedRatio: supportedAttempts > 0 ? measuredAttempts / supportedAttempts : null,
  };
}

function physicalAccumulator(provider: string, model: string, accountRef?: string): PhysicalAccumulator {
  return {
    provider,
    model,
    ...(accountRef ? { accountRef } : {}),
    requests: 0,
    physicalAttempts: 0,
    physicalSends: 0,
    repeatedSendAttempts: 0,
    recoveryAttempts: 0,
    recoveryEvents: 0,
    comboFailoverRequests: 0,
    usage: blankUsage(),
    coverage: blankCoverage(),
  };
}

export async function computeRoutingAnalytics(
  filters: RoutingAnalyticsFilters,
  options: { maxRows?: number } = {},
): Promise<RoutingAnalyticsResult> {
  const generatedAt = Date.now();
  await openRequestHistoryIndex();
  const handle = requestHistoryDb();
  const maxRows = Math.min(
    Math.max(1, Math.trunc(options.maxRows ?? ANALYTICS_MAX_ROWS)),
    ANALYTICS_MAX_ROWS,
  );

  const where: string[] = [];
  const values: Array<string | number> = [];
  const add = (clause: string, value: string | number) => {
    where.push(clause);
    values.push(value);
  };
  if (filters.provider !== undefined) add("provider = ?", filters.provider);
  if (filters.model !== undefined) add("model = ?", filters.model);
  if (filters.profileId !== undefined) add("profile_id = ?", filters.profileId);
  if (filters.surface === "codex") where.push("surface IS NULL");
  else if (filters.surface === "claude") where.push("surface IN ('claude', 'claude-desktop')");
  else if (filters.surface === "grok") where.push("surface = 'grok'");
  if (filters.from !== undefined) add("timestamp >= ?", filters.from);
  if (filters.to !== undefined) add("timestamp <= ?", filters.to);
  const whereSql = where.length > 0 ? ` WHERE ${where.join(" AND ")}` : "";

  const rows = handle.query(
    `SELECT request_id AS requestId, timestamp, provider, model,
            api_key_id AS apiKeyId, profile_id AS profileId,
            profile_revision AS profileRevision, status,
            duration_ms AS durationMs, first_output_ms AS firstOutputMs,
            close_reason AS closeReason, terminal_status AS terminalStatus,
            usage_status AS usageStatus, attempt_count AS attemptCount,
            fallback, row_json AS rowJson
     FROM requests${whereSql} ORDER BY timestamp DESC, request_id DESC LIMIT ?`,
  ).all(...values, maxRows + 1) as ScannedRow[];

  const scanned = rows.slice(0, maxRows);
  const historyTruncated = rows.length > maxRows;
  const expanded: ExpandedRow[] = scanned.map(row => {
    const parsed = parseEntry(row.rowJson);
    const attempts = physicalAttemptsFor(row, parsed.entry, parsed.malformedAttempts);
    const hasDurableAttempts = parsed.entry?.attempts !== undefined;
    return {
      row,
      entry: parsed.entry,
      attempts,
      fallback: hasDurableAttempts ? attempts.length > 1 : row.fallback === 1,
      attemptCount: hasDurableAttempts ? attempts.length : row.attemptCount,
      comboFailover: parsed.entry?.comboTargetAdvanced === true,
    };
  });

  let successes = 0;
  let failures = 0;
  let cancelled = 0;
  let fallbacks = 0;
  let totalAttempts = 0;
  let incompleteStreams = 0;
  let cooldownFailures = 0;
  const durations: number[] = [];
  const firstOutputs: number[] = [];
  let costTotalUsd = 0;
  let costCount = 0;

  const byKey = new Map<string, Bucket>();
  const byProfile = new Map<string, AnalyticsProfileRow>();

  for (const { row, entry, attempts, fallback, attemptCount } of expanded) {
    const kind = classifyRow(row);
    if (kind === "success") successes += 1;
    else if (kind === "failure") failures += 1;
    else cancelled += 1;
    if (fallback) fallbacks += 1;
    totalAttempts += attemptCount;
    if (row.terminalStatus === "incomplete") incompleteStreams += 1;
    durations.push(row.durationMs);
    if (row.firstOutputMs !== null && row.firstOutputMs !== undefined && row.firstOutputMs >= 0) {
      firstOutputs.push(row.firstOutputMs);
    }

    let rowCostUsd: number | null = null;
    if (kind === "success" && entry) {
      rowCostUsd = successCostUsd(row, entry);
      if (rowCostUsd !== null) {
        costTotalUsd += rowCostUsd;
        costCount += 1;
      }
    }

    if (kind === "failure" && cooldownTriggering(entry, row.status)) cooldownFailures += 1;

    const key = `${row.provider}\0${row.model}\0${row.apiKeyId ?? ""}\0${row.profileId ?? ""}`;
    let bucket: Bucket | undefined = byKey.get(key);
    if (!bucket) {
      bucket = {
        provider: row.provider,
        model: row.model,
        ...(row.apiKeyId ? { accountRef: row.apiKeyId } : {}),
        ...(row.profileId ? { profileId: row.profileId } : {}),
        requests: 0,
        successes: 0,
        failures: 0,
        cancelled: 0,
        successRate: null,
        durations: [],
        costUsdSum: 0,
        costRows: 0,
      };
      byKey.set(key, bucket);
    }
    bucket.requests += 1;
    if (kind === "success") bucket.successes += 1;
    else if (kind === "failure") bucket.failures += 1;
    else bucket.cancelled += 1;
    bucket.durations.push(row.durationMs);
    if (rowCostUsd !== null) {
      bucket.costUsdSum += rowCostUsd;
      bucket.costRows += 1;
    }

    if (row.profileId) {
      let profile = byProfile.get(row.profileId);
      if (!profile) {
        profile = {
          profileId: row.profileId,
          ...(row.profileRevision ? { profileRevision: row.profileRevision } : {}),
          requests: 0,
          successes: 0,
          failures: 0,
          fallbacks: 0,
          successRate: null,
        };
        byProfile.set(row.profileId, profile);
      }
      profile.requests += 1;
      if (kind === "success") profile.successes += 1;
      else if (kind === "failure") profile.failures += 1;
      if (fallback) profile.fallbacks += 1;
    }
  }

  const overall = physicalAccumulator("", "");
  const byPhysicalRoute = new Map<string, PhysicalAccumulator>();
  let comboFailoverRequests = 0;
  let alertCount = 0;
  const redAlerts: AnalyticsRedAlert[] = [];
  const appendAlert = (alert: AnalyticsRedAlert) => {
    alertCount += 1;
    if (redAlerts.length === ANALYTICS_MAX_RED_ALERTS) redAlerts.shift();
    redAlerts.push(alert);
  };
  const sequence = new Map<string, {
    warmed: boolean;
    previousRaw?: number;
    previousRead?: number;
  }>();

  const chronological = expanded.slice().reverse();
  for (const { row, entry, attempts, comboFailover } of chronological) {
    if (comboFailover) comboFailoverRequests += 1;
    const seenRoutes = new Set<string>();
    const conversationId = entry?.conversationId?.trim() || undefined;
    for (const [attemptIndex, attempt] of attempts.entries()) {
      const measuredCache = attempt.usageStatus === "reported" || attempt.usageStatus === "estimated"
        ? attempt.cache
        : undefined;
      const routeKey = `${attempt.provider}\0${attempt.model}\0${attempt.accountRef ?? ""}`;
      let route = byPhysicalRoute.get(routeKey);
      if (!route) {
        route = physicalAccumulator(attempt.provider, attempt.model, attempt.accountRef);
        byPhysicalRoute.set(routeKey, route);
      }
      if (!seenRoutes.has(routeKey)) {
        seenRoutes.add(routeKey);
        route.requests += 1;
        if (comboFailover) route.comboFailoverRequests += 1;
      }
      for (const accumulator of [overall, route]) {
        accumulator.physicalAttempts += 1;
        accumulator.physicalSends += attempt.sendCount;
        if (attempt.sendCount > 1) accumulator.repeatedSendAttempts += 1;
        if (attempt.recoveryCount > 0) accumulator.recoveryAttempts += 1;
        accumulator.recoveryEvents += attempt.recoveryCount;
        addAttemptUsage(accumulator.usage, measuredCache);
        addCoverage(accumulator.coverage, attempt);
      }

      const baseAlert = {
        requestId: row.requestId,
        timestamp: row.timestamp,
        ...(conversationId ? { conversationId } : {}),
        provider: attempt.provider,
        model: attempt.model,
        ...(attempt.accountRef ? { accountRef: attempt.accountRef } : {}),
        attemptOrdinal: attempt.ordinal,
      };
      if (attempt.recoveryCount > 0) {
        appendAlert({
          ...baseAlert,
          kind: "recovery",
          value: attempt.recoveryCount,
          ...(attempt.recoveryKinds.length > 0 ? { recoveryKinds: attempt.recoveryKinds } : {}),
        });
      }
      if (attempt.sendCount > 1) {
        appendAlert({ ...baseAlert, kind: "repeated-send", value: attempt.sendCount });
      }
      if (comboFailover && attemptIndex === attempts.length - 1) {
        appendAlert({ ...baseAlert, kind: "combo-failover", value: attempts.length });
      }

      const sequentialKey = `${conversationId ?? row.requestId}\0${routeKey}`;
      let state = sequence.get(sequentialKey);
      if (!state) {
        state = { warmed: false };
        sequence.set(sequentialKey, state);
      }
      if (!measuredCache) {
        state.previousRaw = undefined;
        state.previousRead = undefined;
        continue;
      }
      const { raw, read, write } = measuredCache;
      const readShare = measuredCache.inclusive > 0 ? read / measuredCache.inclusive : null;
      if (raw > 100_000 && state.previousRaw !== undefined && state.previousRaw > 100_000) {
        appendAlert({ ...baseAlert, kind: "consecutive-high-raw-input", value: raw, previousValue: state.previousRaw });
      }
      if (state.warmed) {
        if (write > 10_000) {
          appendAlert({ ...baseAlert, kind: "high-cache-write-after-warmup", value: write });
        }
        if (readShare !== null && readShare < 0.9) {
          appendAlert({ ...baseAlert, kind: "low-cache-read-share-after-warmup", value: readShare });
        }
        if (state.previousRead !== undefined && read < state.previousRead) {
          appendAlert({ ...baseAlert, kind: "falling-cache-read", value: read, previousValue: state.previousRead });
        }
      }
      state.warmed ||= read > 0;
      state.previousRaw = raw;
      state.previousRead = read;
    }
  }
  const sequentialRoutes = sequence.size;
  const warmedRoutes = [...sequence.values()].filter(state => state.warmed).length;

  durations.sort((a, b) => a - b);
  firstOutputs.sort((a, b) => a - b);
  const total = scanned.length;
  const rate = (count: number): number | null => (total > 0 ? count / total : null);
  const timestamps = scanned.map(row => row.timestamp).filter(Number.isFinite);
  const observedStart = timestamps.length > 0 ? Math.min(...timestamps) : undefined;
  const observedEnd = timestamps.length > 0 ? Math.max(...timestamps) : undefined;
  const windowStart = filters.from ?? observedStart;
  const windowEnd = filters.to ?? (filters.from !== undefined ? generatedAt : observedEnd);
  const windowMs = windowStart !== undefined && windowEnd !== undefined && windowEnd > windowStart
    ? windowEnd - windowStart
    : 0;
  const requestRatePerHour = (count: number): number | null => windowMs > 0
    ? count / (windowMs / HOUR_MS)
    : null;

  const breakdown: AnalyticsBreakdownRow[] = [...byKey.values()].map(bucket => {
    const sorted = bucket.durations.sort((a, b) => a - b);
    const p50DurationMs = percentile(sorted, 50);
    return {
      provider: bucket.provider,
      model: bucket.model,
      ...(bucket.accountRef ? { accountRef: bucket.accountRef } : {}),
      ...(bucket.profileId ? { profileId: bucket.profileId } : {}),
      requests: bucket.requests,
      successes: bucket.successes,
      failures: bucket.failures,
      cancelled: bucket.cancelled,
      successRate: bucket.requests > 0 ? bucket.successes / bucket.requests : null,
      ...(p50DurationMs !== undefined ? { p50DurationMs } : {}),
      ...(bucket.requests > 0
        ? { estimatedCostUsdPerSuccessfulRequest: bucket.costRows > 0
          ? bucket.costUsdSum / bucket.costRows
          : null }
        : {}),
    };
  }).sort((a, b) => b.requests - a.requests);

  const profileBreakdown: AnalyticsProfileRow[] = [...byProfile.values()].map(profile => ({
    ...profile,
    successRate: profile.requests > 0 ? profile.successes / profile.requests : null,
  })).sort((a, b) => b.requests - a.requests);

  const physicalBreakdown: AnalyticsPhysicalBreakdownRow[] = [...byPhysicalRoute.values()].map(route => ({
    provider: route.provider,
    model: route.model,
    ...(route.accountRef ? { accountRef: route.accountRef } : {}),
    requests: route.requests,
    physicalAttempts: route.physicalAttempts,
    physicalSends: route.physicalSends,
    repeatedSendAttempts: route.repeatedSendAttempts,
    recoveryAttempts: route.recoveryAttempts,
    recoveryEvents: route.recoveryEvents,
    recoveryRate: route.physicalAttempts > 0 ? route.recoveryAttempts / route.physicalAttempts : null,
    comboFailoverRequests: route.comboFailoverRequests,
    comboFailoverRate: route.requests > 0 ? route.comboFailoverRequests / route.requests : null,
    requestRatePerHour: requestRatePerHour(route.requests),
    attemptUsage: finalUsage(route.usage),
    usageCoverage: finalCoverage(route.coverage),
  })).sort((a, b) => b.physicalAttempts - a.physicalAttempts
    || a.provider.localeCompare(b.provider)
    || a.model.localeCompare(b.model)
    || (a.accountRef ?? "").localeCompare(b.accountRef ?? ""));

  const confidence: AnalyticsConfidence | null = total === 0
    ? null
    : total >= 100 ? "high" : total >= 20 ? "medium" : "low";
  const physicalUsageCoverage = finalCoverage(overall.coverage);

  return {
    generatedAt,
    totalRequests: total,
    scannedRows: scanned.length,
    historyTruncated,
    confidence,
    successRate: rate(successes),
    failureRate: rate(failures),
    cancelledRate: rate(cancelled),
    fallbackRate: rate(fallbacks),
    totalAttempts,
    averageAttemptsPerRequest: total > 0 ? totalAttempts / total : null,
    physicalAttempts: overall.physicalAttempts,
    physicalSends: overall.physicalSends,
    repeatedSendAttempts: overall.repeatedSendAttempts,
    recoveryAttempts: overall.recoveryAttempts,
    recoveryEvents: overall.recoveryEvents,
    recoveryRate: overall.physicalAttempts > 0 ? overall.recoveryAttempts / overall.physicalAttempts : null,
    comboFailoverRequests,
    comboFailoverRate: rate(comboFailoverRequests),
    requestRatePerHour: requestRatePerHour(total),
    attemptUsage: finalUsage(overall.usage),
    physicalUsageCoverage,
    physicalBreakdown,
    redAlerts: redAlerts.sort((a, b) => b.timestamp - a.timestamp
      || b.requestId.localeCompare(a.requestId)
      || b.attemptOrdinal - a.attemptOrdinal),
    redAlertsPartial: historyTruncated || alertCount > ANALYTICS_MAX_RED_ALERTS,
    sequentialRoutes,
    warmedRoutes,
    incompleteStreamRate: rate(incompleteStreams),
    cooldownTriggeringFailures: cooldownFailures,
    durationMs: {
      ...(percentile(durations, 50) !== undefined ? { p50: percentile(durations, 50) } : {}),
      ...(percentile(durations, 95) !== undefined ? { p95: percentile(durations, 95) } : {}),
      ...(percentile(durations, 99) !== undefined ? { p99: percentile(durations, 99) } : {}),
      sampleCount: durations.length,
    },
    firstOutputMs: {
      ...(percentile(firstOutputs, 50) !== undefined ? { p50: percentile(firstOutputs, 50) } : {}),
      ...(percentile(firstOutputs, 95) !== undefined ? { p95: percentile(firstOutputs, 95) } : {}),
      ...(percentile(firstOutputs, 99) !== undefined ? { p99: percentile(firstOutputs, 99) } : {}),
      sampleCount: firstOutputs.length,
      coverage: total > 0 ? firstOutputs.length / total : null,
    },
    estimatedCostUsdPerSuccessfulRequest: costCount > 0 ? costTotalUsd / costCount : null,
    estimatedCostUsdTotalSuccessful: costCount > 0 ? costTotalUsd : null,
    usageCoverage: physicalUsageCoverage.ratio,
    priceCoverage: successes > 0 ? costCount / successes : null,
    breakdown,
    profileBreakdown,
  };
}
