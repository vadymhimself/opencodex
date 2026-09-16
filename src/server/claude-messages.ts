/**
 * Anthropic Messages inbound (/v1/messages + /v1/messages/count_tokens) for Claude Code.
 *
 * Translate-and-replay (devlog/260711_claude_inbound/010): the Anthropic request is
 * converted to a /v1/responses body and replayed through handleResponses on an
 * internal Request, so routing/OAuth/account-pool/failover/sidecars are inherited
 * unchanged. The Responses output (SSE or JSON) is converted back to Anthropic shape.
 */
import {
  anthropicMessagesUrl,
  canReplayAnthropicSource,
  isAnthropicMessageResponse,
  isExactCanonicalAnthropicMessagesUrl,
  usageFromAnthropic,
} from "../adapters/anthropic";
import type { AdapterRequest, AnthropicMessagesSource } from "../adapters/base";
import { FORWARD_HEADERS } from "../adapters/openai-responses";
import { enforceAnthropicImageLimits, sniffImageDimensions } from "../adapters/anthropic-image-guard";
import { normalizeAnthropicImages } from "../adapters/anthropic-image-normalize";
import { AnthropicRequestError, anthropicRequestCorrelationKey, anthropicToResponsesTranslation, extractOcxEffortDirective, extractOcxRouteDirective, resolveInboundModel, type ClaudeCacheKeySource } from "../claude/inbound";
import { resolveDesktop3pAlias } from "../claude/desktop-3p";
import { recordDesktopRequest } from "../claude/desktop-health";
import { stripOneMillionMarker } from "../claude/context-windows";
import { captureClaudeInbound } from "../claude/inbound-debug";
import {
  applyUpstreamRecoveryInit,
  isTransientUpstreamStatus,
} from "../lib/upstream-retry";
import { resolveClientRetryAfter } from "../lib/retry-after";
import {
  anthropicErrorBody,
  anthropicErrorResponse,
  collectAnthropicMessage,
  responsesJsonToAnthropicMessage,
  responsesSseToAnthropicSse,
} from "../claude/outbound";
import { clearableDeadline, idleDeadline } from "../lib/abort";
import {
  boundedBodyBufferGrowthsForTests,
  readBoundedResponseBytes,
} from "../lib/bounded-body";
import { estimateTokens } from "../lib/token-estimate";
import {
  NoEligiblePolicyCandidateError,
  UnknownRoutingPolicyError,
  routeConcreteModel,
  routeModel,
} from "../router";
import {
  comboRequestHasImageInput,
  getCombo,
  pickComboTarget,
  resolveComboId,
} from "../combos";
import { parseRequest } from "../responses/parser";
import { evidenceFromBody } from "../routing/request-evidence";
import { isCanonicalOpenAiForwardProvider } from "../providers/openai-tiers-destination";
import { resolveProviderTransport } from "../providers/xai-transport";
import { resolveAdapter, resolveWireProtocolOverride } from "./adapter-resolve";
import { providerFetch } from "./responses/fetch-helpers";
import {
  getValidAccessTokenSnapshot,
  publicOAuthAuthenticationErrorMessage,
} from "../oauth";
import {
  anthropicSessionKeyFromParts,
  bindAnthropicSessionAffinity,
  getAnthropicPoolAccessToken,
  getAnthropicPoolRetryAfterSeconds,
  isAnthropicAccountPoolEnabled,
  promoteAnthropicActiveAccount,
  resolveAnthropicAccountForSession,
} from "../oauth/anthropic-routing";
import type { OcxConfig } from "../types";
import { readJsonRequestBody } from "./request-decompress";
import { addFinalRequestLog, httpStatusForRequestLogTerminal, httpStatusFromTerminalError, recordFirstOutput, type RequestLogContext, type RequestLogEntry } from "./request-log";
import {
  conversationIdFromClaudeMetadata,
  sessionIdHeaderFromRequest,
} from "./request-log-conversation";
import {
  createSseInspector,
  isAnthropicSourceReplayResponse,
  responseWithDeferredRequestLog,
} from "./relay";
import { handleResponses } from "./responses";
import { comboTargetAcceptsAnthropicSource } from "./responses/core";
import {
  isApiAuthRequired,
  isDataPlaneAdmissionSecret,
  isProxyAdmissionSecret,
  type RequestPolicyView,
  type DataPlaneAdmission,
} from "./auth-cors";
import type { AdmissionLease } from "../lib/admission";
import { tryClaimNativeMainProfileForTurn } from "../codex/native-main-admission";
import { CODEX_MAIN_PROFILE_MAINTENANCE_MESSAGE } from "../codex/auth-context";
import {
  createTranslatorBudget,
  finalizeTranslatorBudgetResponse,
  isTranslatorBudgetExceededError,
  type TranslatorBudget,
} from "../lib/translator-budget";
import {
  parseRequestEffortRowId,
  type ParsedEffortRowId,
} from "./effort-row";
import {
  parseFastOnlyRowId,
  parseSyntheticRowId,
  type ParsedFastRowId,
} from "./fast-row";

type Rec = Record<string, unknown>;

/**
 * Decode a Claude selector that may carry the fast marker.
 *
 * The exact form is tried first, so a real model whose alias genuinely ends in the marker
 * keeps winning. Only then is the marker treated as synthetic and the bare base decoded:
 * a Desktop 3P alias is a HASH registered WITHOUT the marker, so an exact lookup can never
 * resolve a synthetic one.
 */
function decodeClaudeFastSelector(raw: string, cc?: OcxConfig["claudeCode"]): string {
  const exact = resolveInboundModel(raw, cc);
  if (exact !== raw || !raw.endsWith("--fast")) return exact;
  const bare = raw.slice(0, -"--fast".length);
  const decodedBase = resolveInboundModel(bare, cc);
  return decodedBase === bare ? exact : `${decodedBase}--fast`;
}

function isRec(v: unknown): v is Rec {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

/** Resolve Claude-only sidecar overrides without mutating the shared server config. */
export function buildClaudeReplayConfig(config: OcxConfig): OcxConfig {
  return {
    ...config,
    webSearchSidecar: {
      ...config.webSearchSidecar,
      ...config.claudeCode?.webSearchSidecar,
    },
    visionSidecar: {
      ...config.visionSidecar,
      ...config.claudeCode?.visionSidecar,
    },
  };
}

function claudeInboundDisabled(config: OcxConfig): Response | null {
  if (config.claudeCode?.enabled === false) {
    return anthropicErrorResponse(403, "Claude inbound is disabled (GUI: Claude ON toggle / config.claudeCode.enabled)", "permission_error");
  }
  return null;
}

async function readAnthropicBody(req: Request, budget: TranslatorBudget): Promise<unknown> {
  try {
    return await readJsonRequestBody(req, budget);
  } catch (err) {
    if (isTranslatorBudgetExceededError(err)) throw err;
    throw new AnthropicRequestError(err instanceof Error && err.message ? err.message : "Invalid JSON body");
  }
}

// ── Native Anthropic passthrough (subscription OAuth pierce) ──────────────────────
// When Claude Code runs with ONLY ANTHROPIC_BASE_URL set (subscription mode — the
// connectors warning stays off), it sends its OWN claude.ai OAuth Bearer to us.
// Requests for genuine claude/anthropic models that no alias/modelMap claims are
// forwarded VERBATIM to api.anthropic.com with the caller's credential and all
// end-to-end headers, so betas/thinking signatures/billing identity stay native.
// (Evidence: teamclaude --no-mitm + Vercel gateway docs, devlog 003/060.)

const PASSTHROUGH_STRIP_HEADERS = new Set([
  "connection", "keep-alive", "transfer-encoding", "upgrade", "te", "trailer",
  "proxy-authenticate", "proxy-authorization", "host", "content-length",
  "accept-encoding", "x-opencodex-api-key", "origin",
]);

function singleCredentialToken(name: "authorization" | "x-api-key", value: string | null): string | null {
  const raw = value?.trim() ?? "";
  // Fetch Headers comma-joins duplicate fields. Neither Anthropic credential format permits a
  // comma, so treating a joined value as one token could hide an admission secret behind a real
  // provider credential. Ambiguous credential headers fail closed.
  if (!raw || raw.includes(",")) return null;
  if (name === "authorization") {
    const match = /^Bearer\s+(.+)$/i.exec(raw);
    return match?.[1]?.trim() || null;
  }
  return raw;
}

function hasAnthropicNativeCredential(req: Request, config: OcxConfig): boolean {
  const bearer = singleCredentialToken("authorization", req.headers.get("authorization"));
  const apiKey = singleCredentialToken("x-api-key", req.headers.get("x-api-key"));
  return (!!bearer && bearer.startsWith("sk-ant-") && !isProxyAdmissionSecret(bearer, config))
    || (!!apiKey && apiKey.startsWith("sk-ant-") && !isProxyAdmissionSecret(apiKey, config));
}

function wantsNativePassthrough(
  req: Request,
  config: OcxConfig,
  requestPolicy: RequestPolicyView,
  model: unknown,
): model is string {
  if (config.claudeCode?.nativePassthrough === false) return false;
  if (typeof model !== "string" || !/^(claude|anthropic)/i.test(model)) return false;
  // Authorization and x-api-key both belong to the upstream on this branch. An exposed listener
  // therefore requires the dedicated admission header even though the routed Messages surface
  // keeps accepting all three legacy admission forms.
  if (isApiAuthRequired(requestPolicy)) {
    const dedicated = req.headers.get("x-opencodex-api-key")?.trim() ?? "";
    if (!isDataPlaneAdmissionSecret(dedicated, config)) return false;
  }
  if (!hasAnthropicNativeCredential(req, config)) return false;
  // An alias or modelMap hit means the user asked for a ROUTED model: translate instead.
  return resolveInboundModel(model, config.claudeCode) === model;
}

function shouldForwardNativeHeader(name: string, value: string, config: OcxConfig): boolean {
  const lowerName = name.toLowerCase();
  if (PASSTHROUGH_STRIP_HEADERS.has(lowerName)) return false;
  if (lowerName !== "authorization" && lowerName !== "x-api-key") return true;
  const token = singleCredentialToken(lowerName, value);
  return !!token && !isProxyAdmissionSecret(token, config);
}

/** Format a 32-hex cache key as a uuid-shaped session id (version/variant nibbles forced). */
function uuidFromHex(hex32: string): string {
  const h = (hex32 + "0".repeat(32)).slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-8${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

/** Body-occupancy guard for the native passthrough (devlog 260716_passthrough_followups/010). */
export interface PassthroughBodyGuard {
  /** Idle window in ms — raw upstream-byte inactivity while a read is pending. 0 disables. */
  stallMs: number;
  /** Cumulative body byte cap. 0 disables. */
  maxBytes: number;
  /** Client request signal for deterministic cancel classification. */
  reqSignal?: AbortSignal;
}

type PassthroughCloseReason = "terminal" | "client_cancel" | "body_stall" | "body_overflow";
type AnthropicTapFinalMeta = {
  closeReason: PassthroughCloseReason;
  terminalStatus?: RequestLogEntry["terminalStatus"];
};

/**
 * Tap an Anthropic-vocabulary SSE stream for usage and terminal logging while
 * forwarding successful bytes unchanged. Transport failures get one Anthropic
 * error frame because response headers may already be committed.
 */
export function tapAnthropicSseForLog(
  upstream: ReadableStream<Uint8Array>,
  logCtx: RequestLogContext,
  finalize: (status: number, meta: AnthropicTapFinalMeta) => void,
  guard?: PassthroughBodyGuard,
  onFirstOutput?: () => void,
): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  let usageAcc: Rec = {};
  let terminalStatus: RequestLogEntry["terminalStatus"];
  let firstOutputRecorded = false;
  let pendingValidatedFrame: Uint8Array | undefined;
  let pendingUpstreamChunk: Uint8Array | undefined;
  let pendingUpstreamOffset = 0;
  let protocolError: string | undefined;
  const inspector = createSseInspector({
    classifyTerminal: payload => {
      if (!isRec(payload)) return null;
      if (payload.type === "message_stop") return "completed";
      if (payload.type === "error") return "failed";
      return null;
    },
    stopAtTerminal: true,
    strictJsonRecords: true,
    onValidatedFrame: frame => {
      if (protocolError) return false;
      pendingValidatedFrame = frame;
      return false;
    },
    onProtocolError: message => { protocolError = message; },
    onTerminal: status => { terminalStatus = status; },
    onParsedPayload: payload => {
      if (!isRec(payload)) return;
      if (typeof payload.type === "string" && payload.type.startsWith("response.")) {
        protocolError = `unexpected Responses event on Anthropic stream: ${payload.type}`;
        return;
      }
      if (payload.type === "message_start" && isRec(payload.message) && isRec(payload.message.usage)) {
        usageAcc = { ...usageAcc, ...payload.message.usage };
      } else if (payload.type === "message_delta" && isRec(payload.usage)) {
        usageAcc = { ...usageAcc, ...payload.usage };
      }
      if (!firstOutputRecorded
        && (payload.type === "content_block_start" || payload.type === "content_block_delta")) {
        firstOutputRecorded = true;
        onFirstOutput?.();
      }
    },
  });
  const reader = upstream.getReader();
  let settled = false;
  let bodyBytes = 0;
  let tapController: ReadableStreamDefaultController<Uint8Array> | undefined;

  const recordUsage = () => {
    logCtx.usage = usageFromAnthropic(Object.keys(usageAcc).length > 0 ? usageAcc : undefined);
  };
  const settle = (status: number, meta: AnthropicTapFinalMeta) => {
    if (settled) return false;
    settled = true;
    idle.cancel();
    detachAbort();
    inspector.dispose();
    pendingValidatedFrame = undefined;
    pendingUpstreamChunk = undefined;
    pendingUpstreamOffset = 0;
    recordUsage();
    finalize(status, meta);
    return true;
  };
  const failBody = (
    status: number,
    closeReason: "terminal" | "body_stall" | "body_overflow",
    terminal: "failed" | "incomplete",
    errType: string,
    message: string,
  ) => {
    if (!settle(status, { closeReason, terminalStatus: terminal })) return;
    const payload = JSON.stringify({ type: "error", error: { type: errType, message } });
    try {
      tapController?.enqueue(encoder.encode(`\n\nevent: error\ndata: ${payload}\n\n`));
      tapController?.close();
    } catch { /* client already torn down */ }
    reader.cancel(new DOMException(message, closeReason === "body_stall" ? "TimeoutError" : "Error")).catch(() => {});
  };
  const flushValidatedFrame = (
    controller: ReadableStreamDefaultController<Uint8Array>,
  ): boolean => {
    const frame = pendingValidatedFrame;
    pendingValidatedFrame = undefined;
    if (!frame) return false;
    controller.enqueue(frame);
    return true;
  };
  const idle = idleDeadline(guard?.stallMs ?? 0, () => {
    failBody(
      504,
      "body_stall",
      "failed",
      "timeout_error",
      `anthropic passthrough body stalled: no upstream bytes for ${Math.round((guard?.stallMs ?? 0) / 1000)}s`,
    );
  });
  const onClientAbort = () => {
    if (!settle(499, { closeReason: "client_cancel" })) return;
    try { tapController?.close(); } catch { /* downstream already torn down */ }
    reader.cancel(guard?.reqSignal?.reason).catch(() => {});
  };
  const detachAbort = (() => {
    const signal = guard?.reqSignal;
    if (!signal) return () => {};
    if (signal.aborted) {
      queueMicrotask(onClientAbort);
      return () => {};
    }
    signal.addEventListener("abort", onClientAbort, { once: true });
    return () => signal.removeEventListener("abort", onClientAbort);
  })();

  return new ReadableStream<Uint8Array>({
    start(controller) {
      tapController = controller;
    },
    async pull(controller) {
      if (settled) return;
      try {
        while (!settled) {
          if (!pendingUpstreamChunk) {
            idle.reset();
            const { done, value } = await reader.read();
            idle.pause();
            if (settled) return;
            if (done) {
              inspector.finish();
              flushValidatedFrame(controller);
              if (protocolError) {
                failBody(
                  502,
                  "terminal",
                  "failed",
                  "api_error",
                  `anthropic passthrough protocol error: ${protocolError}`,
                );
                return;
              }
              if (terminalStatus === "completed") {
                settle(200, { closeReason: "terminal", terminalStatus });
                controller.close();
              } else if (terminalStatus === "failed") {
                settle(502, { closeReason: "terminal", terminalStatus });
                controller.close();
              } else {
                failBody(
                  502,
                  "terminal",
                  "incomplete",
                  "api_error",
                  "upstream response was incomplete (adapter_eof)",
                );
              }
              return;
            }
            bodyBytes += value.byteLength;
            if (guard && guard.maxBytes > 0 && bodyBytes > guard.maxBytes) {
              failBody(
                502,
                "body_overflow",
                "failed",
                "api_error",
                `anthropic passthrough body exceeded ${guard.maxBytes} bytes`,
              );
              return;
            }
            pendingUpstreamChunk = value;
            pendingUpstreamOffset = 0;
          }

          const consumed = inspector.feed(
            pendingUpstreamChunk.subarray(pendingUpstreamOffset),
          );
          if (consumed === undefined) {
            pendingUpstreamChunk = undefined;
            pendingUpstreamOffset = 0;
          } else {
            pendingUpstreamOffset += consumed;
            if (pendingUpstreamOffset >= pendingUpstreamChunk.byteLength) {
              pendingUpstreamChunk = undefined;
              pendingUpstreamOffset = 0;
            }
          }
          const emitted = flushValidatedFrame(controller);
          if (protocolError) {
            failBody(
              502,
              "terminal",
              "failed",
              "api_error",
              `anthropic passthrough protocol error: ${protocolError}`,
            );
            return;
          }
          if (terminalStatus !== undefined) {
            const status = terminalStatus === "completed" ? 200 : 502;
            if (settle(status, { closeReason: "terminal", terminalStatus })) {
              controller.close();
              reader.cancel("Anthropic protocol terminal reached").catch(() => {});
            }
            return;
          }
          if (emitted) return;
        }
      } catch (error) {
        if (settled) return;
        failBody(
          502,
          "terminal",
          "failed",
          "api_error",
          `anthropic passthrough body failed: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    },
    cancel(reason) {
      if (!settled) settle(499, { closeReason: "client_cancel" });
      reader.cancel(reason).catch(() => {});
    },
  });
}

async function anthropicNativePassthrough(
  req: Request,
  config: OcxConfig,
  logCtx: RequestLogContext,
  logIds: { requestId: string; start: number } | undefined,
  body: Rec,
  pathname: string,
): Promise<Response> {
  const model = typeof body.model === "string" ? body.model : "unknown";
  logCtx.model = model;
  logCtx.provider = "anthropic-native";
  logCtx.requestedModel = model;
  let logged = false;
  const finalize = (status: number, meta: { closeReason: PassthroughCloseReason | "non_stream" }) => {
    if (!logIds || logged) return;
    logged = true;
    addFinalRequestLog(logIds.requestId, logIds.start, logCtx, status, meta);
  };

  const base = (config.claudeCode?.anthropicBaseUrl ?? "https://api.anthropic.com").replace(/\/$/, "");
  const search = new URL(req.url).search;
  // Native passthrough bypasses the anthropic adapter, so the generous image pipeline
  // (devlog/260714_image_normalization_pipeline/040) must run here: tier-normalize then
  // guard the already-Anthropic-wire messages before serialization. Applies to
  // count_tokens too — counts must match what the real send will contain, and the 32MB
  // body cap applies to it equally. Non-message bodies pass through untouched.
  if (Array.isArray(body.messages)) {
    await normalizeAnthropicImages(body.messages);
    enforceAnthropicImageLimits(body.messages);
  }
  const headers = new Headers();
  req.headers.forEach((value, name) => {
    if (shouldForwardNativeHeader(name, value, config)) headers.set(name, value);
  });
  headers.set("content-type", "application/json");

  const result = await fetchWithHeaderDeadline(
    `${base}${pathname}${search}`,
    { method: "POST", headers, body: JSON.stringify(body) },
    config.connectTimeoutMs ?? 200_000,
    req.signal,
  );
  if (result.kind === "timeout") {
    finalize(504, { closeReason: "non_stream" });
    return anthropicErrorResponse(504, "anthropic passthrough timed out waiting for response headers", "timeout_error");
  }
  if (result.kind === "error") {
    const err = result.error;
    finalize(502, { closeReason: "non_stream" });
    return anthropicErrorResponse(502, `anthropic passthrough failed: ${err instanceof Error ? err.message : String(err)}`, "api_error");
  }
  const upstream = result.upstream;

  const contentType = upstream.headers.get("content-type") ?? "application/json";
  const bodyGuard = resolvePassthroughBodyGuard(config, req.signal);
  if (upstream.ok && contentType.includes("text/event-stream") && upstream.body) {
    return new Response(tapAnthropicSseForLog(upstream.body, logCtx, finalize, bodyGuard), {
      status: upstream.status,
      headers: {
        "Content-Type": contentType,
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
      },
    });
  }
  // Non-stream (count_tokens, errors, stream:false): relay verbatim under the same
  // idle/size bounds — headers are NOT yet sent here, so real statuses are available.
  const bodyResult = await readBoundedPassthroughBody(upstream, bodyGuard);
  if (bodyResult.kind === "client_cancel") {
    finalize(499, { closeReason: "client_cancel" });
    return anthropicErrorResponse(499, "client closed request during anthropic passthrough", "api_error");
  }
  if (bodyResult.kind === "stall") {
    finalize(504, { closeReason: "body_stall" });
    return anthropicErrorResponse(504, `anthropic passthrough body stalled: no upstream bytes for ${Math.round(bodyGuard.stallMs / 1000)}s`, "timeout_error");
  }
  if (bodyResult.kind === "overflow") {
    finalize(502, { closeReason: "body_overflow" });
    return anthropicErrorResponse(502, `anthropic passthrough body exceeded ${bodyGuard.maxBytes} bytes`, "api_error");
  }
  const text = bodyResult.text;
  if (upstream.ok) {
    try {
      const parsed = JSON.parse(text) as { usage?: Rec };
      if (isRec(parsed?.usage)) logCtx.usage = usageFromAnthropic(parsed.usage);
    } catch { /* count_tokens etc. */ }
  }
  finalize(upstream.status, { closeReason: "non_stream" });
  const retryAfter = upstream.headers.get("retry-after");
  return new Response(text, {
    status: upstream.status,
    headers: { "Content-Type": contentType, ...(retryAfter ? { "Retry-After": retryAfter } : {}) },
  });
}

const DEFAULT_BODY_STALL_SEC = 90;
const DEFAULT_BODY_MAX_BYTES = 64 * 1024 * 1024;

/**
 * Normalize the claudeCode body-guard config (devlog 260716_passthrough_followups/010).
 * Policy: exactly 0 disables; finite positive values are honored (stall clamped to
 * min 1s); negative/non-finite/absent values fall back to the defaults.
 */
export function resolvePassthroughBodyGuard(config: OcxConfig, reqSignal?: AbortSignal): PassthroughBodyGuard {
  const rawSec = config.claudeCode?.bodyStallSec;
  const stallSec = rawSec === 0
    ? 0
    : typeof rawSec === "number" && Number.isFinite(rawSec) && rawSec > 0
      ? Math.max(1, rawSec)
      : DEFAULT_BODY_STALL_SEC;
  const rawBytes = config.claudeCode?.bodyMaxBytes;
  const maxBytes = rawBytes === 0
    ? 0
    : typeof rawBytes === "number" && Number.isFinite(rawBytes) && rawBytes > 0
      ? Math.floor(rawBytes)
      : DEFAULT_BODY_MAX_BYTES;
  return { stallMs: stallSec * 1000, maxBytes, ...(reqSignal ? { reqSignal } : {}) };
}

type BoundedPassthroughBytes =
  | { kind: "ok"; bytes: Uint8Array }
  | { kind: "stall" }
  | { kind: "overflow" }
  | { kind: "client_cancel" };

type BoundedPassthroughBody =
  | { kind: "ok"; text: string }
  | Exclude<BoundedPassthroughBytes, { kind: "ok" }>;

export function passthroughBodyBufferGrowthsForTests(): number {
  return boundedBodyBufferGrowthsForTests();
}

export async function readBoundedPassthroughBytes(
  upstream: Response,
  guard: PassthroughBodyGuard,
): Promise<BoundedPassthroughBytes> {
  try {
    const result = await readBoundedResponseBytes(upstream, {
      signal: guard.reqSignal,
      maxBytes: guard.maxBytes > 0 ? guard.maxBytes : Number.MAX_SAFE_INTEGER,
      ...(guard.stallMs > 0 ? { inactivityTimeoutMs: guard.stallMs } : {}),
    });
    return result.oversized
      ? { kind: "overflow" }
      : { kind: "ok", bytes: result.bytes };
  } catch (error) {
    if (guard.reqSignal?.aborted) return { kind: "client_cancel" };
    if (error instanceof DOMException && error.name === "TimeoutError") return { kind: "stall" };
    throw error;
  }
}

export async function readBoundedPassthroughBody(
  upstream: Response,
  guard: PassthroughBodyGuard,
): Promise<BoundedPassthroughBody> {
  const result = await readBoundedPassthroughBytes(upstream, guard);
  return result.kind === "ok"
    ? { kind: "ok", text: new TextDecoder().decode(result.bytes) }
    : result;
}

/**
 * Header-phase fetch guarded by a clearable deadline (PR #136 follow-up hardening).
 *
 * The deadline covers ONLY the wait for response headers; once `fetch` settles —
 * fulfilled OR rejected — the timer must die. The `finally` block guarantees
 * `clear()` on every path (success, upstream reject, deadline expiry), fixing the
 * timer leak where a rejected fetch left the deadline running until expiry.
 * `didExpire()` stays truthful after `clear()` (see src/lib/abort.ts), so timeout
 * classification inside the catch is unaffected by the finally cleanup.
 *
 * `makeDeadline`/`fetchImpl` are injectable for deterministic unit tests.
 */
export type HeaderDeadlineFetchResult =
  | { kind: "response"; upstream: Response }
  | { kind: "timeout" }
  | { kind: "error"; error: unknown };

export async function fetchWithHeaderDeadline(
  input: string | URL,
  init: RequestInit,
  timeoutMs: number,
  parent?: AbortSignal,
  makeDeadline: typeof clearableDeadline = clearableDeadline,
  fetchImpl: typeof fetch = fetch,
): Promise<HeaderDeadlineFetchResult> {
  const deadline = makeDeadline(timeoutMs, parent);
  try {
    const upstream = await fetchImpl(input, { ...init, signal: deadline.signal, timeout: 0 });
    return { kind: "response", upstream };
  } catch (error) {
    if (deadline.didExpire()) return { kind: "timeout" };
    return { kind: "error", error };
  } finally {
    deadline.clear();
  }
}

export async function handleClaudeMessages(
  req: Request,
  config: OcxConfig,
  logCtx: RequestLogContext,
  logIds?: { requestId: string; start: number; turnAdmissionLease?: AdmissionLease; admission?: DataPlaneAdmission },
  requestPolicy: RequestPolicyView = config,
): Promise<Response> {
  const translatorBudget = createTranslatorBudget();
  try {
    return finalizeTranslatorBudgetResponse(
      await handleClaudeMessagesWithBudget(req, config, logCtx, translatorBudget, logIds, requestPolicy),
      translatorBudget,
    );
  } catch (error) {
    translatorBudget.dispose();
    throw error;
  }
}

async function handleClaudeMessagesWithBudget(
  req: Request,
  config: OcxConfig,
  logCtx: RequestLogContext,
  translatorBudget: TranslatorBudget,
  logIds?: { requestId: string; start: number; turnAdmissionLease?: AdmissionLease; admission?: DataPlaneAdmission },
  requestPolicy: RequestPolicyView = config,
): Promise<Response> {
  logCtx.surface = "claude";
  const disabled = claudeInboundDisabled(config);
  if (disabled) {
    if (logIds) addFinalRequestLog(logIds.requestId, logIds.start, logCtx, 403, { closeReason: "non_stream" });
    return disabled;
  }

  let anthropicBody: unknown;
  let originalAnthropicBody: Rec | undefined;
  let anthropicMessagesSource: AnthropicMessagesSource | undefined;
  let comboRandomSeed: string | undefined;
  let internalBody: Rec;
  let cacheKeySource: ClaudeCacheKeySource = null;
  let effortOverride: string | null = null;
  let effortRow: ParsedEffortRowId | null = null;
  let fastRow: ParsedFastRowId | null = null;
  let requestedModel = "";
  try {
    anthropicBody = await readAnthropicBody(req, translatorBudget);
    // Defensive [1m] strip (devlog 138): clients normally remove the context-variant
    // marker themselves; the 1M signal we act on is the anthropic-beta header.
    // Case-insensitive — the CLI matches /\[1m\]/i (audit 021 #7).
    if (isRec(anthropicBody) && typeof anthropicBody.model === "string") {
      anthropicBody.model = stripOneMillionMarker(anthropicBody.model);
    }
    // ocx-route override (devlog 072): injected agent bodies pin their model via a
    // system-prompt directive because 2.1.207 ignores custom ids in agent
    // frontmatter. Must run BEFORE the native-passthrough branch — the CLI sends
    // these subagent turns under a fallback claude model id.
    if (isRec(anthropicBody)) {
      const routeOverride = extractOcxRouteDirective(anthropicBody);
      if (routeOverride && typeof anthropicBody.model === "string") {
        anthropicBody.model = stripOneMillionMarker(routeOverride);
        effortOverride = extractOcxEffortDirective(anthropicBody);
      }
    }
    if (isRec(anthropicBody) && typeof anthropicBody.model === "string") {
      requestedModel = anthropicBody.model;
      // Decode for Fast only. A Claude alias is `claude-ocx-<provider>--<model>`, so it
      // already uses `--` as its own separator: stripping the marker off the RAW alias would
      // turn `claude-ocx-p--foo--fast` into `claude-ocx-p--foo` and route a DIFFERENT model.
      // Effort parsing keeps the raw selector, so its behaviour is untouched.
      ({ fastRow, effortRow } = parseSyntheticRowId(
        requestedModel,
        config,
        () => decodeClaudeFastSelector(requestedModel, config.claudeCode),
      ));
      if (effortRow) {
        anthropicBody.model = effortRow.baseId;
        effortOverride = effortRow.effort;
      }
      if (fastRow) anthropicBody.model = fastRow.baseId;
    }
    // Debug capture (opt-in allowlist scalars) BEFORE the passthrough branch so
    // native, routed, and disabled-alias paths are all observable (devlog 130 B1).
    captureClaudeInbound(
      "messages",
      anthropicBody,
      isRec(anthropicBody) && typeof anthropicBody.model === "string"
        ? resolveInboundModel(anthropicBody.model, config.claudeCode)
        : undefined,
      req.headers.get("anthropic-beta") ?? undefined,
    );
    // Client surface discrimination: Desktop 3P aliases resolve through the
    // desktop registry; Code uses readable aliases or direct model names.
    if (isRec(anthropicBody) && typeof anthropicBody.model === "string" && resolveDesktop3pAlias(anthropicBody.model)) {
      logCtx.surface = "claude-desktop";
      recordDesktopRequest();
    }
    // Correlate before native passthrough so Anthropic-credential turns still filter/total (#330 / #522).
    if (isRec(anthropicBody)) {
      const claudeConversationId = conversationIdFromClaudeMetadata(
        isRec(anthropicBody.metadata) ? anthropicBody.metadata : undefined,
      );
      if (claudeConversationId) logCtx.conversationId = claudeConversationId;
    }
    // A fast row blocks passthrough, unlike the chat case: this path forwards to Anthropic's
    // own API, whose wire has no service_tier field and whose FastWire kind has an empty
    // adapter set by design, so the tier would be silently dropped.
    if (!effortRow && !fastRow && isRec(anthropicBody) && wantsNativePassthrough(req, config, requestPolicy, anthropicBody.model)) {
      return await anthropicNativePassthrough(req, config, logCtx, logIds, anthropicBody, "/v1/messages");
    }
    if (isRec(anthropicBody) && effortOverride) {
      anthropicBody.output_config = {
        ...(isRec(anthropicBody.output_config) ? anthropicBody.output_config : {}),
        effort: effortOverride,
      };
      delete anthropicBody.thinking;
    }
    if (isRec(anthropicBody)) {
      originalAnthropicBody = structuredClone(anthropicBody);
      comboRandomSeed = anthropicRequestCorrelationKey(anthropicBody);
    }
    const translation = anthropicToResponsesTranslation(anthropicBody, config.claudeCode);
    internalBody = translation.body;
    if (originalAnthropicBody) {
      anthropicMessagesSource = {
        body: originalAnthropicBody,
        headers: {
          anthropicVersion: req.headers.get("anthropic-version")?.trim() || undefined,
          anthropicBeta: req.headers.get("anthropic-beta")?.trim() || undefined,
        },
        ...(translation.requiresExactAnthropicReplay ? { requiresExactReplay: true } : {}),
      };
    }
    // The Anthropic translator builds its body from model/input/store/stream plus sampling
    // fields only, so the caller intent is applied to the TRANSLATED body rather than the
    // inbound one.
    if (fastRow) internalBody.service_tier = "priority";
    translatorBudget.chargeRetained(new TextEncoder().encode(JSON.stringify(internalBody)).byteLength, { kind: "request_copies" });
    cacheKeySource = translation.cacheKeySource;
  } catch (err) {
    const overflow = isTranslatorBudgetExceededError(err);
    const status = overflow ? 413 : err instanceof AnthropicRequestError ? 400 : 500;
    if (logIds) addFinalRequestLog(logIds.requestId, logIds.start, logCtx, status, { closeReason: "non_stream" });
    return anthropicErrorResponse(
      status,
      overflow ? "request translation buffer exceeded the safe limit" : err instanceof Error ? err.message : String(err),
      overflow ? "request_too_large" : undefined,
      overflow ? "translation_buffer_limit" : undefined,
    );
  }

  if (!requestedModel) requestedModel = (anthropicBody as Rec).model as string;
  const stream = internalBody.stream === true;
  // Routed adapters only support streamed turns; always stream internally and fold
  // the translated Anthropic SSE into a message JSON for non-streaming clients.
  internalBody.stream = true;

  // Native ChatGPT passthrough (openai-responses forward) accepts only Codex-shaped
  // bodies: it 400s on sampling params ("Unsupported parameter: max_output_tokens",
  // verified live 2026-07-11). Strip them for that route; routed providers keep them.
  let nativeRoute = false;
  const routeSource = anthropicMessagesSource;
  const comboRequestCompatible = routeSource
    ? (target: Readonly<{ provider: string; model: string }>) => comboTargetAcceptsAnthropicSource(
        config,
        target,
        routeSource,
        internalBody,
        "anthropic",
      )
    : undefined;
  try {
    const route = routeModel(
      config,
      internalBody.model as string,
      evidenceFromBody(internalBody),
      {
        ...(comboRequestCompatible ? {
          comboEligible: comboRequestCompatible,
          comboRequestCompatible,
        } : {}),
        ...(comboRandomSeed ? { comboRandomSeed } : {}),
      },
    );
    // Settle the wire once so the sampling decision below reads the effective
    // adapter rather than the provider-wide default (#404).
    route.provider = resolveWireProtocolOverride(route.providerName, route.modelId, route.provider, "anthropic");
    logCtx.routeDecision = route.routeDecision;
    if (isCanonicalOpenAiForwardProvider(route.provider)) {
      nativeRoute = true;
      delete internalBody.max_output_tokens;
      delete internalBody.temperature;
      delete internalBody.top_p;
      delete internalBody.stop;
      delete internalBody.user;
    }
    // Estimated-usage adapters (cursor/kiro) report no per-turn input tokens; stash a
    // request-side estimate so the log's in:0 rows get a floor. NEVER set this for
    // accurate-usage adapters — the request-log merge is max(reported, estimate) and
    // would overwrite real usage (audit 133 R1#7).
    if (route.provider.adapter === "cursor" || route.provider.adapter === "kiro") {
      logCtx.usageLogInputTokens = estimateClaudeRequestTokens(anthropicBody as Rec, requestedModel);
    }
    // Effort safety valve (devlog 136 B6, audit 139 R2#2): opus-shaped aliases make
    // every routed model look like a reasoning model to Claude clients, so a forced
    // effort (CLAUDE_CODE_ALWAYS_ENABLE_EFFORT) would leak reasoning params to routes
    // that affirmatively expose NO effort control. Strip only on a definitive [] from
    // supportedLadderFor; unknown (undefined) passes through untouched.
    if (internalBody.reasoning !== undefined) {
      const { supportedLadderFor } = await import("./effort-policy");
      const ladder = supportedLadderFor({ provider: route.provider, modelId: route.modelId });
      if (ladder !== undefined && ladder.length === 0) delete internalBody.reasoning;
    }
  } catch (err) {
    if (err instanceof UnknownRoutingPolicyError) {
      logCtx.requestedModel = requestedModel;
      if (logIds) addFinalRequestLog(logIds.requestId, logIds.start, logCtx, 404, { closeReason: "non_stream" });
      return anthropicErrorResponse(404, err.message, "invalid_request_error");
    }
    if (err instanceof NoEligiblePolicyCandidateError) {
      logCtx.routeDecision = err.trace;
      if (logIds) addFinalRequestLog(logIds.requestId, logIds.start, logCtx, 404, { closeReason: "non_stream" });
      return anthropicErrorResponse(404, err.message, "invalid_request_error");
    }
    /* unknown model: let handleResponses shape the 404 */
  }

  const headers = new Headers({ "content-type": "application/json" });
  for (const name of FORWARD_HEADERS) {
    // The caller's bearer is the proxy admission token (ocx claude placeholder), never a
    // ChatGPT credential — forwarding it upstream turns into {"detail":"Unauthorized"}.
    if (name === "authorization") continue;
    const value = req.headers.get(name);
    if (value) headers.set(name, value);
  }
  // Routed replays need main ChatGPT auth so OpenAI-backed sidecars remain reachable;
  // native replays have no caller ChatGPT credential. This enrichment is optional:
  // auth-context later rejects a real physical-main selection, while routed/pool
  // traffic continues without reading native credentials during a fence/recovery.
  if (tryClaimNativeMainProfileForTurn(logIds?.turnAdmissionLease)) {
    const { getMainAccountToken } = await import("../codex/main-account");
    const token = getMainAccountToken();
    if (token) {
      headers.set("authorization", `Bearer ${token.accessToken}`);
      headers.set("chatgpt-account-id", token.chatgptAccountId);
    }
  }
  if (nativeRoute) {
    // ChatGPT-backend prompt-cache affinity rides the session_id HEADER (codex
    // clients always send their session uuid; devlog 090 follow-up: body-level
    // prompt_cache_key alone still yielded cached_tokens:0). Claude Code never sends
    // the header, so synthesize a stable per-session uuid from the same cache key —
    // but ONLY for a real per-session key (metadata.user_id). The system-hash fallback
    // key is shared across Desktop conversations, and a shared session_id's backend
    // semantics are unproven (audit 133 R2#3): body prompt_cache_key only there.
    if (cacheKeySource === "metadata" && !headers.has("session_id") && typeof internalBody.prompt_cache_key === "string") {
      headers.set("session_id", uuidFromHex(internalBody.prompt_cache_key));
    }
  }
  const internalBodyJson = JSON.stringify(internalBody);
  translatorBudget.chargeRetained(new TextEncoder().encode(internalBodyJson).byteLength, { kind: "request_copies" });
  const internalReq = new Request("http://localhost/v1/responses", {
    method: "POST",
    headers,
    body: internalBodyJson,
  });

  // Request-log wiring mirrors the /v1/responses route: native passthrough finalizes
  // via the terminal callbacks; routed streams get the Responses-vocabulary log tap
  // BEFORE translation (the translated Anthropic stream has no response.completed
  // frame, so tapping it records a bogus 502 with no usage/cache detail).
  let nativeLogged = false;
  const finalizeNativeLog = (status: number, meta: AnthropicTapFinalMeta) => {
    if (!logIds || nativeLogged) return;
    nativeLogged = true;
    addFinalRequestLog(logIds.requestId, logIds.start, logCtx, status, meta);
  };
  const upstream = await handleResponses(internalReq, buildClaudeReplayConfig(config), logCtx, {
    // Routing keeps Claude-only sidecar overrides; admission policy must follow the live owner.
    codexAuthPolicy: config,
    ...(logIds?.admission ? { admission: logIds.admission } : {}),
    ...(logIds?.turnAdmissionLease ? { turnAdmissionLease: logIds.turnAdmissionLease } : {}),
    abortSignal: req.signal,
    promptCacheKeyIsSharedCohort: cacheKeySource === "system",
    // The body is Responses-shaped by now, but the client spoke Anthropic Messages.
    // Without this the replay would look native and a Responses-scoped wire default
    // would fire, disagreeing with the pre-flight decision above.
    inboundWire: "anthropic",
    ...(anthropicMessagesSource ? { anthropicMessagesSource } : {}),
    ...(comboRandomSeed ? { comboRandomSeed } : {}),
    stripClaudeMainAuthForNoncanonicalForward: true,
    translatorBudget,
    ...(logIds ? { onFirstOutput: () => recordFirstOutput(logCtx, logIds.start) } : {}),
    onNativePassthroughTerminal: status => finalizeNativeLog(httpStatusForRequestLogTerminal(status, logCtx), { terminalStatus: status, closeReason: "terminal" }),
    onNativePassthroughCancel: () => finalizeNativeLog(499, { closeReason: "client_cancel" }),
  });
  const response = logIds ? responseWithDeferredRequestLog(upstream, logIds.requestId, logIds.start, logCtx) : upstream;

  if (isAnthropicSourceReplayResponse(response)) {
    const finalizeSource = (status: number, meta: AnthropicTapFinalMeta) =>
      finalizeNativeLog(status, meta);
    if (!response.ok) {
      finalizeSource(response.status, { closeReason: "terminal" });
      return response;
    }

    const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
    if (stream) {
      if (!contentType.includes("text/event-stream") || !response.body) {
        void response.body?.cancel().catch(() => {});
        finalizeSource(502, { terminalStatus: "failed", closeReason: "terminal" });
        return anthropicErrorResponse(502, "canonical Anthropic returned JSON for a streaming request", "api_error");
      }
      return new Response(
        tapAnthropicSseForLog(
          response.body,
          logCtx,
          finalizeSource,
          resolvePassthroughBodyGuard(config, req.signal),
          logIds ? () => recordFirstOutput(logCtx, logIds.start) : undefined,
        ),
        { status: response.status, statusText: response.statusText, headers: response.headers },
      );
    }

    if (!contentType.includes("application/json")) {
      void response.body?.cancel().catch(() => {});
      finalizeSource(502, { terminalStatus: "failed", closeReason: "terminal" });
      return anthropicErrorResponse(502, "canonical Anthropic response was not valid JSON", "api_error");
    }
    let bytes: Uint8Array;
    try {
      const result = await readBoundedResponseBytes(response, {
        signal: req.signal,
        maxBytes: resolvePassthroughBodyGuard(config, req.signal).maxBytes || Number.MAX_SAFE_INTEGER,
      });
      if (result.oversized) {
        finalizeSource(502, { terminalStatus: "failed", closeReason: "terminal" });
        return anthropicErrorResponse(502, "canonical Anthropic response exceeded the configured body limit", "api_error");
      }
      bytes = result.bytes;
    } catch {
      finalizeSource(502, { terminalStatus: "failed", closeReason: "terminal" });
      return anthropicErrorResponse(502, "canonical Anthropic response was not valid JSON", "api_error");
    }
    let payload: unknown;
    try {
      payload = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    } catch {
      finalizeSource(502, { terminalStatus: "failed", closeReason: "terminal" });
      return anthropicErrorResponse(502, "canonical Anthropic response was not valid JSON", "api_error");
    }
    if (isRec(payload) && payload.type === "error" && isRec(payload.error)) {
      const error = payload.error;
      const status = httpStatusFromTerminalError({
        type: typeof error.type === "string" ? error.type : undefined,
        code: error.code === null || typeof error.code === "string" ? error.code : undefined,
        message: typeof error.message === "string" ? error.message : undefined,
      });
      logCtx.terminalHttpStatus = status;
      finalizeSource(status, { terminalStatus: "failed", closeReason: "terminal" });
      return new Response(new Uint8Array(bytes), {
        status,
        ...(status === response.status ? { statusText: response.statusText } : {}),
        headers: response.headers,
      });
    }
    if (!isAnthropicMessageResponse(payload)) {
      finalizeSource(502, { terminalStatus: "failed", closeReason: "terminal" });
      return anthropicErrorResponse(502, "canonical Anthropic returned an invalid JSON response", "api_error");
    }
    logCtx.usage = usageFromAnthropic(isRec(payload.usage) ? payload.usage : undefined);
    finalizeSource(response.status, { closeReason: "terminal" });
    return new Response(new Uint8Array(bytes), {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  }

  if (!response.ok) {
    // Re-shape the OpenAI-style error envelope into the Anthropic one, preserving status.
    let message = `upstream error (${response.status})`;
    try {
      const text = await response.text();
      try {
        const parsed = JSON.parse(text) as { error?: { message?: string; type?: string } | string; message?: string };
        const nested = typeof parsed?.error === "object" && parsed.error ? parsed.error.message : undefined;
        const flat = typeof parsed?.error === "string" ? parsed.error : parsed?.message;
        message = nested || flat || (text ? `upstream error (${response.status}): ${text.slice(0, 400)}` : message);
      } catch {
        if (text) message = `upstream error (${response.status}): ${text.slice(0, 400)}`;
      }
    } catch { /* keep fallback message */ }
    const upstreamRetryAfter = response.headers.get("retry-after");
    const retryAfter = resolveClientRetryAfter({
      status: response.status,
      message,
      upstreamRetryAfter,
    })
      // Instant-retry "0" is a valid client directive but rejected by cooldown parsers.
      // Preserve it so it still wins over the transient "2" fallback (claude-529 mapping).
      ?? (upstreamRetryAfter?.trim() === "0" ? "0" : undefined);
    // Transient upstream 5xx (already retried pre-stream, 010): reclassify as Anthropic
    // 529 overloaded_error so the Claude Code client applies its built-in backoff retry
    // instead of dying on a fatal api_error (260716 sol-builder incident). The request
    // log keeps the upstream status (captured in the deferred-log closure before this
    // rewrite): log = upstream truth, client = retry signal.
    // Retryable 429s also get Retry-After (#507) so Codex-shaped clients and Claude Code
    // share a backoff hint when the upstream omitted the header.
    const nativeMainFence = response.status === 503
      && upstreamRetryAfter?.trim() === "1"
      && message === CODEX_MAIN_PROFILE_MAINTENANCE_MESSAGE;
    const transient = !nativeMainFence && isTransientUpstreamStatus(response.status);
    const outStatus = nativeMainFence ? 503 : transient ? 529 : response.status;
    const out = new Response(JSON.stringify(anthropicErrorBody(outStatus, message)), {
      status: outStatus,
      headers: {
        "Content-Type": "application/json",
        ...(retryAfter ? { "Retry-After": retryAfter } : (transient ? { "Retry-After": "2" } : {})),
      },
    });
    return out;
  }

  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("text/event-stream") && response.body) {
    const anthropicSse = responsesSseToAnthropicSse(response.body, requestedModel, { translatorBudget });
    if (stream) {
      return new Response(anthropicSse, {
        status: 200,
        headers: {
          "Content-Type": "text/event-stream; charset=utf-8",
          "Cache-Control": "no-cache",
          "Connection": "keep-alive",
        },
      });
    }
    let message: Rec;
    try {
      message = await collectAnthropicMessage(anthropicSse, requestedModel, translatorBudget);
    } catch (error) {
      if (isTranslatorBudgetExceededError(error)) {
        return anthropicErrorResponse(413, error.message, "request_too_large", error.code);
      }
      return anthropicErrorResponse(502, error instanceof Error ? error.message : String(error), "api_error");
    }
    const isError = (message as Rec).type === "error";
    const translatedError = isError && typeof (message as Rec).error === "object"
      ? (message as { error: { code?: unknown; message?: unknown } }).error
      : undefined;
    if (translatedError?.code === "translation_buffer_limit") {
      return anthropicErrorResponse(
        413,
        typeof translatedError.message === "string"
          ? translatedError.message
          : "upstream translation buffer exceeded the safe limit",
        "request_too_large",
        "translation_buffer_limit",
      );
    }
    return new Response(JSON.stringify(message), {
      status: isError ? 502 : 200,
      headers: { "Content-Type": "application/json" },
    });
  }

  // Defensive: some passthrough paths may answer JSON despite stream:true.
  let json: unknown;
  try {
    json = await response.json();
  } catch {
    return anthropicErrorResponse(502, "internal replay returned a non-JSON response", "api_error");
  }
  const status = (json as Rec)?.status;
  if (status === "failed") {
    const error = (json as { error?: { message?: string; code?: string } }).error;
    if (error?.code === "translation_buffer_limit") {
      return anthropicErrorResponse(
        413,
        error.message ?? "upstream translation buffer exceeded the safe limit",
        "request_too_large",
        "translation_buffer_limit",
      );
    }
    return anthropicErrorResponse(502, error?.message ?? "upstream request failed", "api_error");
  }
  const message = responsesJsonToAnthropicMessage(json, requestedModel);
  if ((message as Rec).type === "error") {
    return new Response(JSON.stringify(message), {
      status: 529,
      headers: { "Content-Type": "application/json", "Retry-After": "2" },
    });
  }
  if (!stream) {
    return new Response(JSON.stringify(message), { status: 200, headers: { "Content-Type": "application/json" } });
  }
  // Streaming client + JSON upstream: synthesize a minimal valid Anthropic stream.
  const encoder = new TextEncoder();
  const frames: string[] = [];
  const emit = (name: string, data: Rec) => frames.push(`event: ${name}\ndata: ${JSON.stringify(data)}\n\n`);
  emit("message_start", { type: "message_start", message: { ...message, content: [], stop_reason: null, usage: { input_tokens: 0, output_tokens: 0 } } });
  const blocks = Array.isArray((message as Rec).content) ? (message as Rec).content as Rec[] : [];
  blocks.forEach((block, index) => {
    emit("content_block_start", { type: "content_block_start", index, content_block: block });
    emit("content_block_stop", { type: "content_block_stop", index });
  });
  emit("message_delta", { type: "message_delta", delta: { stop_reason: (message as Rec).stop_reason ?? "end_turn", stop_sequence: (message as Rec).stop_sequence ?? null }, usage: (message as Rec).usage ?? {} });
  emit("message_stop", { type: "message_stop" });
  return new Response(encoder.encode(frames.join("")), {
    status: 200,
    headers: { "Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-cache" },
  });
}

/** Per-attachment token estimate for a base64 payload: real image dimensions when the
 * header is sniffable (Anthropic prices images at ~pixels/750), else decoded bytes/512,
 * min 256 — the same shape as the Kiro usage estimator (estimateKiroImageTokens). */
function estimateBase64AttachmentTokens(data: string): number {
  const dims = sniffImageDimensions(data);
  if (dims) return Math.max(256, Math.ceil((dims.width * dims.height) / 750));
  const unpadded = data.endsWith("==") ? data.length - 2 : data.endsWith("=") ? data.length - 1 : data.length;
  return Math.max(256, Math.ceil(Math.floor((unpadded * 3) / 4) / 512));
}

/**
 * Char-based token estimate for an Anthropic-shaped request body. Base64 attachment
 * payloads (image/document blocks in message content, including blocks nested in
 * tool_result.content) are counted as a bounded per-attachment estimate instead of raw
 * characters: one 2MB screenshot is ~2.7M base64 chars, which the plain chars/token
 * divide reports as hundreds of thousands of tokens versus a real cost around 1.6k.
 * That breaks the >2x drift bound the estimator is held to (devlog 260711_claude_inbound
 * 040 §3). Text and url sources are left in place and counted as characters, as is
 * anything outside protocol content positions (tool_use.input, tool schemas).
 */
export function estimateClaudeRequestTokens(
  raw: { system?: unknown; messages?: unknown; tools?: unknown },
  modelId: string | undefined,
): number {
  let attachmentTokens = 0;
  // Blank base64 payloads ONLY in protocol content positions: message content blocks and
  // blocks nested in tool_result.content. tool_use.input and tool schemas can legitimately
  // contain attachment-shaped JSON, and those bytes ARE serialized into function_call
  // arguments / tool definitions for routed providers, so they must keep counting as text.
  // system is text-only per the Anthropic protocol (no attachment sources), so it is
  // stringified as-is.
  const sanitizeBlock = (block: unknown): unknown => {
    if (!block || typeof block !== "object") return block;
    const b = block as Record<string, unknown>;
    if (b.type === "image" || b.type === "document") {
      const source = b.source as { type?: unknown; data?: unknown } | undefined;
      if (source && typeof source === "object" && source.type === "base64" && typeof source.data === "string") {
        attachmentTokens += estimateBase64AttachmentTokens(source.data);
        return { ...b, source: { ...(source as Record<string, unknown>), data: "" } };
      }
      return block;
    }
    if (b.type === "tool_result" && Array.isArray(b.content)) {
      return { ...b, content: (b.content as unknown[]).map(sanitizeBlock) };
    }
    return block;
  };
  const sanitizedMessages = (messages: unknown): unknown =>
    Array.isArray(messages)
      ? messages.map(message => {
          if (!message || typeof message !== "object") return message;
          const m = message as Record<string, unknown>;
          return Array.isArray(m.content) ? { ...m, content: (m.content as unknown[]).map(sanitizeBlock) } : message;
        })
      : messages;
  const parts: string[] = [];
  if (raw.system !== undefined) parts.push(typeof raw.system === "string" ? raw.system : JSON.stringify(raw.system));
  if (raw.messages !== undefined) parts.push(JSON.stringify(sanitizedMessages(raw.messages)));
  if (raw.tools !== undefined) parts.push(JSON.stringify(raw.tools));
  return Math.max(1, estimateTokens(parts.join("\n"), modelId) + attachmentTokens);
}

export async function handleClaudeCountTokens(
  req: Request,
  config: OcxConfig,
  requestPolicy: RequestPolicyView = config,
): Promise<Response> {
  const disabled = claudeInboundDisabled(config);
  if (disabled) return disabled;

  const translatorBudget = createTranslatorBudget();
  try {
    let body: unknown;
    try {
      body = await readAnthropicBody(req, translatorBudget);
    } catch (err) {
      const overflow = isTranslatorBudgetExceededError(err);
      return anthropicErrorResponse(
        overflow ? 413 : err instanceof AnthropicRequestError ? 400 : 500,
        overflow
          ? "request translation buffer exceeded the safe limit"
          : err instanceof Error ? err.message : String(err),
        overflow ? "request_too_large" : undefined,
        overflow ? "translation_buffer_limit" : undefined,
      );
    }
    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return anthropicErrorResponse(400, "request body must be a JSON object");
    }
    const raw = body as Rec;
    if (typeof raw.model !== "string" || raw.model.length === 0) {
      return anthropicErrorResponse(400, "model is required");
    }
    let model = raw.model;
    const stripped = stripOneMillionMarker(model);
    if (stripped !== model) {
      model = stripped;
      raw.model = model;
    }
    const countRoute = extractOcxRouteDirective(raw);
    if (countRoute) {
      model = stripOneMillionMarker(countRoute);
      raw.model = model;
    }
    // Counting never requests a generation tier. Correct only synthetic Fast identity.
    const countFastRow = parseFastOnlyRowId(
      config, () => decodeClaudeFastSelector(model, config.claudeCode),
    );
    if (countFastRow) {
      model = countFastRow.baseId;
      raw.model = model;
    }
    const comboRandomSeed = anthropicRequestCorrelationKey(raw);
    captureClaudeInbound(
      "count_tokens",
      raw,
      resolveInboundModel(model, config.claudeCode),
      req.headers.get("anthropic-beta") ?? undefined,
    );
    if (wantsNativePassthrough(req, config, requestPolicy, model)) {
      return await anthropicNativePassthrough(
        req,
        config,
        { model, provider: "anthropic-native", surface: "claude" },
        undefined,
        raw,
        "/v1/messages/count_tokens",
      );
    }

    let translated: ReturnType<typeof anthropicToResponsesTranslation>;
    let parsed: ReturnType<typeof parseRequest>;
    try {
      translated = anthropicToResponsesTranslation(raw, config.claudeCode);
      translatorBudget.chargeRetained(
        new TextEncoder().encode(JSON.stringify(translated.body)).byteLength,
        { kind: "request_copies" },
      );
      parsed = parseRequest(translated.body);
    } catch (err) {
      const overflow = isTranslatorBudgetExceededError(err);
      return anthropicErrorResponse(
        overflow ? 413 : 400,
        overflow
          ? "request translation buffer exceeded the safe limit"
          : err instanceof Error ? err.message : String(err),
        overflow ? "request_too_large" : "invalid_request_error",
        overflow ? "translation_buffer_limit" : undefined,
      );
    }

    const anthropicVersion = req.headers.get("anthropic-version")?.trim();
    const anthropicBeta = req.headers.get("anthropic-beta")?.trim();
    const anthropicSource: AnthropicMessagesSource = {
      body: raw,
      headers: {
        ...(anthropicVersion ? { anthropicVersion } : {}),
        ...(anthropicBeta ? { anthropicBeta } : {}),
      },
      ...(translated.requiresExactAnthropicReplay ? { requiresExactReplay: true } : {}),
    };

    let route: ReturnType<typeof routeModel>;
    try {
      const comboId = resolveComboId(config, parsed.modelId);
      const combo = comboId ? getCombo(config, comboId) : undefined;
      if (comboId && combo) {
        if (combo.imageInput === "disabled" && comboRequestHasImageInput(translated.body)) {
          return anthropicErrorResponse(
            400,
            `Combo "${comboId}" does not accept image input`,
            "invalid_request_error",
          );
        }
        const sourceEligible = (target: (typeof combo.targets)[number]): boolean =>
          comboTargetAcceptsAnthropicSource(
            config,
            target,
            anthropicSource,
            translated.body,
            "anthropic",
          );
        const pick = pickComboTarget(config, comboId, {
          eligible: sourceEligible,
          ...(comboRandomSeed === undefined ? {} : { randomSeed: comboRandomSeed }),
        });
        if (!pick) {
          if (!combo.targets.some(sourceEligible)) {
            return anthropicErrorResponse(
              400,
              "request cannot be replayed exactly to canonical Anthropic",
              "invalid_request_error",
            );
          }
          return anthropicErrorResponse(
            503,
            `No available targets for combo: ${comboId}`,
            "api_error",
          );
        }
        route = {
          ...routeConcreteModel(config, `${pick.target.provider}/${pick.target.model}`),
          combo: pick,
          routeKind: "combo",
          routeReason: "combo-pick",
        };
      } else {
        route = routeModel(config, parsed.modelId, evidenceFromBody(translated.body));
      }
    } catch (err) {
      if (err instanceof NoEligiblePolicyCandidateError) {
        return anthropicErrorResponse(404, err.message, "invalid_request_error");
      }
      return new Response(
        JSON.stringify({ input_tokens: estimateClaudeRequestTokens(raw, model) }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }

    route.provider = resolveProviderTransport(
      route.providerName,
      route.provider,
      typeof parsed.options.promptCacheKey === "string"
        ? parsed.options.promptCacheKey
        : undefined,
    );
    let adapterProvider = resolveWireProtocolOverride(
      route.providerName,
      route.modelId,
      route.provider,
      "anthropic",
    );
    let adapter = resolveAdapter(adapterProvider, config.cacheRetention);
    if (
      adapter.name !== "anthropic"
      || !isExactCanonicalAnthropicMessagesUrl(anthropicMessagesUrl(adapterProvider.baseUrl))
    ) {
      return new Response(
        JSON.stringify({ input_tokens: estimateClaudeRequestTokens(raw, model) }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    }
    if (!canReplayAnthropicSource(raw, route.modelId, adapterProvider)) {
      return anthropicErrorResponse(
        400,
        "request cannot be replayed exactly to canonical Anthropic",
        "invalid_request_error",
      );
    }

    if (route.provider.authMode === "oauth") {
      try {
        if (route.providerName === "anthropic" && isAnthropicAccountPoolEnabled(config)) {
          const sessionKey = anthropicSessionKeyFromParts({
            sessionIdHeader: sessionIdHeaderFromRequest(req.headers),
            threadIdHeader: req.headers.get("thread-id"),
            promptCacheKey: typeof parsed.options.promptCacheKey === "string"
              ? parsed.options.promptCacheKey
              : null,
            clientThreadId: typeof parsed._clientThreadId === "string"
              ? parsed._clientThreadId
              : null,
            promptCacheKeyIsSharedCohort: translated.cacheKeySource === "system",
          });
          const selection = resolveAnthropicAccountForSession(sessionKey, config);
          if (!selection.accountId) {
            if (selection.reason === "all-cooled") {
              const retryAfter = getAnthropicPoolRetryAfterSeconds();
              const response = anthropicErrorResponse(
                429,
                "All Anthropic OAuth accounts are temporarily rate-limited",
                "rate_limit_error",
              );
              if (retryAfter !== null) response.headers.set("Retry-After", String(retryAfter));
              return response;
            }
            return anthropicErrorResponse(
              401,
              "No eligible Anthropic OAuth account available",
              "authentication_error",
            );
          }
          const accessToken = await getAnthropicPoolAccessToken(selection.accountId);
          bindAnthropicSessionAffinity(sessionKey, selection.accountId);
          promoteAnthropicActiveAccount(selection.accountId);
          route.provider = { ...route.provider, apiKey: accessToken };
        } else {
          const snapshot = await getValidAccessTokenSnapshot(route.providerName);
          route.provider = { ...route.provider, apiKey: snapshot.accessToken };
        }
      } catch (err) {
        return anthropicErrorResponse(
          401,
          publicOAuthAuthenticationErrorMessage(err),
          "authentication_error",
        );
      }
      adapterProvider = resolveWireProtocolOverride(
        route.providerName,
        route.modelId,
        route.provider,
        "anthropic",
      );
      adapter = resolveAdapter(adapterProvider, config.cacheRetention);
    }

    parsed.modelId = route.modelId;
    if (parsed._rawBody) (parsed._rawBody as { model?: string }).model = route.modelId;
    let adapterRequest: AdapterRequest | undefined;
    try {
      adapterRequest = await adapter.buildRequest(parsed, {
        headers: new Headers(),
        translatorBudget,
        abortSignal: req.signal,
        anthropicMessagesSource: anthropicSource,
      });
      translatorBudget.chargeRetained(
        new TextEncoder().encode(adapterRequest.body).byteLength,
        { kind: "request_copies" },
      );
    } catch (err) {
      adapterRequest?.releaseBodyObservation?.();
      if (req.signal.aborted) {
        return anthropicErrorResponse(
          499,
          "client closed request during Anthropic token count",
          "api_error",
        );
      }
      const overflow = isTranslatorBudgetExceededError(err);
      return anthropicErrorResponse(
        overflow ? 413 : 400,
        overflow
          ? "request translation buffer exceeded the safe limit"
          : err instanceof Error ? err.message : String(err),
        overflow ? "request_too_large" : "invalid_request_error",
        overflow ? "translation_buffer_limit" : undefined,
      );
    }
    try {
      if (
        adapterRequest.anthropicSourceReplay !== true
        || !isExactCanonicalAnthropicMessagesUrl(adapterRequest.url)
      ) {
        return anthropicErrorResponse(
          400,
          "request cannot be replayed exactly to canonical Anthropic",
          "invalid_request_error",
        );
      }
      const countUrl = new URL(adapterRequest.url);
      countUrl.pathname = "/v1/messages/count_tokens";
      const countInit = applyUpstreamRecoveryInit({
        method: adapterRequest.method,
        headers: adapterRequest.headers,
        body: adapterRequest.body,
      }, "connection-reset");
      const result = await fetchWithHeaderDeadline(
        countUrl,
        countInit,
        config.connectTimeoutMs ?? 200_000,
        req.signal,
        clearableDeadline,
        providerFetch(route.provider, undefined, {
          providerName: route.providerName,
          modelId: route.modelId,
        }),
      );
      if (result.kind === "timeout") {
        return anthropicErrorResponse(
          504,
          "Anthropic token count timed out waiting for response headers",
          "timeout_error",
        );
      }
      if (result.kind === "error") {
        if (req.signal.aborted) {
          return anthropicErrorResponse(
            499,
            "client closed request during Anthropic token count",
            "api_error",
          );
        }
        return anthropicErrorResponse(
          502,
          `Anthropic token count failed: ${result.error instanceof Error ? result.error.message : String(result.error)}`,
          "api_error",
        );
      }
      const upstream = result.upstream;
      let bodyResult: BoundedPassthroughBytes;
      try {
        bodyResult = await readBoundedPassthroughBytes(
          upstream,
          resolvePassthroughBodyGuard(config, req.signal),
        );
      } catch (err) {
        return anthropicErrorResponse(
          502,
          `Anthropic token count body failed: ${err instanceof Error ? err.message : String(err)}`,
          "api_error",
        );
      }
      if (bodyResult.kind === "client_cancel") {
        return anthropicErrorResponse(
          499,
          "client closed request during Anthropic token count",
          "api_error",
        );
      }
      if (bodyResult.kind === "stall") {
        return anthropicErrorResponse(504, "Anthropic token count body stalled", "timeout_error");
      }
      if (bodyResult.kind === "overflow") {
        return anthropicErrorResponse(
          502,
          "Anthropic token count body exceeded safe limit",
          "api_error",
        );
      }
      const contentType = upstream.headers.get("content-type") ?? "application/json";
      const retryAfter = upstream.headers.get("retry-after");
      return new Response(Uint8Array.from(bodyResult.bytes).buffer, {
        status: upstream.status,
        statusText: upstream.statusText,
        headers: {
          "Content-Type": contentType,
          ...(retryAfter ? { "Retry-After": retryAfter } : {}),
        },
      });
    } finally {
      adapterRequest.releaseBodyObservation?.();
    }
  } finally {
    translatorBudget.dispose();
  }
}
