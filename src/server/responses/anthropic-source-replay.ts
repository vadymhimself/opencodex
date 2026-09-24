/**
 * Anthropic canonical-replay lane.
 *
 * Upstream has no equivalent: these helpers decide whether a combo target can replay a caller's
 * Anthropic request byte-for-byte, finalize that replay, and classify the credential failures the
 * pool rotates on. They lived in responses/core.ts before upstream split that file, so they are a
 * module here rather than scattered across its successors.
 */
import type { OcxConfig, OcxParsedRequest, OcxProviderConfig } from "../../types";
import type { AnthropicMessagesSource, ProviderAdapter } from "../../adapters/base";
import type { AttemptRecoveryKind } from "../../usage/log";
import {
  ANTHROPIC_POOL_MAX_FAILOVERS_PER_REQUEST,
  formatAnthropicProviderForLog,
  getAnthropicPoolAccessSnapshot,
  rotateAnthropicAccountOnCredentialDenial,
} from "../../oauth/anthropic-routing";
import { bindRouteReasoningReplayScope } from "./core-replay";
import type { ResponsesTransport } from "./request-transport";
import { anthropicMessagesUrl, canReplayAnthropicSource, isExactCanonicalAnthropicMessagesUrl } from "../../adapters/anthropic";
import { resolveAdapter, resolveWireProtocolOverride } from "../adapter-resolve";
import { routeConcreteModel } from "../../router";
import { comboRequestHasImageInput } from "../../combos/request";
import { isModelTextOnly } from "../../vision";
import {
  isAnthropicSourceReplayResponse,
  isEagerRelaySseResponse,
  isNativePassthroughSseResponse,
  markAnthropicSourceReplayResponse,
  markEagerRelaySseResponse,
  markNativePassthroughSseResponse,
  sanitizePassthroughHeaders,
} from "../relay";
import { MAX_UPSTREAM_JSON_BODY_BYTES, linkAbortSignal, runTurnAdapterSseResponses } from "./core-lifetime";
import { consumeComboFailure } from "./core-combo-failure";
import { preflightComboStreamResponse } from "./combo-stream-preflight";
import type { InboundWire } from "../../providers/registry/types";
import type { AdmissionLease } from "../../lib/admission";
import { readBoundedResponseBody } from "../../lib/bounded-body";
import { trackStreamLifetime } from "../lifecycle";
import type { RequestLogContext } from "../request-log";

export function preserveResponseMarkers(source: Response, target: Response): Response {
  if (isNativePassthroughSseResponse(source)) markNativePassthroughSseResponse(target);
  if (isAnthropicSourceReplayResponse(source)) markAnthropicSourceReplayResponse(target);
  if (isEagerRelaySseResponse(source)) markEagerRelaySseResponse(target);
  if (runTurnAdapterSseResponses.has(source)) runTurnAdapterSseResponses.add(target);
  return target;
}

export function trackAcceptedComboResponse(
  response: Response,
  lease: AdmissionLease | undefined,
  signal: AbortSignal | undefined,
): Response {
  if (!lease || !response.body) return response;

  const controller = new AbortController();
  const cleanupAbort = linkAbortSignal(controller, signal);
  return preserveResponseMarkers(response, new Response(
    trackStreamLifetime(response.body, controller, cleanupAbort, lease),
    {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    },
  ));
}

export async function isAnthropicOAuthSubscriptionDenied(
  response: Response,
  signal?: AbortSignal,
): Promise<boolean> {
  if (response.status !== 403) return false;
  try {
    const body = await readBoundedResponseBody(response.clone(), { signal, fatalUtf8: true });
    if (!body.displaySafe || body.truncated || body.timedOut) return false;
    const payload = JSON.parse(body.text) as unknown;
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
    const nested = (payload as { error?: unknown }).error;
    const error = nested && typeof nested === "object" && !Array.isArray(nested)
      ? nested as Record<string, unknown>
      : payload as Record<string, unknown>;
    if (error.type !== "oauth_org_not_allowed" && error.code !== "oauth_org_not_allowed") return false;
    if (typeof error.message !== "string") return false;
    return error.message.includes("Your organization has disabled Claude subscription access for Claude Code")
      && error.message.includes("Use an Anthropic API key instead, or ask your admin to enable access");
  } catch {
    return false;
  }
}

/**
 * Rotate the Anthropic OAuth account after a credential DENIAL (403 oauth_org_not_allowed).
 *
 * The sibling 429 arms cool a quota window; this one says the credential itself is refused, so
 * `rotateAnthropicAccountOnCredentialDenial` marks the rejected generation as needing reauth
 * before picking the next account. One helper for all three loops (dispatch, continuation,
 * sidecars): they share the request-scoped transport state, and three inlined copies of this
 * gate is exactly how the 429 arms drifted apart.
 *
 * Returns null whenever the rotation cannot be completed, and the caller keeps the original 403.
 */
export async function rotateAnthropicProviderOnCredentialDenial(args: {
  config: OcxConfig;
  route: { providerName: string; modelId: string; provider: OcxProviderConfig };
  inboundWire: InboundWire | undefined;
  logCtx: RequestLogContext;
  transportState: Pick<
    ResponsesTransport,
    "anthropicPoolFailovers" | "anthropicSessionKey" | "replayOAuthCredentialSnapshot"
    | "applyFailoverSnapshot" | "resolveSelectionAdapter"
  >;
  parsed: OcxParsedRequest;
  /** Terminal-guard continuations rebuild from a clone; keep both owners bound. */
  retryParsed?: OcxParsedRequest;
  response: Response;
  signal?: AbortSignal;
}): Promise<{ adapter: ProviderAdapter; recoveryKind: AttemptRecoveryKind } | null> {
  const { config, route, inboundWire, logCtx, transportState, parsed, response, signal } = args;
  const retryParsed = args.retryParsed ?? parsed;
  const rejected = transportState.replayOAuthCredentialSnapshot;
  if (
    route.providerName !== "anthropic"
    || route.provider.authMode !== "oauth"
    || !rejected
    || transportState.anthropicPoolFailovers >= ANTHROPIC_POOL_MAX_FAILOVERS_PER_REQUEST
    || !await isAnthropicOAuthSubscriptionDenied(response, signal)
  ) return null;

  try {
    const nextAccountId = await rotateAnthropicAccountOnCredentialDenial(
      config,
      rejected,
      transportState.anthropicSessionKey,
    );
    if (!nextAccountId) return null;
    // applyFailoverSnapshot owns the commit race, the re-stamped log identity and the replay
    // credential snapshot this gate reads, so a second denial names the account that served it.
    const admitted = await transportState.applyFailoverSnapshot(
      await getAnthropicPoolAccessSnapshot(nextAccountId),
      retryParsed,
    );
    if (!admitted) return null;
    transportState.anthropicPoolFailovers += 1;
    logCtx.provider = formatAnthropicProviderForLog("anthropic", admitted.accountId, config);
    const rotatedAdapter = transportState.resolveSelectionAdapter(
      resolveWireProtocolOverride(route.providerName, route.modelId, route.provider, inboundWire),
      config.cacheRetention,
    );
    for (const owner of retryParsed === parsed ? [parsed] : [retryParsed, parsed]) {
      bindRouteReasoningReplayScope({
        parsed: owner,
        providerName: route.providerName,
        provider: route.provider,
        adapterName: rotatedAdapter.name,
        oauthCredentialSnapshot: transportState.replayOAuthCredentialSnapshot,
      });
    }
    return { adapter: rotatedAdapter, recoveryKind: "anthropic-oauth-403" };
  } catch {
    return null;
  }
}

export function credentialRecoveryStatus(recovery?: AttemptRecoveryKind): 401 | 403 | 429 | undefined {
  if (recovery === "oauth-401" || recovery === "key-401") return 401;
  if (recovery === "anthropic-oauth-403") return 403;
  if (
    recovery === "key-429"
    || recovery === "anthropic-oauth-429"
    || recovery === "oauth-account-429"
  ) return 429;
  return undefined;
}

export async function finalizeAnthropicSourceReplay(
  response: Response,
  logCtx: RequestLogContext,
  upstream: AbortController,
  cleanupUpstreamAbort: () => void,
  options: {
    abortSignal?: AbortSignal;
    comboAttempt?: boolean;
    expectsSse: boolean;
    stallMs?: number;
    turnAdmissionLease?: AdmissionLease;
  },
): Promise<Response> {
  let sourceResponse = markAnthropicSourceReplayResponse(new Response(
    response.body,
    {
      status: response.status,
      statusText: response.statusText,
      headers: sanitizePassthroughHeaders(response.headers),
    },
  ));

  if (options.comboAttempt) {
    const body = sourceResponse.body;
    if (!body) {
      cleanupUpstreamAbort();
      upstream.abort();
      return sourceResponse;
    }
    return markAnthropicSourceReplayResponse(new Response(
      trackStreamLifetime(body, upstream, cleanupUpstreamAbort),
      {
        status: sourceResponse.status,
        statusText: sourceResponse.statusText,
        headers: sourceResponse.headers,
      },
    ));
  }

  if (!sourceResponse.ok) {
    const failure = await consumeComboFailure(sourceResponse, options.abortSignal);
    if (!failure.passthroughResponse) {
      cleanupUpstreamAbort();
      upstream.abort();
      return failure.response;
    }
    sourceResponse = failure.passthroughResponse;
  } else if (sourceResponse.ok) {
    const preflight = await preflightComboStreamResponse(sourceResponse, logCtx, {
      abortSignal: options.abortSignal,
      expectedTransport: options.expectsSse ? "sse" : "json",
      stallMs: options.stallMs,
      maxJsonBytes: MAX_UPSTREAM_JSON_BODY_BYTES,
      deferValidationToCaller: true,
    });
    if (preflight.kind === "failed") {
      if (!preflight.passthroughResponse) {
        cleanupUpstreamAbort();
        upstream.abort();
        return preflight.response;
      }
      sourceResponse = markAnthropicSourceReplayResponse(preflight.passthroughResponse);
    } else {
      sourceResponse = markAnthropicSourceReplayResponse(preflight.response);
    }
  }

  const body = sourceResponse.body;
  if (!body) {
    cleanupUpstreamAbort();
    upstream.abort();
    return sourceResponse;
  }
  return markAnthropicSourceReplayResponse(new Response(
    trackStreamLifetime(body, upstream, cleanupUpstreamAbort, options.turnAdmissionLease),
    {
      status: sourceResponse.status,
      statusText: sourceResponse.statusText,
      headers: sourceResponse.headers,
    },
  ));
}

export function comboTargetAcceptsAnthropicSource(
  config: OcxConfig,
  target: Readonly<{ provider: string; model: string }>,
  source: AnthropicMessagesSource,
  body: unknown,
  inboundWire: InboundWire,
): boolean {
  const configuredProvider = config.providers[target.provider];
  if (!configuredProvider) return false;
  try {
    // Compatibility must not depend on operational availability. Route through a
    // shallow enabled view so disabled targets retain their real wire capabilities.
    const routingConfig = configuredProvider.disabled === true
      ? {
          ...config,
          providers: {
            ...config.providers,
            [target.provider]: { ...configuredProvider, disabled: false },
          },
        }
      : config;
    const route = routeConcreteModel(routingConfig, `${target.provider}/${target.model}`);
    const adapterProvider = resolveWireProtocolOverride(
      route.providerName,
      route.modelId,
      route.provider,
      inboundWire,
    );
    const canonicalAnthropicTarget = inboundWire === "anthropic"
      && resolveAdapter(adapterProvider, config.cacheRetention).name === "anthropic"
      && isExactCanonicalAnthropicMessagesUrl(anthropicMessagesUrl(adapterProvider.baseUrl));
    return canonicalAnthropicTarget
      ? canReplayAnthropicSource(source.body, route.modelId, adapterProvider)
        && !(isModelTextOnly(adapterProvider, route.modelId) && comboRequestHasImageInput(body))
      : source.requiresExactReplay !== true;
  } catch {
    return false;
  }
}
