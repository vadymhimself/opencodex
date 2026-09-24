import { isAnthropicMessageResponse, usageFromAnthropic } from "../../adapters/anthropic";
import type { ResponsesTerminalStatus } from "../../bridge";
import { comboFailureDecision } from "../../combos";
import { idleDeadline } from "../../lib/abort";
import { readBoundedResponseBytes } from "../../lib/bounded-body";
import { httpStatusFromTerminalError } from "../../lib/errors";
import type { RequestFailureStage } from "../../lib/request-failure-model";
import type { RequestLogContext } from "../request-log";
import { createSseInspector } from "../relay";
import { MAX_CLIENT_SSE_FRAME_BYTES } from "../sse-frame-buffer";

const COMBO_STREAM_PREFLIGHT_MAX_BYTES = MAX_CLIENT_SSE_FRAME_BYTES;
// Keep retained object count proportional to the same byte budget used by the
// shared SSE framer. Tiny or empty upstream reads must not bypass the byte cap.
const COMBO_STREAM_PREFLIGHT_MAX_CHUNKS = Math.max(
  1,
  Math.ceil(COMBO_STREAM_PREFLIGHT_MAX_BYTES / 1024),
);

function cancelWithoutWaiting(cancel: () => Promise<void>): void {
  try {
    void cancel().catch(() => undefined);
  } catch {
    // Non-conforming streams may throw synchronously from cancel().
  }
}

const PRE_OUTPUT_CONTROL_EVENTS = new Set([
  "response.created",
  "response.in_progress",
  "response.queued",
  "response.heartbeat",
  "message_start",
  "message_delta",
  "content_block_stop",
  "ping",
]);

const TERMINAL_EVENTS = new Set([
  "response.completed",
  "response.failed",
  "response.incomplete",
]);

/** Messages-protocol terminals, which the Anthropic replay lane must not read as output. */
const ANTHROPIC_TERMINAL_EVENTS = new Set(["message_stop", "error"]);

const RETRYABLE_ZERO_OUTPUT_INCOMPLETE_REASONS = new Set([
  "adapter_eof",
  "missing_terminal_event",
  "upstream_stall_timeout",
]);

function bareErrorStatus(payload: unknown): number | undefined {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return undefined;
  const event = payload as Record<string, unknown>;
  if (event.type !== "error") return undefined;
  const nested = event.error;
  const error = nested && typeof nested === "object" && !Array.isArray(nested)
    ? nested as Record<string, unknown>
    : event;
  const explicitStatus = [
    event.status,
    event.status_code,
    event.http_status,
    error.status,
    error.status_code,
    error.http_status,
  ]
    .map(value => typeof value === "number" && Number.isInteger(value)
      ? value
      : typeof value === "string" && /^\d{3}$/.test(value.trim())
        ? Number(value)
        : undefined)
    .find(value => value !== undefined && value >= 400 && value <= 599);
  const code = typeof error.code === "string"
    ? error.code
    : typeof event.code === "string" ? event.code : null;
  if (explicitStatus === undefined && code === "invalid_request_error") return 400;
  return explicitStatus ?? httpStatusFromTerminalError({
    type: typeof error.type === "string" && error.type !== "error" ? error.type : undefined,
    code,
    message: typeof error.message === "string"
      ? error.message
      : typeof event.message === "string" ? event.message : undefined,
  });
}

function bareErrorIsRetryable(payload: unknown): boolean {
  const status = bareErrorStatus(payload);
  if (status === undefined || !payload || typeof payload !== "object" || Array.isArray(payload)) {
    return false;
  }
  const event = payload as Record<string, unknown>;
  const nested = event.error;
  const error = nested && typeof nested === "object" && !Array.isArray(nested)
    ? nested as Record<string, unknown>
    : event;
  const code = typeof error.code === "string"
    ? error.code
    : typeof event.code === "string" ? event.code : null;
  const message = typeof error.message === "string"
    ? error.message
    : typeof event.message === "string" ? event.message : "";
  return comboFailureDecision(status, message, { code }) === "hop";
}

function retryableZeroOutputTerminal(payload: unknown): boolean {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  const event = payload as {
    type?: unknown;
    response?: { incomplete_details?: { reason?: unknown } };
  };
  if (bareErrorIsRetryable(event)) return true;
  if (event.type === "response.failed") return true;
  if (event.type !== "response.incomplete") return false;
  const reason = event.response?.incomplete_details?.reason;
  return reason === undefined
    || (typeof reason === "string" && RETRYABLE_ZERO_OUTPUT_INCOMPLETE_REASONS.has(reason));
}

/**
 * Decide when replaying the request on another combo target would risk duplicating
 * client-visible output or a tool-side effect. Unknown event types commit the child
 * conservatively; only the small Responses lifecycle preamble remains replayable.
 */
export function comboStreamPayloadCommitsOutput(payload: unknown): boolean {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return true;
  const type = (payload as { type?: unknown }).type;
  if (typeof type !== "string") return true;
  if (type === "response.created") {
    // A created event is a control frame only while its snapshot is empty. An origin that
    // resumes a turn can put completed items in it, and treating that as a prelude would let
    // a replacement re-emit output the caller already received.
    const response = (payload as { response?: unknown }).response;
    if (response && typeof response === "object" && !Array.isArray(response)) {
      const output = (response as { output?: unknown }).output;
      if (Array.isArray(output) && output.length > 0) return true;
    }
  }
  return !PRE_OUTPUT_CONTROL_EVENTS.has(type) && !TERMINAL_EVENTS.has(type);
}

/**
 * How far this SSE body got, in the vocabulary of src/lib/request-failure-model.ts.
 *
 * The preflight cannot separate `semantic-output` from `side-effect`: it classifies any event
 * that is not a lifecycle control frame as committing, without reading item types. Both stages
 * refuse a resend, so the distinction would change no decision -- it is named here so a later
 * reader does not mistake the collapse for an omission.
 *
 * A terminal that settled carrying no output is `protocol-prelude`, not `terminal`. That is
 * the failure model's own rule: `terminal` means the answer was delivered, and an empty
 * completion delivered none.
 *
 * Only the two nothing-observed stages actually reach a read error today: the loop below hands
 * the body back as `accepted` the moment output commits or a terminal arrives, so a stream
 * that committed anything never reports a stage at all. The committed branches stay because
 * this has to be total for any other caller, and because a later change to that loop must not
 * be able to promote a committed stream into a replaceable one by omission.
 */
function observedResponsesStage(state: {
  readonly outputCommitted: boolean;
  readonly terminalStatus: ResponsesTerminalStatus | undefined;
  readonly responseCreated: boolean;
}): RequestFailureStage {
  if (state.outputCommitted) return "semantic-output";
  if (state.terminalStatus === "completed") return "terminal";
  if (state.responseCreated || state.terminalStatus !== undefined) return "protocol-prelude";
  return "headers-only";
}

function replayBufferedResponse(
  response: Response,
  reader: ReadableStreamDefaultReader<Uint8Array>,
  buffered: Uint8Array[],
  closeAfterBuffered = false,
): Response {
  let index = 0;
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (index < buffered.length) {
        controller.enqueue(buffered[index++]!);
        if (closeAfterBuffered && index === buffered.length) {
          controller.close();
          cancelWithoutWaiting(() => reader.cancel("combo stream protocol terminal reached"));
        }
        return;
      }
      if (closeAfterBuffered) {
        controller.close();
        reader.cancel("combo stream protocol terminal reached").catch(() => undefined);
        return;
      }
      try {
        const next = await reader.read();
        if (next.done) controller.close();
        else controller.enqueue(next.value);
      } catch (error) {
        try { controller.error(error); } catch { /* consumer already closed */ }
      }
    },
    cancel(reason) {
      cancelWithoutWaiting(() => reader.cancel(reason));
    },
  });
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

function failedStreamResponse(
  response: Response,
  status: number,
  error: Record<string, unknown>,
  usage?: unknown,
): Response {
  const headers = new Headers(response.headers);
  headers.set("content-type", "application/json");
  headers.delete("content-length");
  headers.delete("content-encoding");
  return new Response(JSON.stringify({
    error,
    // The combo classifier needs only the error and optional usage. Do not carry
    // response ids, provider metadata, or future terminal fields into the client
    // error envelope merely because they shared the terminal snapshot.
    response: {
      error,
      ...(usage && typeof usage === "object" && !Array.isArray(usage) ? { usage } : {}),
    },
  }), { status, headers });
}

function failedTransportResponse(
  response: Response,
  status: number,
  message: string,
): Response {
  return failedStreamResponse(response, status, {
    type: status === 504 ? "timeout_error" : "upstream_error",
    code: status === 504 ? "upstream_timeout" : "upstream_stream_error",
    message,
  });
}

function failedTerminalResponse(
  response: Response,
  terminalPayload: Record<string, unknown>,
  logCtx: RequestLogContext,
): Response {
  const nested = terminalPayload.response;
  const terminalResponse = nested && typeof nested === "object" && !Array.isArray(nested)
    ? nested as Record<string, unknown>
    : {};
  const nestedError = terminalResponse.error;
  const topLevelError = terminalPayload.error;
  const error = nestedError && typeof nestedError === "object" && !Array.isArray(nestedError)
    ? nestedError as Record<string, unknown>
    : topLevelError && typeof topLevelError === "object" && !Array.isArray(topLevelError)
      ? topLevelError as Record<string, unknown>
    : {
      type: "upstream_error",
      code: "upstream_server_error",
      message: typeof terminalPayload.message === "string"
        ? terminalPayload.message
        : logCtx.upstreamError ?? "Provider stream failed before producing output",
    };
  // Bare errors carry their own status; a `response.failed` terminal has none, so classify
  // its error instead of defaulting every such terminal to 502.
  const status = logCtx.terminalHttpStatus
    ?? bareErrorStatus(terminalPayload)
    ?? httpStatusFromTerminalError({
      type: typeof error.type === "string" ? error.type : undefined,
      code: error.code === null || typeof error.code === "string" ? error.code : undefined,
      message: typeof error.message === "string" ? error.message : undefined,
    });
  logCtx.terminalHttpStatus = status;
  return failedStreamResponse(response, status, error, terminalResponse.usage);
}

export type ComboStreamPreflightResult =
  | { kind: "accepted"; response: Response }
  | { kind: "failed"; response: Response; passthroughResponse?: Response }
  /**
   * The body errored mid-stream and `replayReadErrors` asked for the prefix back rather than
   * a rethrow. `stage` is how far the inspection actually got; whether that permits a
   * replacement is the resend gate's decision, not this function's. Callers that only act on
   * a projected terminal can treat this exactly as `accepted`, which is what it was before
   * the stage became observable.
   */
  | { kind: "read-error"; response: Response; error: unknown; stage: RequestFailureStage };

export interface ComboStreamPreflightOptions {
  stallMs?: number;
  abortSignal?: AbortSignal;
  expectedTransport?: "sse" | "json";
  maxJsonBytes?: number;
  deferValidationToCaller?: boolean;
  /** Accept an SSE body whose content-type header is absent entirely (never a wrong type). */
  allowMissingContentType?: boolean;
  /** Hand a post-header read failure to the caller behind the buffered prefix instead of throwing. */
  replayReadErrors?: boolean;
}

const DEFAULT_COMBO_STREAM_PREFLIGHT_STALL_MS = 90_000;
const DEFAULT_COMBO_JSON_PREFLIGHT_MAX_BYTES = 64 * 1024 * 1024;

/**
 * Buffer a Responses SSE only until the request becomes unsafe to replay or reaches a
 * terminal. Combo failover and native post-header reset recovery share this protocol
 * boundary, because they are asking the same question about the same bytes. This owns exactly
 * one body reader. The aggregate buffer is capped by bytes and retained chunks; hitting either
 * cap commits the current target instead of growing memory or guessing that replay is safe.
 */
export async function preflightComboStreamResponse(
  response: Response,
  logCtx: RequestLogContext,
  // Combo callers pass their own retryable-terminal predicate positionally; the Anthropic
  // source-replay lane passes only options. Accept both rather than fork the entry point.
  retryableTerminalOrOptions?: ((payload: unknown) => boolean) | ComboStreamPreflightOptions,
  positionalOptions?: ComboStreamPreflightOptions,
): Promise<ComboStreamPreflightResult> {
  const retryableTerminal = typeof retryableTerminalOrOptions === "function"
    ? retryableTerminalOrOptions
    : retryableZeroOutputTerminal;
  const options = (typeof retryableTerminalOrOptions === "object" && retryableTerminalOrOptions !== null
    ? retryableTerminalOrOptions
    : positionalOptions) ?? {};
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  const deferValidationToCaller = options.deferValidationToCaller === true;
  // A caller that names the transport it expects, or arms a stall budget, wants this preflight
  // to police the transport: a stall, an EOF before any terminal, or a read failure becomes an
  // HTTP failure it can classify. Every other caller keeps the relay contract, where post-header
  // transport failures stay the relay's to own and a read error propagates.
  const ownsTransportFailures = options.expectedTransport !== undefined || options.stallMs !== undefined;
  if (response.ok && options.expectedTransport === "sse"
    && (!response.body || !contentType.includes("text/event-stream"))) {
    if (deferValidationToCaller) return { kind: "accepted", response };
    if (response.body) {
      cancelWithoutWaiting(() => response.body!.cancel("combo source replay required SSE"));
    }
    return {
      kind: "failed",
      response: failedTransportResponse(response, 502, "Provider returned a non-SSE response for a streaming request"),
    };
  }
  if (response.ok && options.expectedTransport === "json") {
    if (!response.body || !contentType.includes("application/json")) {
      if (deferValidationToCaller) return { kind: "accepted", response };
      if (response.body) {
        cancelWithoutWaiting(() => response.body!.cancel("combo source replay required JSON"));
      }
      return {
        kind: "failed",
        response: failedTransportResponse(response, 502, "Provider returned a non-JSON response for a non-streaming request"),
      };
    }
    // Direct Anthropic source replay validates and retains exact JSON in its caller.
    // Reading here first would impose a second, unrelated size limit and turn body
    // read failures into generic Responses errors before the Claude endpoint sees them.
    if (deferValidationToCaller) return { kind: "accepted", response };
    let bytes: Uint8Array<ArrayBuffer>;
    try {
      const body = await readBoundedResponseBytes(response, {
        signal: options.abortSignal,
        maxBytes: options.maxJsonBytes && options.maxJsonBytes > 0
          ? options.maxJsonBytes
          : DEFAULT_COMBO_JSON_PREFLIGHT_MAX_BYTES,
        inactivityTimeoutMs: options.stallMs ?? DEFAULT_COMBO_STREAM_PREFLIGHT_STALL_MS,
      });
      if (body.oversized) {
        return {
          kind: "failed",
          response: failedTransportResponse(response, 502, "Provider JSON response was incomplete or too large"),
        };
      }
      bytes = body.bytes;
    } catch (error) {
      if (options.abortSignal?.aborted) throw error;
      const timedOut = error instanceof DOMException && error.name === "TimeoutError";
      return {
        kind: "failed",
        response: failedTransportResponse(
          response,
          timedOut ? 504 : 502,
          timedOut ? "Provider JSON response stalled before completion" : "Provider JSON response could not be read",
        ),
      };
    }
    let payload: unknown;
    try {
      payload = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    } catch {
      if (deferValidationToCaller) {
        return {
          kind: "accepted",
          response: new Response(bytes, {
            status: response.status,
            statusText: response.statusText,
            headers: response.headers,
          }),
        };
      }
      return {
        kind: "failed",
        response: failedTransportResponse(response, 502, "Provider returned malformed JSON for a non-streaming request"),
      };
    }
    if (payload && typeof payload === "object" && !Array.isArray(payload)
      && (payload as { type?: unknown }).type === "error") {
      const failed = failedTerminalResponse(response, payload as Record<string, unknown>, logCtx);
      return {
        kind: "failed",
        response: failed,
        passthroughResponse: new Response(bytes, {
          status: failed.status,
          ...(failed.status === response.status ? { statusText: response.statusText } : {}),
          headers: response.headers,
        }),
      };
    }
    if (!isAnthropicMessageResponse(payload)) {
      if (deferValidationToCaller) {
        return {
          kind: "accepted",
          response: new Response(bytes, {
            status: response.status,
            statusText: response.statusText,
            headers: response.headers,
          }),
        };
      }
      return {
        kind: "failed",
        response: failedTransportResponse(response, 502, "Provider returned an invalid Anthropic message response"),
      };
    }
    return {
      kind: "accepted",
      response: new Response(bytes, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      }),
    };
  }
  const isEventStream = contentType.includes("text/event-stream")
    || (!contentType && options.allowMissingContentType === true);
  if (!response.ok || !response.body || !isEventStream) {
    return { kind: "accepted", response };
  }

  const reader = response.body.getReader();
  const strictSource = options.expectedTransport === "sse";
  const strictValidation = strictSource && !deferValidationToCaller;
  const buffered: Uint8Array[] = [];
  let bufferedBytes = 0;
  let outputCommitted = false;
  let responseCreated = false;
  let terminalStatus: ResponsesTerminalStatus | undefined;
  let failedPayload: Record<string, unknown> | undefined;
  let anthropicUsage: Record<string, unknown> | undefined;
  let protocolError: string | undefined;
  let preflightOverflow = false;
  let overflowFrame: Uint8Array | undefined;
  let stalled = false;
  let aborted = false;
  let readerTransferred = false;
  const signal = options.abortSignal;
  const idle = idleDeadline(options.stallMs ?? DEFAULT_COMBO_STREAM_PREFLIGHT_STALL_MS, () => {
    stalled = true;
    cancelWithoutWaiting(() => reader.cancel(new DOMException("combo stream preflight stalled", "TimeoutError")));
  });
  const onAbort = () => {
    aborted = true;
    cancelWithoutWaiting(() => reader.cancel(signal?.reason));
  };
  if (signal?.aborted) onAbort();
  else signal?.addEventListener("abort", onAbort, { once: true });
  const retainAnthropicUsage = (usage: unknown): void => {
    if (!usage || typeof usage !== "object" || Array.isArray(usage)) return;
    const supported = Object.fromEntries(Object.entries(usage).filter(([key, value]) => (
      typeof value === "number"
      || (key === "server_tool_use" && value !== null && typeof value === "object" && !Array.isArray(value))
    )));
    anthropicUsage = { ...(anthropicUsage ?? {}), ...supported };
    logCtx.usage = usageFromAnthropic(anthropicUsage);
    if (logCtx.activeAttempt) logCtx.activeAttempt.usage = logCtx.usage;
  };
  const inspector = createSseInspector({
    logCtx,
    // A payload the inspector could not parse still reached this proxy, and it may be output.
    // Committing on it is what keeps an unreadable frame from reading as an empty prelude.
    onOpaquePayload: () => { outputCommitted = true; },
    // Anthropic source replay speaks the Messages protocol, so it needs its own terminal
    // classifier. The combo lane keeps the standard Responses one, where a bare `error` is
    // deliberately NOT a protocol terminal.
    ...(strictSource
      ? {
          classifyTerminal: (payload: unknown) => {
            if (!payload || typeof payload !== "object" || Array.isArray(payload)) return null;
            const type = (payload as { type?: unknown }).type;
            if (type === "message_stop") return "completed" as const;
            if (type === "error") return "failed" as const;
            return null;
          },
          stopAtTerminal: true,
        }
      : {}),
    strictJsonRecords: strictValidation,
    onValidatedFrame: strictValidation
      ? frame => {
          if (bufferedBytes + frame.byteLength > COMBO_STREAM_PREFLIGHT_MAX_BYTES
            || buffered.length >= COMBO_STREAM_PREFLIGHT_MAX_CHUNKS) {
            preflightOverflow = true;
            overflowFrame = frame;
            return false;
          }
          buffered.push(frame);
          bufferedBytes += frame.byteLength;
        }
      : undefined,
    onInspectionLimit: strictValidation ? undefined : () => { outputCommitted = true; },
    onProtocolError: strictValidation
      ? message => { protocolError = message; }
      : undefined,
    onParsedPayload: payload => {
      if (strictSource) {
        // Replay lane: usage accrues across the whole Messages stream, so this must keep
        // observing after output commits.
        const messagesType = payload && typeof payload === "object" && !Array.isArray(payload)
          ? (payload as { type?: unknown }).type
          : undefined;
        if (!(typeof messagesType === "string" && ANTHROPIC_TERMINAL_EVENTS.has(messagesType))
          && comboStreamPayloadCommitsOutput(payload)) {
          outputCommitted = true;
        }
        if (!payload || typeof payload !== "object" || Array.isArray(payload)) return;
        const event = payload as Record<string, unknown>;
        const type = event.type;
        if (type === "message_start") {
          const message = event.message;
          if (message && typeof message === "object" && !Array.isArray(message)) {
            retainAnthropicUsage((message as Record<string, unknown>).usage);
          }
        } else if (type === "message_delta") {
          retainAnthropicUsage(event.usage);
        }
        if (type === "error" || retryableTerminal(payload)) failedPayload = event;
        return;
      }
      if (payload !== null && typeof payload === "object" && !Array.isArray(payload)
        && (payload as { type?: unknown }).type === "response.created") responseCreated = true;
      if (terminalStatus !== undefined || outputCommitted || failedPayload) return;
      const retryable = retryableTerminal(payload);
      const matchedBareError = retryable && payload !== null && typeof payload === "object"
        && !Array.isArray(payload) && (payload as { type?: unknown }).type === "error";
      // A zero-output bare error is terminal evidence. Explicit client errors stay
      // committed; unknown and retryable upstream failures may advance the combo.
      if (comboStreamPayloadCommitsOutput(payload) && !matchedBareError) outputCommitted = true;
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) return;
      if (retryable) failedPayload = payload as Record<string, unknown>;
    },
    onTerminal: status => { terminalStatus = status; },
  });

  idle.reset();
  try {
    for (;;) {
      let next: Awaited<ReturnType<typeof reader.read>>;
      try {
        next = await reader.read();
      } catch (error) {
        if (aborted) throw signal?.reason ?? new DOMException("request aborted", "AbortError");
        if (stalled) {
          if (deferValidationToCaller && strictSource) {
            readerTransferred = true;
            return {
              kind: "accepted",
              response: replayBufferedResponse(response, reader, buffered, true),
            };
          }
          return {
            kind: "failed",
            response: failedTransportResponse(response, 504, "Provider stream stalled before producing output"),
          };
        }
        if (ownsTransportFailures) {
          if (deferValidationToCaller && strictSource) {
            readerTransferred = true;
            return {
              kind: "accepted",
              response: replayBufferedResponse(response, reader, buffered, true),
            };
          }
          return {
            kind: "failed",
            response: failedTransportResponse(response, 502, "Provider stream read failed before producing output"),
          };
        }
        if (!options.replayReadErrors) {
          // The caller receives the errored reader through the thrown failure; cancelling it in
          // the finally below would erase the transport failure the relay still has to see.
          readerTransferred = true;
          throw error;
        }
        // The native relay still owns post-header transport failures. Preserve
        // the bounded prefix and the errored reader; cancelling it here would
        // erase the failure before either client relay or inspection sees it.
        readerTransferred = true;
        const replay = replayBufferedResponse(response, reader, buffered);
        const stage = observedResponsesStage({ outputCommitted, terminalStatus, responseCreated });
        return { kind: "read-error", response: replay, error, stage };
      }
      if (aborted) throw signal?.reason ?? new DOMException("request aborted", "AbortError");
      if (stalled) {
        if (deferValidationToCaller && strictSource) {
          readerTransferred = true;
          return {
            kind: "accepted",
            response: replayBufferedResponse(response, reader, buffered, true),
          };
        }
        return {
          kind: "failed",
          response: failedTransportResponse(response, 504, "Provider stream stalled before producing output"),
        };
      }
      if (!next.done && next.value.byteLength > 0) idle.reset();
      if (next.done) {
        inspector.finish();
      } else {
        const terminalOffset = inspector.feed(next.value);
        if (strictValidation && preflightOverflow) {
          if (overflowFrame) buffered.push(overflowFrame);
          if (terminalOffset !== undefined && terminalOffset < next.value.byteLength) {
            buffered.push(next.value.subarray(terminalOffset));
          }
          readerTransferred = true;
          return {
            kind: "accepted",
            response: replayBufferedResponse(response, reader, buffered),
          };
        }
        if (!strictValidation) {
          const throughTerminal = terminalOffset === undefined
            ? next.value
            : next.value.subarray(0, terminalOffset);
          if (bufferedBytes + throughTerminal.byteLength > COMBO_STREAM_PREFLIGHT_MAX_BYTES) {
            readerTransferred = true;
            return {
              kind: "accepted",
              response: replayBufferedResponse(
                response,
                reader,
                [...buffered, throughTerminal],
                terminalOffset !== undefined,
              ),
            };
          }
          const retained = throughTerminal.slice();
          buffered.push(retained);
          bufferedBytes += retained.byteLength;
        }
      }

      if (protocolError) {
        if (outputCommitted) {
          const payload = JSON.stringify({
            type: "error",
            error: {
              type: "api_error",
              message: `anthropic passthrough protocol error: ${protocolError}`,
            },
          });
          buffered.push(new TextEncoder().encode(`event: error\ndata: ${payload}\n\n`));
          readerTransferred = true;
          return {
            kind: "accepted",
            response: replayBufferedResponse(response, reader, buffered, true),
          };
        }
        return {
          kind: "failed",
          response: failedTransportResponse(
            response,
            502,
            `Provider stream protocol error: ${protocolError}`,
          ),
        };
      }

      // A bare error event is not a protocol terminal (terminalStatus stays undefined),
      // so its retryable classification doubles as the terminal evidence.
      if ((terminalStatus === "failed" || terminalStatus === "incomplete"
        || failedPayload?.type === "error")
        && !outputCommitted && (strictSource || failedPayload)) {
        return {
          kind: "failed",
          response: failedTerminalResponse(response, failedPayload ?? {}, logCtx),
          // The replay lane must be able to hand the client the provider's own bytes.
          ...(strictSource
            ? {
                passthroughResponse: new Response(
                  new Blob(buffered.map(chunk => Uint8Array.from(chunk).buffer)),
                  {
                    status: response.status,
                    statusText: response.statusText,
                    headers: response.headers,
                  },
                ),
              }
            : {}),
        };
      }
      if (ownsTransportFailures && next.done && terminalStatus === undefined && !outputCommitted) {
        if (deferValidationToCaller && strictSource) {
          readerTransferred = true;
          return {
            kind: "accepted",
            response: replayBufferedResponse(response, reader, buffered),
          };
        }
        return {
          kind: "failed",
          response: failedTransportResponse(response, 502, "Provider stream ended before a terminal event"),
        };
      }
      if (next.done || terminalStatus !== undefined || outputCommitted
        || (!strictValidation && (
          bufferedBytes >= COMBO_STREAM_PREFLIGHT_MAX_BYTES
          || buffered.length >= COMBO_STREAM_PREFLIGHT_MAX_CHUNKS
        ))) {
        readerTransferred = true;
        return {
          kind: "accepted",
          response: replayBufferedResponse(response, reader, buffered, strictSource && terminalStatus !== undefined),
        };
      }
    }
  } finally {
    idle.cancel();
    signal?.removeEventListener("abort", onAbort);
    inspector.dispose();
    if (!readerTransferred) {
      cancelWithoutWaiting(() => reader.cancel("combo stream preflight finished"));
    }
  }
}

/** Produce a replacement body for a mid-stream failure at `stage`, or null to keep the error. */
export type ProtocolSafeResetRecovery = (
  error: unknown,
  stage: RequestFailureStage,
) => Promise<Response | null>;

/**
 * Defer protocol inspection until the downstream actually pulls the body.
 *
 * Direct passthrough must return response headers before the first SSE event arrives, so the
 * inspection cannot be awaited at the dispatch site the way combo routing awaits it. Wrapping
 * the body moves it to the first pull, which is the earliest moment the client is willing to
 * wait anyway.
 */
export function deferProtocolSafeResetRecovery(
  response: Response,
  logCtx: RequestLogContext,
  recover: ProtocolSafeResetRecovery,
  options?: { allowMissingContentType?: boolean },
): Response {
  if (!response.body) return response;

  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let initialization: Promise<void> | undefined;
  let closed = false;

  const cancelBody = (body: ReadableStream<Uint8Array> | null, reason?: unknown): void => {
    try { void body?.cancel(reason).catch(() => {}); } catch { /* already locked or closed */ }
  };
  const initialize = async (): Promise<void> => {
    const preflight = await preflightComboStreamResponse(
      response,
      logCtx,
      () => false,
      { allowMissingContentType: options?.allowMissingContentType === true, replayReadErrors: true },
    );
    let selected = preflight.response;
    if (preflight.kind === "read-error") {
      const replacement = await recover(preflight.error, preflight.stage);
      if (replacement) {
        cancelBody(selected.body, "using protocol-safe replacement stream");
        selected = replacement;
      }
    }
    if (closed) {
      cancelBody(selected.body, "downstream cancelled before protocol preflight completed");
      return;
    }
    reader = selected.body?.getReader();
  };

  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        initialization ??= initialize();
        await initialization;
        if (closed) return;
        if (!reader) {
          closed = true;
          controller.close();
          return;
        }
        const next = await reader.read();
        if (closed) return;
        if (next.done) {
          closed = true;
          try { reader.releaseLock(); } catch { /* already released */ }
          reader = undefined;
          controller.close();
          return;
        }
        controller.enqueue(next.value);
      } catch (error) {
        if (closed) return;
        closed = true;
        try { reader?.releaseLock(); } catch { /* errored reader */ }
        reader = undefined;
        controller.error(error);
      }
    },
    cancel(reason) {
      if (closed) return;
      closed = true;
      if (reader) {
        try { void reader.cancel(reason).catch(() => {}); } catch { /* already closed */ }
        try { reader.releaseLock(); } catch { /* already released */ }
        reader = undefined;
      } else if (initialization === undefined) {
        // Nothing has read the upstream yet, so this body is still ours to cancel.
        cancelBody(response.body, reason);
      }
      // A cancel while the preflight is mid-flight falls through deliberately. That body is
      // locked by the preflight's own reader, so cancelling it here would reject and be
      // swallowed; `initialize` sees `closed` when it settles and releases whichever body it
      // ended up selecting, which is the one that actually has to be let go.
    },
  }, { highWaterMark: 0 });

  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}
