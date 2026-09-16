import { isAnthropicMessageResponse, usageFromAnthropic } from "../../adapters/anthropic";
import type { ResponsesTerminalStatus } from "../../bridge";
import { idleDeadline } from "../../lib/abort";
import { readBoundedResponseBytes } from "../../lib/bounded-body";
import { httpStatusFromTerminalError, type RequestLogContext } from "../request-log";
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
  "message_stop",
  "error",
]);

const RETRYABLE_ZERO_OUTPUT_INCOMPLETE_REASONS = new Set([
  "adapter_eof",
  "missing_terminal_event",
  "upstream_stall_timeout",
]);

function retryableZeroOutputTerminal(payload: unknown): boolean {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  const event = payload as {
    type?: unknown;
    response?: { incomplete_details?: { reason?: unknown } };
  };
  if (event.type === "response.failed" || event.type === "error") return true;
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
  return !PRE_OUTPUT_CONTROL_EVENTS.has(type) && !TERMINAL_EVENTS.has(type);
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
  const rootError = terminalPayload.error;
  const error = rootError && typeof rootError === "object" && !Array.isArray(rootError)
    ? rootError as Record<string, unknown>
    : nestedError && typeof nestedError === "object" && !Array.isArray(nestedError)
      ? nestedError as Record<string, unknown>
      : {
          type: "upstream_error",
          code: "upstream_server_error",
          message: logCtx.upstreamError ?? "Provider stream failed before producing output",
        };
  const usage = terminalResponse.usage;
  const status = logCtx.terminalHttpStatus ?? httpStatusFromTerminalError({
    type: typeof error.type === "string" ? error.type : undefined,
    code: error.code === null || typeof error.code === "string" ? error.code : undefined,
    message: typeof error.message === "string" ? error.message : undefined,
  });
  logCtx.terminalHttpStatus = status;
  return failedStreamResponse(
    response,
    status,
    error,
    usage,
  );
}

export type ComboStreamPreflightResult =
  | { kind: "accepted"; response: Response }
  | { kind: "failed"; response: Response; passthroughResponse?: Response };

export interface ComboStreamPreflightOptions {
  stallMs?: number;
  abortSignal?: AbortSignal;
  expectedTransport?: "sse" | "json";
  maxJsonBytes?: number;
  deferValidationToCaller?: boolean;
}

const DEFAULT_COMBO_STREAM_PREFLIGHT_STALL_MS = 90_000;
const DEFAULT_COMBO_JSON_PREFLIGHT_MAX_BYTES = 64 * 1024 * 1024;

/**
 * Buffer a combo child's downstream SSE only until the request becomes unsafe to
 * replay or reaches a terminal. This owns exactly one body reader. The aggregate
 * buffer is capped by bytes and retained chunks; hitting either cap commits the
 * current target instead of growing memory or guessing that replay is safe.
 */
export async function preflightComboStreamResponse(
  response: Response,
  logCtx: RequestLogContext,
  options: ComboStreamPreflightOptions = {},
): Promise<ComboStreamPreflightResult> {
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  const deferValidationToCaller = options.deferValidationToCaller === true;
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
  if (!response.ok || !response.body || !contentType.includes("text/event-stream")) {
    return { kind: "accepted", response };
  }

  const reader = response.body.getReader();
  const strictSource = options.expectedTransport === "sse";
  const strictValidation = strictSource && !deferValidationToCaller;
  const buffered: Uint8Array[] = [];
  let bufferedBytes = 0;
  let outputCommitted = false;
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
    classifyTerminal: payload => {
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) return null;
      const type = (payload as { type?: unknown }).type;
      if (strictSource) {
        if (type === "message_stop") return "completed";
        if (type === "error") return "failed";
        return null;
      }
      if (type === "response.completed" || type === "message_stop") return "completed";
      if (type === "response.incomplete") return "incomplete";
      if (type === "response.failed" || type === "error") return "failed";
      return null;
    },
    stopAtTerminal: true,
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
      if (comboStreamPayloadCommitsOutput(payload)) outputCommitted = true;
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) return;
      const event = payload as Record<string, unknown>;
      const type = event.type;
      if (strictSource && type === "message_start") {
        const message = event.message;
        if (message && typeof message === "object" && !Array.isArray(message)) {
          retainAnthropicUsage((message as Record<string, unknown>).usage);
        }
      } else if (strictSource && type === "message_delta") {
        retainAnthropicUsage(event.usage);
      }
      if (type === "error" || retryableZeroOutputTerminal(payload)) {
        failedPayload = event;
      }
    },
    onTerminal: status => { terminalStatus = status; },
  });

  idle.reset();
  try {
    for (;;) {
      let next: Awaited<ReturnType<typeof reader.read>>;
      try {
        next = await reader.read();
      } catch {
        if (aborted) throw signal?.reason ?? new DOMException("request aborted", "AbortError");
        if (deferValidationToCaller && strictSource) {
          readerTransferred = true;
          return {
            kind: "accepted",
            response: replayBufferedResponse(response, reader, buffered, true),
          };
        }
        if (stalled) {
          return {
            kind: "failed",
            response: failedTransportResponse(response, 504, "Provider stream stalled before producing output"),
          };
        }
        return {
          kind: "failed",
          response: failedTransportResponse(response, 502, "Provider stream read failed before producing output"),
        };
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

      if (terminalStatus !== undefined && terminalStatus !== "completed" && !outputCommitted
        && (strictSource || failedPayload)) {
        return {
          kind: "failed",
          response: failedTerminalResponse(response, failedPayload ?? {}, logCtx),
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
      if (next.done && terminalStatus === undefined && !outputCommitted) {
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
          response: replayBufferedResponse(response, reader, buffered, terminalStatus !== undefined),
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
